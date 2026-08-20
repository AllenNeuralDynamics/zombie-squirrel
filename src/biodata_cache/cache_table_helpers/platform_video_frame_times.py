"""Behavior-camera per-frame timing cache table (partitioned by raw asset_name).

camstim acquisitions (mesoscope / multiplane-ophys, Dynamic Routing, and some
Dynamic Foraging sessions) free-run their behavior cameras at ~60 fps with jitter
and dropped frames, so frame *i* is **not** at ``t0 + i / fps``. The true per-frame
time comes from the NI-DAQ sync file (a classic AllenSDK ``sync.h5``) that records a
rising edge per camera exposure. Harp-based rigs (VR foraging, harp Dynamic
Foraging) carry their own per-frame clock and have no sync file — those assets are
self-selected out here (no sync file → empty partition → viewer falls back to the
scalar offset).

Verified empirically (multiplane-ophys_717824_2024-04-12_09-34-43): the derived NWB
timestamps are **identical** to the sync file's ``vsync_2p`` rising edges
(max deviation 0.0 s) and ``session_start_time == timestamps_reference_time``, so the
NWB session clock *is* the sync-file sample clock: ``t = sample_index / sample_rate``
with **no additional offset**. Camera frame time is therefore just the
``<cam>_cam_frame_readout`` rising-edge time, with lost-frame indices removed so the
row count matches the encoded mp4 frame count.

The sync file is ~10-30 MB and encoded as a 100 kHz transition list; decoding rising
edges per line client-side on every page load is wasteful, so it is precomputed here
into a compact per-frame table the browser reads with DuckDB.
"""

import ast
import logging
import re
import tempfile

import boto3
import numpy as np
import pandas as pd
from botocore.config import Config

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.asset_basics import asset_basics
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

_S3_URI_RE = re.compile(r"^s3://([^/]+)/(.+)$")
_MAX_WORKERS = 16
_TABLE = "video_frame_times"

# sync line-label suffix that marks a per-camera frame-readout (one rising edge per
# exposed frame). The plan also documents a ``<cam>_cam_exposing`` gate; the
# frame-readout edge is used because its count matches FramesRecorded + lost frames.
_FRAME_READOUT_RE = re.compile(r"^(?P<cam>[a-z0-9]+)_cam_frame_readout$", re.IGNORECASE)

# Map a sync camera prefix (or a video CameraLabel) to a canonical name so the two
# can be matched. camstim abbreviates "Behavior" to "beh" on the sync line.
_CAM_CANON = {
    "beh": "behavior",
    "behavior": "behavior",
    "face": "face",
    "eye": "eye",
    "nose": "nose",
    "body": "body",
    "side": "side",
    "bottom": "bottom",
    "front": "front",
}


def _log(message: str) -> None:
    """Emit a structured cache log message for the frame-times table."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES[_TABLE],
            message=message,
        ).to_json()
    )


def _parse_s3(location: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into ``(bucket, key)`` with no trailing slash."""
    match = _S3_URI_RE.match(location)
    if match is None:
        raise ValueError(f"Not an S3 URI: {location}")
    return match.group(1), match.group(2).rstrip("/")


def _canon(name: str) -> str:
    """Canonicalize a camera prefix / label for matching sync lines to videos."""
    key = re.sub(r"[^a-z0-9]", "", str(name).lower())
    return _CAM_CANON.get(key, key)


def _list_keys(client, bucket: str, prefix: str) -> list[str]:
    """Return all object keys directly (non-recursively is fine) under a prefix."""
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys


def _read_sync(client, bucket: str, key: str):
    """Download and open a candidate ``.h5`` as an AllenSDK sync file.

    Returns ``(sample_index, bit_state, line_labels, sample_rate)`` if the file is a
    sync file (has ``data`` + ``meta`` with camera frame-readout lines), else None.
    """
    import h5py

    with tempfile.NamedTemporaryFile(suffix=".h5") as tmp:
        client.download_fileobj(bucket, key, tmp)
        tmp.flush()
        try:
            with h5py.File(tmp.name, "r") as f:
                if "data" not in f or "meta" not in f:
                    return None
                meta_raw = f["meta"][()]
                if isinstance(meta_raw, bytes):
                    meta_raw = meta_raw.decode()
                meta = ast.literal_eval(meta_raw)
                line_labels = meta.get("line_labels")
                if not line_labels or not any(_FRAME_READOUT_RE.match(str(lbl)) for lbl in line_labels):
                    return None
                sample_rate = float(meta.get("ni_daq", {}).get("sample_rate"))
                data = f["data"][:]
        except (OSError, ValueError, SyntaxError, TypeError):
            return None
    return data[:, 0].astype(np.int64), data[:, 1], line_labels, sample_rate


def _find_sync(client, bucket: str, key: str):
    """Locate and read the sync file under the raw asset's ``behavior/`` folder.

    Returns the parsed sync tuple (see ``_read_sync``) or None if the asset has no
    sync file (Harp rig — not a camstim acquisition).
    """
    h5_keys = [k for k in _list_keys(client, bucket, f"{key}/behavior") if k.lower().endswith(".h5")]
    # Prefer a filename that mentions "sync"; otherwise probe each .h5 (a camstim
    # behavior/stimulus .h5 is rejected by _read_sync's line-label check).
    h5_keys.sort(key=lambda k: (0 if "sync" in k.lower() else 1, k))
    for h5_key in h5_keys:
        parsed = _read_sync(client, bucket, h5_key)
        if parsed is not None:
            return parsed
    return None


def _rising_edges(sample_index: np.ndarray, bit_state: np.ndarray, bit: int, sample_rate: float) -> np.ndarray:
    """Return rising-edge times (seconds) for one sync line bit."""
    state = (bit_state >> bit) & 1
    idx = np.where((state[1:] == 1) & (state[:-1] == 0))[0] + 1
    return sample_index[idx] / sample_rate


def _lost_frame_indices(lost_frames) -> set[int]:
    """Parse an MVR ``LostFrames`` list (e.g. ["13-14", "42"]) into a set of indices.

    Each entry is a single index or an inclusive ``start-end`` range in the camera's
    attempted-frame sequence (the same sequence the sync readout edges enumerate).
    """
    lost: set[int] = set()
    for entry in lost_frames or []:
        text = str(entry).strip()
        if "-" in text:
            start, end = text.split("-", 1)
            lost.update(range(int(start), int(end) + 1))
        elif text:
            lost.add(int(text))
    return lost


def _read_video_reports(client, bucket: str, key: str) -> dict[str, dict]:
    """Return ``{CameraLabel: {frames_recorded, lost_count, lost_indices}}`` from the
    per-camera MVR ``*.json`` sidecars under ``behavior-videos/``."""
    import json

    reports: dict[str, dict] = {}
    for obj_key in _list_keys(client, bucket, f"{key}/behavior-videos"):
        if not obj_key.lower().endswith(".json"):
            continue
        try:
            body = client.get_object(Bucket=bucket, Key=obj_key)["Body"].read()
            report = json.loads(body).get("RecordingReport", {})
        except (json.JSONDecodeError, KeyError):
            continue
        label = report.get("CameraLabel")
        if not label:
            continue
        reports[label] = {
            "frames_recorded": int(report.get("FramesRecorded", 0)),
            "lost_count": int(report.get("FramesLostCount", 0)),
            "lost_indices": _lost_frame_indices(report.get("LostFrames")),
        }
    return reports


def _camera_frame_times(edges: np.ndarray, report: dict, label: str) -> np.ndarray | None:
    """Align sync readout ``edges`` to the encoded mp4 frames for one camera.

    Removes the reported lost-frame indices so the returned array length matches
    ``FramesRecorded``. Returns None (and logs) if the counts cannot be reconciled.
    """
    frames_recorded = report["frames_recorded"]
    lost_indices = {i for i in report["lost_indices"] if 0 <= i < len(edges)}

    if len(edges) - len(lost_indices) == frames_recorded:
        if lost_indices:
            keep = np.ones(len(edges), dtype=bool)
            keep[list(lost_indices)] = False
            edges = edges[keep]
        return edges

    # Counts don't reconcile with the lost-frame report: fall back to trimming from
    # the end (extra trailing exposures after recording stopped are common) and log.
    if len(edges) >= frames_recorded > 0:
        _log(
            f"  {label}: edge/frame mismatch (edges={len(edges)}, recorded={frames_recorded}, "
            f"lost={len(lost_indices)}); trimming to first {frames_recorded} edges"
        )
        return edges[:frames_recorded]

    _log(
        f"  {label}: cannot reconcile edges={len(edges)} with recorded={frames_recorded}; skipping camera"
    )
    return None


def _fetch_asset_frame_times(asset_name: str, location: str | None = None) -> pd.DataFrame:
    """Fetch and cache per-camera frame times for one raw asset from its sync file.

    ``asset_name`` is the *raw* acquisition asset (where ``behavior/`` and
    ``behavior-videos/`` live). Returns an empty DataFrame; callers read back from the
    backend.
    """
    setup_logging()
    cache_key = f"{registry.NAMES[_TABLE]}/{asset_name}"
    _log(f"Updating cache for asset {asset_name}")

    registry.BACKEND.clear_partition(cache_key)

    if location is None:
        basics = asset_basics()
        asset = basics[basics["name"] == asset_name]
        if asset.empty:
            _log(f"Asset {asset_name} not found in asset_basics")
            return pd.DataFrame()
        location = asset.iloc[0]["location"]
    if not location:
        _log(f"No location for asset {asset_name}")
        return pd.DataFrame()

    bucket, key = _parse_s3(location)
    client = boto3.client("s3", config=Config(max_pool_connections=_MAX_WORKERS))

    sync = _find_sync(client, bucket, key)
    if sync is None:
        _log(f"No sync file for asset {asset_name} (Harp rig or non-camstim) — skipping")
        return pd.DataFrame()
    sample_index, bit_state, line_labels, sample_rate = sync

    # Map canonical camera name -> sync line bit, from the frame-readout line labels.
    line_bits: dict[str, int] = {}
    for bit, label in enumerate(line_labels):
        m = _FRAME_READOUT_RE.match(str(label))
        if m:
            line_bits[_canon(m.group("cam"))] = bit

    reports = _read_video_reports(client, bucket, key)
    if not reports:
        _log(f"No behavior-videos MVR sidecars for asset {asset_name} — skipping")
        return pd.DataFrame()

    frames = []
    for video_label, report in sorted(reports.items()):
        bit = line_bits.get(_canon(video_label))
        if bit is None:
            _log(f"  {video_label}: no matching sync frame-readout line — skipping camera")
            continue
        edges = _rising_edges(sample_index, bit_state, bit, sample_rate)
        times = _camera_frame_times(edges, report, video_label)
        if times is None or len(times) == 0:
            continue
        frames.append(
            pd.DataFrame(
                {
                    "asset_name": asset_name,
                    "camera": video_label,
                    "frame_index": np.arange(len(times), dtype=np.int64),
                    "t": times.astype(np.float32),
                }
            )
        )
        _log(f"  {video_label}: {len(times)} frame times ({report['lost_count']} lost)")

    if not frames:
        _log(f"No camera frame times extracted for asset {asset_name}")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True).sort_values(["camera", "frame_index"]).reset_index(drop=True)
    registry.BACKEND.write(cache_key, df)
    _log(f"Cached {len(df)} frame times across {df['camera'].nunique()} cameras for asset {asset_name}")
    return pd.DataFrame()


@registry.register_table(registry.NAMES[_TABLE])
def platform_video_frame_times(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    """Return per-camera behavior-video frame times for a single raw asset.

    One row per (camera, frame): ``t`` is the session-clock time (seconds, shared with
    the NWB timestamps) of the encoded mp4 frame ``frame_index``. Data is cached per
    raw ``asset_name`` partition.

    Args:
        asset_name: Raw acquisition asset name (holds ``behavior/`` + ``behavior-videos/``).
        force_update: If True, bypass cache and recompute from the S3 sync file, writing
            the result. An empty DataFrame is returned; read again without force_update
            (or use lazy=True) to retrieve the data.
        lazy: If True, return the partition's storage location string (for DuckDB) instead
            of loading the DataFrame.
        location: Optional S3 location of the raw asset (avoids reading asset_basics on a
            bulk force_update).

    Returns:
        DataFrame of frame-time records; the partition location string if lazy=True; or an
        empty DataFrame if force_update=True (data is written to the cache).

    Raises:
        ValueError: If the cache is empty for the asset and force_update is False.
    """
    cache_key = f"{registry.NAMES[_TABLE]}/{asset_name}"

    if lazy:
        if force_update:
            _fetch_asset_frame_times(asset_name, location=location)
        return registry.BACKEND.get_location(cache_key)

    if force_update:
        return _fetch_asset_frame_times(asset_name, location=location)

    df = registry.BACKEND.read(cache_key)
    if df.empty:
        raise ValueError(
            f"Cache is empty for asset {asset_name}. Use force_update=True to fetch data from S3."
        )
    return df


def platform_video_frame_times_columns() -> list[Column]:
    """Return platform_behavior-videos_frame-times cache table column definitions."""
    return [
        Column(name="asset_name", description="Raw acquisition asset name (holds behavior-videos)"),
        Column(name="camera", description="Camera label (e.g. Behavior, Eye, Face, Nose); matches the mp4 filename label"),
        Column(name="frame_index", description="0-based index of the encoded mp4 frame"),
        Column(name="t", description="Session-clock time in seconds of the frame (shared with NWB timestamps)"),
    ]

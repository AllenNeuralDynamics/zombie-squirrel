"""SWDB curated-set cache tables (mostly partitioned by asset_name).

The SWDB (Summer Workshop on the Dynamic Brain) dashboard is backed by small,
curated *sets* of data assets. Unlike every other platform table in this package,
the source assets here are **merged NWB files** produced specifically for the
workshop: a single HDF5 ``.nwb`` per asset that folds behavior, DLC/eye tracking,
receptive-field mapping, optotagging, running speed and sorted units into one
file. They are also true HDF5, not the ``.nwb.zarr`` stores the other helpers read,
so this module uses ``h5py`` over a ranged-HTTP file object rather than
``zarr.open_consolidated``.

Reading those files directly from the browser is impractical (each is ~3.7 GB, and
HDF5 needs synchronous random access), so this job flattens the parts a dashboard
actually plots into six small parquet tables:

===============================  ==========================================
``platform_swdb_sessions``       one row per asset (unpartitioned catalog)
``platform_swdb_trials``         one row per behavior trial
``platform_swdb_performance``    one row per task block
``platform_swdb_events``         long-format event stream (licks, rewards,
                                 epochs, opto, RF-mapping trials)
``platform_swdb_eye``            per-frame eye/pupil/CR ellipse fits
``platform_swdb_running``        per-sample running speed
===============================  ==========================================

All times are kept in the NWB's own session clock (t=0 is
``session_start_time``); no origin shifting happens here so the cache stays a
faithful copy of the source. Consumers that want "t=0 at the first trial" shift on
read.

The asset list is curated and hardcoded in ``SWDB_SETS`` — this is deliberate. The
set is a fixed workshop artifact, not a query over ``asset_basics``, so the job is
reproducible and independent of the nightly metadata sync.

Only the columns the dashboard plots are extracted. The raw DLC keypoint table
(``processing/behavior/dlc_eye_camera``, 108 columns x ~460k frames per asset) is
deliberately skipped: ``processing/behavior/eye_tracking`` already holds the
ellipse fits derived from those keypoints, which is what a viewer draws. Set
``include_dlc=True`` on the extractor to add it if raw keypoints are ever needed.
"""

import json
import logging
import re

import boto3
import numpy as np
import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

_S3_URI_RE = re.compile(r"^s3://([^/]+)/(.+)$")
_DEFAULT_BUCKET = "aind-open-data"
_REGION = "us-west-2"

# Ranged-read block size for the HDF5 file object. The merged NWBs store every
# dataset contiguous and uncompressed, so a slice is one contiguous byte range and
# a modest block size keeps request counts low without over-reading.
_BLOCK_SIZE = 1 << 22  # 4 MiB

# ---------------------------------------------------------------------------
# The curated sets
# ---------------------------------------------------------------------------

SWDB_SETS = {
    "dynamic-routing": {
        "title": "Dynamic Routing",
        "description": (
            "Merged NWB sessions from the Dynamic Routing visual/auditory task-switching "
            "project. Each file combines behavior trials, DLC eye tracking, receptive-field "
            "mapping, optotagging and sorted units."
        ),
        "assets": [
            "ecephys_664851_2023-11-13_12-49-51_nwb_2026-07-24_13-29-15",
            "ecephys_667252_2023-09-26_11-29-49_nwb_2026-07-24_13-26-17",
            "ecephys_668755_2023-08-31_12-33-31_nwb_2026-07-24_13-23-03",
            "ecephys_686176_2023-12-04_13-06-37_nwb_2026-07-24_13-26-39",
            "ecephys_686740_2023-10-24_12-46-24_nwb_2026-07-24_13-25-30",
            "ecephys_702131_2024-02-26_15-36-14_nwb_2026-07-24_13-21-50",
            "ecephys_712815_2024-05-22_12-26-32_nwb_2026-07-24_13-18-47",
            "ecephys_713655_2024-08-09_10-41-47_nwb_2026-07-24_13-26-26",
            "ecephys_742903_2024-10-22_10-58-35_nwb_2026-07-24_13-28-08",
            "ecephys_743199_2024-12-05_12-42-34_nwb_2026-07-24_13-22-03",
            "ecephys_746439_2025-01-31_11-23-11_nwb_2026-07-24_13-20-12",
            "ecephys_759434_2025-02-04_12-27-22_nwb_2026-07-24_13-22-13",
        ],
    },
}


def swdb_asset_names() -> list[str]:
    """Return every asset name across all SWDB sets, in set order."""
    return [name for spec in SWDB_SETS.values() for name in spec["assets"]]


def swdb_set_for_asset(asset_name: str) -> str | None:
    """Return the set id containing ``asset_name``, or None if it is not in a set."""
    for set_id, spec in SWDB_SETS.items():
        if asset_name in spec["assets"]:
            return set_id
    return None


def _log(message: str) -> None:
    """Emit a structured cache log message for the SWDB tables."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES["swdb_sessions"],
            message=message,
        ).to_json()
    )


# ---------------------------------------------------------------------------
# S3 / HDF5 access
# ---------------------------------------------------------------------------


def _parse_s3(location: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into its bucket and key."""
    match = _S3_URI_RE.match(location.rstrip("/"))
    if not match:
        raise ValueError(f"Not an S3 URI: {location}")
    return match.group(1), match.group(2)


def _default_location(asset_name: str) -> str:
    """Return the conventional S3 location for a curated SWDB asset."""
    return f"s3://{_DEFAULT_BUCKET}/{asset_name}"


def _https_url(bucket: str, key: str) -> str:
    """Return the virtual-hosted HTTPS URL for an S3 object."""
    return f"https://{bucket}.s3.{_REGION}.amazonaws.com/{key}"


def _find_nwb_key(bucket: str, prefix: str) -> str | None:
    """Return the key of the single top-level ``.nwb`` file in an asset, if present."""
    client = boto3.client("s3")
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/", Delimiter="/"):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".nwb"):
                return obj["Key"]
    return None


def _open_nwb(location: str):
    """Open the merged HDF5 NWB for an asset over ranged HTTP.

    Returns ``(h5py.File, file_object)``; the caller closes both. Returns
    ``(None, None)`` when the asset has no top-level ``.nwb``.
    """
    import fsspec
    import h5py

    bucket, prefix = _parse_s3(location)
    key = _find_nwb_key(bucket, prefix)
    if key is None:
        return None, None

    # Ranged HTTPS rather than boto3: h5py needs a seekable file object and these
    # assets live in a public bucket. Every dataset is contiguous, so HDF5's random
    # access maps onto a small number of byte-range GETs.
    fileobj = fsspec.open(_https_url(bucket, key), "rb", block_size=_BLOCK_SIZE).open()
    return h5py.File(fileobj, "r"), fileobj


# ---------------------------------------------------------------------------
# HDF5 → pandas helpers
# ---------------------------------------------------------------------------


def _decode(value):
    """Decode HDF5 bytes (and arrays of bytes) into str, leaving other types alone."""
    if isinstance(value, bytes):
        return value.decode("utf-8", "replace")
    return value


def _scalar(root, path):
    """Read a scalar dataset as a decoded Python value, or None if absent."""
    if path not in root:
        return None
    return _decode(root[path][()])


def _column(group, name):
    """Read one DynamicTable/TimeIntervals column as a 1-D numpy array, or None."""
    if name not in group:
        return None
    data = group[name][()]
    if data.dtype == object:
        return np.array([_decode(v) for v in data], dtype=object)
    return data


def _table_frame(root, path, columns) -> pd.DataFrame:
    """Read selected columns of an HDF5 table group into a DataFrame.

    Missing groups yield an empty frame and missing columns are simply absent, so a
    structural variation between assets (e.g. no ``optotagging_trials``) degrades
    instead of failing.
    """
    if path not in root:
        return pd.DataFrame()
    group = root[path]
    data = {}
    for name in columns:
        values = _column(group, name)
        if values is not None:
            data[name] = values
    if not data:
        return pd.DataFrame()
    # Ragged columns would broadcast-fail, so keep a single consistent length: the
    # most common one, and on a tie the longest. A ragged NWB column stores a short
    # `_index` companion alongside the full-length data columns, so preferring the
    # longer length on a tie keeps the real table rather than the index.
    lengths = {len(v) for v in data.values()}
    if len(lengths) > 1:
        keep = max(lengths, key=lambda n: (sum(1 for v in data.values() if len(v) == n), n))
        data = {k: v for k, v in data.items() if len(v) == keep}
    return pd.DataFrame(data)


# ---------------------------------------------------------------------------
# Column groups extracted from each NWB
# ---------------------------------------------------------------------------

_TRIAL_COLUMNS = [
    "trial_index",
    "block_index",
    "trial_index_in_block",
    "repeat_index",
    "rewarded_modality",
    "stim_name",
    "start_time",
    "stop_time",
    "quiescent_start_time",
    "quiescent_stop_time",
    "stim_start_time",
    "stim_stop_time",
    "response_window_start_time",
    "response_window_stop_time",
    "post_response_window_start_time",
    "post_response_window_stop_time",
    "response_time",
    "reward_time",
    "grating_phase",
    "is_response",
    "is_correct",
    "is_incorrect",
    "is_hit",
    "is_miss",
    "is_false_alarm",
    "is_correct_reject",
    "is_target",
    "is_nontarget",
    "is_catch",
    "is_go",
    "is_nogo",
    "is_aud_stim",
    "is_vis_stim",
    "is_aud_target",
    "is_vis_target",
    "is_aud_nontarget",
    "is_vis_nontarget",
    "is_aud_rewarded",
    "is_vis_rewarded",
    "is_rewarded",
    "is_contingent_reward",
    "is_noncontingent_reward",
    "is_reward_scheduled",
    "is_instruction",
    "is_block_switch",
    "is_repeat",
    "is_opto",
]

_PERFORMANCE_COLUMNS = [
    "block_index",
    "start_time",
    "stop_time",
    "rewarded_modality",
    "is_first_block_aud",
    "n_trials",
    "n_responses",
    "n_hits",
    "n_contingent_rewards",
    "hit_rate",
    "false_alarm_rate",
    "catch_response_rate",
    "vis_dprime",
    "aud_dprime",
    "cross_modality_dprime",
    "signed_cross_modality_dprime",
    "vis_target_response_rate",
    "vis_nontarget_response_rate",
    "aud_target_response_rate",
    "aud_nontarget_response_rate",
]

# Ellipse-fit columns from the DLC-derived eye tracking table. The 12-point raw
# keypoints behind these live in dlc_eye_camera and are intentionally not cached.
_EYE_COLUMNS = [
    "timestamps",
    "pupil_area",
    "pupil_center_x",
    "pupil_center_y",
    "pupil_width",
    "pupil_height",
    "pupil_phi",
    "pupil_average_confidence",
    "pupil_is_bad_frame",
    "eye_area",
    "eye_center_x",
    "eye_center_y",
    "eye_width",
    "eye_height",
    "eye_is_bad_frame",
    "cr_area",
    "cr_center_x",
    "cr_center_y",
    "cr_is_bad_frame",
]


def _events_frame(root) -> pd.DataFrame:
    """Build the long-format event stream for one session.

    One table holds every sparse time-stamped thing a viewer overlays, so adding a
    new event kind later needs no new cache table.
    """
    parts = []

    def add(kind, t, t_stop=None, label=None, value=None):
        """Append one event kind as a block of rows."""
        if t is None or len(t) == 0:
            return
        n = len(t)
        parts.append(
            pd.DataFrame(
                {
                    "kind": np.full(n, kind, dtype=object),
                    "t": np.asarray(t, dtype="float64"),
                    "t_stop": (np.full(n, np.nan) if t_stop is None else np.asarray(t_stop, dtype="float64")),
                    "label": (np.full(n, None, dtype=object) if label is None else np.asarray(label, dtype=object)),
                    "value": (np.full(n, np.nan) if value is None else np.asarray(value, dtype="float64")),
                }
            )
        )

    licks = _table_frame(root, "processing/behavior/licks", ["timestamps", "duration", "is_likely_lick"])
    if not licks.empty:
        stop = licks["timestamps"] + licks["duration"] if "duration" in licks else None
        add(
            "lick",
            licks["timestamps"],
            t_stop=stop,
            value=licks["is_likely_lick"].astype("float64") if "is_likely_lick" in licks else None,
        )

    rewards = _table_frame(root, "processing/behavior/rewards", ["timestamps", "is_solenoid_time"])
    if not rewards.empty:
        add(
            "reward",
            rewards["timestamps"],
            value=rewards["is_solenoid_time"].astype("float64") if "is_solenoid_time" in rewards else None,
        )

    quiescent = _table_frame(root, "processing/behavior/quiescent_interval_violations", ["timestamps"])
    if not quiescent.empty:
        add("quiescent_violation", quiescent["timestamps"])

    epochs = _table_frame(root, "intervals/epochs", ["start_time", "stop_time", "script_name"])
    if not epochs.empty:
        add(
            "epoch",
            epochs["start_time"],
            t_stop=epochs["stop_time"],
            label=epochs["script_name"] if "script_name" in epochs else None,
        )

    opto = _table_frame(
        root,
        "intervals/optotagging_trials",
        ["start_time", "stop_time", "location", "power", "wavelength", "bregma_x", "bregma_y"],
    )
    if not opto.empty:
        add(
            "opto",
            opto["start_time"],
            t_stop=opto["stop_time"],
            label=opto["location"] if "location" in opto else None,
            value=opto["power"] if "power" in opto else None,
        )

    vis_rf = _table_frame(root, "intervals/vis_rf_mapping_trials", ["start_time", "stop_time"])
    if not vis_rf.empty:
        add("vis_rf", vis_rf["start_time"], t_stop=vis_rf["stop_time"])

    aud_rf = _table_frame(root, "intervals/aud_rf_mapping_trials", ["start_time", "stop_time", "freq"])
    if not aud_rf.empty:
        add(
            "aud_rf",
            aud_rf["start_time"],
            t_stop=aud_rf["stop_time"],
            value=aud_rf["freq"] if "freq" in aud_rf else None,
        )

    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True).sort_values(["t", "kind"]).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def _cache_key(table_key: str, asset_name: str) -> str:
    """Return the partition cache key for one SWDB table and asset."""
    return f"{registry.NAMES[table_key]}/{asset_name}"


def extract_swdb_asset(asset_name: str, location: str | None = None, include_dlc: bool = False) -> dict:
    """Extract every per-asset SWDB table from one merged NWB in a single pass.

    Opening a 3.7 GB HDF5 file is the expensive part, so all five partitioned
    tables are written from one open handle rather than one job per table.

    Args:
        asset_name: The merged-NWB asset name.
        location: S3 location of the asset; defaults to the conventional
            ``s3://aind-open-data/<asset_name>``.
        include_dlc: Also cache the raw DLC eye keypoint table. Off by default —
            it is ~100x larger than the ellipse fits it produced.

    Returns:
        A summary dict for the sessions catalog (empty if the asset was unreadable).
    """
    setup_logging()
    location = location or _default_location(asset_name)
    _log(f"Extracting SWDB tables for asset {asset_name}")

    root, fileobj = _open_nwb(location)
    if root is None:
        _log(f"No .nwb file found for asset {asset_name}")
        return {}

    try:
        session_start = _scalar(root, "session_start_time")
        subject_id = _scalar(root, "general/subject/subject_id")

        trials = _table_frame(root, "intervals/trials", _TRIAL_COLUMNS)
        performance = _table_frame(root, "intervals/performance", _PERFORMANCE_COLUMNS)
        events = _events_frame(root)
        eye = _table_frame(root, "processing/behavior/eye_tracking", _EYE_COLUMNS)
        running = _table_frame(root, "processing/behavior/running_speed", ["timestamps", "data"])
        if not running.empty:
            running = running.rename(columns={"data": "speed"})

        n_units = 0
        if "units/id" in root:
            n_units = int(root["units/id"].shape[0])

        if include_dlc:
            dlc = _table_frame(root, "processing/behavior/dlc_eye_camera", None)
        else:
            dlc = pd.DataFrame()
    finally:
        root.close()
        if fileobj is not None:
            fileobj.close()

    for table_key, frame in (
        ("swdb_trials", trials),
        ("swdb_performance", performance),
        ("swdb_events", events),
        ("swdb_eye", eye),
        ("swdb_running", running),
    ):
        key = _cache_key(table_key, asset_name)
        registry.BACKEND.clear_partition(key)
        if frame.empty:
            _log(f"No rows for {registry.NAMES[table_key]} / {asset_name}")
            continue
        registry.BACKEND.write(key, frame)
        _log(f"Cached {len(frame)} rows to {registry.NAMES[table_key]} / {asset_name}")

    if include_dlc and not dlc.empty:
        key = _cache_key("swdb_dlc", asset_name)
        registry.BACKEND.clear_partition(key)
        registry.BACKEND.write(key, dlc)

    return _summary_row(
        asset_name=asset_name,
        location=location,
        subject_id=subject_id,
        session_start=session_start,
        trials=trials,
        performance=performance,
        events=events,
        eye=eye,
        running=running,
        n_units=n_units,
    )


def _summary_row(
    asset_name,
    location,
    subject_id,
    session_start,
    trials,
    performance,
    events,
    eye,
    running,
    n_units,
) -> dict:
    """Build the one-row sessions-catalog record for an extracted asset."""
    epochs = events[events["kind"] == "epoch"] if not events.empty else pd.DataFrame()
    kinds = set(events["kind"].unique()) if not events.empty else set()

    def _count(kind):
        """Number of events of one kind."""
        return int((events["kind"] == kind).sum()) if not events.empty else 0

    session_end = 0.0
    for frame, col in ((trials, "stop_time"), (epochs, "t_stop"), (running, "timestamps")):
        if not frame.empty and col in frame:
            session_end = max(session_end, float(np.nanmax(frame[col].to_numpy(dtype="float64"))))

    return {
        "set_id": swdb_set_for_asset(asset_name),
        "asset_name": asset_name,
        "location": location,
        "subject_id": str(subject_id) if subject_id is not None else _subject_from_name(asset_name),
        "session_date": _date_from_name(asset_name),
        "session_start_time": session_start,
        "session_duration_s": session_end,
        "n_trials": int(len(trials)),
        "n_blocks": int(performance["block_index"].nunique()) if "block_index" in performance else 0,
        "n_licks": _count("lick"),
        "n_rewards": _count("reward"),
        "n_opto_trials": _count("opto"),
        "n_vis_rf_trials": _count("vis_rf"),
        "n_aud_rf_trials": _count("aud_rf"),
        "n_eye_frames": int(len(eye)),
        "n_running_samples": int(len(running)),
        "n_units": int(n_units),
        "epochs": json.dumps([str(v) for v in epochs["label"].tolist()]) if "label" in epochs else "[]",
        "has_optotagging": "opto" in kinds,
        "has_rf_mapping": bool({"vis_rf", "aud_rf"} & kinds),
        "has_eye_tracking": not eye.empty,
        "has_units": n_units > 0,
    }


_NAME_RE = re.compile(r"^[a-z]+_(?P<subject>\d+)_(?P<date>\d{4}-\d{2}-\d{2})")


def _subject_from_name(asset_name: str) -> str | None:
    """Parse the subject id out of an asset name."""
    match = _NAME_RE.match(asset_name)
    return match.group("subject") if match else None


def _date_from_name(asset_name: str) -> str | None:
    """Parse the acquisition date out of an asset name."""
    match = _NAME_RE.match(asset_name)
    return match.group("date") if match else None


# ---------------------------------------------------------------------------
# Registered tables
# ---------------------------------------------------------------------------


def _read_partition(table_key: str, asset_name: str, force_update: bool, lazy: bool, location: str | None):
    """Shared read/extract/lazy behaviour for the five partitioned SWDB tables."""
    key = _cache_key(table_key, asset_name)

    if lazy:
        if force_update:
            extract_swdb_asset(asset_name, location=location)
        return registry.BACKEND.get_location(key)

    if force_update:
        extract_swdb_asset(asset_name, location=location)
        return pd.DataFrame()

    df = registry.BACKEND.read(key)
    if df.empty:
        raise ValueError(f"Cache is empty for asset {asset_name}. Use force_update=True to fetch data from S3.")
    return df


@registry.register_table(registry.NAMES["swdb_trials"])
def platform_swdb_trials(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    """Return behavior trials for one SWDB merged-NWB asset.

    One row per trial with the full task-flag set. Note that a ``force_update``
    extracts *every* SWDB table for the asset in one pass, since they share a single
    expensive HDF5 open.

    Args:
        asset_name: The merged-NWB asset name.
        force_update: Re-extract from the source NWB and write the cache. An empty
            DataFrame is returned; read again without force_update to get the data.
        lazy: Return the partition's storage location string instead of the data.
        location: Optional S3 location override for the asset.

    Returns:
        DataFrame of trials; the location string if lazy=True; or an empty
        DataFrame if force_update=True.

    Raises:
        ValueError: If the cache is empty and force_update is False.
    """
    return _read_partition("swdb_trials", asset_name, force_update, lazy, location)


@registry.register_table(registry.NAMES["swdb_performance"])
def platform_swdb_performance(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    """Return per-block task performance for one SWDB merged-NWB asset.

    One row per block with d-prime, hit rate and response-rate breakdowns.

    Args:
        asset_name: The merged-NWB asset name.
        force_update: Re-extract from the source NWB and write the cache.
        lazy: Return the partition's storage location string instead of the data.
        location: Optional S3 location override for the asset.

    Returns:
        DataFrame of per-block performance; the location string if lazy=True; or an
        empty DataFrame if force_update=True.

    Raises:
        ValueError: If the cache is empty and force_update is False.
    """
    return _read_partition("swdb_performance", asset_name, force_update, lazy, location)


@registry.register_table(registry.NAMES["swdb_events"])
def platform_swdb_events(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    """Return the long-format event stream for one SWDB merged-NWB asset.

    Licks, rewards, quiescent-interval violations, task epochs, optotagging trials
    and receptive-field mapping trials in one table, discriminated by ``kind``.

    Args:
        asset_name: The merged-NWB asset name.
        force_update: Re-extract from the source NWB and write the cache.
        lazy: Return the partition's storage location string instead of the data.
        location: Optional S3 location override for the asset.

    Returns:
        DataFrame of events; the location string if lazy=True; or an empty
        DataFrame if force_update=True.

    Raises:
        ValueError: If the cache is empty and force_update is False.
    """
    return _read_partition("swdb_events", asset_name, force_update, lazy, location)


@registry.register_table(registry.NAMES["swdb_eye"])
def platform_swdb_eye(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    """Return per-frame eye/pupil/corneal-reflection ellipse fits for one SWDB asset.

    Derived from the DLC eye-camera keypoints; one row per eye-camera frame.

    Args:
        asset_name: The merged-NWB asset name.
        force_update: Re-extract from the source NWB and write the cache.
        lazy: Return the partition's storage location string instead of the data.
        location: Optional S3 location override for the asset.

    Returns:
        DataFrame of ellipse fits; the location string if lazy=True; or an empty
        DataFrame if force_update=True.

    Raises:
        ValueError: If the cache is empty and force_update is False.
    """
    return _read_partition("swdb_eye", asset_name, force_update, lazy, location)


@registry.register_table(registry.NAMES["swdb_running"])
def platform_swdb_running(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    """Return per-sample running speed for one SWDB merged-NWB asset.

    Args:
        asset_name: The merged-NWB asset name.
        force_update: Re-extract from the source NWB and write the cache.
        lazy: Return the partition's storage location string instead of the data.
        location: Optional S3 location override for the asset.

    Returns:
        DataFrame of running speed; the location string if lazy=True; or an empty
        DataFrame if force_update=True.

    Raises:
        ValueError: If the cache is empty and force_update is False.
    """
    return _read_partition("swdb_running", asset_name, force_update, lazy, location)


@registry.register_table(registry.NAMES["swdb_sessions"])
def platform_swdb_sessions(force_update: bool = False, lazy: bool = False) -> pd.DataFrame | str:
    """Return the SWDB session catalog: one row per curated asset.

    This is the only unpartitioned SWDB table and the single small file a dashboard
    landing page needs — it carries per-set membership, subject, counts and
    modality flags for every asset. Built by re-extracting each asset, so it is
    normally refreshed by the ``swdb`` sync job rather than called directly.

    Args:
        force_update: Re-extract every curated asset and rebuild the catalog.
        lazy: Return the table's storage location string instead of the data.

    Returns:
        DataFrame of session records; the location string if lazy=True.

    Raises:
        ValueError: If the cache is empty and force_update is False.
    """
    key = registry.NAMES["swdb_sessions"]

    if lazy:
        if force_update:
            build_swdb_sessions()
        return registry.BACKEND.get_location(key)

    if force_update:
        return build_swdb_sessions()

    df = registry.BACKEND.read(key)
    if df.empty:
        raise ValueError("Cache is empty for platform_swdb_sessions. Use force_update=True to fetch data from S3.")
    return df


def build_swdb_sessions(summaries: list[dict] | None = None) -> pd.DataFrame:
    """Write the SWDB sessions catalog, merging over any already-cached rows.

    Rows for assets present in ``summaries`` replace their cached counterparts;
    rows for assets that were skipped this run (already extracted) are preserved.
    Without that merge, an incremental re-run — where most assets are skipped —
    would shrink the catalog to just the handful of assets it happened to rebuild.

    Args:
        summaries: Pre-computed summary rows from ``extract_swdb_asset``. When None,
            every curated asset is re-extracted first.

    Returns:
        An empty DataFrame; read the table back to retrieve it.
    """
    setup_logging()
    if summaries is None:
        summaries = []
        for asset_name in swdb_asset_names():
            try:
                row = extract_swdb_asset(asset_name)
                if row:
                    summaries.append(row)
            except Exception as exc:
                logging.exception(f"swdb extraction failed for asset {asset_name}: {exc}")

    rows = [row for row in summaries if row]
    if not rows:
        _log("No SWDB session summaries to write")
        return pd.DataFrame()

    key = registry.NAMES["swdb_sessions"]
    df = pd.DataFrame(rows)

    try:
        existing = registry.BACKEND.read(key)
    except Exception:
        existing = pd.DataFrame()
    if not existing.empty and "asset_name" in existing.columns:
        retained = existing[~existing["asset_name"].isin(set(df["asset_name"]))]
        df = pd.concat([retained, df], ignore_index=True)

    df = df.sort_values(["set_id", "session_date"]).reset_index(drop=True)
    registry.BACKEND.write(key, df)
    _log(f"Cached {len(df)} SWDB session records ({len(rows)} rebuilt this run)")
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Column definitions
# ---------------------------------------------------------------------------


def platform_swdb_sessions_columns() -> list[Column]:
    """Return platform_swdb_sessions cache table column definitions."""
    return [
        Column(name="set_id", description="Curated SWDB set this asset belongs to (e.g. dynamic-routing)"),
        Column(name="asset_name", description="Merged-NWB data asset name"),
        Column(name="location", description="S3 location of the asset"),
        Column(name="subject_id", description="Subject id (labtracks)"),
        Column(name="session_date", description="Acquisition date parsed from the asset name (YYYY-MM-DD)"),
        Column(name="session_start_time", description="NWB session_start_time; t=0 of every cached time column"),
        Column(name="session_duration_s", description="Latest observed time in the session, in seconds"),
        Column(name="n_trials", description="Number of behavior trials"),
        Column(name="n_blocks", description="Number of task blocks"),
        Column(name="n_licks", description="Number of lick events"),
        Column(name="n_rewards", description="Number of water rewards delivered"),
        Column(name="n_opto_trials", description="Number of optotagging trials"),
        Column(name="n_vis_rf_trials", description="Number of visual receptive-field mapping trials"),
        Column(name="n_aud_rf_trials", description="Number of auditory receptive-field mapping trials"),
        Column(name="n_eye_frames", description="Number of eye-camera frames with ellipse fits"),
        Column(name="n_running_samples", description="Number of running-speed samples"),
        Column(name="n_units", description="Number of sorted units in the merged NWB"),
        Column(name="epochs", description="JSON list of task epoch script names in session order"),
        Column(name="has_optotagging", description="True if the asset has optotagging trials"),
        Column(name="has_rf_mapping", description="True if the asset has receptive-field mapping trials"),
        Column(name="has_eye_tracking", description="True if the asset has eye-tracking ellipse fits"),
        Column(name="has_units", description="True if the asset has sorted units"),
    ]


def platform_swdb_trials_columns() -> list[Column]:
    """Return platform_swdb_trials cache table column definitions."""
    described = {
        "trial_index": "Trial index within the session",
        "block_index": "Task block index",
        "trial_index_in_block": "Trial index within its block",
        "repeat_index": "Repeat count for this trial's stimulus",
        "rewarded_modality": "Rewarded modality for the block ('vis' or 'aud')",
        "stim_name": "Stimulus name (vis1, vis2, sound1, sound2, catch)",
        "start_time": "Trial start, seconds from session_start_time",
        "stop_time": "Trial stop, seconds from session_start_time",
        "quiescent_start_time": "Start of the pre-stimulus quiescent window",
        "quiescent_stop_time": "End of the pre-stimulus quiescent window",
        "stim_start_time": "Stimulus onset",
        "stim_stop_time": "Stimulus offset",
        "response_window_start_time": "Start of the response window",
        "response_window_stop_time": "End of the response window",
        "post_response_window_start_time": "Start of the post-response window",
        "post_response_window_stop_time": "End of the post-response window",
        "response_time": "Time of the first lick inside the response window",
        "reward_time": "Time the reward was delivered",
        "grating_phase": "Phase of the visual grating stimulus",
    }
    columns = [Column(name=name, description=desc) for name, desc in described.items()]
    flags = {
        "is_response": "the subject licked in the response window",
        "is_correct": "the trial was scored correct",
        "is_incorrect": "the trial was scored incorrect",
        "is_hit": "a target trial with a response",
        "is_miss": "a target trial without a response",
        "is_false_alarm": "a nontarget trial with a response",
        "is_correct_reject": "a nontarget trial without a response",
        "is_target": "the stimulus was the rewarded target",
        "is_nontarget": "the stimulus was a nontarget",
        "is_catch": "no stimulus was presented",
        "is_go": "the trial was a go trial",
        "is_nogo": "the trial was a no-go trial",
        "is_aud_stim": "the stimulus was auditory",
        "is_vis_stim": "the stimulus was visual",
        "is_aud_target": "the stimulus was the auditory target",
        "is_vis_target": "the stimulus was the visual target",
        "is_aud_nontarget": "the stimulus was the auditory nontarget",
        "is_vis_nontarget": "the stimulus was the visual nontarget",
        "is_aud_rewarded": "the block rewarded the auditory modality",
        "is_vis_rewarded": "the block rewarded the visual modality",
        "is_rewarded": "a reward was delivered",
        "is_contingent_reward": "the reward was earned by responding",
        "is_noncontingent_reward": "the reward was free (autoreward)",
        "is_reward_scheduled": "a reward was scheduled for this trial",
        "is_instruction": "the trial was an instruction/autoreward trial",
        "is_block_switch": "the trial is the first of a new block",
        "is_repeat": "the trial repeated the previous stimulus",
        "is_opto": "optogenetic stimulation was applied",
    }
    columns.extend(Column(name=name, description=f"True if {desc}") for name, desc in flags.items())
    return columns


def platform_swdb_performance_columns() -> list[Column]:
    """Return platform_swdb_performance cache table column definitions."""
    described = {
        "block_index": "Task block index",
        "start_time": "Block start, seconds from session_start_time",
        "stop_time": "Block stop, seconds from session_start_time",
        "rewarded_modality": "Rewarded modality for the block ('vis' or 'aud')",
        "is_first_block_aud": "True if the session's first block rewarded audition",
        "n_trials": "Number of trials in the block",
        "n_responses": "Number of trials with a response",
        "n_hits": "Number of hits",
        "n_contingent_rewards": "Number of rewards earned by responding",
        "hit_rate": "Fraction of target trials with a response",
        "false_alarm_rate": "Fraction of nontarget trials with a response",
        "catch_response_rate": "Fraction of catch trials with a response",
        "vis_dprime": "d-prime for visual target vs nontarget",
        "aud_dprime": "d-prime for auditory target vs nontarget",
        "cross_modality_dprime": "d-prime for rewarded vs unrewarded modality targets",
        "signed_cross_modality_dprime": "Cross-modality d-prime signed by the rewarded modality",
        "vis_target_response_rate": "Response rate to the visual target",
        "vis_nontarget_response_rate": "Response rate to the visual nontarget",
        "aud_target_response_rate": "Response rate to the auditory target",
        "aud_nontarget_response_rate": "Response rate to the auditory nontarget",
    }
    return [Column(name=name, description=desc) for name, desc in described.items()]


def platform_swdb_events_columns() -> list[Column]:
    """Return platform_swdb_events cache table column definitions."""
    return [
        Column(
            name="kind",
            description=("Event type: lick, reward, quiescent_violation, epoch, opto, vis_rf or aud_rf"),
        ),
        Column(name="t", description="Event time, seconds from session_start_time"),
        Column(name="t_stop", description="Event end time where the event has duration, else null"),
        Column(
            name="label",
            description="Event label: epoch script name, opto target location, else null",
        ),
        Column(
            name="value",
            description=(
                "Numeric payload: is_likely_lick for licks, is_solenoid_time for rewards, "
                "power for opto, frequency for aud_rf, else null"
            ),
        ),
    ]


def platform_swdb_eye_columns() -> list[Column]:
    """Return platform_swdb_eye cache table column definitions."""
    columns = [Column(name="timestamps", description="Frame time, seconds from session_start_time")]
    for feature, label in (("pupil", "pupil"), ("eye", "eye"), ("cr", "corneal reflection")):
        columns.append(Column(name=f"{feature}_area", description=f"Fitted {label} ellipse area in pixels^2"))
        columns.append(Column(name=f"{feature}_center_x", description=f"Fitted {label} ellipse center x in pixels"))
        columns.append(Column(name=f"{feature}_center_y", description=f"Fitted {label} ellipse center y in pixels"))
        columns.append(
            Column(name=f"{feature}_is_bad_frame", description=f"True if the {label} fit failed for this frame")
        )
    for feature, label in (("pupil", "pupil"), ("eye", "eye")):
        columns.append(Column(name=f"{feature}_width", description=f"Fitted {label} ellipse semi-major axis in pixels"))
        columns.append(
            Column(name=f"{feature}_height", description=f"Fitted {label} ellipse semi-minor axis in pixels")
        )
    columns.append(Column(name="pupil_phi", description="Fitted pupil ellipse rotation in radians"))
    columns.append(
        Column(name="pupil_average_confidence", description="Mean DLC keypoint confidence for the pupil fit")
    )
    return columns


def platform_swdb_running_columns() -> list[Column]:
    """Return platform_swdb_running cache table column definitions."""
    return [
        Column(name="timestamps", description="Sample time, seconds from session_start_time"),
        Column(name="speed", description="Linear forward running speed on the disk, low-pass filtered, in cm/s"),
    ]

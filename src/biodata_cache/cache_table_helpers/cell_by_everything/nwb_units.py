"""Self-contained NWB-Zarr unit reading for sources with no regularly-run cache table.

Most cell sources are projections over a cache table that the sync pipeline
rebuilds on every run. The public Visual Coding Neuropixels collection is not:
``platform_visual_coding_neuropixels_units`` is a one-off table published by
``scripts/build_platform_visual_coding_neuropixels_units.py`` and is *not* part of
the pipeline, so depending on it would silently pin these tables to whenever that
script last ran.

The reading logic is therefore **deliberately copied here rather than imported**
from ``platform_ecephys_units`` / ``platform_visual_coding_neuropixels_units``.
The duplication is the point: this module must keep working when those one-off
tables go stale, and it must not become a shared utility whose behaviour changes
under it. Fix bugs here independently.

Only the handful of small per-unit location arrays are downloaded -- never the
ragged ``spike_times`` or the ``waveform_mean`` cube.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor

import boto3
import numpy as np
import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.utils import CacheLogMessage

PUBLIC_BUCKET = "aind-open-data"
_UNITS_GROUP = "units"
_NWB_DIR_SUFFIXES = (".nwb.zarr", ".nwb")
_MAX_WORKERS = 16

# The AllenSDK-derived per-unit columns needed for the cell tables.
_LOCATION_COLUMNS = [
    "id",
    "ecephys_probe_id",
    "ecephys_structure_acronym",
    "anterior_posterior_ccf_coordinate",
    "dorsal_ventral_ccf_coordinate",
    "left_right_ccf_coordinate",
]

# AllenSDK's `left_right_ccf_coordinate` follows the raw CCFv3 volume axis
# (small = left, midline at 5700 um). cell_properties.ccf_ml uses the Dynamic
# Routing convention, which runs the opposite way, so mirror once on read and
# every cell in the table shares one convention.
_CCF_ML_MIDLINE_UM = 5700


def _log(message: str) -> None:
    """Emit a structured cache log message for the cell tables."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES["cell_index"],
            message=message,
        ).to_json()
    )


def _list_nwb_dirs(client, bucket: str, prefix: str) -> list[str]:
    """Return common-prefix directories under ``prefix`` that look like an NWB store."""
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    return [
        entry["Prefix"].rstrip("/")
        for entry in resp.get("CommonPrefixes", [])
        if entry["Prefix"].rstrip("/").endswith(_NWB_DIR_SUFFIXES)
    ]


def _find_nwb_prefixes(client, bucket: str, key: str) -> list[str]:
    """Return the S3 key prefixes of every NWB store belonging to one asset.

    Checks the ``<key>/nwb/*.nwb/`` layout used by sorted derived assets first,
    then the asset root for directly-exported NWB assets.
    """
    prefixes = _list_nwb_dirs(client, bucket, f"{key}/nwb/")
    if prefixes:
        return prefixes
    return _list_nwb_dirs(client, bucket, f"{key}/")


def _load_units_metadata(client, bucket: str, nwb_prefix: str) -> tuple[bytes, dict] | None:
    """Return the raw consolidated metadata bytes and parsed dict if a ``/units`` group exists.

    An aborted zarr write can leave a present-but-empty or truncated
    ``.zmetadata``; such a file is skipped cheaply rather than crashing the read.
    """
    body = client.get_object(Bucket=bucket, Key=f"{nwb_prefix}/.zmetadata")["Body"].read()
    if len(body) == 0:
        _log(f"Skipping empty .zmetadata under {nwb_prefix}")
        return None
    try:
        metadata = json.loads(body).get("metadata", {})
    except json.JSONDecodeError:
        _log(f"Skipping malformed .zmetadata under {nwb_prefix}")
        return None
    if f"{_UNITS_GROUP}/id/.zarray" not in metadata:
        return None
    return body, metadata


def _download_units_store(client, bucket: str, nwb_prefix: str, zmetadata: bytes, arrays: list[str]) -> dict:
    """Concurrently download the consolidated metadata and the requested units arrays.

    Returns an in-memory zarr store dict keyed by NWB-relative paths.
    """
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for array in arrays:
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{nwb_prefix}/{_UNITS_GROUP}/{array}/"):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

    def _fetch(s3_key: str) -> tuple[str, bytes]:
        """Download one object and return its NWB-relative key with bytes."""
        return s3_key[len(nwb_prefix) + 1 :], client.get_object(Bucket=bucket, Key=s3_key)["Body"].read()

    store = {".zmetadata": zmetadata}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        for rel_key, body in executor.map(_fetch, keys):
            store[rel_key] = body
    return store


def _probe_names_by_probe_id(client, bucket: str, nwb_prefix: str) -> dict[int, str]:
    """Resolve each electrode group's display name (e.g. "probeA") against its numeric probe id.

    The AllenSDK units table carries only a numeric ``ecephys_probe_id``; the
    readable name lives on each electrode group's ``.zattrs`` sidecar.
    """
    prefix = f"{nwb_prefix}/general/extracellular_ephys/"
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    names = [entry["Prefix"][len(prefix) :].rstrip("/") for entry in resp.get("CommonPrefixes", [])]

    mapping: dict[int, str] = {}
    for name in names:
        if not name or name == "electrodes":
            continue
        try:
            body = client.get_object(Bucket=bucket, Key=f"{prefix}{name}/.zattrs")["Body"].read()
            probe_id = json.loads(body).get("probe_id")
            if probe_id is not None:
                mapping[int(probe_id)] = name
        except Exception as exc:
            _log(f"Could not resolve probe name for {prefix}{name}: {type(exc).__name__}: {exc}")
    return mapping


def read_allensdk_unit_locations(asset_name: str) -> pd.DataFrame:
    """Return one row per unit with probe name, structure and CCF position for one asset.

    Reads the public AllenSDK-derived NWB-Zarr store directly, so it does not
    depend on any cache table.

    Args:
        asset_name: Public derived asset name, used as the S3 key prefix in
            ``aind-open-data``.

    Returns:
        DataFrame with ``probe_name``, ``unit_id``, ``structure``, ``ccf_ap``,
        ``ccf_dv`` and ``ccf_ml`` columns, or empty if the asset has no readable
        units.
    """
    import zarr
    from botocore.config import Config

    client = boto3.client("s3", config=Config(max_pool_connections=_MAX_WORKERS * 2))
    nwb_prefixes = _find_nwb_prefixes(client, PUBLIC_BUCKET, asset_name)
    if not nwb_prefixes:
        _log(f"No NWB-Zarr store found for asset {asset_name}")
        return pd.DataFrame()

    frames = []
    for nwb_prefix in nwb_prefixes:
        meta = _load_units_metadata(client, PUBLIC_BUCKET, nwb_prefix)
        if meta is None:
            continue
        zmetadata, metadata = meta
        available = [c for c in _LOCATION_COLUMNS if f"{_UNITS_GROUP}/{c}/.zarray" in metadata]
        if "id" not in available:
            continue
        store = _download_units_store(client, PUBLIC_BUCKET, nwb_prefix, zmetadata, available)
        units = zarr.open_consolidated(store, mode="r")[_UNITS_GROUP]

        frame = pd.DataFrame({column: np.asarray(units[column][:]) for column in available})
        probe_names = _probe_names_by_probe_id(client, PUBLIC_BUCKET, nwb_prefix)
        probe_ids = frame.get("ecephys_probe_id", pd.Series(dtype="float64"))

        def _probe_name(probe_id, names=probe_names):
            """Return the readable probe name, falling back to the numeric id."""
            if pd.isna(probe_id):
                return None
            return names.get(int(probe_id), f"probe {int(probe_id)}")

        frame["probe_name"] = probe_ids.map(_probe_name)
        frames.append(frame)

    if not frames:
        return pd.DataFrame()

    out = pd.concat(frames, ignore_index=True).rename(
        columns={
            "id": "unit_id",
            "ecephys_structure_acronym": "structure",
            "anterior_posterior_ccf_coordinate": "ccf_ap",
            "dorsal_ventral_ccf_coordinate": "ccf_dv",
        }
    )
    if "left_right_ccf_coordinate" in out.columns:
        out["ccf_ml"] = 2 * _CCF_ML_MIDLINE_UM - out["left_right_ccf_coordinate"]
    return out.drop(columns=["ecephys_probe_id", "left_right_ccf_coordinate"], errors="ignore")

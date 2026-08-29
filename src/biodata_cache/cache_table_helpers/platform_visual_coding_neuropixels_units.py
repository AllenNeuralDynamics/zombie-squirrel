"""Visual Coding Neuropixels unit-location cache table (one small unpartitioned table).

Zombie's SWDB "all neuron locations" 3D overview needs every unit's CCF
position, probe, and target structure across the whole public Visual Coding
Neuropixels collection at once. Reading that live from each session's NWB-Zarr
in the browser (raw `/units` coordinate arrays plus a probe-id-to-name lookup
per session) is dozens of small S3 round trips per session, times 57 sessions.

This table precomputes the same handful of columns server-side, once, for
every session in the collection, and writes them as a single small parquet
file (one row per unit, ~a few thousand rows total) so the frontend overview
becomes a single fetch instead of hundreds.

Unlike `platform_ecephys_units` (a partitioned, per-asset, all-quality-metrics
table for Dynamic Routing's own sorted units), this table is deliberately
small, unpartitioned, and location-only: the overview always wants every
session's units at once, and probe identity in the source AllenSDK NWB is a
numeric `ecephys_probe_id` rather than a name, so it is resolved here against
each probe's electrode-group `.zattrs` sidecar instead of at read time.
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor

import boto3
import numpy as np
import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.platform_ecephys_units import (
    _download_units_store,
    _find_nwb_prefixes,
    _load_units_metadata,
)
from biodata_cache.cache_table_helpers.swdb_public_assets import SWDB_2026_DERIVED_ASSETS
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

_PUBLIC_BUCKET = "aind-open-data"
_UNITS_GROUP = "units"
_LOCATION_COLUMNS = [
    "id", "ecephys_probe_id", "ecephys_structure_acronym",
    "anterior_posterior_ccf_coordinate", "dorsal_ventral_ccf_coordinate",
    "left_right_ccf_coordinate",
]
_MAX_WORKERS = 8

# AllenSDK's `left_right_ccf_coordinate` follows the raw CCFv3 volume axis
# (small = left, large = right, midline at 5700 um). Zombie's shared
# CCF->three.js transform expects Dynamic Routing's `ccf_ml` convention, which
# runs the opposite way (small = right). Mirror once here so every consumer
# of this table already has the convention the shared 3D viewer expects.
_CCF_ML_MIDLINE_UM = 5700


def _log(message: str) -> None:
    """Emit a structured cache log message for this table."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES["visual_coding_neuropixels_units"],
            message=message,
        ).to_json()
    )


def _probe_names_by_probe_id(client, bucket: str, nwb_prefix: str) -> dict[int, str]:
    """Resolve each electrode group's display name (e.g. "probeA") against its numeric `probe_id`.

    There is no per-unit probe-name field in the units table, only the numeric
    id each electrode group's `.zattrs` sidecar also carries.
    """
    prefix = f"{nwb_prefix}/general/extracellular_ephys/"
    resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    names = [
        entry["Prefix"][len(prefix):].rstrip("/")
        for entry in resp.get("CommonPrefixes", [])
    ]
    names = [name for name in names if name and name != "electrodes"]

    mapping: dict[int, str] = {}
    for name in names:
        try:
            body = client.get_object(Bucket=bucket, Key=f"{prefix}{name}/.zattrs")["Body"].read()
            attrs = json.loads(body)
            if attrs.get("probe_id") is not None:
                mapping[int(attrs["probe_id"])] = name
        except Exception as exc:
            _log(f"Could not resolve probe name for {prefix}{name}: {type(exc).__name__}: {exc}")
    return mapping


def _fetch_asset_unit_locations(asset_name: str) -> pd.DataFrame:
    """Return one row per unit with CCF location, structure, and probe name for one asset."""
    import zarr
    from botocore.config import Config

    client = boto3.client("s3", config=Config(max_pool_connections=32))
    bucket, key = _PUBLIC_BUCKET, asset_name
    nwb_prefixes = _find_nwb_prefixes(client, bucket, key)
    if not nwb_prefixes:
        _log(f"No NWB-Zarr store found for asset {asset_name}")
        return pd.DataFrame()

    frames = []
    for nwb_prefix in nwb_prefixes:
        meta = _load_units_metadata(client, bucket, nwb_prefix)
        if meta is None:
            continue
        zmetadata, metadata = meta
        available = [c for c in _LOCATION_COLUMNS if f"{_UNITS_GROUP}/{c}/.zarray" in metadata]
        if "id" not in available:
            continue
        store = _download_units_store(client, bucket, nwb_prefix, zmetadata, available)
        root = zarr.open_consolidated(store, mode="r")
        units = root[_UNITS_GROUP]

        data = {column: np.asarray(units[column][:]) for column in available}
        df = pd.DataFrame(data)
        probe_names = _probe_names_by_probe_id(client, bucket, nwb_prefix)
        df["probe_name"] = df.get("ecephys_probe_id", pd.Series(dtype="float64")).map(
            lambda probe_id: probe_names.get(int(probe_id), f"probe {int(probe_id)}") if pd.notna(probe_id) else None
        )
        df["asset_name"] = asset_name
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def _build_visual_coding_neuropixels_units() -> pd.DataFrame:
    """Fetch and combine unit locations for every asset in the public collection."""
    setup_logging()
    asset_names = SWDB_2026_DERIVED_ASSETS["vcn"]

    frames = []
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        for asset_name, result in zip(asset_names, executor.map(_fetch_asset_unit_locations, asset_names)):
            if result.empty:
                continue
            frames.append(result)
            _log(f"Fetched {len(result)} unit locations for asset {asset_name}")

    if not frames:
        raise RuntimeError("No Visual Coding Neuropixels unit locations were fetched")

    df = pd.concat(frames, ignore_index=True)
    df = df.rename(columns={
        "id": "unit_id",
        "ecephys_structure_acronym": "structure",
        "anterior_posterior_ccf_coordinate": "ccf_ap",
        "dorsal_ventral_ccf_coordinate": "ccf_dv",
    })
    if "left_right_ccf_coordinate" in df.columns:
        df["ccf_ml"] = 2 * _CCF_ML_MIDLINE_UM - df["left_right_ccf_coordinate"]
        df = df.drop(columns=["left_right_ccf_coordinate"])
    df = df.drop(columns=["ecephys_probe_id"], errors="ignore")

    order = ["asset_name", "unit_id", "probe_name", "structure", "ccf_ap", "ccf_dv", "ccf_ml"]
    df = df[[c for c in order if c in df.columns]]
    return df.sort_values(["asset_name", "probe_name", "unit_id"]).reset_index(drop=True)


@registry.register_table(registry.NAMES["visual_coding_neuropixels_units"])
def platform_visual_coding_neuropixels_units(force_update: bool = False) -> pd.DataFrame:
    """Return CCF unit locations for every Visual Coding Neuropixels session.

    One row per unit across the whole public collection (~57 sessions), with
    CCF position (already mirrored to the Dynamic Routing `ccf_ml`
    convention), target structure, and probe name. Cached as a single small
    unpartitioned table since the frontend overview always wants every
    session's units at once.

    Args:
        force_update: If True, pull fresh data from the public NWB-Zarr files
            and write the result to the cache, unless the cache already holds
            data (existing data is not overwritten unless empty).

    Returns:
        DataFrame with one row per unit (see
        platform_visual_coding_neuropixels_units_columns).
    """
    df = registry.BACKEND.read(registry.NAMES["visual_coding_neuropixels_units"])
    if df.empty or force_update:
        df = _build_visual_coding_neuropixels_units()
        registry.BACKEND.write(registry.NAMES["visual_coding_neuropixels_units"], df)
    return df


def platform_visual_coding_neuropixels_units_columns() -> list[Column]:
    """Return platform_visual_coding_neuropixels_units cache table column definitions."""
    return [
        Column(name="asset_name", description="Public derived Visual Coding Neuropixels asset name"),
        Column(name="unit_id", description="NWB units-table row id"),
        Column(name="probe_name", description="Probe display name (e.g. 'probeA'), resolved from the numeric electrode-group probe id"),
        Column(name="structure", description="CCF target structure acronym for the unit's peak channel"),
        Column(name="ccf_ap", description="Anterior-posterior CCF coordinate (microns)"),
        Column(name="ccf_dv", description="Dorsal-ventral CCF coordinate (microns)"),
        Column(name="ccf_ml", description="Medial-lateral CCF coordinate (microns), mirrored to the Dynamic Routing ccf_ml convention (small = right)"),
    ]

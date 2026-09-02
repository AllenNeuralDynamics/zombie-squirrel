"""Ecephys sorted units cache table (partitioned by asset_name).

Pulls the per-unit sorting results (quality metrics, waveform metrics, location,
labels) from the per-session NWB (Zarr) ``/units`` group on S3 and stores them
one row per unit, one partition per asset. This is the companion to
``platform_ecephys_spikes``: the two share the join key
``(asset_name, experiment, device_name, unit_name)``.

Only the small per-unit arrays are downloaded. The full ``waveform_mean`` cube is
read solely to extract each unit's extremum-channel mean waveform (a 1D vector);
``waveform_sd``, ``electrodes`` and the ragged ``spike_times`` are never fetched.
An asset may contain several NWB files (one per experiment/recording); each file
with a ``/units`` group contributes rows, and files without one are skipped.
"""

import gc
import logging
import re
from concurrent.futures import ThreadPoolExecutor

import boto3
import numpy as np
import pandas as pd
from botocore.config import Config

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.asset_basics import asset_basics
from biodata_cache.cache_table_helpers.shared.nwb_zarr import (
    NWB_DIR_SUFFIXES,
    download_zarr_store,
    find_nwb_prefixes,
    list_nwb_dirs,
    load_zmetadata,
    parse_s3,
)
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

_UNITS_GROUP = "units"
# Arrays that are ragged, multi-dimensional, or otherwise not one scalar per unit.
_NON_SCALAR_ARRAYS = {
    "spike_times",
    "spike_times_index",
    "electrodes",
    "electrodes_index",
    "waveform_mean",
    "waveform_sd",
}
_FRONT_COLUMNS = ["experiment", "device_name", "unit_name"]
_EXPERIMENT_RE = re.compile(r"(experiment\d+_recording\d+)")
_MAX_WORKERS = 32


def _log(message: str) -> None:
    """Emit a structured cache log message for the ecephys_units table."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES["ecephys_units"],
            message=message,
        ).to_json()
    )


_parse_s3 = parse_s3
_list_nwb_dirs = list_nwb_dirs
_find_nwb_prefixes = find_nwb_prefixes
_NWB_DIR_SUFFIXES = NWB_DIR_SUFFIXES


def _experiment_name(nwb_prefix: str) -> str:
    """Return the ``experimentN_recordingM`` tag from a NWB prefix, or its filename stem."""
    filename = nwb_prefix.rstrip("/").split("/")[-1]
    match = _EXPERIMENT_RE.search(filename)
    if match:
        return match.group(1)
    for suffix in _NWB_DIR_SUFFIXES:
        if filename.endswith(suffix):
            return filename[: -len(suffix)]
    return filename


def _scalar_columns(metadata: dict) -> tuple[list[str], int | None]:
    """Return the per-unit scalar array names and the unit count from consolidated metadata.

    A scalar column is any direct child array of the ``/units`` group whose shape is
    one value per unit (excluding the known ragged/multi-dimensional arrays). Reading
    the set dynamically keeps the table robust to sorting-pipeline version changes.
    """
    n_units = None
    id_zarray = metadata.get(f"{_UNITS_GROUP}/id/.zarray")
    name_zarray = metadata.get(f"{_UNITS_GROUP}/unit_name/.zarray")
    if id_zarray is not None:
        n_units = id_zarray["shape"][0]
    elif name_zarray is not None:
        n_units = name_zarray["shape"][0]
    if n_units is None:
        return [], None

    columns = []
    for meta_key, spec in metadata.items():
        if not (meta_key.startswith(f"{_UNITS_GROUP}/") and meta_key.endswith("/.zarray")):
            continue
        name = meta_key[len(_UNITS_GROUP) + 1 : -len("/.zarray")]
        if "/" in name or name in _NON_SCALAR_ARRAYS:
            continue
        if spec.get("shape") == [n_units]:
            columns.append(name)
    return sorted(columns), n_units


def _load_units_metadata(client, bucket: str, nwb_prefix: str) -> tuple[bytes, dict] | None:
    """Return the raw consolidated metadata bytes and parsed dict if a ``/units`` group exists.

    NWB files without sorted units (video/pose recordings, or per-experiment
    recordings without a sorting) return None so they can be skipped cheaply.
    """
    return load_zmetadata(
        client,
        bucket,
        nwb_prefix,
        required_any_paths=(f"{_UNITS_GROUP}/id/.zarray", f"{_UNITS_GROUP}/unit_name/.zarray"),
    )


def _download_units_store(client, bucket: str, nwb_prefix: str, zmetadata: bytes, arrays: list[str]) -> dict:
    """Concurrently download the consolidated metadata and the requested units arrays.

    Only the ``.zmetadata`` and every chunk under the requested ``units/<array>``
    groups are fetched. Returns an in-memory zarr store dict keyed by NWB-relative
    paths.
    """
    return download_zarr_store(
        client,
        bucket,
        nwb_prefix,
        zmetadata,
        [f"{_UNITS_GROUP}/{array}" for array in arrays],
        max_workers=_MAX_WORKERS,
    )


def _open_units_group(client, bucket: str, nwb_prefix: str) -> tuple[object, list[str], dict, bytes] | None:
    """Open the NWB ``/units`` group for one NWB file, or None if it has no units.

    Returns the zarr ``/units`` group (scalar arrays only), the list of per-unit
    scalar column names, the parsed consolidated metadata, and the raw metadata
    bytes. The large ``waveform_mean`` cube is NOT downloaded here; it is read
    lazily in bands by ``_extremum_waveforms``. ``waveform_sd`` and the ragged
    ``spike_times`` are skipped entirely.
    """
    import zarr

    meta = _load_units_metadata(client, bucket, nwb_prefix)
    if meta is None:
        return None
    zmetadata, metadata = meta
    scalar_cols, _ = _scalar_columns(metadata)
    if not scalar_cols:
        return None
    store = _download_units_store(client, bucket, nwb_prefix, zmetadata, scalar_cols)
    root = zarr.open_consolidated(store, mode="r")
    return root[_UNITS_GROUP], scalar_cols, metadata, zmetadata


def _extremum_waveforms(
    client, bucket: str, nwb_prefix: str, zmetadata: bytes, metadata: dict, extremum_idx
) -> np.ndarray | None:
    """Return each unit's extremum-channel mean waveform without materializing the cube.

    ``waveform_mean`` stores every unit's mean waveform across all probe channels
    (``num_units x num_samples x num_channels``), which can be many GiB. Only the
    extremum (peak-amplitude) channel is needed per unit. zarr cannot decompress
    less than a whole chunk, and the cube is chunked along the unit axis, so the
    smallest safe read is one unit-chunk band: each band is downloaded, decompressed
    once, and then reduced to one waveform per unit strictly one unit at a time
    before the band is freed. Peak memory is bounded to a single decompressed band.
    """
    import zarr

    spec = metadata.get(f"{_UNITS_GROUP}/waveform_mean/.zarray")
    if spec is None or len(spec.get("shape", [])) != 3:
        return None
    n_units, n_samples, n_channels = spec["shape"]
    band = spec["chunks"][0]
    extremum_idx = np.clip(np.asarray(extremum_idx).astype(int), 0, n_channels - 1)

    prefix = f"{nwb_prefix}/{_UNITS_GROUP}/waveform_mean/"
    keys_by_band: dict[str, list[str]] = {}
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            unit_chunk = obj["Key"][len(prefix) :].split(".")[0]
            if unit_chunk.isdigit():
                keys_by_band.setdefault(unit_chunk, []).append(obj["Key"])

    def _fetch(s3_key: str) -> tuple[str, bytes]:
        """Download one chunk object and return its NWB-relative key with bytes."""
        body = client.get_object(Bucket=bucket, Key=s3_key)["Body"].read()
        return s3_key[len(nwb_prefix) + 1 :], body

    out = np.zeros((n_units, n_samples), dtype="float32")
    for start in range(0, n_units, band):
        stop = min(start + band, n_units)
        store = {".zmetadata": zmetadata}
        band_keys = keys_by_band.get(str(start // band), [])
        if band_keys:
            with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
                for rel_key, body in executor.map(_fetch, band_keys):
                    store[rel_key] = body
        root = zarr.open_consolidated(store, mode="r")
        block = np.asarray(root[f"{_UNITS_GROUP}/waveform_mean"][start:stop, :, :])
        # block is a self-contained decompressed array; drop the compressed band
        # bytes and zarr handle now so only one band's data is resident at a time.
        del root, store, band_keys
        gc.collect()
        for local_idx in range(stop - start):
            unit = start + local_idx
            out[unit] = block[local_idx, :, extremum_idx[unit]].astype("float32")
        del block
        gc.collect()
    return out


def _extract_units(units, scalar_cols: list[str], experiment: str) -> pd.DataFrame:
    """Build a one-row-per-unit DataFrame of the scalar columns for a ``/units`` group.

    Every per-unit scalar array becomes a column. The extremum-channel mean
    waveform is added separately by the caller (see ``_extremum_waveforms``).

    A column that shape-qualifies as "one value per unit" can still fail to
    decode: HDMF object-reference columns (e.g. ``electrode_group``, which
    duplicates the plain string already in ``electrode_group_name``) are
    stored with a pickle codec whose payload names classes from optional
    extension packages (e.g. ``hdmf_zarr``) that may not be installed here.
    Such a column is skipped rather than aborting the whole unit table.
    """
    data: dict[str, object] = {}
    n_units = None
    for name in scalar_cols:
        try:
            arr = np.asarray(units[name][:])
        except Exception as exc:
            _log(f"Skipping unreadable units column {name!r}: {type(exc).__name__}: {exc}")
            continue
        data[name] = arr
        n_units = len(arr)
    if not data or not n_units:
        return pd.DataFrame()

    data["experiment"] = np.array([experiment] * n_units, dtype=object)
    df = pd.DataFrame(data)

    ordered = [c for c in _FRONT_COLUMNS if c in df.columns]
    ordered += [c for c in df.columns if c not in _FRONT_COLUMNS]
    return df[ordered]


def _fetch_asset_ecephys_units(asset_name: str, location: str | None = None) -> pd.DataFrame:
    """Fetch and cache the sorted units for one asset from its S3 NWB files.

    Reads every NWB file under ``<asset>/nwb/`` and concatenates the units from
    those that contain a ``/units`` group. Returns an empty DataFrame; callers
    should read back from the backend.

    Args:
        asset_name: Derived asset name whose units to fetch.
        location: The asset's S3 location. When provided (bulk sync path), the
            full asset_basics table is not read; when None (single-asset path),
            the location is looked up from asset_basics.
    """
    setup_logging()
    cache_key = f"{registry.NAMES['ecephys_units']}/{asset_name}"

    if registry.BACKEND.partition_exists(cache_key):
        _log(f"Partition already exists for asset {asset_name}, skipping")
        return pd.DataFrame()

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
    nwb_prefixes = _find_nwb_prefixes(client, bucket, key)
    if not nwb_prefixes:
        _log(f"No NWB files found for asset {asset_name}")
        return pd.DataFrame()

    frames = []
    for nwb_prefix in nwb_prefixes:
        try:
            opened = _open_units_group(client, bucket, nwb_prefix)
            if opened is None:
                continue
            units, scalar_cols, metadata, zmetadata = opened
            session_df = _extract_units(units, scalar_cols, _experiment_name(nwb_prefix))
            del units
            if session_df.empty:
                continue
            if "extremum_channel_index" in session_df.columns and f"{_UNITS_GROUP}/waveform_mean/.zarray" in metadata:
                waveforms = _extremum_waveforms(
                    client,
                    bucket,
                    nwb_prefix,
                    zmetadata,
                    metadata,
                    session_df["extremum_channel_index"].to_numpy(),
                )
                if waveforms is not None:
                    session_df["waveform"] = [row for row in waveforms]
                    del waveforms
            frames.append(session_df)
            del session_df, metadata, zmetadata
            gc.collect()
        except Exception as exc:
            _log(
                f"Failed to read units from {nwb_prefix} for asset {asset_name}, skipping: {type(exc).__name__}: {exc}"
            )
            gc.collect()
            continue

    if not frames:
        _log(f"No units extracted for asset {asset_name}")
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    if "unit_name" not in df.columns and "unit_id" in df.columns:
        # A directly-exported NWB (as opposed to one from the "sorted" pipeline)
        # identifies units by unit_id, not the unit_name UUID. Alias it so this
        # table's join key with platform_ecephys_spikes is always unit_name,
        # regardless of which identifier the source NWB actually stores.
        df["unit_name"] = df["unit_id"]
    sort_cols = [c for c in ["experiment", "device_name", "unit_name"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols).reset_index(drop=True)
    registry.BACKEND.write(cache_key, df)
    _log(f"Cached ecephys units for asset {asset_name}")
    return pd.DataFrame()


@registry.register_table(registry.NAMES["ecephys_units"])
def platform_ecephys_units(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    """Return sorted ecephys units for a single asset.

    One row per unit across every NWB ``/units`` group found in the asset (each
    experiment/recording, each probe, each unit), with quality metrics, waveform
    metrics, location, labels, and the extremum-channel mean waveform. Assets
    without sorted units produce no rows. Data is cached per asset_name partition.

    Args:
        asset_name: Derived asset name whose units to fetch.
        force_update: If True, pull fresh data from the S3 NWB files and write the
            result to the cache, unless the partition already exists (existing
            partitions are skipped, not overwritten). An empty DataFrame is
            returned; read again without force_update (or use lazy=True) to
            retrieve the data.
        lazy: If True, return the partition's storage location string (for DuckDB)
            instead of loading the DataFrame.
        location: Optional S3 location of the asset. When provided during a
            force_update, the full asset_basics table is not read (used by the
            bulk sync to avoid re-reading asset_basics once per asset).

    Returns:
        DataFrame with one row per unit (see platform_ecephys_units_columns); the
        partition location string if lazy=True; or an empty DataFrame if
        force_update=True (data is written to the cache).

    Raises:
        ValueError: If the cache is empty for the asset and force_update is False.
    """
    cache_key = f"{registry.NAMES['ecephys_units']}/{asset_name}"

    if lazy:
        if force_update:
            _fetch_asset_ecephys_units(asset_name, location=location)
        return registry.BACKEND.get_location(cache_key)

    if force_update:
        return _fetch_asset_ecephys_units(asset_name, location=location)

    df = registry.BACKEND.read(cache_key)
    if df.empty:
        raise ValueError(f"Cache is empty for asset {asset_name}. Use force_update=True to fetch data from S3.")

    return df


def platform_ecephys_units_columns() -> list[Column]:
    """Return platform_ecephys_units cache table column definitions.

    The written table includes every per-unit scalar array found in the NWB, so
    the exact columns can vary by sorting-pipeline version; the entries below cover
    the common Kilosort4 output.
    """
    return [
        Column(name="experiment", description="Source NWB recording tag (e.g. 'experiment1_recording1')"),
        Column(
            name="device_name",
            description="Probe the unit was recorded on (e.g. 'Probe A'); joinable with platform_ecephys_spikes",
        ),
        Column(name="unit_name", description="Unit identifier (UUID); joinable with platform_ecephys_spikes"),
        Column(name="id", description="NWB units-table row id"),
        Column(name="ks_unit_id", description="Kilosort unit id"),
        Column(name="original_cluster_id", description="Original sorter cluster id"),
        Column(name="default_qc", description="Whether the unit passes the default QC criteria"),
        Column(name="decoder_label", description="Predicted unit type from the noise/MUA/SUA decoder"),
        Column(name="decoder_probability", description="Confidence of the decoder_label prediction"),
        Column(name="num_spikes", description="Total number of spikes"),
        Column(name="firing_rate", description="Mean firing rate (Hz)"),
        Column(name="firing_range", description="Range of the binned firing rate (Hz)"),
        Column(name="presence_ratio", description="Fraction of the recording in which the unit is active"),
        Column(name="amplitude", description="Unit spike amplitude"),
        Column(name="amplitude_median", description="Median spike amplitude"),
        Column(
            name="amplitude_cutoff", description="Estimated fraction of missed spikes from the amplitude distribution"
        ),
        Column(name="snr", description="Signal-to-noise ratio of the unit"),
        Column(name="isi_violations_ratio", description="Inter-spike-interval violation ratio (contamination proxy)"),
        Column(name="isi_violations_count", description="Number of inter-spike-interval violations"),
        Column(name="rp_contamination", description="Refractory-period contamination estimate"),
        Column(name="rp_violations", description="Number of refractory-period violations"),
        Column(name="sliding_rp_violation", description="Sliding refractory-period violation metric"),
        Column(name="d_prime", description="d-prime cluster separation metric"),
        Column(name="isolation_distance", description="Isolation distance cluster-quality metric"),
        Column(name="l_ratio", description="L-ratio cluster-quality metric"),
        Column(name="nn_hit_rate", description="Nearest-neighbor hit rate"),
        Column(name="nn_miss_rate", description="Nearest-neighbor miss rate"),
        Column(name="silhouette", description="Silhouette cluster-quality score"),
        Column(name="drift_ptp", description="Peak-to-peak spatial drift"),
        Column(name="drift_std", description="Standard deviation of spatial drift"),
        Column(name="drift_mad", description="Median absolute deviation of spatial drift"),
        Column(name="amplitude_cv_median", description="Median of the amplitude coefficient of variation"),
        Column(name="amplitude_cv_range", description="Range of the amplitude coefficient of variation"),
        Column(name="depth", description="Estimated unit depth along the probe (microns)"),
        Column(name="shank", description="Probe shank index the unit is on"),
        Column(name="estimated_x", description="Estimated unit x position (microns)"),
        Column(name="estimated_y", description="Estimated unit y position (microns)"),
        Column(name="estimated_z", description="Estimated unit z position (microns)"),
        Column(name="extremum_channel_index", description="Index of the channel with the largest waveform amplitude"),
        Column(name="half_width", description="Waveform half width (ms)"),
        Column(name="peak_to_valley", description="Waveform peak-to-valley duration (ms)"),
        Column(name="peak_trough_ratio", description="Waveform peak-to-trough amplitude ratio"),
        Column(name="recovery_slope", description="Waveform recovery slope"),
        Column(name="repolarization_slope", description="Waveform repolarization slope"),
        Column(name="spread", description="Spatial spread of the waveform (microns)"),
        Column(name="velocity_above", description="Waveform propagation velocity above the soma"),
        Column(name="velocity_below", description="Waveform propagation velocity below the soma"),
        Column(name="exp_decay", description="Exponential decay constant of the waveform amplitude over channels"),
        Column(name="num_positive_peaks", description="Number of positive peaks in the waveform"),
        Column(name="num_negative_peaks", description="Number of negative peaks in the waveform"),
        Column(
            name="waveform",
            description="Extremum-channel mean waveform, a float32 vector of length num_samples (volts)",
        ),
    ]

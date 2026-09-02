"""Ecephys spike times cache table (partitioned by asset_name).

Pulls the sorted spike times from the per-session NWB (Zarr) files on S3 and
stores them in long form, one partition per asset. One row per spike: the unit
it belongs to, the probe (device) it was recorded on, the source NWB recording,
and the spike timestamp on the acquisition clock.

Spike sorting output is read only from the NWB ``/units`` group; the separate
``spikesorted/`` SpikeInterface folders are not touched. An asset may contain
several NWB files (one per experiment/recording); each file that has a ``/units``
group contributes rows, and files without one are skipped. Many ecephys derived
assets (pose tracking, facemap, etc.) have no NWB at all and produce no rows.
"""

import logging
import re

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
_SPIKE_ARRAYS = ["spike_times", "spike_times_index", "unit_name", "unit_id", "device_name"]
_EXPERIMENT_RE = re.compile(r"(experiment\d+_recording\d+)")
_MAX_WORKERS = 32
# Upper bound on the number of spikes materialized into a single DataFrame/parquet
# chunk. Spike times are read from zarr in per-unit bands sized to stay under this
# limit so peak memory is bounded regardless of how many spikes an asset contains.
_MAX_SPIKES_PER_CHUNK = 50_000_000


def _log(message: str) -> None:
    """Emit a structured cache log message for the ecephys_spikes table."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES["ecephys_spikes"],
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


def _load_units_metadata(client, bucket: str, nwb_prefix: str) -> tuple[bytes, dict] | None:
    """Return the raw consolidated metadata bytes and parsed dict if a ``/units`` group exists.

    The consolidated ``.zmetadata`` is fetched and inspected for the spike-times
    array; NWB files with no sorted units (video/pose recordings, or per-experiment
    recordings without a sorting) return None so they can be skipped cheaply.
    """
    return load_zmetadata(
        client,
        bucket,
        nwb_prefix,
        required_paths=(f"{_UNITS_GROUP}/spike_times/.zarray",),
    )


def _download_units_store(client, bucket: str, nwb_prefix: str, zmetadata: bytes, arrays: list[str]) -> dict:
    """Concurrently download the consolidated metadata and the requested units arrays.

    Only the ``.zmetadata`` and every chunk under the requested ``units/<array>``
    groups are fetched (the large ``waveform_mean``/``waveform_sd`` cubes are never
    downloaded). Returns an in-memory zarr store dict keyed by NWB-relative paths.
    """
    store = download_zarr_store(
        client,
        bucket,
        nwb_prefix,
        zmetadata,
        [f"{_UNITS_GROUP}/{array}" for array in arrays],
        max_workers=_MAX_WORKERS,
        skip_empty=True,
    )
    return store


def _open_units_group(client, bucket: str, nwb_prefix: str):
    """Open the NWB ``/units`` zarr group for one NWB file, or None if it has no units.

    Reads only the consolidated metadata and the spike arrays via boto3
    (concurrently), avoiding any full-file download. ``device_name`` is downloaded
    only when the sorting pipeline recorded it.
    """
    import zarr

    meta = _load_units_metadata(client, bucket, nwb_prefix)
    if meta is None:
        return None
    zmetadata, metadata = meta
    arrays = [a for a in _SPIKE_ARRAYS if f"{_UNITS_GROUP}/{a}/.zarray" in metadata]
    store = _download_units_store(client, bucket, nwb_prefix, zmetadata, arrays)
    root = zarr.open_consolidated(store, mode="r")
    return root[_UNITS_GROUP]


def _extract_spikes(units, experiment: str):
    """Yield long-form spike DataFrames for one NWB ``/units`` group in bounded bands.

    One row per spike. The ragged ``spike_times`` array is split per unit using the
    cumulative ``spike_times_index`` offsets. To keep peak memory bounded regardless
    of asset size, units are grouped into bands of at most ``_MAX_SPIKES_PER_CHUNK``
    spikes, and each band reads only its slice of ``spike_times`` from zarr (never the
    whole array). ``device_name`` and ``unit_name`` are stored as pandas categoricals
    so per-spike columns hold small integer codes rather than 8-byte object references.

    Not every NWB layout has a ``unit_name`` (UUID) array - a directly-exported NWB
    (as opposed to one produced by the "sorted" pipeline) identifies units by
    ``unit_id`` instead. The output column is always called ``unit_name`` regardless,
    since that is the join key ``platform_ecephys_units`` also standardizes on (see
    the equivalent fallback in that module) - callers should not need to know which
    identifier a given asset's NWB actually stores.
    """
    index = np.asarray(units["spike_times_index"][:], dtype="int64")
    if index.size == 0:
        return

    if "unit_name" in units:
        unit_name = np.asarray(units["unit_name"][:], dtype=object)
    elif "unit_id" in units:
        unit_name = np.asarray(units["unit_id"][:], dtype=object)
    else:
        unit_name = np.array(["" for _ in range(len(index))], dtype=object)
    if "device_name" in units:
        device_name = np.asarray(units["device_name"][:], dtype=object)
    else:
        device_name = np.array(["" for _ in range(len(index))], dtype=object)

    spike_times_arr = units["spike_times"]
    starts = np.concatenate([[0], index[:-1]])
    counts = index - starts
    n_units = len(index)

    band_start = 0
    while band_start < n_units:
        band_spikes = 0
        band_end = band_start
        while band_end < n_units and (
            band_end == band_start or band_spikes + counts[band_end] <= _MAX_SPIKES_PER_CHUNK
        ):
            band_spikes += int(counts[band_end])
            band_end += 1

        off_start = int(starts[band_start])
        off_end = int(index[band_end - 1])
        if off_end <= off_start:
            band_start = band_end
            continue

        spike_times = np.asarray(spike_times_arr[off_start:off_end], dtype="float64")
        band_counts = counts[band_start:band_end]
        df = pd.DataFrame(
            {
                "experiment": pd.Categorical([experiment] * spike_times.size),
                "device_name": pd.Categorical(np.repeat(device_name[band_start:band_end], band_counts)),
                "unit_name": pd.Categorical(np.repeat(unit_name[band_start:band_end], band_counts)),
                "spike_time": spike_times,
            }
        )
        yield df
        del spike_times, df
        band_start = band_end


def _fetch_asset_ecephys_spikes(asset_name: str, location: str | None = None) -> pd.DataFrame:
    """Fetch and cache the sorted spike times for one asset from its S3 NWB files.

    Reads every NWB file under ``<asset>/nwb/`` and concatenates the spikes from
    those that contain a ``/units`` group. Returns an empty DataFrame; callers
    should read back from the backend.

    Args:
        asset_name: Derived asset name whose spikes to fetch.
        location: The asset's S3 location. When provided (bulk sync path), the
            full asset_basics table is not read; when None (single-asset path),
            the location is looked up from asset_basics.
    """
    setup_logging()
    cache_key = f"{registry.NAMES['ecephys_spikes']}/{asset_name}"

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

    chunk_idx = 0
    for nwb_prefix in nwb_prefixes:
        units = _open_units_group(client, bucket, nwb_prefix)
        if units is None:
            continue
        experiment = _experiment_name(nwb_prefix)
        for band_df in _extract_spikes(units, experiment):
            band_df = band_df.sort_values(["device_name", "unit_name", "spike_time"]).reset_index(drop=True)
            registry.BACKEND.write_chunk(cache_key, band_df, chunk_idx)
            chunk_idx += 1
            del band_df
        del units

    if chunk_idx == 0:
        _log(f"No spikes extracted for asset {asset_name}")
        return pd.DataFrame()

    _log(f"Cached ecephys spikes for asset {asset_name}")
    return pd.DataFrame()


@registry.register_table(registry.NAMES["ecephys_spikes"])
def platform_ecephys_spikes(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    """Return sorted ecephys spike times for a single asset.

    One row per spike across every NWB ``/units`` group found in the asset
    (each experiment/recording, each probe, each unit). Assets without sorted
    units produce no rows. Data is cached per asset_name partition.

    Args:
        asset_name: Derived asset name whose spikes to fetch.
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
        DataFrame with columns experiment, device_name, unit_name, and spike_time;
        the partition location string if lazy=True; or an empty DataFrame if
        force_update=True (data is written to the cache).

    Raises:
        ValueError: If the cache is empty for the asset and force_update is False.
    """
    cache_key = f"{registry.NAMES['ecephys_spikes']}/{asset_name}"

    if lazy:
        if force_update:
            _fetch_asset_ecephys_spikes(asset_name, location=location)
        return registry.BACKEND.get_location(cache_key)

    if force_update:
        return _fetch_asset_ecephys_spikes(asset_name, location=location)

    df = registry.BACKEND.read(cache_key)
    if df.empty:
        raise ValueError(f"Cache is empty for asset {asset_name}. Use force_update=True to fetch data from S3.")

    return df


def platform_ecephys_spikes_columns() -> list[Column]:
    """Return platform_ecephys_spikes cache table column definitions."""
    return [
        Column(name="experiment", description="Source NWB recording tag (e.g. 'experiment1_recording1')"),
        Column(
            name="device_name",
            description="Probe the unit was recorded on (e.g. 'Probe A'); joinable with platform_ecephys_units",
        ),
        Column(name="unit_name", description="Unit identifier (UUID); joinable with platform_ecephys_units"),
        Column(name="spike_time", description="Spike timestamp in seconds on the acquisition clock"),
    ]

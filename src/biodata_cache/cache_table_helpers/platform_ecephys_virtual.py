"""Virtual-Zarr ecephys cache (partitioned by ``asset_name``).

The source ecephys recordings are already Zarr stores.  This cache therefore
publishes a small `fsspec-reference-zarr-v1` manifest instead of copying any
array bytes.  The manifest exposes the spike-time arrays and only the unit
arrays consumed by the ecephys viewer.  Chunk values remain in the source NWB
Zarr stores and are fetched by byte range by a reference-store client.

The manifest is deliberately compatible with the interchange format used by
VirtualiZarr/Kerchunk.  Keeping the builder here also lets the cache publish a
strict subset of the very wide NWB ``/units`` group and gives browser clients a
stable, versioned URL.
"""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone

import boto3
import pandas as pd
from botocore.config import Config
from botocore.exceptions import ClientError

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.asset_basics import asset_basics
from biodata_cache.cache_table_helpers.shared.nwb_zarr import (
    NWB_DIR_SUFFIXES,
    find_nwb_prefixes,
    load_zmetadata,
    parse_s3,
)
from biodata_cache.models import Column
from biodata_cache.utils import BDC_VERSION, CacheLogMessage, setup_logging

_UNITS_GROUP = "units"
_MANIFEST_STORAGE_NAME = "platform_ecephys_virtual"
_INDEX_STORAGE_NAME = "platform_ecephys_virtual_index"
_SPIKE_ARRAYS = ("spike_times", "spike_times_index")
# These are the fields used by zombie's ecephys raster, MIDI selector, and unit
# detail view.  ``unit_id`` is retained as a fallback for NWBs without
# ``unit_name``; both arrays are tiny compared with spike_times/waveform_mean.
_UNIT_ARRAYS = (
    "unit_name",
    "id",
    "unit_id",
    "device_name",
    "decoder_label",
    "default_qc",
    "firing_rate",
    "snr",
    "num_spikes",
    "presence_ratio",
    "isi_violations_ratio",
    "amplitude_median",
    "depth",
    "extremum_channel_index",
    "waveform_mean",
)
_EXPERIMENT_RE = re.compile(r"(experiment\d+_recording\d+)")
_MAX_WORKERS = 32


def _table_name() -> str:
    """Return the registered virtual table name."""
    return registry.NAMES["ecephys_virtual"]


def _log(message: str) -> None:
    """Emit a structured cache log message."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=_table_name(),
            message=message,
        ).to_json()
    )


def _experiment_name(nwb_prefix: str) -> str:
    """Return the experiment/recording tag or a safe NWB filename stem."""
    filename = nwb_prefix.rstrip("/").split("/")[-1]
    match = _EXPERIMENT_RE.search(filename)
    if match:
        return match.group(1)
    for suffix in NWB_DIR_SUFFIXES:
        if filename.endswith(suffix):
            return filename[: -len(suffix)]
    return filename


def _object_uri(bucket: str, key: str) -> str:
    """Return an S3 URI suitable for a reference manifest."""
    return f"s3://{bucket}/{key}"


def _inline(value: object) -> str:
    """Encode inline JSON metadata as a reference-spec string."""
    return json.dumps(value, separators=(",", ":"))


def _manifest_key(asset_name: str) -> str:
    """Return the versioned object key for an asset manifest."""
    return f"{_table_name()}/asset_name={asset_name}/virtual-zarr.json"


def _index_key(asset_name: str) -> str:
    """Return the internal Parquet index partition key for an asset."""
    return f"{_INDEX_STORAGE_NAME}/{asset_name}"


def _catalog_key(asset_name: str) -> str:
    """Return the versioned object key for an asset catalog."""
    return f"{_table_name()}/asset_name={asset_name}/catalog.json"


def _manifest_location(asset_name: str) -> str:
    """Return the public storage URL for an asset's virtual-Zarr manifest."""
    prefix = registry.BACKEND.get_location(_MANIFEST_STORAGE_NAME, partitioned=True)
    return f"{prefix}asset_name={asset_name}/virtual-zarr.json"


def _catalog_location(asset_name: str) -> str:
    """Return the public storage URL for an asset's manifest catalog."""
    prefix = registry.BACKEND.get_location(_MANIFEST_STORAGE_NAME, partitioned=True)
    return f"{prefix}asset_name={asset_name}/catalog.json"


def _write_index(asset_name: str) -> None:
    """Write the tiny queryable index row alongside the registry contract."""
    registry.BACKEND.write(
        _index_key(asset_name),
        pd.DataFrame(
            [{
                "asset_name": asset_name,
                "manifest_url": _manifest_location(asset_name),
                "catalog_url": _catalog_location(asset_name),
            }]
        ),
    )


def _manifest_exists(asset_name: str) -> bool:
    """Return whether a valid manifest already exists in the active cache version."""
    try:
        manifest = json.loads(registry.BACKEND.get_json(_manifest_key(asset_name)))
    except ClientError as exc:
        code = str(exc.response.get("Error", {}).get("Code", ""))
        if code in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise
    except (TypeError, ValueError, json.JSONDecodeError):
        return False
    return manifest.get("version") == 1 and isinstance(manifest.get("refs"), dict)


def _list_chunk_objects(client, bucket: str, nwb_prefix: str, array_name: str) -> list[tuple[str, int]]:
    """List non-metadata objects belonging to one source Zarr array."""
    array_prefix = f"{nwb_prefix}/{_UNITS_GROUP}/{array_name}/"
    paginator = client.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=array_prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            relative = key[len(array_prefix) :]
            if not relative or relative in {".zarray", ".zattrs"} or relative.startswith("."):
                continue
            objects.append((key, int(obj["Size"])))
    return sorted(objects)


def _add_source_refs(
    client,
    refs: dict[str, object],
    bucket: str,
    nwb_prefix: str,
    group_path: str,
) -> dict[str, object] | None:
    """Add selected metadata/chunk references for one NWB Zarr store."""
    meta = load_zmetadata(
        client,
        bucket,
        nwb_prefix,
        required_paths=tuple(f"{_UNITS_GROUP}/{name}/.zarray" for name in _SPIKE_ARRAYS),
    )
    if meta is None:
        return None
    _, metadata = meta

    available_arrays = [
        name
        for name in (*_SPIKE_ARRAYS, *_UNIT_ARRAYS)
        if name != "waveform_mean" and f"{_UNITS_GROUP}/{name}/.zarray" in metadata
    ]
    if (
        f"{_UNITS_GROUP}/waveform_mean/.zarray" in metadata
        and f"{_UNITS_GROUP}/extremum_channel_index/.zarray" in metadata
    ):
        available_arrays.append("waveform_mean")
    if not any(name in available_arrays for name in ("unit_name", "id", "unit_id")):
        return None

    # Reference manifests accept either inline JSON strings or [url, offset,
    # length] byte references.  Inline all Zarr metadata so the browser can
    # open arrays without fetching the source store's large .zmetadata file.
    source_metadata_paths = [
        ".zgroup",
        ".zattrs",
        f"{_UNITS_GROUP}/.zgroup",
        f"{_UNITS_GROUP}/.zattrs",
    ] + [
        f"{_UNITS_GROUP}/{name}/{suffix}"
        for name in available_arrays
        for suffix in (".zarray", ".zattrs")
    ]
    subset_metadata: dict[str, object] = {}
    refs[f"{group_path}/.zgroup"] = _inline(metadata.get(".zgroup", {"zarr_format": 2}))
    if ".zattrs" in metadata:
        refs[f"{group_path}/.zattrs"] = _inline(metadata[".zattrs"])
    refs[f"{group_path}/{_UNITS_GROUP}/.zgroup"] = _inline(
        metadata.get(f"{_UNITS_GROUP}/.zgroup", {"zarr_format": 2})
    )
    for path in source_metadata_paths:
        if path in metadata:
            target = f"{group_path}/{path}"
            refs[target] = _inline(metadata[path])
            subset_metadata[path] = metadata[path]

    array_catalog: dict[str, object] = {}
    for array_name in available_arrays:
        source_spec = metadata[f"{_UNITS_GROUP}/{array_name}/.zarray"]
        array_catalog[array_name] = {
            "shape": source_spec.get("shape"),
            "chunks": source_spec.get("chunks"),
            "dtype": source_spec.get("dtype"),
            "compressor": source_spec.get("compressor"),
        }
        for key, size in _list_chunk_objects(client, bucket, nwb_prefix, array_name):
            relative = key[len(nwb_prefix) + 1 :]
            refs[f"{group_path}/{relative}"] = [_object_uri(bucket, key), 0, size]

    # This per-source .zmetadata makes the manifest useful to Python/fsspec
    # clients as well as to the browser's direct array reader.
    refs[f"{group_path}/.zmetadata"] = _inline(
        {"zarr_consolidated_format": 1, "metadata": subset_metadata}
    )
    id_spec = next(
        (
            metadata.get(f"{_UNITS_GROUP}/{name}/.zarray")
            for name in ("unit_name", "id", "unit_id")
            if f"{_UNITS_GROUP}/{name}/.zarray" in metadata
        ),
        None,
    )
    return {
        "experiment": _experiment_name(nwb_prefix),
        "source_uri": _object_uri(bucket, nwb_prefix),
        "group_path": group_path,
        "arrays": array_catalog,
        "unit_count": id_spec.get("shape", [None])[0] if id_spec else None,
    }


def build_virtual_manifest(client, asset_name: str, location: str) -> tuple[dict, dict] | None:
    """Build an asset-scoped virtual-Zarr manifest and its catalog."""
    bucket, key = parse_s3(location)
    nwb_prefixes = find_nwb_prefixes(client, bucket, key)
    if not nwb_prefixes:
        return None

    refs: dict[str, object] = {
        ".zgroup": _inline({"zarr_format": 2}),
        "sessions/.zgroup": _inline({"zarr_format": 2}),
    }
    sources = []
    used_groups: set[str] = set()
    for nwb_prefix in nwb_prefixes:
        experiment = _experiment_name(nwb_prefix)
        group_path = f"sessions/{re.sub(r'[^A-Za-z0-9_.-]+', '_', experiment)}"
        suffix = 2
        while group_path in used_groups:
            group_path = f"sessions/{re.sub(r'[^A-Za-z0-9_.-]+', '_', experiment)}-{suffix}"
            suffix += 1
        used_groups.add(group_path)
        source_refs: dict[str, object] = {}
        source = _add_source_refs(client, source_refs, bucket, nwb_prefix, group_path)
        if source is None:
            continue
        refs.update(source_refs)
        sources.append(source)

    if not sources:
        return None

    generated_at = datetime.now(timezone.utc).isoformat()
    metadata = {
        "format": "fsspec-reference-zarr-v1",
        "cache_table": _table_name(),
        "cache_version": BDC_VERSION,
        "generated_at": generated_at,
        "asset_name": asset_name,
        "source_store_count": len(sources),
        "spike_arrays": list(_SPIKE_ARRAYS),
        "unit_arrays": list(_UNIT_ARRAYS),
        "sources": sources,
    }
    manifest = {"version": 1, "refs": refs, "metadata": metadata}
    return manifest, {"metadata": metadata}


def _fetch_asset_ecephys_virtual(asset_name: str, location: str | None = None) -> str | None:
    """Build and publish one asset's virtual-Zarr manifest."""
    setup_logging()
    if _manifest_exists(asset_name):
        if not registry.BACKEND.partition_exists(_index_key(asset_name)):
            _write_index(asset_name)
        _log(f"Manifest already exists for asset {asset_name}, skipping")
        return _manifest_location(asset_name)

    if location is None:
        basics = asset_basics()
        asset = basics[basics["name"] == asset_name]
        if asset.empty:
            _log(f"Asset {asset_name} not found in asset_basics")
            return None
        location = asset.iloc[0]["location"]
    if not location:
        _log(f"No location for asset {asset_name}")
        return None

    _log(f"Updating virtual-Zarr cache for asset {asset_name}")
    client = boto3.client("s3", config=Config(max_pool_connections=_MAX_WORKERS))
    result = build_virtual_manifest(client, asset_name, location)
    if result is None:
        _log(f"No eligible NWB Zarr stores found for asset {asset_name}")
        return None

    manifest, catalog = result
    registry.BACKEND.put_json(_manifest_key(asset_name), json.dumps(manifest, separators=(",", ":")))
    registry.BACKEND.put_json(_catalog_key(asset_name), json.dumps(catalog, indent=2))
    _write_index(asset_name)
    _log(f"Published virtual-Zarr cache for asset {asset_name}")
    return _manifest_location(asset_name)


@registry.register_table(registry.NAMES["ecephys_virtual"])
def platform_ecephys_virtual(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> str:
    """Return or publish the versioned virtual-Zarr manifest URL for one asset.

    This is a manifest-only cache, so it intentionally returns a URL rather
    than a pandas DataFrame.  ``lazy`` is accepted to match other cache helper
    signatures and is equivalent to the default behavior.
    """
    del lazy
    if force_update:
        manifest_url = _fetch_asset_ecephys_virtual(asset_name, location=location)
        if manifest_url is None:
            raise ValueError(f"No virtual-Zarr source found for asset {asset_name}")
        return manifest_url
    if not _manifest_exists(asset_name):
        raise ValueError(f"Virtual-Zarr cache is empty for asset {asset_name}. Use force_update=True to build it.")
    return _manifest_location(asset_name)


def platform_ecephys_virtual_columns() -> list[Column]:
    """Return registry columns for the manifest-only virtual ecephys table."""
    return [
        Column(name="asset_name", description="Derived ecephys asset owning the manifest"),
        Column(name="manifest_url", description="Versioned fsspec-reference-Zarr manifest URL"),
        Column(name="catalog_url", description="Versioned manifest catalog URL"),
    ]

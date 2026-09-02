"""Shared discovery helpers for the virtual-Zarr spike-read experiment."""

from __future__ import annotations

import json
from dataclasses import dataclass

import boto3
import duckdb
from botocore.config import Config

SOURCE_BUCKET = "aind-open-data"
CACHE_BUCKET = "allen-data-views"
VIRTUAL_BUCKET = "aind-scratch-data"
VIRTUAL_PREFIX = "virtual-zarr-test"
CACHE_ROOT = "data-asset-cache"
SPIKE_TABLE = "platform_ecephys_spikes"
ZARR_SUFFIXES = (".nwb", ".nwb.zarr")


@dataclass(frozen=True)
class Asset:
    """One metadata asset selected for the experiment."""

    name: str
    location: str
    acquisition_start_time: str


@dataclass(frozen=True)
class SourceStore:
    """One NWB Zarr store contributing spike times to an asset."""

    asset_name: str
    source_uri: str
    group_path: str


def s3_uri(bucket: str, key: str) -> str:
    """Return an S3 URI without a trailing slash."""
    return f"s3://{bucket}/{key.rstrip('/')}"


def split_s3_uri(uri: str) -> tuple[str, str]:
    """Split ``s3://bucket/key`` into its bucket and key."""
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected an S3 URI, got {uri!r}")
    bucket, _, key = uri[5:].partition("/")
    if not bucket or not key:
        raise ValueError(f"Expected an S3 URI with a key, got {uri!r}")
    return bucket, key.rstrip("/")


def make_s3_client():
    """Create an S3 client with enough connections for the small fan-out here."""
    return boto3.client("s3", config=Config(max_pool_connections=64))


def _version_key(version: str) -> tuple[int, ...]:
    """Sort cache versions numerically, tolerating a leading ``bdc-v``."""
    number = version.removeprefix("bdc-v")
    return tuple(int(part) for part in number.split(".") if part.isdigit())


def resolve_cache_version(client, requested: str | None) -> str:
    """Resolve ``latest`` to the newest published biodata-cache version."""
    if requested and requested != "latest":
        return requested if requested.startswith("bdc-v") else f"bdc-v{requested}"

    response = client.get_object(Bucket=CACHE_BUCKET, Key=f"{CACHE_ROOT}/cache_versions.json")
    versions = json.loads(response["Body"].read().decode())
    versions = [version for version in versions if version.startswith("bdc-v")]
    if not versions:
        raise RuntimeError("No published biodata-cache versions were found")
    return max(versions, key=_version_key)


def load_ecephys_assets(client, subject_id: str, cache_version: str | None) -> tuple[str, list[Asset]]:
    """Load derived ecephys session assets for one subject from asset_basics."""
    version = resolve_cache_version(client, cache_version)
    basics_uri = s3_uri(CACHE_BUCKET, f"{CACHE_ROOT}/{version}/asset_basics.pqt")
    query = """
        SELECT name, location, acquisition_start_time
        FROM read_parquet(?)
        WHERE subject_id = ?
          AND data_level = 'derived'
          AND list_contains(modalities, 'ecephys')
          AND acquisition_start_time IS NOT NULL
        ORDER BY acquisition_start_time, name
    """
    with duckdb.connect() as connection:
        frame = connection.execute(query, [basics_uri, subject_id]).df()

    assets = [
        Asset(name=str(name), location=str(location), acquisition_start_time=str(started))
        for name, location, started in frame.itertuples(index=False, name=None)
    ]
    if not assets:
        raise RuntimeError(f"No derived ecephys assets found for subject {subject_id} in {version}")
    return version, assets


def _list_common_prefixes(client, bucket: str, prefix: str) -> list[str]:
    """List all immediate child prefixes under an S3 prefix."""
    paginator = client.get_paginator("list_objects_v2")
    prefixes: list[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        prefixes.extend(entry["Prefix"].rstrip("/") for entry in page.get("CommonPrefixes", []))
    return sorted(set(prefixes))


def find_nwb_prefixes(client, location: str) -> list[str]:
    """Find NWB/Zarr stores in an asset's conventional S3 layout."""
    bucket, key = split_s3_uri(location)
    prefixes = [
        prefix
        for prefix in _list_common_prefixes(client, bucket, f"{key}/nwb/")
        if prefix.endswith(ZARR_SUFFIXES)
    ]
    if prefixes:
        return prefixes
    return [
        prefix
        for prefix in _list_common_prefixes(client, bucket, f"{key}/")
        if prefix.endswith(ZARR_SUFFIXES)
    ]


def source_group_path(asset_name: str, nwb_prefix: str) -> str:
    """Choose a stable path for one source store in the virtual root."""
    recording_name = nwb_prefix.rstrip("/").rsplit("/", 1)[-1]
    return f"sessions/{asset_name}/{recording_name}"


def discover_sources(client, assets: list[Asset]) -> list[SourceStore]:
    """Return all source NWB stores for the selected assets."""
    sources: list[SourceStore] = []
    for asset in assets:
        bucket, _ = split_s3_uri(asset.location)
        for nwb_prefix in find_nwb_prefixes(client, asset.location):
            sources.append(
                SourceStore(
                    asset_name=asset.name,
                    source_uri=s3_uri(bucket, nwb_prefix),
                    group_path=source_group_path(asset.name, nwb_prefix),
                )
            )
    return sources


def list_cache_files(client, cache_version: str, asset_name: str) -> list[str]:
    """Return all parquet files in one published ecephys spike partition."""
    prefix = f"{CACHE_ROOT}/{cache_version}/{SPIKE_TABLE}/asset_name={asset_name}/"
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []
    for page in paginator.paginate(Bucket=CACHE_BUCKET, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []) if obj["Key"].endswith(".pqt"))
    return [s3_uri(CACHE_BUCKET, key) for key in sorted(keys)]


def fetch_json(client, uri: str) -> dict:
    """Fetch and decode one JSON object from S3."""
    bucket, key = split_s3_uri(uri)
    response = client.get_object(Bucket=bucket, Key=key)
    return json.loads(response["Body"].read().decode())

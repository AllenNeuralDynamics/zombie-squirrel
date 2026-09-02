"""Small shared helpers for reading NWB Zarr stores from S3."""

import json
import re
from concurrent.futures import ThreadPoolExecutor

_S3_URI_RE = re.compile(r"^s3://([^/]+)/(.+)$")
NWB_DIR_SUFFIXES = (".nwb.zarr", ".nwb")
DEFAULT_MAX_WORKERS = 32


def parse_s3(location: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into ``(bucket, key)``."""
    match = _S3_URI_RE.match(location)
    if match is None:
        raise ValueError(f"Not an S3 URI: {location}")
    return match.group(1), match.group(2).rstrip("/")


def list_nwb_dirs(client, bucket: str, prefix: str) -> list[str]:
    """Return NWB store directories directly below an S3 prefix."""
    response = client.list_objects_v2(Bucket=bucket, Prefix=prefix, Delimiter="/")
    directories = []
    for entry in response.get("CommonPrefixes", []):
        candidate = entry["Prefix"].rstrip("/")
        if candidate.endswith(NWB_DIR_SUFFIXES):
            directories.append(candidate)
    return directories


def find_nwb_prefixes(client, bucket: str, key: str) -> list[str]:
    """Return NWB store prefixes for one asset."""
    prefixes = list_nwb_dirs(client, bucket, f"{key}/nwb/")
    return prefixes or list_nwb_dirs(client, bucket, f"{key}/")


def load_zmetadata(
    client,
    bucket: str,
    nwb_prefix: str,
    *,
    required_paths: tuple[str, ...] = (),
    required_any_paths: tuple[str, ...] = (),
) -> tuple[bytes, dict] | None:
    """Read consolidated Zarr metadata when the requested paths are present."""
    body = client.get_object(Bucket=bucket, Key=f"{nwb_prefix}/.zmetadata")["Body"].read()
    if not body:
        return None
    try:
        metadata = json.loads(body).get("metadata", {})
    except json.JSONDecodeError:
        return None
    if required_paths and not all(path in metadata for path in required_paths):
        return None
    if required_any_paths and not any(path in metadata for path in required_any_paths):
        return None
    return body, metadata


def download_zarr_store(
    client,
    bucket: str,
    nwb_prefix: str,
    zmetadata: bytes,
    array_paths: list[str],
    *,
    max_workers: int = DEFAULT_MAX_WORKERS,
    skip_empty: bool = False,
) -> dict:
    """Download consolidated metadata and chunks for the requested Zarr arrays."""
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for path in array_paths:
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{nwb_prefix}/{path}/"):
            for obj in page.get("Contents", []):
                keys.append(obj["Key"])

    def fetch(s3_key: str) -> tuple[str, bytes]:
        """Download one object and return its NWB-relative key."""
        body = client.get_object(Bucket=bucket, Key=s3_key)["Body"].read()
        return s3_key[len(nwb_prefix) + 1 :], body

    store = {".zmetadata": zmetadata}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for relative_key, body in executor.map(fetch, keys):
            if skip_empty and not body:
                continue
            store[relative_key] = body
    return store

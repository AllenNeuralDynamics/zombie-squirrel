"""Build and publish a narrow, kerchunk-style virtual Zarr spike store.

The source assets are already Zarr stores, so this builder creates a standard
fsspec reference manifest directly rather than rewriting them. Each source NWB
is exposed at ``sessions/<asset>/<recording>/`` and only the consolidated
metadata plus ``units/spike_times`` objects are referenced.

Usage:
    source ~/.zshrc
    switch prod
    .venv/bin/python scripts/virtual-zarr-test/build_virtual_zarr.py
    .venv/bin/python scripts/virtual-zarr-test/build_virtual_zarr.py --subject-id 795133
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from common import (
    VIRTUAL_BUCKET,
    VIRTUAL_PREFIX,
    discover_sources,
    list_cache_files,
    load_ecephys_assets,
    make_s3_client,
    s3_uri,
    split_s3_uri,
)


def _object_ref(uri: str, _size: int) -> list[str]:
    """Represent one source Zarr object without copying its bytes."""
    return [uri]


def _get_object_with_size(client, bucket: str, key: str) -> tuple[bytes, int]:
    """Read an object and return its bytes and content length."""
    response = client.get_object(Bucket=bucket, Key=key)
    body = response["Body"].read()
    return body, int(response.get("ContentLength", len(body)))


def _root_object_sizes(client, bucket: str, prefix: str) -> dict[str, int]:
    """Return sizes of direct files at a Zarr store root."""
    response = client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/", Delimiter="/", MaxKeys=100)
    return {obj["Key"]: int(obj["Size"]) for obj in response.get("Contents", [])}


def _spike_chunk_objects(client, bucket: str, prefix: str) -> list[tuple[str, int]]:
    """Return metadata and chunk objects for one Zarr spike-times array."""
    array_prefix = f"{prefix}/units/spike_times/"
    paginator = client.get_paginator("list_objects_v2")
    objects: list[tuple[str, int]] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=array_prefix):
        objects.extend(
            (obj["Key"], int(obj["Size"]))
            for obj in page.get("Contents", [])
            if obj["Key"] != array_prefix
        )
    return sorted(objects)


def _add_source_refs(client, refs: dict[str, object], source) -> dict:
    """Add one source store's metadata and spike chunks to the manifest."""
    bucket, prefix = split_s3_uri(source.source_uri)
    zmetadata_key = f"{prefix}/.zmetadata"
    zmetadata, zmetadata_size = _get_object_with_size(client, bucket, zmetadata_key)
    parsed = json.loads(zmetadata)
    array_spec = parsed.get("metadata", {}).get("units/spike_times/.zarray")
    root_sizes = _root_object_sizes(client, bucket, prefix)

    metadata_uri = s3_uri(bucket, zmetadata_key)
    refs[f"{source.group_path}/.zmetadata"] = _object_ref(metadata_uri, zmetadata_size)
    for filename in (".zgroup", ".zattrs"):
        key = f"{prefix}/{filename}"
        if key in root_sizes:
            refs[f"{source.group_path}/{filename}"] = _object_ref(s3_uri(bucket, key), root_sizes[key])

    chunks = _spike_chunk_objects(client, bucket, prefix) if array_spec else []
    for key, size in chunks:
        relative = key[len(prefix) + 1 :]
        refs[f"{source.group_path}/{relative}"] = _object_ref(s3_uri(bucket, key), size)

    return {
        "asset_name": source.asset_name,
        "source_uri": source.source_uri,
        "group_path": source.group_path,
        "has_spike_times": array_spec is not None,
        "spike_shape": array_spec.get("shape") if array_spec else None,
        "spike_chunks": len(chunks),
    }


def build_manifest(client, subject_id: str, cache_version: str | None) -> tuple[str, str, dict, dict]:
    """Build the manifest and catalog objects for one subject."""
    resolved_version, assets = load_ecephys_assets(client, subject_id, cache_version)
    sources = discover_sources(client, assets)
    if not sources:
        raise RuntimeError(f"No NWB Zarr stores found for subject {subject_id}")

    refs: dict[str, object] = {
        ".zgroup": json.dumps({"zarr_format": 2}),
        "sessions/.zgroup": json.dumps({"zarr_format": 2}),
    }
    source_catalog = [_add_source_refs(client, refs, source) for source in sources]
    cache_files = {asset.name: list_cache_files(client, resolved_version, asset.name) for asset in assets}
    session_count = len({asset.acquisition_start_time for asset in assets})

    metadata = {
        "format": "fsspec-reference-zarr-v1",
        "subject_id": subject_id,
        "cache_version": resolved_version,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "asset_count": len(assets),
        "session_count": session_count,
        "source_store_count": len(sources),
        "array": "units/spike_times",
        "assets": [
            {
                "name": asset.name,
                "location": asset.location,
                "acquisition_start_time": asset.acquisition_start_time,
                "cache_files": cache_files[asset.name],
            }
            for asset in assets
        ],
        "sources": source_catalog,
    }
    manifest = {"version": 1, "refs": refs, "metadata": metadata}
    catalog = {"metadata": metadata}
    key_prefix = f"{VIRTUAL_PREFIX}/subject={subject_id}"
    manifest_uri = s3_uri(VIRTUAL_BUCKET, f"{key_prefix}/virtual-zarr.json")
    catalog_uri = s3_uri(VIRTUAL_BUCKET, f"{key_prefix}/catalog.json")
    return manifest_uri, catalog_uri, manifest, catalog


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject-id", default="795133")
    parser.add_argument("--cache-version", default="latest", help="Cache version such as 0.40 or bdc-v0.40")
    return parser.parse_args()


def main() -> None:
    """Build and upload the virtual Zarr manifest and its catalog."""
    args = parse_args()
    client = make_s3_client()
    manifest_uri, catalog_uri, manifest, catalog = build_manifest(client, args.subject_id, args.cache_version)

    manifest_bucket, manifest_key = split_s3_uri(manifest_uri)
    catalog_bucket, catalog_key = split_s3_uri(catalog_uri)
    client.put_object(
        Bucket=manifest_bucket,
        Key=manifest_key,
        Body=json.dumps(manifest, separators=(",", ":")).encode(),
        ContentType="application/json",
    )
    client.put_object(
        Bucket=catalog_bucket,
        Key=catalog_key,
        Body=json.dumps(catalog, indent=2).encode(),
        ContentType="application/json",
    )

    metadata = manifest["metadata"]
    assets = metadata["assets"]
    missing_cache = [asset["name"] for asset in assets if not asset["cache_files"]]
    missing_spikes = [source["group_path"] for source in metadata["sources"] if not source["has_spike_times"]]
    print(
        json.dumps(
            {
                "subject_id": args.subject_id,
                "cache_version": metadata["cache_version"],
                "assets": metadata["asset_count"],
                "sessions": metadata["session_count"],
                "source_stores": metadata["source_store_count"],
                "virtual_zarr": manifest_uri,
                "catalog": catalog_uri,
                "manifest_references": len(manifest["refs"]),
                "assets_missing_cache_partition": missing_cache,
                "sources_missing_spike_times": missing_spikes,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()

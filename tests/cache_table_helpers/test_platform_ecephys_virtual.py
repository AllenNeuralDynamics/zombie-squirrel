"""Tests for the manifest-only virtual ecephys cache table."""

import json
from unittest.mock import MagicMock, patch

import biodata_cache.cache_table_helpers.platform_ecephys_virtual as virtual


class _Paginator:
    """Return one synthetic chunk for every requested source array."""

    def paginate(self, *, Bucket, Prefix):  # noqa: N803 - mirrors boto3's API
        del Bucket
        yield {"Contents": [{"Key": f"{Prefix}0", "Size": 128}]}


def _metadata():
    """Return the minimum consolidated metadata for a viewer manifest."""
    metadata = {
        ".zgroup": {"zarr_format": 2},
        "units/.zgroup": {"zarr_format": 2},
    }
    for name in (*virtual._SPIKE_ARRAYS, "unit_name", "device_name", "depth", "extremum_channel_index", "waveform_mean"):
        shape = [3]
        if name == "spike_times":
            shape = [6]
        elif name == "spike_times_index":
            shape = [3]
        elif name == "waveform_mean":
            shape = [3, 4, 2]
        metadata[f"units/{name}/.zarray"] = {
            "zarr_format": 2,
            "shape": shape,
            "chunks": shape,
            "dtype": "<f8",
            "compressor": None,
        }
    return metadata


def test_build_virtual_manifest_references_only_selected_arrays():
    client = MagicMock()
    client.get_paginator.return_value = _Paginator()
    metadata = _metadata()

    with (
        patch.object(virtual, "find_nwb_prefixes", return_value=["asset/nwb/experiment1_recording1.nwb.zarr"]),
        patch.object(virtual, "load_zmetadata", return_value=(b"{}", metadata)),
    ):
        manifest, catalog = virtual.build_virtual_manifest(
            client,
            "ecephys_asset",
            "s3://aind-open-data/asset",
        )

    source = catalog["metadata"]["sources"][0]
    assert manifest["version"] == 1
    assert source["experiment"] == "experiment1_recording1"
    assert source["unit_count"] == 3
    assert "sessions/experiment1_recording1/units/spike_times/.zarray" in manifest["refs"]
    assert "sessions/experiment1_recording1/units/waveform_mean/0" in manifest["refs"]
    assert manifest["refs"]["sessions/experiment1_recording1/units/spike_times/0"] == [
        "s3://aind-open-data/asset/nwb/experiment1_recording1.nwb.zarr/units/spike_times/0",
        0,
        128,
    ]
    assert source["arrays"]["depth"]["shape"] == [3]
    assert json.loads(manifest["refs"]["sessions/experiment1_recording1/.zmetadata"])["metadata"]


def test_build_virtual_manifest_skips_sources_without_unit_identity():
    client = MagicMock()
    client.get_paginator.return_value = _Paginator()
    metadata = _metadata()
    metadata.pop("units/unit_name/.zarray")

    with (
        patch.object(virtual, "find_nwb_prefixes", return_value=["asset/nwb/session.nwb.zarr"]),
        patch.object(virtual, "load_zmetadata", return_value=(b"{}", metadata)),
    ):
        result = virtual.build_virtual_manifest(client, "asset", "s3://bucket/asset")

    assert result is None


def test_build_virtual_manifest_accepts_nwb_unit_id_fallback():
    client = MagicMock()
    client.get_paginator.return_value = _Paginator()
    metadata = _metadata()
    unit_name_spec = metadata.pop("units/unit_name/.zarray")
    metadata["units/id/.zarray"] = {**unit_name_spec, "dtype": "<i8"}

    with (
        patch.object(virtual, "find_nwb_prefixes", return_value=["asset/nwb/session.nwb.zarr"]),
        patch.object(virtual, "load_zmetadata", return_value=(b"{}", metadata)),
    ):
        manifest, catalog = virtual.build_virtual_manifest(client, "asset", "s3://bucket/asset")

    assert manifest["metadata"]["source_store_count"] == 1
    assert catalog["metadata"]["sources"][0]["unit_count"] == 3


def test_fetch_publishes_manifest_and_catalog():
    backend = MagicMock()
    backend.get_location.side_effect = lambda name, partitioned=False: (
        f"s3://bucket/data-asset-cache/bdc-v0.42/{name}/" if partitioned else "s3://bucket/path"
    )
    built = (
        {"version": 1, "refs": {".zgroup": '{"zarr_format":2}'}, "metadata": {}},
        {"metadata": {}},
    )
    with (
        patch.object(virtual.registry, "BACKEND", backend),
        patch.object(virtual, "build_virtual_manifest", return_value=built),
        patch.object(virtual.boto3, "client", return_value=MagicMock()),
    ):
        result = virtual._fetch_asset_ecephys_virtual("asset", "s3://bucket/source")

    assert result.endswith("platform_ecephys_virtual/asset_name=asset/virtual-zarr.json")
    assert backend.put_json.call_count == 2
    backend.write.assert_called_once()
    assert backend.write.call_args.args[0] == "platform_ecephys_virtual_index/asset"
    keys = {call.args[0] for call in backend.put_json.call_args_list}
    assert keys == {
        "platform_ecephys_virtual/asset_name=asset/virtual-zarr.json",
        "platform_ecephys_virtual/asset_name=asset/catalog.json",
    }

"""Unit tests for shared NWB Zarr access helpers."""

import json
from unittest.mock import MagicMock

import pytest

from biodata_cache.cache_table_helpers.shared.nwb_zarr import (
    download_zarr_store,
    find_nwb_prefixes,
    load_zmetadata,
    parse_s3,
)


def test_parse_s3():
    assert parse_s3("s3://bucket/path/to/asset/") == ("bucket", "path/to/asset")


def test_parse_s3_rejects_non_s3_uri():
    with pytest.raises(ValueError, match="Not an S3 URI"):
        parse_s3("/local/path")


def test_find_nwb_prefixes_prefers_nwb_directory():
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {"CommonPrefixes": [{"Prefix": "asset/nwb/session.nwb.zarr/"}]},
    ]
    assert find_nwb_prefixes(client, "bucket", "asset") == ["asset/nwb/session.nwb.zarr"]
    client.list_objects_v2.assert_called_once_with(Bucket="bucket", Prefix="asset/nwb/", Delimiter="/")


def test_load_zmetadata_checks_requested_paths():
    client = MagicMock()
    body = json.dumps({"metadata": {"units/id/.zarray": {}}}).encode()
    client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=body))}

    assert load_zmetadata(client, "bucket", "asset", required_paths=("units/id/.zarray",)) == (
        body,
        {"units/id/.zarray": {}},
    )
    assert load_zmetadata(client, "bucket", "asset", required_paths=("units/missing/.zarray",)) is None


def test_download_zarr_store_returns_relative_keys():
    client = MagicMock()
    client.get_paginator.return_value.paginate.return_value = [
        {"Contents": [{"Key": "asset/units/id/0"}]},
    ]
    client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=b"chunk"))}

    store = download_zarr_store(client, "bucket", "asset", b"metadata", ["units/id"])

    assert store == {".zmetadata": b"metadata", "units/id/0": b"chunk"}

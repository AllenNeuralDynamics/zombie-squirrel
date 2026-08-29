"""Unit tests for platform_ecephys_spikes cache table."""

import json
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from biodata_cache.cache_table_helpers.platform_ecephys_spikes import (
    _download_units_store,
    _experiment_name,
    _extract_spikes,
    _fetch_asset_ecephys_spikes,
    _find_nwb_prefixes,
    _load_units_metadata,
    _open_units_group,
    _parse_s3,
    platform_ecephys_spikes,
    platform_ecephys_spikes_columns,
)


class _FakeUnits:
    def __init__(self, arrays):
        self._arrays = arrays

    def __contains__(self, key):
        return key in self._arrays

    def __getitem__(self, key):
        return _FakeArray(self._arrays[key])


class _FakeArray:
    def __init__(self, data):
        self._data = np.array(data, dtype=object) if data and isinstance(data[0], str) else np.array(data)

    def __getitem__(self, key):
        return self._data[key]


def _basics_df():
    return pd.DataFrame(
        {
            "subject_id": ["841364", "841364", "999999"],
            "modalities": [["ecephys"], ["ecephys"], ["ecephys"]],
            "data_level": ["derived", "raw", "derived"],
            "location": ["s3://bucket/abc", "s3://bucket/raw", "s3://bucket/other"],
            "name": ["asset_derived", "asset_raw", "asset_other"],
        }
    )


def test_parse_s3_valid():
    assert _parse_s3("s3://bucket/a/b/") == ("bucket", "a/b")


def test_parse_s3_invalid_raises():
    with pytest.raises(ValueError, match="Not an S3 URI"):
        _parse_s3("/local/path")


def test_find_nwb_prefixes_returns_all_nwb_dirs():
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "CommonPrefixes": [
            {"Prefix": "k/nwb/exp1.nwb/"},
            {"Prefix": "k/nwb/other/"},
            {"Prefix": "k/nwb/exp2.nwb/"},
        ]
    }
    assert _find_nwb_prefixes(client, "bucket", "k") == ["k/nwb/exp1.nwb", "k/nwb/exp2.nwb"]


def test_find_nwb_prefixes_none():
    client = MagicMock()
    client.list_objects_v2.return_value = {}
    assert _find_nwb_prefixes(client, "bucket", "k") == []


def test_find_nwb_prefixes_falls_back_to_asset_root_for_zarr_layout():
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {},  # no <key>/nwb/ subfolder
        {"CommonPrefixes": [{"Prefix": "k/662892_2023-08-24.nwb.zarr/"}, {"Prefix": "k/original_metadata/"}]},
    ]
    assert _find_nwb_prefixes(client, "bucket", "k") == ["k/662892_2023-08-24.nwb.zarr"]


def test_find_nwb_prefixes_prefers_nwb_subfolder_when_present():
    client = MagicMock()
    client.list_objects_v2.return_value = {"CommonPrefixes": [{"Prefix": "k/nwb/exp1.nwb/"}]}
    assert _find_nwb_prefixes(client, "bucket", "k") == ["k/nwb/exp1.nwb"]
    assert client.list_objects_v2.call_count == 1


def test_experiment_name_parses_tag():
    assert _experiment_name("k/nwb/ecephys_1_2_experiment2_recording1.nwb") == "experiment2_recording1"


def test_experiment_name_falls_back_to_stem():
    assert _experiment_name("k/nwb/weird_name.nwb") == "weird_name"


def test_experiment_name_strips_zarr_suffix_for_flat_layout():
    assert _experiment_name("k/662892_2023-08-24.nwb.zarr") == "662892_2023-08-24"


def test_load_units_metadata_present():
    client = MagicMock()
    meta = {"metadata": {"units/spike_times/.zarray": {}, "units/id/.zarray": {}}}
    body = json.dumps(meta).encode()
    client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=body))}
    result = _load_units_metadata(client, "bucket", "nwbpfx")
    assert result is not None
    raw, parsed = result
    assert raw == body
    assert "units/spike_times/.zarray" in parsed


def test_load_units_metadata_absent_returns_none():
    client = MagicMock()
    body = json.dumps({"metadata": {"acquisition/.zgroup": {}}}).encode()
    client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=body))}
    assert _load_units_metadata(client, "bucket", "nwbpfx") is None


def test_download_units_store():
    client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "nwbpfx/units/spike_times/0"}]},
        {},
    ]
    client.get_paginator.return_value = paginator
    client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=b"chunk"))}

    store = _download_units_store(client, "bucket", "nwbpfx", b"ZMETA", ["spike_times"])

    assert store[".zmetadata"] == b"ZMETA"
    assert store["units/spike_times/0"] == b"chunk"


def test_extract_spikes_builds_long_form():
    units = _FakeUnits(
        {
            "spike_times": [0.1, 0.2, 0.3, 1.0, 2.0],
            "spike_times_index": [3, 5],
            "unit_name": ["u0", "u1"],
            "device_name": ["Probe A", "Probe A"],
        }
    )
    df = pd.concat(_extract_spikes(units, "experiment1_recording1"), ignore_index=True)

    assert list(df.columns) == ["experiment", "device_name", "unit_name", "spike_time"]
    assert len(df) == 5
    assert list(df[df["unit_name"] == "u0"]["spike_time"]) == [0.1, 0.2, 0.3]
    assert list(df[df["unit_name"] == "u1"]["spike_time"]) == [1.0, 2.0]
    assert set(df["experiment"]) == {"experiment1_recording1"}


def test_extract_spikes_falls_back_to_unit_id_when_no_unit_name():
    """A directly-exported NWB has unit_id but no unit_name UUID - the output
    column is still called unit_name, matching platform_ecephys_units' own alias."""
    units = _FakeUnits(
        {
            "spike_times": [0.1, 0.2, 0.3, 1.0, 2.0],
            "spike_times_index": [3, 5],
            "unit_id": ["662892_2023-08-24_A-1", "662892_2023-08-24_A-2"],
            "device_name": ["Probe A", "Probe A"],
        }
    )
    df = pd.concat(_extract_spikes(units, "experiment1_recording1"), ignore_index=True)

    assert list(df.columns) == ["experiment", "device_name", "unit_name", "spike_time"]
    assert set(df["unit_name"]) == {"662892_2023-08-24_A-1", "662892_2023-08-24_A-2"}


def test_extract_spikes_missing_both_identifiers_defaults_empty():
    units = _FakeUnits({"spike_times": [0.1, 0.2], "spike_times_index": [2]})
    df = pd.concat(_extract_spikes(units, "exp"), ignore_index=True)
    assert set(df["unit_name"]) == {""}


def test_extract_spikes_missing_device_name_defaults_empty():
    units = _FakeUnits(
        {
            "spike_times": [0.1, 0.2],
            "spike_times_index": [2],
            "unit_name": ["u0"],
        }
    )
    df = pd.concat(_extract_spikes(units, "exp"), ignore_index=True)
    assert set(df["device_name"]) == {""}


def test_extract_spikes_empty_index_returns_empty():
    units = _FakeUnits({"spike_times": [], "spike_times_index": [], "unit_name": []})
    assert list(_extract_spikes(units, "exp")) == []


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._download_units_store")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._load_units_metadata")
@patch("zarr.open_consolidated")
def test_open_units_group_found(mock_open, mock_load, mock_download):
    mock_load.return_value = (b"ZMETA", {"units/spike_times/.zarray": {}, "units/device_name/.zarray": {}})
    mock_download.return_value = {".zmetadata": b"ZMETA"}
    mock_open.return_value = {"units": "UNITS"}

    result = _open_units_group(MagicMock(), "bucket", "nwbpfx")

    assert result == "UNITS"
    assert set(mock_download.call_args[0][4]) <= {"spike_times", "spike_times_index", "unit_name", "device_name"}


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._load_units_metadata")
def test_open_units_group_no_units_returns_none(mock_load):
    mock_load.return_value = None
    assert _open_units_group(MagicMock(), "bucket", "nwbpfx") is None


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.boto3")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._find_nwb_prefixes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._open_units_group")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._extract_spikes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.registry")
def test_fetch_asset_concatenates_multiple_nwbs(
    mock_registry, mock_extract, mock_open, mock_find, mock_boto3
):
    mock_registry.NAMES = {"ecephys_spikes": "platform_ecephys_spikes"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = False
    mock_find.return_value = ["k/nwb/experiment1_recording1.nwb", "k/nwb/experiment2_recording1.nwb"]
    mock_open.side_effect = ["UNITS1", None]
    mock_extract.return_value = [
        pd.DataFrame(
            {"experiment": ["e"], "device_name": ["Probe A"], "unit_name": ["u0"], "spike_time": [0.1]}
        )
    ]

    result = _fetch_asset_ecephys_spikes("asset_derived", location="s3://bucket/abc")

    mock_registry.BACKEND.write_chunk.assert_called_once()
    assert mock_registry.BACKEND.write_chunk.call_args[0][0] == "platform_ecephys_spikes/asset_derived"
    assert mock_extract.call_count == 1
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.boto3")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._find_nwb_prefixes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.asset_basics")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.registry")
def test_fetch_asset_looks_up_location_when_missing(mock_registry, mock_basics, mock_find, mock_boto3):
    mock_registry.NAMES = {"ecephys_spikes": "platform_ecephys_spikes"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = False
    mock_basics.return_value = _basics_df()
    mock_find.return_value = []

    result = _fetch_asset_ecephys_spikes("asset_derived")

    mock_basics.assert_called_once()
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.asset_basics")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.registry")
def test_fetch_asset_skips_unknown_asset(mock_registry, mock_basics):
    mock_registry.NAMES = {"ecephys_spikes": "platform_ecephys_spikes"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = False
    mock_basics.return_value = _basics_df()

    result = _fetch_asset_ecephys_spikes("missing_asset")

    mock_registry.BACKEND.write.assert_not_called()
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.boto3")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._find_nwb_prefixes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._open_units_group")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.registry")
def test_fetch_asset_no_units_writes_nothing(mock_registry, mock_open, mock_find, mock_boto3):
    mock_registry.NAMES = {"ecephys_spikes": "platform_ecephys_spikes"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = False
    mock_find.return_value = ["k/nwb/experiment1_recording1.nwb"]
    mock_open.return_value = None

    result = _fetch_asset_ecephys_spikes("asset_derived", location="s3://bucket/abc")

    mock_registry.BACKEND.write.assert_not_called()
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.boto3")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._find_nwb_prefixes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.registry")
def test_fetch_asset_skips_existing_partition(mock_registry, mock_find, mock_boto3):
    mock_registry.NAMES = {"ecephys_spikes": "platform_ecephys_spikes"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = True

    result = _fetch_asset_ecephys_spikes("asset_derived", location="s3://bucket/abc")

    mock_registry.BACKEND.partition_exists.assert_called_once_with("platform_ecephys_spikes/asset_derived")
    mock_registry.BACKEND.clear_partition.assert_not_called()
    mock_registry.BACKEND.write.assert_not_called()
    mock_find.assert_not_called()
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.registry")
def test_platform_ecephys_spikes_reads_from_cache(mock_registry):
    mock_registry.NAMES = {"ecephys_spikes": "platform_ecephys_spikes"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.read.return_value = pd.DataFrame({"spike_time": [1.0]})

    result = platform_ecephys_spikes(asset_name="asset_derived")

    mock_registry.BACKEND.read.assert_called_once_with("platform_ecephys_spikes/asset_derived")
    assert not result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.registry")
def test_platform_ecephys_spikes_raises_on_empty_without_force(mock_registry):
    mock_registry.NAMES = {"ecephys_spikes": "platform_ecephys_spikes"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.read.return_value = pd.DataFrame()

    with pytest.raises(ValueError, match="Cache is empty"):
        platform_ecephys_spikes(asset_name="asset_derived")


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._fetch_asset_ecephys_spikes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.registry")
def test_platform_ecephys_spikes_force_update_fetches(mock_registry, mock_fetch):
    mock_registry.NAMES = {"ecephys_spikes": "platform_ecephys_spikes"}
    mock_registry.BACKEND = MagicMock()
    mock_fetch.return_value = pd.DataFrame()

    platform_ecephys_spikes(asset_name="asset_derived", force_update=True, location="s3://bucket/abc")

    mock_fetch.assert_called_once_with("asset_derived", location="s3://bucket/abc")


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._fetch_asset_ecephys_spikes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.registry")
def test_platform_ecephys_spikes_lazy_returns_location(mock_registry, mock_fetch):
    mock_registry.NAMES = {"ecephys_spikes": "platform_ecephys_spikes"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.get_location.return_value = "s3://loc/data.pqt"

    result = platform_ecephys_spikes(asset_name="asset_derived", lazy=True)

    mock_fetch.assert_not_called()
    mock_registry.BACKEND.get_location.assert_called_once_with("platform_ecephys_spikes/asset_derived")
    assert result == "s3://loc/data.pqt"


@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes._fetch_asset_ecephys_spikes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_spikes.registry")
def test_platform_ecephys_spikes_lazy_force_update_fetches(mock_registry, mock_fetch):
    mock_registry.NAMES = {"ecephys_spikes": "platform_ecephys_spikes"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.get_location.return_value = "s3://loc/data.pqt"

    platform_ecephys_spikes(asset_name="asset_derived", lazy=True, force_update=True, location="s3://bucket/abc")

    mock_fetch.assert_called_once_with("asset_derived", location="s3://bucket/abc")


def test_columns_definition():
    cols = platform_ecephys_spikes_columns()
    names = [c.name for c in cols]
    assert names == ["experiment", "device_name", "unit_name", "spike_time"]

"""Unit tests for platform_ecephys_units cache table."""

import json
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from biodata_cache.cache_table_helpers.platform_ecephys_units import (
    _download_units_store,
    _experiment_name,
    _extract_units,
    _extremum_waveforms,
    _fetch_asset_ecephys_units,
    _find_nwb_prefixes,
    _load_units_metadata,
    _open_units_group,
    _parse_s3,
    _scalar_columns,
    platform_ecephys_units,
    platform_ecephys_units_columns,
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
        arr = np.array(data)
        self._data = arr

    def __getitem__(self, key):
        return self._data[key]


def _metadata(n_units=2, extra=None):
    meta = {
        "units/.zgroup": {},
        "units/id/.zarray": {"shape": [n_units]},
        "units/unit_name/.zarray": {"shape": [n_units]},
        "units/device_name/.zarray": {"shape": [n_units]},
        "units/snr/.zarray": {"shape": [n_units]},
        "units/spike_times/.zarray": {"shape": [500]},
        "units/spike_times_index/.zarray": {"shape": [n_units]},
        "units/electrodes/.zarray": {"shape": [1000]},
        "units/electrodes_index/.zarray": {"shape": [n_units]},
        "units/waveform_mean/.zarray": {"shape": [n_units, 40, 8]},
        "units/waveform_sd/.zarray": {"shape": [n_units, 40, 8]},
    }
    if extra:
        meta.update(extra)
    return meta


def _basics_df():
    return pd.DataFrame(
        {
            "modalities": [["ecephys"], ["ecephys"]],
            "data_level": ["derived", "raw"],
            "location": ["s3://bucket/abc", "s3://bucket/raw"],
            "name": ["asset_derived", "asset_raw"],
        }
    )


def test_parse_s3_valid():
    assert _parse_s3("s3://bucket/a/b/") == ("bucket", "a/b")


def test_parse_s3_invalid_raises():
    with pytest.raises(ValueError, match="Not an S3 URI"):
        _parse_s3("nope")


def test_find_nwb_prefixes_returns_all():
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "CommonPrefixes": [{"Prefix": "k/nwb/a.nwb/"}, {"Prefix": "k/nwb/x/"}, {"Prefix": "k/nwb/b.nwb/"}]
    }
    assert _find_nwb_prefixes(client, "bucket", "k") == ["k/nwb/a.nwb", "k/nwb/b.nwb"]


def test_find_nwb_prefixes_falls_back_to_asset_root_for_zarr_layout():
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {},  # no <key>/nwb/ subfolder
        {"CommonPrefixes": [{"Prefix": "k/662892_2023-08-24.nwb.zarr/"}, {"Prefix": "k/original_metadata/"}]},
    ]
    assert _find_nwb_prefixes(client, "bucket", "k") == ["k/662892_2023-08-24.nwb.zarr"]


def test_find_nwb_prefixes_prefers_nwb_subfolder_when_present():
    client = MagicMock()
    client.list_objects_v2.return_value = {"CommonPrefixes": [{"Prefix": "k/nwb/a.nwb/"}]}
    assert _find_nwb_prefixes(client, "bucket", "k") == ["k/nwb/a.nwb"]
    # Only the <key>/nwb/ prefix was queried; the root-fallback listing never ran.
    assert client.list_objects_v2.call_count == 1


def test_experiment_name_parses_and_falls_back():
    assert _experiment_name("k/nwb/e_1_experiment3_recording2.nwb") == "experiment3_recording2"
    assert _experiment_name("k/nwb/plain.nwb") == "plain"


def test_experiment_name_strips_zarr_suffix_for_flat_layout():
    assert _experiment_name("k/662892_2023-08-24.nwb.zarr") == "662892_2023-08-24"


def test_scalar_columns_selects_per_unit_arrays():
    cols, n = _scalar_columns(_metadata(n_units=2))
    assert n == 2
    assert set(cols) == {"id", "unit_name", "device_name", "snr"}
    assert "spike_times" not in cols
    assert "spike_times_index" not in cols
    assert "electrodes_index" not in cols
    assert "waveform_mean" not in cols


def test_scalar_columns_no_units_returns_empty():
    cols, n = _scalar_columns({"acquisition/.zgroup": {}})
    assert cols == []
    assert n is None


def test_load_units_metadata_present():
    client = MagicMock()
    body = json.dumps({"metadata": _metadata()}).encode()
    client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=body))}
    result = _load_units_metadata(client, "bucket", "pfx")
    assert result is not None
    assert result[0] == body


def test_load_units_metadata_absent():
    client = MagicMock()
    body = json.dumps({"metadata": {"acquisition/.zgroup": {}}}).encode()
    client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=body))}
    assert _load_units_metadata(client, "bucket", "pfx") is None


def test_download_units_store():
    client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": [{"Key": "pfx/units/snr/0"}]}]
    client.get_paginator.return_value = paginator
    client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=b"c"))}
    store = _download_units_store(client, "bucket", "pfx", b"ZM", ["snr"])
    assert store[".zmetadata"] == b"ZM"
    assert store["units/snr/0"] == b"c"


class _RaisingArray:
    """Stand-in for a units column whose zarr chunk fails to decode."""

    def __getitem__(self, key):
        raise ModuleNotFoundError("No module named 'hdmf_zarr'")


def test_extract_units_skips_column_that_fails_to_decode():
    class _UnitsWithUndecodableColumn(_FakeUnits):
        def __getitem__(self, key):
            if key == "electrode_group":
                return _RaisingArray()
            return super().__getitem__(key)

    raising_units = _UnitsWithUndecodableColumn({"id": [0, 1], "unit_name": ["u0", "u1"]})
    df = _extract_units(raising_units, ["id", "unit_name", "electrode_group"], "exp")

    assert "electrode_group" not in df.columns
    assert list(df["id"]) == [0, 1]
    assert list(df["unit_name"]) == ["u0", "u1"]


def test_extract_units_builds_rows_scalar_only():
    units = _FakeUnits(
        {
            "id": [0, 1],
            "unit_name": ["u0", "u1"],
            "device_name": ["Probe A", "Probe A"],
            "snr": [3.5, 4.5],
            "extremum_channel_index": [1, 2],
        }
    )
    scalar_cols = ["id", "unit_name", "device_name", "snr", "extremum_channel_index"]
    df = _extract_units(units, scalar_cols, "experiment1_recording1")

    assert len(df) == 2
    assert list(df.columns)[:3] == ["experiment", "device_name", "unit_name"]
    assert "waveform" not in df.columns
    assert set(df["experiment"]) == {"experiment1_recording1"}


def test_extremum_waveforms_extracts_peak_channel_in_bands():
    # 4 units, 3 samples, 5 channels; unit-chunk band of 2 -> two bands.
    cube = np.arange(4 * 3 * 5).reshape(4, 3, 5).astype(float)
    extremum_idx = np.array([1, 2, 3, 4])
    metadata = {"units/waveform_mean/.zarray": {"shape": [4, 3, 5], "chunks": [2, 3, 5]}}

    client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "pfx/units/waveform_mean/0.0.0"}, {"Key": "pfx/units/waveform_mean/1.0.0"}]}
    ]
    client.get_paginator.return_value = paginator
    client.get_object.return_value = {"Body": MagicMock(read=MagicMock(return_value=b"c"))}

    class _FakeWave:
        def __getitem__(self, key):
            return cube[key]

    with patch("zarr.open_consolidated", return_value={"units/waveform_mean": _FakeWave()}):
        out = _extremum_waveforms(client, "bucket", "pfx", b"ZM", metadata, extremum_idx)

    assert out.shape == (4, 3)
    assert out.dtype == np.float32
    for u in range(4):
        assert list(out[u]) == list(cube[u, :, extremum_idx[u]])


def test_extremum_waveforms_none_when_not_3d():
    metadata = {"units/waveform_mean/.zarray": {"shape": [4, 3], "chunks": [2, 3]}}
    assert _extremum_waveforms(MagicMock(), "b", "pfx", b"ZM", metadata, np.array([0])) is None


def test_extract_units_without_waveform_inputs():
    units = _FakeUnits({"id": [0], "unit_name": ["u0"], "snr": [2.0]})
    df = _extract_units(units, ["id", "unit_name", "snr"], "exp")
    assert "waveform" not in df.columns
    assert len(df) == 1

def test_extract_units_empty_returns_empty():
    assert _extract_units(_FakeUnits({}), [], "exp").empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._download_units_store")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._load_units_metadata")
@patch("zarr.open_consolidated")
def test_open_units_group_found_skips_waveform_download(mock_open, mock_load, mock_download):
    meta = _metadata(n_units=2)
    mock_load.return_value = (b"ZM", meta)
    mock_download.return_value = {".zmetadata": b"ZM"}
    mock_open.return_value = {"units": "UNITS"}

    units, cols, metadata, zmetadata = _open_units_group(MagicMock(), "bucket", "pfx")

    assert units == "UNITS"
    assert set(cols) == {"id", "unit_name", "device_name", "snr"}
    assert metadata is meta
    assert zmetadata == b"ZM"
    assert "waveform_mean" not in mock_download.call_args[0][4]


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._load_units_metadata")
def test_open_units_group_no_units(mock_load):
    mock_load.return_value = None
    assert _open_units_group(MagicMock(), "bucket", "pfx") is None


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.boto3")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._find_nwb_prefixes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._open_units_group")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._extract_units")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_fetch_asset_concatenates_and_writes(mock_registry, mock_extract, mock_open, mock_find, mock_boto3):
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = False
    mock_find.return_value = ["k/nwb/experiment1_recording1.nwb", "k/nwb/experiment2_recording1.nwb"]
    mock_open.side_effect = [("UNITS1", ["id", "unit_name"], {}, b"ZM"), None]
    mock_extract.return_value = pd.DataFrame(
        {"experiment": ["e"], "device_name": ["Probe A"], "unit_name": ["u0"], "snr": [3.0]}
    )

    result = _fetch_asset_ecephys_units("asset_derived", location="s3://bucket/abc")

    mock_registry.BACKEND.write.assert_called_once()
    assert mock_registry.BACKEND.write.call_args[0][0] == "platform_ecephys_units/asset_derived"
    assert mock_extract.call_count == 1
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.boto3")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._find_nwb_prefixes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._open_units_group")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._extract_units")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_fetch_asset_aliases_unit_id_when_no_unit_name(mock_registry, mock_extract, mock_open, mock_find, mock_boto3):
    """A directly-exported NWB has unit_id but no unit_name UUID - alias it so the
    written table's join key with platform_ecephys_spikes is always unit_name."""
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = False
    mock_find.return_value = ["k/662892_2023-08-24.nwb.zarr"]
    mock_open.return_value = ("UNITS1", ["id", "unit_id"], {}, b"ZM")
    mock_extract.return_value = pd.DataFrame(
        {"experiment": ["e"], "device_name": ["Probe A"], "unit_id": ["662892_2023-08-24_A-1"], "snr": [3.0]}
    )

    _fetch_asset_ecephys_units("asset_derived", location="s3://bucket/abc")

    written = mock_registry.BACKEND.write.call_args[0][1]
    assert list(written["unit_name"]) == ["662892_2023-08-24_A-1"]
    assert list(written["unit_id"]) == ["662892_2023-08-24_A-1"]


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.boto3")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._find_nwb_prefixes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.asset_basics")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_fetch_asset_looks_up_location(mock_registry, mock_basics, mock_find, mock_boto3):
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = False
    mock_basics.return_value = _basics_df()
    mock_find.return_value = []

    result = _fetch_asset_ecephys_units("asset_derived")

    mock_basics.assert_called_once()
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.asset_basics")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_fetch_asset_unknown_asset(mock_registry, mock_basics):
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = False
    mock_basics.return_value = _basics_df()

    result = _fetch_asset_ecephys_units("missing")

    mock_registry.BACKEND.write.assert_not_called()
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.boto3")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._find_nwb_prefixes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._open_units_group")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_fetch_asset_no_units_writes_nothing(mock_registry, mock_open, mock_find, mock_boto3):
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = False
    mock_find.return_value = ["k/nwb/experiment1_recording1.nwb"]
    mock_open.return_value = None

    result = _fetch_asset_ecephys_units("asset_derived", location="s3://bucket/abc")

    mock_registry.BACKEND.write.assert_not_called()
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.boto3")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._find_nwb_prefixes")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_fetch_asset_skips_existing_partition(mock_registry, mock_find, mock_boto3):
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_registry.BACKEND.partition_exists.return_value = True

    result = _fetch_asset_ecephys_units("asset_derived", location="s3://bucket/abc")

    mock_registry.BACKEND.partition_exists.assert_called_once_with("platform_ecephys_units/asset_derived")
    mock_registry.BACKEND.clear_partition.assert_not_called()
    mock_registry.BACKEND.write.assert_not_called()
    mock_find.assert_not_called()
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_platform_ecephys_units_reads_from_cache(mock_registry):
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.read.return_value = pd.DataFrame({"snr": [1.0]})

    result = platform_ecephys_units(asset_name="asset_derived")

    mock_registry.BACKEND.read.assert_called_once_with("platform_ecephys_units/asset_derived")
    assert not result.empty


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_platform_ecephys_units_raises_on_empty(mock_registry):
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.read.return_value = pd.DataFrame()

    with pytest.raises(ValueError, match="Cache is empty"):
        platform_ecephys_units(asset_name="asset_derived")


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._fetch_asset_ecephys_units")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_platform_ecephys_units_force_update(mock_registry, mock_fetch):
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_fetch.return_value = pd.DataFrame()

    platform_ecephys_units(asset_name="asset_derived", force_update=True, location="s3://bucket/abc")

    mock_fetch.assert_called_once_with("asset_derived", location="s3://bucket/abc")


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._fetch_asset_ecephys_units")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_platform_ecephys_units_lazy_returns_location(mock_registry, mock_fetch):
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.get_location.return_value = "s3://loc/data.pqt"

    result = platform_ecephys_units(asset_name="asset_derived", lazy=True)

    mock_fetch.assert_not_called()
    mock_registry.BACKEND.get_location.assert_called_once_with("platform_ecephys_units/asset_derived")
    assert result == "s3://loc/data.pqt"


@patch("biodata_cache.cache_table_helpers.platform_ecephys_units._fetch_asset_ecephys_units")
@patch("biodata_cache.cache_table_helpers.platform_ecephys_units.registry")
def test_platform_ecephys_units_lazy_force_update(mock_registry, mock_fetch):
    mock_registry.NAMES = {"ecephys_units": "platform_ecephys_units"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.get_location.return_value = "s3://loc/data.pqt"

    platform_ecephys_units(asset_name="asset_derived", lazy=True, force_update=True, location="s3://bucket/abc")

    mock_fetch.assert_called_once_with("asset_derived", location="s3://bucket/abc")


def test_columns_definition_has_join_keys_and_waveform():
    cols = platform_ecephys_units_columns()
    names = [c.name for c in cols]
    assert names[:3] == ["experiment", "device_name", "unit_name"]
    assert "waveform" in names
    assert "snr" in names

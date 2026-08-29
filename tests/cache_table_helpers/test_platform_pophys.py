"""Unit tests for platform_pophys cache table."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from biodata_cache.cache_table_helpers.platform_pophys import (
    _download_zarr_store,
    _extract_legacy_plane_rois,
    _extract_legacy_single_plane_rois,
    _extract_plane_rois,
    _fetch_asset_pophys,
    _find_nwb_prefix,
    _legacy_plane_names,
    _mask_contour,
    _parse_location_attr,
    _parse_s3,
    _plane_array_prefixes,
    _plane_names,
    _projection_png,
    platform_pophys,
    platform_pophys_columns,
)


class _FakeGroup(dict):
    def group_keys(self):
        return list(self.keys())


class _FakeOptophys(dict):
    def __init__(self, values):
        super().__init__(values)
        self.attrs = {}


class _FakeRoot:
    def __init__(self, processing, optophys=None):
        self._processing = processing
        self._optophys = optophys or {}

    def __getitem__(self, key):
        if key == "processing":
            return self._processing
        raise KeyError(key)

    def get(self, key):
        return self._optophys.get(key)


def _square_mask(n=12):
    mask = np.zeros((n, n), dtype="float32")
    mask[3:9, 3:9] = 1.0
    return mask


def _plane_dict(n_roi=2):
    masks = np.stack([_square_mask() for _ in range(n_roi)])
    roi_table = {
        "image_mask": masks,
        "id": np.array([10, 11][:n_roi], dtype="int64"),
        "is_soma": np.array([1, 0][:n_roi], dtype="int64"),
        "soma_probability": np.array([0.9, 0.2][:n_roi], dtype="float32"),
    }
    return {
        "image_segmentation": {"roi_table": roi_table},
        "images": {
            "max_projection": np.ones((12, 12), dtype="float32"),
            "average_projection": np.ones((12, 12), dtype="float32"),
        },
    }


def _fake_root():
    processing = _FakeGroup({"VISp_0": _plane_dict()})
    optophys = {
        "general/optophysiology/VISp_0": SimpleNamespace(
            attrs={"location": "Structure: VISp Depth: 160", "imaging_rate": 9.44, "grid_spacing": [0.78, 0.78]}
        )
    }
    return _FakeRoot(processing, optophys)


def _fake_legacy_root():
    pixels = np.array(
        [(3, 3, 1.0), (4, 3, 1.0), (5, 3, 1.0), (3, 4, 1.0), (4, 4, 1.0), (5, 4, 1.0)],
        dtype=[("x", "u4"), ("y", "u4"), ("weight", "f4")],
    )
    roi_table = {
        "id": np.array([42], dtype="int64"),
        "is_soma": np.array([True]),
        "pixel_mask": pixels,
        "pixel_mask_index": np.array([len(pixels)], dtype="uint32"),
    }
    images = {
        "max_projection_denoised_plane-0": np.ones((8, 8), dtype="float32"),
        "mean_projection_denoised_plane-0": np.ones((8, 8), dtype="float32"),
    }
    processing = _FakeGroup(
        {"plane-0": {"image_segmentation": {"roi_table": roi_table}, "images": images}}
    )
    optophys = {"general/optophysiology/plane-0": _FakeOptophys({
        "location": np.array(["242 um"]),
        "imaging_rate": np.array([6.0]),
        "grid_spacing": np.array([1.0, 1.0]),
    })}
    return _FakeRoot(processing, optophys)


def _fake_legacy_single_root():
    pixels = np.array(
        [(3, 3, 1.0), (4, 3, 1.0), (5, 3, 1.0), (3, 4, 1.0), (4, 4, 1.0), (5, 4, 1.0)],
        dtype=[("x", "u4"), ("y", "u4"), ("weight", "f4")],
    )
    return _FakeLegacySingleRoot(
        {
            "processing/ophys/ImageSegmentation/PlaneSegmentation": {
                "id": np.array([42], dtype="int64"),
                "pixel_mask": pixels,
                "pixel_mask_index": np.array([len(pixels)], dtype="uint16"),
            },
            "processing/ophys/SummaryImages": {
                "maximum_intensity_projection": np.ones((8, 8), dtype="float32"),
            },
        },
        {
            "general/optophysiology/ImagingPlane": _FakeOptophys(
                {
                    "location": np.array(["242 um"]),
                    "imaging_rate": np.array([6.0]),
                    "grid_spacing": np.array([1.0, 1.0]),
                }
            )
        },
    )


class _FakeLegacySingleRoot:
    def __init__(self, groups, optophys):
        self._groups = groups
        self._optophys = optophys

    def __getitem__(self, key):
        return self._groups[key]

    def get(self, key):
        return self._optophys.get(key) or self._groups.get(key)


def test_parse_s3_valid():
    assert _parse_s3("s3://bucket/a/b/") == ("bucket", "a/b")


def test_parse_s3_invalid_raises():
    with pytest.raises(ValueError, match="Not an S3 URI"):
        _parse_s3("/local/path")


def test_find_nwb_prefix_found():
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {"CommonPrefixes": [{"Prefix": "k/other/"}, {"Prefix": "k/pophys.nwb.zarr/"}]},
    ]
    assert _find_nwb_prefix(client, "bucket", "k") == "k/pophys.nwb.zarr"


def test_find_nwb_prefix_under_nwb_subfolder():
    client = MagicMock()
    client.list_objects_v2.side_effect = [
        {"CommonPrefixes": [{"Prefix": "k/other/"}]},
        {"CommonPrefixes": [{"Prefix": "k/nwb/session.nwb/"}]},
    ]
    assert _find_nwb_prefix(client, "bucket", "k") == "k/nwb/session.nwb"


def test_find_nwb_prefix_accepts_bci_behavior_nwb():
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "CommonPrefixes": [{"Prefix": "k/MOp2_3_0/"}, {"Prefix": "k/k_behavior_nwb/"}],
    }
    assert _find_nwb_prefix(client, "bucket", "k") == "k/k_behavior_nwb"


def test_find_nwb_prefix_not_found():
    client = MagicMock()
    client.list_objects_v2.return_value = {}
    assert _find_nwb_prefix(client, "bucket", "k") is None


def test_legacy_plane_names_from_metadata():
    metadata = {"processing/plane-0/image_segmentation/roi_table/pixel_mask/.zarray": {}}
    assert _legacy_plane_names(metadata) == ["plane-0"]


def test_legacy_plane_names_from_shared_table():
    metadata = {"processing/ophys/ImageSegmentation/PlaneSegmentation/pixel_mask/.zarray": {}}
    assert _legacy_plane_names(metadata) == ["ophys"]


def test_plane_names_from_metadata():
    metadata = {
        "processing/VISp_1/image_segmentation/roi_table/id/.zarray": {},
        "processing/VISp_0/image_segmentation/roi_table/id/.zarray": {},
        "processing/VISp_0/images/max_projection/.zarray": {},
    }
    assert _plane_names(metadata) == ["VISp_0", "VISp_1"]


def test_plane_array_prefixes():
    prefixes = _plane_array_prefixes("VISp_0")
    assert "processing/VISp_0/image_segmentation/roi_table/image_mask" in prefixes
    assert "processing/VISp_0/images/max_projection" in prefixes
    assert len(prefixes) == 6


def test_parse_location_attr():
    assert _parse_location_attr("Structure: VISp Depth: 160") == ("VISp", 160.0)


def test_parse_location_attr_empty():
    assert _parse_location_attr("") == (None, None)


def test_parse_location_attr_no_match():
    assert _parse_location_attr("nothing here") == (None, None)


def test_parse_location_attr_depth_only():
    assert _parse_location_attr("242 um") == (None, 242.0)


def test_mask_contour_returns_polygon():
    contour = _mask_contour(_square_mask() > 0)
    assert contour is not None
    assert all(len(pt) == 2 for pt in contour)


def test_mask_contour_empty_mask():
    assert _mask_contour(np.zeros((12, 12), dtype=bool)) is None


def test_projection_png_signature():
    png = _projection_png(np.linspace(0, 1, 144).reshape(12, 12).astype("float32"))
    assert png[:8] == b"\x89PNG\r\n\x1a\n"


def test_projection_png_all_nan():
    png = _projection_png(np.full((4, 4), np.nan, dtype="float32"))
    assert png[:8] == b"\x89PNG\r\n\x1a\n"


def test_download_zarr_store():
    client = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = [
        {"Contents": [{"Key": "nwbpfx/processing/VISp_0/image_segmentation/roi_table/id/0"}]},
        {},
    ]
    client.get_paginator.return_value = paginator
    body = MagicMock()
    body.read.return_value = b"bytes"
    client.get_object.return_value = {"Body": body}

    store = _download_zarr_store(client, "bucket", "nwbpfx", ["VISp_0"], b"META")

    assert store[".zmetadata"] == b"META"
    assert "processing/VISp_0/image_segmentation/roi_table/id/0" in store


@patch("biodata_cache.cache_table_helpers.platform_pophys.registry")
def test_extract_plane_rois_builds_records(mock_registry):
    mock_registry.BACKEND = MagicMock()
    rows = _extract_plane_rois(_fake_root(), "VISp_0", "asset_x", "raw_x")

    assert len(rows) == 2
    row = rows[0]
    assert row["asset_name"] == "asset_x"
    assert row["raw_name"] == "raw_x"
    assert row["plane"] == "VISp_0"
    assert row["structure"] == "VISp"
    assert row["depth_um"] == 160.0
    assert row["imaging_rate"] == pytest.approx(9.44)
    assert row["grid_spacing_um"] == "[0.78, 0.78]"
    assert row["roi_id"] == 10
    assert row["is_soma"] == 1
    assert row["area_px"] == 36
    assert isinstance(row["contour"], str)
    assert mock_registry.BACKEND.put_bytes.call_count == 2


@patch("biodata_cache.cache_table_helpers.platform_pophys.registry")
def test_extract_legacy_plane_rois_builds_records(mock_registry):
    mock_registry.BACKEND = MagicMock()
    rows = _extract_legacy_plane_rois(_fake_legacy_root(), "plane-0", "asset_x", "raw_x")

    assert len(rows) == 1
    row = rows[0]
    assert row["roi_id"] == 42
    assert row["is_soma"] == 1
    assert row["soma_probability"] is None
    assert row["depth_um"] == 242.0
    assert row["imaging_rate"] == 6.0
    assert row["grid_spacing_um"] == "[1.0, 1.0]"
    assert row["centroid_x"] == 4.0
    assert row["centroid_y"] == 3.5
    assert mock_registry.BACKEND.put_bytes.call_count == 2


@patch("biodata_cache.cache_table_helpers.platform_pophys.registry")
def test_extract_legacy_single_plane_rois_builds_records(mock_registry):
    mock_registry.BACKEND = MagicMock()
    rows = _extract_legacy_single_plane_rois(_fake_legacy_single_root(), "asset_x", "raw_x")

    assert len(rows) == 1
    row = rows[0]
    assert row["plane"] == "ophys"
    assert row["roi_id"] == 42
    assert row["is_soma"] == 0
    assert row["soma_probability"] is None
    assert row["depth_um"] == 242.0
    assert row["imaging_rate"] == 6.0
    assert row["grid_spacing_um"] == "[1.0, 1.0]"
    assert row["centroid_x"] == 4.0
    assert row["centroid_y"] == 3.5
    assert mock_registry.BACKEND.put_bytes.call_count == 1


@patch("biodata_cache.cache_table_helpers.platform_pophys.registry")
@patch("biodata_cache.cache_table_helpers.platform_pophys._open_nwb_zarr")
def test_fetch_asset_writes_partition(mock_open, mock_registry):
    mock_registry.NAMES = {"pophys": "platform_pophys"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_open.return_value = _fake_root()

    result = _fetch_asset_pophys("asset_x", location="s3://bucket/k", raw_name="raw_x")

    mock_open.assert_called_once_with("s3://bucket/k")
    mock_registry.BACKEND.write.assert_called_once()
    key, df = mock_registry.BACKEND.write.call_args[0]
    assert key == "platform_pophys/asset_x"
    assert list(df["roi_id"]) == [10, 11]
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_pophys.registry")
@patch("biodata_cache.cache_table_helpers.platform_pophys._open_nwb_zarr")
@patch("biodata_cache.cache_table_helpers.platform_pophys._open_legacy_nwb_zarr")
def test_fetch_asset_no_nwb(mock_legacy_open, mock_open, mock_registry):
    mock_registry.NAMES = {"pophys": "platform_pophys"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_open.return_value = None
    mock_legacy_open.return_value = None

    result = _fetch_asset_pophys("asset_x", location="s3://bucket/k")

    mock_registry.BACKEND.write.assert_not_called()
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_pophys.asset_basics")
@patch("biodata_cache.cache_table_helpers.platform_pophys.registry")
def test_fetch_asset_missing_location(mock_registry, mock_basics):
    mock_registry.NAMES = {"pophys": "platform_pophys"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.__class__.__name__ = "MemoryBackend"
    mock_basics.return_value = pd.DataFrame({"name": ["asset_x"], "location": [""]})

    result = _fetch_asset_pophys("asset_x")

    mock_registry.BACKEND.write.assert_not_called()
    assert result.empty


@patch("biodata_cache.cache_table_helpers.platform_pophys.registry")
def test_platform_pophys_lazy_returns_location(mock_registry):
    mock_registry.NAMES = {"pophys": "platform_pophys"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.get_location.return_value = "loc"

    assert platform_pophys("asset_x", lazy=True) == "loc"


@patch("biodata_cache.cache_table_helpers.platform_pophys.registry")
def test_platform_pophys_empty_cache_raises(mock_registry):
    mock_registry.NAMES = {"pophys": "platform_pophys"}
    mock_registry.BACKEND = MagicMock()
    mock_registry.BACKEND.read.return_value = pd.DataFrame()

    with pytest.raises(ValueError, match="Cache is empty"):
        platform_pophys("asset_x")


def test_platform_pophys_columns():
    cols = platform_pophys_columns()
    names = [c.name for c in cols]
    assert "roi_id" in names
    assert "contour" in names
    assert "asset_name" in names

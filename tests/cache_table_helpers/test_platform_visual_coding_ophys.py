"""Tests for the isolated Visual Coding Ophys cache extractor."""

from unittest.mock import MagicMock, patch

import numpy as np

from biodata_cache.cache_table_helpers.platform_visual_coding_ophys import (
    _extract_rois,
    _find_nwb_prefix,
    _mask_contour,
    _parse_location,
    _sparse_mask,
)


class _FakeRoot(dict):
    def get(self, key):
        return super().get(key)


def _root():
    pixels = np.array(
        [(3, 3, 1.0), (4, 3, 1.0), (5, 3, 1.0), (3, 4, 1.0), (4, 4, 1.0), (5, 4, 1.0)],
        dtype=[("x", "u4"), ("y", "u4"), ("weight", "f4")],
    )
    return _FakeRoot({
        "processing/ophys/ImageSegmentation/PlaneSegmentation": {
            "id": np.array([42], dtype="int64"),
            "global_roi_id": np.array([1042], dtype="int64"),
            "pixel_mask": pixels,
            "pixel_mask_index": np.array([len(pixels)], dtype="uint16"),
        },
        "general/optophysiology/ImagingPlane": {
            "location": np.array(["Structure: VISl, Depth: 275 um"]),
            "imaging_rate": np.array([30.0]),
            "grid_spacing": np.array([0.78, 0.78]),
        },
        "processing/ophys/SummaryImages": {
            "maximum_intensity_projection": np.ones((8, 8), dtype="float32"),
        },
    })


def test_find_nwb_prefix_prefers_zarr_root():
    client = MagicMock()
    client.list_objects_v2.return_value = {
        "CommonPrefixes": [
            {"Prefix": "asset/asset.nwb/"},
            {"Prefix": "asset/asset.nwb.zarr/"},
        ]
    }
    assert _find_nwb_prefix(client, "bucket", "asset") == "asset/asset.nwb.zarr"


def test_sparse_mask_and_location_helpers():
    sparse = _sparse_mask(_root()["processing/ophys/ImageSegmentation/PlaneSegmentation"]["pixel_mask"])
    assert sparse is not None
    assert sparse[0].sum() == 6
    assert _parse_location("Structure: VISl, Depth: 275 um") == ("VISl", 275.0)
    assert _mask_contour(sparse[0]) is not None


@patch("biodata_cache.cache_table_helpers.platform_visual_coding_ophys.registry")
def test_extract_rois_writes_visual_coding_projection(mock_registry):
    mock_registry.BACKEND = MagicMock()
    rows = _extract_rois(_root(), "asset_x")

    assert len(rows) == 1
    assert rows[0]["roi_id"] == 42
    assert rows[0]["global_roi_id"] == 1042
    assert rows[0]["plane"] == "ophys"
    assert rows[0]["structure"] == "VISl"
    assert rows[0]["depth_um"] == 275.0
    assert mock_registry.BACKEND.put_bytes.call_args.args[0] == "visual_coding_ophys_fov/asset_x/max.png"

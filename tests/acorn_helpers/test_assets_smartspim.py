"""Unit tests for assets_smartspim acorn."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.acorn_helpers.assets_smartspim import (
    _build_rows,
    _fetch_asset_metadata,
    _list_channels,
    _quantification_link,
    _segmentation_link,
    _stitched_link,
    assets_smartspim,
    assets_smartspim_columns,
)

LOCATION = "s3://aind-open-data/SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00"

EXAMPLE_RECORD = {
    "_id": "abc123",
    "name": "SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00",
    "location": LOCATION,
    "subject": {
        "subject_id": "123456",
        "subject_details": {"genotype": "wt/wt"},
    },
    "data_description": {
        "institution": {"abbreviation": "AIBS"},
    },
    "acquisition": {"acquisition_start_time": "2026-01-01T00:00:00"},
    "processing": {
        "data_processes": [
            {"end_date_time": "2026-01-01T10:00:00"},
            {"end_date_time": "2026-01-02T12:00:00"},
        ]
    },
}


class TestLinkHelpers(unittest.TestCase):
    def test_stitched_link(self):
        result = _stitched_link(LOCATION)
        self.assertEqual(result, f"https://neuroglancer-demo.appspot.com/#!{LOCATION}/neuroglancer_config.json")

    def test_segmentation_link(self):
        result = _segmentation_link(LOCATION, "Ex_561_Em_600")
        self.assertEqual(
            result,
            f"https://neuroglancer-demo.appspot.com/#!{LOCATION}/image_cell_segmentation/Ex_561_Em_600/visualization/neuroglancer_config.json",
        )

    def test_quantification_link(self):
        result = _quantification_link(LOCATION, "Ex_561_Em_600")
        self.assertEqual(
            result,
            f"https://neuroglancer-demo.appspot.com/#!{LOCATION}/image_cell_quantification/Ex_561_Em_600/visualization/neuroglancer_config.json",
        )


class TestListChannels(unittest.TestCase):
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.boto3.client")
    def test_returns_channel_names(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {
            "CommonPrefixes": [
                {"Prefix": "SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00/image_cell_segmentation/Ex_488_Em_525/"},
                {"Prefix": "SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00/image_cell_segmentation/Ex_561_Em_600/"},
            ]
        }

        result = _list_channels(LOCATION)

        self.assertEqual(result, ["Ex_488_Em_525", "Ex_561_Em_600"])
        mock_s3.list_objects_v2.assert_called_once_with(
            Bucket="aind-open-data",
            Prefix="SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00/image_cell_segmentation/",
            Delimiter="/",
        )

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.boto3.client")
    def test_returns_empty_when_no_prefixes(self, mock_boto_client):
        mock_s3 = MagicMock()
        mock_boto_client.return_value = mock_s3
        mock_s3.list_objects_v2.return_value = {}

        result = _list_channels(LOCATION)

        self.assertEqual(result, [])


class TestFetchAssetMetadata(unittest.TestCase):
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.MetadataDbClient")
    def test_returns_dict_keyed_by_name(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.retrieve_docdb_records.return_value = [EXAMPLE_RECORD]

        result = _fetch_asset_metadata(["SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00"])

        self.assertIn("SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00", result)
        self.assertEqual(result["SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00"]["_id"], "abc123")

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.MetadataDbClient")
    def test_passes_correct_filter(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.retrieve_docdb_records.return_value = []

        names = ["asset_a", "asset_b"]
        _fetch_asset_metadata(names)

        call_kwargs = mock_client.retrieve_docdb_records.call_args[1]
        self.assertEqual(call_kwargs["filter_query"], {"name": {"$in": names}})

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.MetadataDbClient")
    def test_batches_large_requests(self, mock_client_class):
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.retrieve_docdb_records.return_value = []

        names = [f"asset_{i}" for i in range(250)]
        _fetch_asset_metadata(names)

        self.assertEqual(mock_client.retrieve_docdb_records.call_count, 3)


class TestBuildRows(unittest.TestCase):
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim._list_channels")
    def test_one_row_per_asset_with_channel_columns(self, mock_list_channels):
        mock_list_channels.return_value = ["Ex_488_Em_525", "Ex_561_Em_600"]
        raw_name = "SmartSPIM_123_2026-01-01_00-00-00"
        stitched_name = "SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00"

        rows = _build_rows({raw_name: stitched_name}, {stitched_name: EXAMPLE_RECORD})

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["channel_1"], "Ex_488_Em_525")
        self.assertEqual(rows[0]["channel_2"], "Ex_561_Em_600")
        self.assertIsNone(rows[0]["channel_3"])

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim._list_channels")
    def test_processed_row_fields_populated(self, mock_list_channels):
        mock_list_channels.return_value = ["Ex_561_Em_600"]
        raw_name = "SmartSPIM_123_2026-01-01_00-00-00"
        stitched_name = "SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00"

        rows = _build_rows({raw_name: stitched_name}, {stitched_name: EXAMPLE_RECORD})
        row = rows[0]

        self.assertEqual(row["subject_id"], "123456")
        self.assertEqual(row["genotype"], "wt/wt")
        self.assertEqual(row["institution"], "AIBS")
        self.assertEqual(row["acquisition_start_time"], "2026-01-01T00:00:00")
        self.assertEqual(row["processing_end_time"], "2026-01-02T12:00:00")
        self.assertEqual(row["stitched_link"], _stitched_link(LOCATION))
        self.assertEqual(row["channel_1"], "Ex_561_Em_600")
        self.assertEqual(row["segmentation_link_1"], _segmentation_link(LOCATION, "Ex_561_Em_600"))
        self.assertEqual(row["quantification_link_1"], _quantification_link(LOCATION, "Ex_561_Em_600"))
        self.assertIsNone(row["channel_2"])
        self.assertIsNone(row["segmentation_link_2"])
        self.assertIsNone(row["quantification_link_2"])
        self.assertTrue(row["processed"])
        self.assertEqual(row["name"], stitched_name)

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim._list_channels")
    def test_unprocessed_row_has_no_links(self, mock_list_channels):
        raw_name = "SmartSPIM_123_2026-01-01_00-00-00"
        raw_record = {
            "name": raw_name,
            "subject": {"subject_id": "123456", "subject_details": {"genotype": "wt/wt"}},
            "data_description": {"institution": {"abbreviation": "AIBS"}},
            "acquisition": {"acquisition_start_time": "2026-01-01T00:00:00"},
        }

        rows = _build_rows({raw_name: None}, {raw_name: raw_record})
        row = rows[0]

        self.assertEqual(row["subject_id"], "123456")
        self.assertEqual(row["acquisition_start_time"], "2026-01-01T00:00:00")
        self.assertIsNone(row["processing_end_time"])
        self.assertIsNone(row["stitched_link"])
        self.assertFalse(row["processed"])
        self.assertEqual(row["name"], raw_name)
        mock_list_channels.assert_not_called()
        for i in range(1, 4):
            self.assertIsNone(row[f"channel_{i}"])
            self.assertIsNone(row[f"segmentation_link_{i}"])
            self.assertIsNone(row[f"quantification_link_{i}"])

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim._list_channels")
    def test_no_channels_produces_null_channel_columns(self, mock_list_channels):
        mock_list_channels.return_value = []
        raw_name = "SmartSPIM_123_2026-01-01_00-00-00"
        stitched_name = "SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00"

        rows = _build_rows({raw_name: stitched_name}, {stitched_name: EXAMPLE_RECORD})

        self.assertEqual(len(rows), 1)
        for i in range(1, 4):
            self.assertIsNone(rows[0][f"channel_{i}"])
            self.assertIsNone(rows[0][f"segmentation_link_{i}"])
            self.assertIsNone(rows[0][f"quantification_link_{i}"])

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim._list_channels")
    def test_uses_last_data_process_end_time(self, mock_list_channels):
        mock_list_channels.return_value = []
        raw_name = "SmartSPIM_123_2026-01-01_00-00-00"
        stitched_name = "SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00"
        record = {**EXAMPLE_RECORD, "processing": {"data_processes": [
            {"end_date_time": "2026-01-01T10:00:00"},
            {"end_date_time": "2026-01-02T12:00:00"},
        ]}}

        rows = _build_rows({raw_name: stitched_name}, {stitched_name: record})

        self.assertEqual(rows[0]["processing_end_time"], "2026-01-02T12:00:00")

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim._list_channels")
    def test_no_data_processes_gives_null_end_time(self, mock_list_channels):
        mock_list_channels.return_value = []
        raw_name = "SmartSPIM_123_2026-01-01_00-00-00"
        stitched_name = "SmartSPIM_123_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00"
        record = {**EXAMPLE_RECORD, "processing": {"data_processes": []}}

        rows = _build_rows({raw_name: stitched_name}, {stitched_name: record})

        self.assertIsNone(rows[0]["processing_end_time"])


class TestAssetsSmartspim(unittest.TestCase):
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.acorns.TREE")
    def test_cache_hit_returns_cached_df(self, mock_tree):
        cached_df = pd.DataFrame({"name": ["asset_a"], "channel": ["Ex_561_Em_600"]})
        mock_tree.scurry.return_value = cached_df

        result = assets_smartspim(force_update=False)

        self.assertEqual(len(result), 1)
        mock_tree.hide.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.acorns.TREE")
    def test_empty_cache_raises_without_force_update(self, mock_tree):
        mock_tree.scurry.return_value = pd.DataFrame()

        with self.assertRaises(ValueError) as ctx:
            assets_smartspim(force_update=False)

        self.assertIn("Cache is empty", str(ctx.exception))
        self.assertIn("force_update=True", str(ctx.exception))

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim._build_rows")
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim._fetch_asset_metadata")
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.source_data")
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.asset_basics")
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.acorns.TREE")
    def test_force_update_builds_and_caches(
        self, mock_tree, mock_asset_basics, mock_source_data, mock_fetch_meta, mock_build_rows
    ):
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_asset_basics.return_value = pd.DataFrame(
            {
                "data_level": ["raw"],
                "modalities": ["SPIM"],
                "name": ["SmartSPIM_raw_2026-01-01_00-00-00"],
            }
        )
        mock_source_data.return_value = pd.DataFrame(
            {
                "name": ["SmartSPIM_raw_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00"],
                "source_data": ["SmartSPIM_raw_2026-01-01_00-00-00"],
                "pipeline_name": ["stitching"],
                "processing_time": ["2026-01-02_00-00-00"],
            }
        )
        mock_fetch_meta.return_value = {}
        mock_build_rows.return_value = [{"name": "SmartSPIM_raw_2026-01-01_00-00-00_stitched_2026-01-02_00-00-00", "processed": True}]

        result = assets_smartspim(force_update=True)

        self.assertEqual(len(result), 1)
        mock_tree.hide.assert_called_once()

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.source_data")
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.asset_basics")
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.acorns.TREE")
    def test_unprocessed_assets_included_with_processed_false(self, mock_tree, mock_asset_basics, mock_source_data):
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_asset_basics.return_value = pd.DataFrame(
            {"data_level": ["raw"], "modalities": ["SPIM"], "name": ["SmartSPIM_raw_2026-01-01_00-00-00"]}
        )
        mock_source_data.return_value = pd.DataFrame(
            {
                "name": ["SmartSPIM_raw_2026-01-01_00-00-00_processed_2026-01-02_00-00-00"],
                "source_data": ["SmartSPIM_raw_2026-01-01_00-00-00"],
                "pipeline_name": ["processing"],
                "processing_time": ["2026-01-02_00-00-00"],
            }
        )

        with patch("zombie_squirrel.acorn_helpers.assets_smartspim._fetch_asset_metadata", return_value={}):
            with patch("zombie_squirrel.acorn_helpers.assets_smartspim._build_rows", return_value=[{"name": "SmartSPIM_raw_2026-01-01_00-00-00", "processed": False}]) as mock_build:
                result = assets_smartspim(force_update=True)

        raw_to_stitched_arg = mock_build.call_args[0][0]
        self.assertIn("SmartSPIM_raw_2026-01-01_00-00-00", raw_to_stitched_arg)
        self.assertIsNone(raw_to_stitched_arg["SmartSPIM_raw_2026-01-01_00-00-00"])
        mock_tree.hide.assert_called_once()

    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.source_data")
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.asset_basics")
    @patch("zombie_squirrel.acorn_helpers.assets_smartspim.acorns.TREE")
    def test_filters_only_raw_spim_assets(self, mock_tree, mock_asset_basics, mock_source_data):
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_asset_basics.return_value = pd.DataFrame(
            {
                "data_level": ["raw", "raw", "derived"],
                "modalities": ["SPIM", "ECEPHYS", "SPIM"],
                "name": ["spim_raw", "ecephys_raw", "spim_derived"],
            }
        )
        mock_source_data.return_value = pd.DataFrame(
            {
                "name": ["spim_raw_stitched_2026-01-02_00-00-00", "ecephys_raw_derived", "spim_derived_stitched"],
                "source_data": ["spim_raw", "ecephys_raw", "spim_derived"],
                "pipeline_name": ["stitching", "pipeline", "stitching"],
                "processing_time": ["2026-01-02_00-00-00", "2026-01-02_00-00-00", "2026-01-02_00-00-00"],
            }
        )

        with patch("zombie_squirrel.acorn_helpers.assets_smartspim._fetch_asset_metadata", return_value={}):
            with patch("zombie_squirrel.acorn_helpers.assets_smartspim._build_rows", return_value=[]) as mock_build:
                assets_smartspim(force_update=True)

        raw_to_stitched_arg = mock_build.call_args[0][0]
        self.assertIn("spim_raw", raw_to_stitched_arg)
        self.assertNotIn("ecephys_raw", raw_to_stitched_arg)
        self.assertNotIn("spim_derived", raw_to_stitched_arg)


class TestAssetsSmartspimColumns(unittest.TestCase):
    def test_returns_expected_columns(self):
        cols = assets_smartspim_columns()
        self.assertEqual(
            cols,
            [
                "subject_id",
                "genotype",
                "institution",
                "acquisition_start_time",
                "processing_end_time",
                "stitched_link",
                "processed",
                "name",
                "channel_1",
                "segmentation_link_1",
                "quantification_link_1",
                "channel_2",
                "segmentation_link_2",
                "quantification_link_2",
                "channel_3",
                "segmentation_link_3",
                "quantification_link_3",
            ],
        )


if __name__ == "__main__":
    unittest.main()

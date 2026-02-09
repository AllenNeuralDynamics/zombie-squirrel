"""Unit tests for asset_basics acorn."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.acorn_helpers.asset_basics import asset_basics


class TestAssetBasics(unittest.TestCase):
    """Tests for asset_basics acorn."""

    @patch("zombie_squirrel.acorn_helpers.asset_basics.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.asset_basics.acorns.TREE")
    def test_asset_basics_cache_hit(self, mock_tree, mock_client_class):
        """Test returning cached asset basics."""
        cached_df = pd.DataFrame(
            {
                "_id": ["id1", "id2"],
                "_last_modified": ["2023-01-01", "2023-01-02"],
                "modalities": ["imaging", "electrophysiology"],
                "project_name": ["proj1", "proj2"],
                "data_level": ["raw", "derived"],
                "subject_id": ["sub001", "sub002"],
                "acquisition_start_time": [
                    "2023-01-01T10:00:00",
                    "2023-01-02T10:00:00",
                ],
                "acquisition_end_time": [
                    "2023-01-01T11:00:00",
                    "2023-01-02T11:00:00",
                ],
            }
        )
        mock_tree.scurry.return_value = cached_df

        result = asset_basics(force_update=False)

        self.assertEqual(len(result), 2)
        self.assertListEqual(list(result["_id"]), ["id1", "id2"])
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.asset_basics.acorns.TREE")
    def test_asset_basics_empty_cache_raises_error(self, mock_tree):
        """Test that empty cache raises ValueError without force_update."""
        mock_tree.scurry.return_value = pd.DataFrame()

        with self.assertRaises(ValueError) as context:
            asset_basics(force_update=False)

        self.assertIn("Cache is empty", str(context.exception))
        self.assertIn("force_update=True", str(context.exception))

    @patch("zombie_squirrel.acorn_helpers.asset_basics.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.asset_basics.acorns.TREE")
    def test_asset_basics_cache_miss(self, mock_tree, mock_client_class):
        """Test fetching asset basics when cache is empty."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "id1",
                "_last_modified": "2023-01-01",
                "data_description": {
                    "modalities": [{"abbreviation": "img"}],
                    "project_name": "proj1",
                    "data_level": "raw",
                },
                "subject": {"subject_id": "sub001"},
                "acquisition": {
                    "acquisition_start_time": "2023-01-01T10:00:00",
                    "acquisition_end_time": "2023-01-01T11:00:00",
                },
            }
        ]

        result = asset_basics(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["_id"], "id1")
        self.assertEqual(result.iloc[0]["modalities"], "img")
        self.assertEqual(result.iloc[0]["project_name"], "proj1")

    @patch("zombie_squirrel.acorn_helpers.asset_basics.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.asset_basics.acorns.TREE")
    def test_asset_basics_with_data_processes(self, mock_tree, mock_client_class):
        """Test asset_basics includes process_date from data_processes."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "id1",
                "_last_modified": "2023-01-01",
                "data_description": {
                    "modalities": [{"abbreviation": "img"}],
                    "project_name": "proj1",
                    "data_level": "raw",
                },
                "subject": {"subject_id": "sub001"},
                "acquisition": {
                    "acquisition_start_time": "2023-01-01T10:00:00",
                    "acquisition_end_time": "2023-01-01T11:00:00",
                },
                "processing": {
                    "data_processes": [
                        {"start_date_time": "2023-01-15T14:30:00"},
                        {"start_date_time": "2023-01-20T09:15:00"},
                    ]
                },
            }
        ]

        result = asset_basics(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["_id"], "id1")
        self.assertEqual(result.iloc[0]["process_date"], "2023-01-20")

    @patch("zombie_squirrel.acorn_helpers.asset_basics.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.asset_basics.acorns.TREE")
    def test_asset_basics_incremental_update(self, mock_tree, mock_client_class):
        """Test incremental cache update with partial data refresh."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.side_effect = [
            [
                {"_id": "id1", "_last_modified": "2023-01-01"},
                {"_id": "id2", "_last_modified": "2023-01-02"},
            ],  # First call: shows id2 is new
            [
                {
                    "_id": "id2",
                    "_last_modified": "2023-01-02",
                    "data_description": {
                        "modalities": [{"abbreviation": "elec"}],
                        "project_name": "proj2",
                        "data_level": "derived",
                    },
                    "subject": {"subject_id": "sub002"},
                    "acquisition": {
                        "acquisition_start_time": "2023-01-02T10:00:00",
                        "acquisition_end_time": "2023-01-02T11:00:00",
                    },
                }
            ],  # Second call: batch fetch for new record
        ]

        result = asset_basics(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["_id"], "id2")

    @patch("zombie_squirrel.acorn_helpers.asset_basics.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.asset_basics.acorns.TREE")
    def test_asset_basics_with_other_identifiers_no_code_ocean(self, mock_tree, mock_client_class):
        """Test asset_basics when other_identifiers exists but has no Code Ocean."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "id1",
                "_last_modified": "2023-01-01",
                "data_description": {
                    "modalities": [{"abbreviation": "img"}],
                    "project_name": "proj1",
                    "data_level": "raw",
                },
                "subject": {"subject_id": "sub001"},
                "acquisition": {
                    "acquisition_start_time": "2023-01-01T10:00:00",
                    "acquisition_end_time": "2023-01-01T11:00:00",
                },
                "other_identifiers": {"Some Other Field": "value123"},
            }
        ]

        result = asset_basics(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["_id"], "id1")
        self.assertIsNone(result.iloc[0]["code_ocean"])

    @patch("zombie_squirrel.acorn_helpers.asset_basics.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.asset_basics.acorns.TREE")
    def test_asset_basics_with_code_ocean_identifier(self, mock_tree, mock_client_class):
        """Test asset_basics when other_identifiers contains Code Ocean."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "id1",
                "_last_modified": "2023-01-01",
                "data_description": {
                    "modalities": [{"abbreviation": "img"}],
                    "project_name": "proj1",
                    "data_level": "raw",
                },
                "subject": {"subject_id": "sub001"},
                "acquisition": {
                    "acquisition_start_time": "2023-01-01T10:00:00",
                    "acquisition_end_time": "2023-01-01T11:00:00",
                },
                "other_identifiers": {
                    "Code Ocean": ["df429003-91a0-45d2-8457-66b156ad8bfa"]
                },
            }
        ]

        result = asset_basics(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["_id"], "id1")
        self.assertEqual(
            result.iloc[0]["code_ocean"], ["df429003-91a0-45d2-8457-66b156ad8bfa"]
        )


if __name__ == "__main__":
    unittest.main()

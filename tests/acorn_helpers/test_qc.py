"""Unit tests for QC acorn."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import zombie_squirrel.acorns as acorns
from zombie_squirrel.acorn_helpers.qc import qc
from zombie_squirrel.forest import MemoryTree


class TestQCMemoryTree(unittest.TestCase):
    """Tests for QC acorn with in-memory tree."""

    def setUp(self):
        """Set up in-memory tree and mock MetadataDbClient."""
        acorns.TREE = MemoryTree()

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_cache_miss_with_force_update(self, mock_client_class):
        """Test fetching QC data when cache is empty with force_update."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "test-asset-001",
                "name": "test-asset",
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Test Metric 1",
                            "stage": "Processing",
                            "modality": {
                                "name": "Test Modality",
                                "abbreviation": "tm",
                            },
                            "value": {"value": "pass", "status": "Pass"},
                            "tags": {"tag1": "value1"},
                            "status_history": [
                                {
                                    "status": "Pass",
                                    "evaluator": "test_user",
                                    "timestamp": "2025-01-01T00:00:00",
                                }
                            ],
                        },
                        {
                            "object_type": "QC metric",
                            "name": "Test Metric 2",
                            "stage": "Acquisition",
                            "modality": {
                                "name": "Test Modality 2",
                                "abbreviation": "tm2",
                            },
                            "value": None,
                            "tags": None,
                            "status_history": [
                                {
                                    "status": "Pending",
                                    "evaluator": "Pending review",
                                    "timestamp": "2025-01-01T00:00:00",
                                }
                            ],
                        },
                    ]
                },
            }
        ]

        df = qc("test-asset", force_update=True)

        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]["name"], "Test Metric 1")
        self.assertEqual(df.iloc[1]["name"], "Test Metric 2")
        self.assertEqual(df.iloc[0]["value"], "{dict}")

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_cache_hit(self, mock_client_class):
        """Test returning cached QC data without refetch."""
        cache_df = pd.DataFrame(
            {
                "name": ["Metric 1", "Metric 2"],
                "stage": ["Processing", "Acquisition"],
                "value": ["pass", "{dict}"],
            }
        )
        acorns.TREE.hide("qc/cached-asset", cache_df)

        df = qc("cached-asset", force_update=False)

        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]["name"], "Metric 1")
        mock_client_class.assert_not_called()

    def test_qc_empty_cache_raises_error(self):
        """Test that empty cache logs error and returns empty dataframe without force_update."""
        df = qc("nonexistent-asset", force_update=False)

        self.assertTrue(df.empty)

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_no_record_found(self, mock_client_class):
        """Test handling when asset record not found in database."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = []

        df = qc("missing-asset", force_update=True)

        self.assertTrue(df.empty)

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_no_metrics_in_record(self, mock_client_class):
        """Test handling when quality_control has no metrics."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "test-asset-002",
                "name": "test-asset",
                "quality_control": {},
            }
        ]

        df = qc("test-asset", force_update=True)

        self.assertTrue(df.empty)

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_dict_value_replaced_and_no_tags_column(self, mock_client_class):
        """Test that dict values are replaced with {dict} and tags column is absent."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "test-asset-003",
                "name": "test-asset",
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Complex Metric",
                            "stage": "Processing",
                            "modality": {"name": "Test", "abbreviation": "t"},
                            "value": {
                                "nested": {"deep": "value"},
                                "status": "Pass",
                            },
                            "tags": {"key1": "val1", "key2": "val2"},
                            "status_history": [],
                        }
                    ]
                },
            }
        ]

        df = qc("test-asset", force_update=True)

        self.assertEqual(df.iloc[0]["value"], "{dict}")
        self.assertNotIn("tags", df.columns)

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_cache_persistence(self, mock_client_class):
        """Test that QC data is cached after first fetch."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "test-asset-004",
                "name": "test-asset",
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Persistent Metric",
                            "stage": "Processing",
                            "modality": {"name": "Test", "abbreviation": "t"},
                            "value": "test_value",
                            "tags": None,
                            "status_history": [],
                        }
                    ]
                },
            }
        ]

        df1 = qc("test-asset", force_update=True)
        self.assertEqual(len(df1), 1)

        mock_client_instance.retrieve_docdb_records.reset_mock()

        df2 = qc("test-asset", force_update=False)
        self.assertEqual(len(df2), 1)
        self.assertEqual(df2.iloc[0]["name"], "Persistent Metric")
        mock_client_instance.retrieve_docdb_records.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_multiple_assets_merge(self, mock_client_class):
        """Test fetching and merging QC data for multiple assets."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        # Single call returns all records for the subject
        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "asset1",
                "name": "asset1",
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Metric A",
                            "stage": "Processing",
                            "modality": {"name": "Test", "abbreviation": "t"},
                            "value": "pass",
                            "tags": None,
                            "status_history": [],
                        }
                    ]
                },
            },
            {
                "_id": "asset2",
                "name": "asset2",
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Metric B",
                            "stage": "Processing",
                            "modality": {"name": "Test", "abbreviation": "t"},
                            "value": "fail",
                            "tags": None,
                            "status_history": [],
                        }
                    ]
                },
            },
        ]

        df = qc("test-subject", asset_names=["asset1", "asset2"], force_update=True)

        self.assertEqual(len(df), 2)
        self.assertIn("asset_name", df.columns)
        self.assertEqual(df[df["name"] == "Metric A"].iloc[0]["asset_name"], "asset1")
        self.assertEqual(df[df["name"] == "Metric B"].iloc[0]["asset_name"], "asset2")

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_multiple_assets_from_cache(self, mock_client_class):
        """Test retrieving multiple cached assets merges them."""
        cache_df1 = pd.DataFrame(
            {
                "name": ["Metric 1"],
                "stage": ["Processing"],
                "value": ["pass"],
            }
        )
        cache_df2 = pd.DataFrame(
            {
                "name": ["Metric 2"],
                "stage": ["Acquisition"],
                "value": ["fail"],
            }
        )
        # Combine both dataframes and cache under a single subject
        cache_df1["asset_name"] = "asset1"
        cache_df2["asset_name"] = "asset2"
        combined_df = pd.concat([cache_df1, cache_df2], ignore_index=True)
        acorns.TREE.hide("qc/test-subject", combined_df)

        df = qc("test-subject", asset_names=["asset1", "asset2"], force_update=False)

        self.assertEqual(len(df), 2)
        self.assertIn("asset_name", df.columns)
        self.assertListEqual(sorted(df["asset_name"].unique().tolist()), ["asset1", "asset2"])
        mock_client_class.assert_not_called()

    def test_qc_multiple_empty_assets_no_force_update(self):
        """Test multiple assets with empty cache and no force_update."""
        df = qc("nonexistent-subject", asset_names=["nonexistent1", "nonexistent2"], force_update=False)

        self.assertTrue(df.empty)

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_single_asset_name_string(self, mock_client_class):
        """Test filtering with a single asset name as string instead of list."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "asset1",
                "name": "asset1",
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Metric A",
                            "stage": "Processing",
                            "modality": {"name": "Test", "abbreviation": "t"},
                            "value": "pass",
                            "tags": None,
                            "status_history": [],
                        }
                    ]
                },
            },
            {
                "_id": "asset2",
                "name": "asset2",
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Metric B",
                            "stage": "Processing",
                            "modality": {"name": "Test", "abbreviation": "t"},
                            "value": "fail",
                            "tags": None,
                            "status_history": [],
                        }
                    ]
                },
            },
        ]

        df = qc("test-subject", asset_names="asset1", force_update=True)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["name"], "Metric A")
        self.assertEqual(df.iloc[0]["asset_name"], "asset1")

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_missing_asset_names(self, mock_client_class):
        """Test requesting non-existent asset names triggers warning."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "asset1",
                "name": "asset1",
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Metric A",
                            "stage": "Processing",
                            "modality": {"name": "Test", "abbreviation": "t"},
                            "value": "pass",
                            "tags": None,
                            "status_history": [],
                        }
                    ]
                },
            },
        ]

        df = qc("test-subject", asset_names=["asset1", "nonexistent"], force_update=True)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["name"], "Metric A")

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_numeric_value_converted_to_string(self, mock_client_class):
        """Test that numeric values in QC metrics are converted to strings."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "test-asset-001",
                "name": "test-asset",
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Numeric Metric",
                            "stage": "Processing",
                            "modality": None,
                            "value": 42,
                            "tags": None,
                            "status_history": [],
                        }
                    ]
                },
            }
        ]

        df = qc("test-asset", force_update=True)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["value"], "42")


if __name__ == "__main__":
    unittest.main()

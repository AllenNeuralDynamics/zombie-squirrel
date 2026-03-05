"""Additional unit tests for QC acorn to improve code coverage."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import zombie_squirrel.acorns as acorns
from zombie_squirrel.acorn_helpers.qc import qc
from zombie_squirrel.forest import MemoryTree


class TestQCCoverageLazy(unittest.TestCase):
    """Tests for QC acorn lazy=True mode."""

    def setUp(self):
        """Set up in-memory tree and mock MetadataDbClient."""
        acorns.TREE = MemoryTree()

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_lazy_with_force_update(self, mock_client_class):
        """Test lazy=True with force_update=True fetches and returns path."""
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
                            "name": "Test Metric",
                            "stage": "Processing",
                            "modality": {
                                "name": "Test Modality",
                                "abbreviation": "tm",
                            },
                            "value": "pass",
                            "tags": None,
                            "status_history": [
                                {
                                    "status": "Pass",
                                    "evaluator": "test_user",
                                    "timestamp": "2025-01-01T00:00:00",
                                }
                            ],
                        }
                    ]
                },
            }
        ]

        path = qc("test-subject", force_update=True, lazy=True)

        self.assertIsInstance(path, str)
        self.assertIn("qc/test-subject", path)

    def test_qc_lazy_without_force_update(self):
        """Test lazy=True without force_update returns path without fetching."""
        path = qc("test-subject", force_update=False, lazy=True)

        self.assertIsInstance(path, str)
        self.assertIn("qc/test-subject", path)


class TestQCCoverageColumnDropping(unittest.TestCase):
    """Tests for column handling in QC acorn."""

    def setUp(self):
        """Set up in-memory tree."""
        acorns.TREE = MemoryTree()

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_columns_in_output(self, mock_client_class):
        """Test that expected columns are present in output."""
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
                            "name": "Test Metric",
                            "stage": "Processing",
                            "modality": {
                                "name": "Test Modality",
                                "abbreviation": "tm",
                            },
                            "value": "pass",
                            "tags": None,
                            "status_history": [],
                        }
                    ]
                },
            }
        ]

        df = qc("test-subject", force_update=True)

        expected_cols = ["name", "stage", "modality", "value", "asset_name", "subject_id", "timestamp"]
        for col in expected_cols:
            self.assertIn(col, df.columns)

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_drops_unwanted_columns(self, mock_client_class):
        """Test that object_type and status_history columns are dropped if present."""
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
                            "name": "Test Metric",
                            "stage": "Processing",
                            "modality": {
                                "name": "Test Modality",
                                "abbreviation": "tm",
                            },
                            "value": "pass",
                            "tags": None,
                            "status_history": [{"status": "Pass"}],
                        }
                    ]
                },
            }
        ]

        original_fields = ["name", "modality", "stage", "value", "status_history"]
        with patch("zombie_squirrel.acorn_helpers.qc.QC_METRIC_FIELDS", original_fields + ["object_type"]):
            df = qc("test-subject", force_update=True)

        self.assertNotIn("object_type", df.columns)
        self.assertNotIn("status_history", df.columns)
        self.assertIn("name", df.columns)


class TestQCCoverageTimestamp(unittest.TestCase):
    """Tests for timestamp parsing in QC acorn."""

    def setUp(self):
        """Set up in-memory tree."""
        acorns.TREE = MemoryTree()

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_timestamp_parsing_with_z_suffix(self, mock_client_class):
        """Test timestamp parsing with Z suffix."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "test-asset-001",
                "name": "test-asset",
                "acquisition": {
                    "acquisition_start_time": "2025-01-15T10:30:45Z",
                },
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Test Metric",
                            "stage": "Processing",
                            "modality": {
                                "name": "Test Modality",
                                "abbreviation": "tm",
                            },
                            "value": "pass",
                            "tags": None,
                            "status_history": [],
                        }
                    ]
                },
            }
        ]

        df = qc("test-subject", force_update=True)

        self.assertEqual(len(df), 1)
        self.assertIsNotNone(df.iloc[0]["timestamp"])
        self.assertEqual(df.iloc[0]["timestamp"].year, 2025)

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_timestamp_parsing_invalid_format(self, mock_client_class):
        """Test timestamp parsing with invalid format handles gracefully."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "test-asset-002",
                "name": "test-asset",
                "acquisition": {
                    "acquisition_start_time": "invalid-timestamp",
                },
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "QC metric",
                            "name": "Test Metric",
                            "stage": "Processing",
                            "modality": {
                                "name": "Test Modality",
                                "abbreviation": "tm",
                            },
                            "value": "pass",
                            "tags": None,
                            "status_history": [],
                        }
                    ]
                },
            }
        ]

        df = qc("test-subject", force_update=True)

        self.assertEqual(len(df), 1)
        self.assertTrue(pd.isna(df.iloc[0]["timestamp"]))

    @patch("zombie_squirrel.acorn_helpers.qc.MetadataDbClient")
    def test_qc_curation_metric_skipped(self, mock_client_class):
        """Test that curation metrics are filtered out."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "test-asset-003",
                "name": "test-asset",
                "quality_control": {
                    "metrics": [
                        {
                            "object_type": "Curation metric",
                            "name": "Curation Metric (should be skipped)",
                            "stage": "Processing",
                            "modality": {
                                "name": "Test Modality",
                                "abbreviation": "tm",
                            },
                            "value": "pass",
                            "tags": None,
                            "status_history": [],
                        },
                        {
                            "object_type": "QC metric",
                            "name": "Regular Metric",
                            "stage": "Processing",
                            "modality": {
                                "name": "Test Modality",
                                "abbreviation": "tm",
                            },
                            "value": "pass",
                            "tags": None,
                            "status_history": [],
                        },
                    ]
                },
            }
        ]

        df = qc("test-subject", force_update=True)

        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["name"], "Regular Metric")


if __name__ == "__main__":
    unittest.main()

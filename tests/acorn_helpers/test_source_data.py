"""Unit tests for source_data acorn."""

import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.acorn_helpers.source_data import source_data


class TestSourceData(unittest.TestCase):
    """Tests for source_data acorn."""

    @patch("zombie_squirrel.acorn_helpers.source_data.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.source_data.acorns.TREE")
    def test_source_data_cache_hit(self, mock_tree, mock_client_class):
        """Test returning cached source data."""
        cached_df = pd.DataFrame(
            {
                "name": ["derived1", "derived2"],
                "source_data": ["raw1", "raw2"],
                "pipeline_name": ["pipeline_a", "pipeline_b"],
                "processing_time": ["2026-01-01_00-00-00", "2026-01-02_00-00-00"],
            }
        )
        mock_tree.scurry.return_value = cached_df

        result = source_data(force_update=False)

        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["source_data"], "raw1")
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.source_data.acorns.TREE")
    def test_source_data_empty_cache_raises_error(self, mock_tree):
        """Test that empty cache raises ValueError without force_update."""
        mock_tree.scurry.return_value = pd.DataFrame()

        with self.assertRaises(ValueError) as context:
            source_data(force_update=False)

        self.assertIn("Cache is empty", str(context.exception))
        self.assertIn("force_update=True", str(context.exception))

    @patch("zombie_squirrel.acorn_helpers.source_data.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.source_data.acorns.TREE")
    def test_source_data_cache_miss(self, mock_tree, mock_client_class):
        """Test fetching source data when cache is empty using real test records."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        resources_path = Path(__file__).parent.parent / "resources"
        with open(resources_path / "v2_derived.json") as f:
            derived_record = json.load(f)

        mock_client_instance.retrieve_docdb_records.return_value = [derived_record]

        result = source_data(force_update=True)

        self.assertGreater(len(result), 0)
        self.assertIn("name", result.columns)
        self.assertIn("source_data", result.columns)
        self.assertIn("pipeline_name", result.columns)
        self.assertIn("processing_time", result.columns)

        row = result[result["name"] == derived_record["name"]]
        self.assertGreater(len(row), 0)
        expected_source = derived_record["data_description"]["source_data"][0]
        self.assertIn(expected_source, row["source_data"].values)
        self.assertEqual(row.iloc[0]["processing_time"], "2026-02-14_12-44-45")

    @patch("zombie_squirrel.acorn_helpers.source_data.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.source_data.acorns.TREE")
    def test_source_data_multiple_sources(self, mock_tree, mock_client_class):
        """Test derived asset with multiple source data entries produces one row each."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "name": "subject_2026-01-01_00-00-00_processed_2026-01-02_12-00-00",
                "data_description": {"source_data": ["src1", "src2"]},
                "processing": {"pipelines": [{"name": "my_pipeline"}]},
            }
        ]

        result = source_data(force_update=True)

        self.assertEqual(len(result), 2)
        self.assertSetEqual(set(result["source_data"].tolist()), {"src1", "src2"})
        self.assertTrue((result["pipeline_name"] == "my_pipeline").all())
        self.assertTrue((result["processing_time"] == "2026-01-02_12-00-00").all())

    @patch("zombie_squirrel.acorn_helpers.source_data.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.source_data.acorns.TREE")
    def test_source_data_no_source_data(self, mock_tree, mock_client_class):
        """Test derived asset with no source data produces one row with empty source_data."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "name": "derived_2026-01-01_00-00-00",
                "data_description": {"source_data": []},
                "processing": {"pipelines": []},
            }
        ]

        result = source_data(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["source_data"], "")
        self.assertEqual(result.iloc[0]["pipeline_name"], "")

    @patch("zombie_squirrel.acorn_helpers.source_data.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.source_data.acorns.TREE")
    def test_source_data_force_update(self, mock_tree, mock_client_class):
        """Test force_update bypasses cache."""
        cached_df = pd.DataFrame(
            {
                "name": ["old_derived"],
                "source_data": ["old_raw"],
                "pipeline_name": ["old_pipeline"],
                "processing_time": ["2025-01-01_00-00-00"],
            }
        )
        mock_tree.scurry.return_value = cached_df

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "name": "new_derived_2026-01-01_12-00-00",
                "data_description": {"source_data": ["new_raw"]},
                "processing": {"pipelines": [{"name": "new_pipeline"}]},
            }
        ]

        result = source_data(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["name"], "new_derived_2026-01-01_12-00-00")
        self.assertEqual(result.iloc[0]["source_data"], "new_raw")


if __name__ == "__main__":
    unittest.main()

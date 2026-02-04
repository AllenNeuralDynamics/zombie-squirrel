"""Unit tests for source_data acorn."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.acorn_contents.source_data import source_data


class TestSourceData(unittest.TestCase):
    """Tests for source_data acorn."""

    @patch("zombie_squirrel.acorn_contents.source_data.MetadataDbClient")
    @patch("zombie_squirrel.acorn_contents.source_data.acorns.TREE")
    def test_source_data_cache_hit(self, mock_tree, mock_client_class):
        """Test returning cached source data."""
        cached_df = pd.DataFrame(
            {
                "_id": ["id1", "id2"],
                "source_data": ["source1, source2", "source3"],
            }
        )
        mock_tree.scurry.return_value = cached_df

        result = source_data(force_update=False)

        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["source_data"], "source1, source2")
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.acorn_contents.source_data.acorns.TREE")
    def test_source_data_empty_cache_raises_error(self, mock_tree):
        """Test that empty cache raises ValueError without force_update."""
        mock_tree.scurry.return_value = pd.DataFrame()

        with self.assertRaises(ValueError) as context:
            source_data(force_update=False)

        self.assertIn("Cache is empty", str(context.exception))
        self.assertIn("force_update=True", str(context.exception))

    @patch("zombie_squirrel.acorn_contents.source_data.MetadataDbClient")
    @patch("zombie_squirrel.acorn_contents.source_data.acorns.TREE")
    def test_source_data_cache_miss(self, mock_tree, mock_client_class):
        """Test fetching source data when cache is empty."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "id1",
                "data_description": {"source_data": ["src1", "src2"]},
            },
            {"_id": "id2", "data_description": {"source_data": []}},
        ]

        result = source_data(force_update=True)

        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["source_data"], "src1, src2")
        self.assertEqual(result.iloc[1]["source_data"], "")

    @patch("zombie_squirrel.acorn_contents.source_data.MetadataDbClient")
    @patch("zombie_squirrel.acorn_contents.source_data.acorns.TREE")
    def test_source_data_force_update(self, mock_tree, mock_client_class):
        """Test force_update bypasses cache."""
        cached_df = pd.DataFrame(
            {
                "_id": ["old_id"],
                "source_data": ["old_source"],
            }
        )
        mock_tree.scurry.return_value = cached_df

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "new_id",
                "data_description": {"source_data": ["new_src"]},
            },
        ]

        result = source_data(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["_id"], "new_id")


if __name__ == "__main__":
    unittest.main()

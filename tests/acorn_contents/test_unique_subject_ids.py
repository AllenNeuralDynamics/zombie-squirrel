"""Unit tests for unique_subject_ids acorn."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.acorn_helpers.unique_subject_ids import unique_subject_ids
from zombie_squirrel.forest import MemoryTree
import zombie_squirrel.acorns as acorns


class TestUniqueSubjectIds(unittest.TestCase):
    """Tests for unique_subject_ids acorn."""

    @patch("zombie_squirrel.acorn_contents.unique_subject_ids.MetadataDbClient")
    @patch("zombie_squirrel.acorn_contents.unique_subject_ids.acorns.TREE")
    def test_unique_subject_ids_cache_hit(self, mock_tree, mock_client_class):
        """Test returning cached subject IDs."""
        cached_df = pd.DataFrame({"subject_id": ["sub001", "sub002"]})
        mock_tree.scurry.return_value = cached_df

        result = unique_subject_ids(force_update=False)

        self.assertEqual(result, ["sub001", "sub002"])
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.acorn_contents.unique_subject_ids.acorns.TREE")
    def test_unique_subject_ids_empty_cache_raises_error(self, mock_tree):
        """Test that empty cache raises ValueError without force_update."""
        mock_tree.scurry.return_value = pd.DataFrame()

        with self.assertRaises(ValueError) as context:
            unique_subject_ids(force_update=False)

        self.assertIn("Cache is empty", str(context.exception))
        self.assertIn("force_update=True", str(context.exception))

    @patch("zombie_squirrel.acorn_contents.unique_subject_ids.MetadataDbClient")
    @patch("zombie_squirrel.acorn_contents.unique_subject_ids.acorns.TREE")
    def test_unique_subject_ids_cache_miss(self, mock_tree, mock_client_class):
        """Test fetching subject IDs when cache is empty."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.aggregate_docdb_records.return_value = [
            {"subject_id": "sub001"},
            {"subject_id": "sub002"},
        ]

        result = unique_subject_ids(force_update=True)

        self.assertEqual(result, ["sub001", "sub002"])
        mock_client_class.assert_called_once()

    @patch("zombie_squirrel.acorn_contents.unique_subject_ids.MetadataDbClient")
    @patch("zombie_squirrel.acorn_contents.unique_subject_ids.acorns.TREE")
    def test_unique_subject_ids_force_update(self, mock_tree, mock_client_class):
        """Test force_update bypasses cache."""
        cached_df = pd.DataFrame({"subject_id": ["old_sub"]})
        mock_tree.scurry.return_value = cached_df

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.aggregate_docdb_records.return_value = [{"subject_id": "new_sub"}]

        result = unique_subject_ids(force_update=True)

        self.assertEqual(result, ["new_sub"])


if __name__ == "__main__":
    unittest.main()

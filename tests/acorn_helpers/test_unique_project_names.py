"""Unit tests for unique_project_names acorn."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.acorn_helpers.unique_project_names import unique_project_names


class TestUniqueProjectNames(unittest.TestCase):
    """Tests for unique_project_names acorn."""

    @patch("zombie_squirrel.acorn_helpers.unique_project_names.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.unique_project_names.acorns.TREE")
    def test_unique_project_names_cache_hit(self, mock_tree, mock_client_class):
        """Test returning cached project names."""
        cached_df = pd.DataFrame({"project_name": ["proj1", "proj2", "proj3"]})
        mock_tree.scurry.return_value = cached_df

        result = unique_project_names(force_update=False)

        self.assertEqual(result, ["proj1", "proj2", "proj3"])
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.unique_project_names.acorns.TREE")
    def test_unique_project_names_empty_cache_raises_error(self, mock_tree):
        """Test that empty cache raises ValueError without force_update."""
        mock_tree.scurry.return_value = pd.DataFrame()

        with self.assertRaises(ValueError) as context:
            unique_project_names(force_update=False)

        self.assertIn("Cache is empty", str(context.exception))
        self.assertIn("force_update=True", str(context.exception))

    @patch("zombie_squirrel.acorn_helpers.unique_project_names.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.unique_project_names.acorns.TREE")
    def test_unique_project_names_cache_miss(self, mock_tree, mock_client_class):
        """Test fetching project names when cache is empty."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.aggregate_docdb_records.return_value = [
            {"project_name": "proj1"},
            {"project_name": "proj2"},
        ]

        result = unique_project_names(force_update=True)

        self.assertEqual(result, ["proj1", "proj2"])
        mock_client_class.assert_called_once()
        mock_client_instance.aggregate_docdb_records.assert_called_once()

    @patch("zombie_squirrel.acorn_helpers.unique_project_names.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.unique_project_names.acorns.TREE")
    def test_unique_project_names_force_update(self, mock_tree, mock_client_class):
        """Test force_update bypasses cache."""
        cached_df = pd.DataFrame({"project_name": ["old_proj"]})
        mock_tree.scurry.return_value = cached_df

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.aggregate_docdb_records.return_value = [{"project_name": "new_proj"}]

        result = unique_project_names(force_update=True)

        self.assertEqual(result, ["new_proj"])
        mock_client_instance.aggregate_docdb_records.assert_called_once()


if __name__ == "__main__":
    unittest.main()

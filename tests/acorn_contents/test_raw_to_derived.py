"""Unit tests for raw_to_derived acorn."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.acorn_helpers.raw_to_derived import raw_to_derived


class TestRawToDerived(unittest.TestCase):
    """Tests for raw_to_derived acorn."""

    @patch("zombie_squirrel.acorn_contents.raw_to_derived.MetadataDbClient")
    @patch("zombie_squirrel.acorn_contents.raw_to_derived.acorns.TREE")
    def test_raw_to_derived_cache_hit(self, mock_tree, mock_client_class):
        """Test returning cached raw to derived mapping."""
        cached_df = pd.DataFrame(
            {
                "_id": ["raw1", "raw2"],
                "derived_records": ["derived1, derived2", "derived3"],
            }
        )
        mock_tree.scurry.return_value = cached_df

        result = raw_to_derived(force_update=False)

        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["derived_records"], "derived1, derived2")
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.acorn_contents.raw_to_derived.acorns.TREE")
    def test_raw_to_derived_empty_cache_raises_error(self, mock_tree):
        """Test that empty cache raises ValueError without force_update."""
        mock_tree.scurry.return_value = pd.DataFrame()

        with self.assertRaises(ValueError) as context:
            raw_to_derived(force_update=False)

        self.assertIn("Cache is empty", str(context.exception))
        self.assertIn("force_update=True", str(context.exception))

    @patch("zombie_squirrel.acorn_contents.raw_to_derived.MetadataDbClient")
    @patch("zombie_squirrel.acorn_contents.raw_to_derived.acorns.TREE")
    def test_raw_to_derived_cache_miss(self, mock_tree, mock_client_class):
        """Test fetching raw to derived mapping when cache is empty."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        # Mock raw and derived records
        mock_client_instance.retrieve_docdb_records.side_effect = [
            [
                {"_id": "raw1"},
                {"_id": "raw2"},
            ],  # First call: raw records
            [
                {
                    "_id": "derived1",
                    "data_description": {"source_data": ["raw1"]},
                },
                {
                    "_id": "derived2",
                    "data_description": {"source_data": ["raw1", "raw2"]},
                },
            ],  # Second call: derived records
        ]

        result = raw_to_derived(force_update=True)

        self.assertEqual(len(result), 2)
        raw1_row = result[result["_id"] == "raw1"]
        raw2_row = result[result["_id"] == "raw2"]
        self.assertEqual(raw1_row.iloc[0]["derived_records"], "derived1, derived2")
        self.assertEqual(raw2_row.iloc[0]["derived_records"], "derived2")

    @patch("zombie_squirrel.acorn_contents.raw_to_derived.MetadataDbClient")
    @patch("zombie_squirrel.acorn_contents.raw_to_derived.acorns.TREE")
    def test_raw_to_derived_no_derived(self, mock_tree, mock_client_class):
        """Test raw records with no derived data."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.side_effect = [
            [{"_id": "raw1"}],  # Raw records
            [],  # No derived records
        ]

        result = raw_to_derived(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["derived_records"], "")

    @patch("zombie_squirrel.acorn_contents.raw_to_derived.MetadataDbClient")
    @patch("zombie_squirrel.acorn_contents.raw_to_derived.acorns.TREE")
    def test_raw_to_derived_force_update(self, mock_tree, mock_client_class):
        """Test force_update bypasses cache."""
        cached_df = pd.DataFrame(
            {
                "_id": ["old_raw"],
                "derived_records": ["old_derived"],
            }
        )
        mock_tree.scurry.return_value = cached_df

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.side_effect = [
            [{"_id": "new_raw"}],
            [],
        ]

        result = raw_to_derived(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["_id"], "new_raw")


if __name__ == "__main__":
    unittest.main()

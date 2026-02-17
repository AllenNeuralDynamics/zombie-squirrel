"""Unit tests for raw_to_derived acorn."""

import json
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.acorn_helpers.raw_to_derived import raw_to_derived


class TestRawToDerived(unittest.TestCase):
    """Tests for raw_to_derived acorn."""

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.acorns.TREE")
    def test_raw_to_derived_cache_hit(self, mock_tree, mock_client_class):
        """Test returning cached raw to derived mapping."""
        cached_df = pd.DataFrame(
            {
                "name": ["raw1", "raw2"],
                "derived_records": ["derived1, derived2", "derived3"],
            }
        )
        mock_tree.scurry.return_value = cached_df

        result = raw_to_derived(force_update=False)

        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["derived_records"], "derived1, derived2")
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.acorns.TREE")
    def test_raw_to_derived_empty_cache_raises_error(self, mock_tree):
        """Test that empty cache raises ValueError without force_update."""
        mock_tree.scurry.return_value = pd.DataFrame()

        with self.assertRaises(ValueError) as context:
            raw_to_derived(force_update=False)

        self.assertIn("Cache is empty", str(context.exception))
        self.assertIn("force_update=True", str(context.exception))

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.acorns.TREE")
    def test_raw_to_derived_cache_miss(self, mock_tree, mock_client_class):
        """Test fetching raw to derived mapping when cache is empty using real test records."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        resources_path = Path(__file__).parent.parent / "resources"
        with open(resources_path / "v2_raw.json") as f:
            raw_record = json.load(f)
        with open(resources_path / "v2_derived.json") as f:
            derived_record = json.load(f)

        mock_client_instance.retrieve_docdb_records.side_effect = [
            [raw_record],
            [derived_record],
        ]

        result = raw_to_derived(force_update=True)

        self.assertEqual(len(result), 1)
        raw_row = result[result["name"] == raw_record["name"]]
        self.assertEqual(len(raw_row), 1)

        expected_derived_name = derived_record["name"]
        actual_derived_records = raw_row.iloc[0]["derived_records"]

        self.assertIn(
            expected_derived_name,
            actual_derived_records,
            f"Expected derived record {expected_derived_name} not found in mapping. Got: {actual_derived_records}",
        )

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.acorns.TREE")
    def test_raw_to_derived_no_derived(self, mock_tree, mock_client_class):
        """Test raw records with no derived data."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.side_effect = [
            [{"_id": "raw1", "name": "raw1_name"}],  # Raw records
            [],  # No derived records
        ]

        result = raw_to_derived(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["derived_records"], "")

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.acorns.TREE")
    def test_raw_to_derived_force_update(self, mock_tree, mock_client_class):
        """Test force_update bypasses cache."""
        cached_df = pd.DataFrame(
            {
                "name": ["old_raw"],
                "derived_records": ["old_derived"],
            }
        )
        mock_tree.scurry.return_value = cached_df

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.side_effect = [
            [{"_id": "new_raw_id", "name": "new_raw"}],
            [],
        ]

        result = raw_to_derived(force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["name"], "new_raw")


if __name__ == "__main__":
    unittest.main()

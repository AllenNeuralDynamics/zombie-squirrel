"""Unit tests for raw_to_derived helper."""

import unittest
from unittest.mock import patch

import pandas as pd

from zombie_squirrel.acorn_helpers.raw_to_derived import raw_to_derived


class TestRawToDerived(unittest.TestCase):
    """Tests for raw_to_derived helper function."""

    def _make_df(self, rows):
        """Create test DataFrame with raw-to-derived columns."""
        return pd.DataFrame(rows, columns=["name", "source_data", "pipeline_name", "processing_time"])

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.source_data")
    def test_returns_matching_derived_names(self, mock_source_data):
        """Test returns list of derived asset names for a given raw asset."""
        mock_source_data.return_value = self._make_df(
            [
                ("derived_a_2026-01-02_00-00-00", "raw_x", "pipeline_a", "2026-01-02_00-00-00"),
                ("derived_b_2026-01-03_00-00-00", "raw_x", "pipeline_b", "2026-01-03_00-00-00"),
                ("derived_c_2026-01-01_00-00-00", "raw_y", "pipeline_a", "2026-01-01_00-00-00"),
            ]
        )

        result = raw_to_derived("raw_x")

        self.assertCountEqual(result, ["derived_a_2026-01-02_00-00-00", "derived_b_2026-01-03_00-00-00"])

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.source_data")
    def test_returns_empty_set_no_match(self, mock_source_data):
        """Test returns empty list when no derived assets exist for the given raw asset."""
        mock_source_data.return_value = self._make_df(
            [
                ("derived_a_2026-01-01_00-00-00", "raw_y", "pipeline_a", "2026-01-01_00-00-00"),
            ]
        )

        result = raw_to_derived("raw_x")

        self.assertEqual(result, [])

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.source_data")
    def test_latest_returns_most_recent_per_pipeline(self, mock_source_data):
        """Test latest=True returns only the most recent derived asset per pipeline."""
        mock_source_data.return_value = self._make_df(
            [
                ("derived_old_2026-01-01_00-00-00", "raw_x", "pipeline_a", "2026-01-01_00-00-00"),
                ("derived_new_2026-01-03_00-00-00", "raw_x", "pipeline_a", "2026-01-03_00-00-00"),
                ("derived_b_2026-01-02_00-00-00", "raw_x", "pipeline_b", "2026-01-02_00-00-00"),
            ]
        )

        result = raw_to_derived("raw_x", latest=True)

        self.assertCountEqual(result, ["derived_new_2026-01-03_00-00-00", "derived_b_2026-01-02_00-00-00"])
        self.assertNotIn("derived_old_2026-01-01_00-00-00", result)

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.source_data")
    def test_latest_false_returns_all(self, mock_source_data):
        """Test latest=False (default) returns all derived assets."""
        mock_source_data.return_value = self._make_df(
            [
                ("derived_old_2026-01-01_00-00-00", "raw_x", "pipeline_a", "2026-01-01_00-00-00"),
                ("derived_new_2026-01-03_00-00-00", "raw_x", "pipeline_a", "2026-01-03_00-00-00"),
            ]
        )

        result = raw_to_derived("raw_x", latest=False)

        self.assertCountEqual(result, ["derived_old_2026-01-01_00-00-00", "derived_new_2026-01-03_00-00-00"])

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.source_data")
    def test_latest_empty_matches(self, mock_source_data):
        """Test latest=True with no matches returns empty list."""
        mock_source_data.return_value = self._make_df(
            [
                ("derived_a_2026-01-01_00-00-00", "raw_y", "pipeline_a", "2026-01-01_00-00-00"),
            ]
        )

        result = raw_to_derived("raw_x", latest=True)

        self.assertEqual(result, [])

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.asset_basics")
    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.source_data")
    def test_modality_filters_by_modality(self, mock_source_data, mock_asset_basics):
        """Test modality parameter filters derived assets by modality."""
        mock_source_data.return_value = self._make_df(
            [
                ("derived_ecephys_2026-01-01_00-00-00", "raw_x", "pipeline_a", "2026-01-01_00-00-00"),
                ("derived_behavior_2026-01-01_00-00-00", "raw_x", "pipeline_b", "2026-01-01_00-00-00"),
            ]
        )
        mock_asset_basics.return_value = pd.DataFrame(
            [
                {"name": "derived_ecephys_2026-01-01_00-00-00", "modalities": "ecephys"},
                {"name": "derived_behavior_2026-01-01_00-00-00", "modalities": "behavior"},
            ]
        )

        result = raw_to_derived("raw_x", modality="ecephys")

        self.assertEqual(result, ["derived_ecephys_2026-01-01_00-00-00"])

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.asset_basics")
    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.source_data")
    def test_modality_latest_returns_most_recent_for_modality(self, mock_source_data, mock_asset_basics):
        """Test latest=True with modality returns most recent per pipeline for that modality."""
        mock_source_data.return_value = self._make_df(
            [
                ("derived_ecephys_old_2026-01-01_00-00-00", "raw_x", "pipeline_a", "2026-01-01_00-00-00"),
                ("derived_ecephys_new_2026-01-03_00-00-00", "raw_x", "pipeline_a", "2026-01-03_00-00-00"),
                ("derived_behavior_2026-01-04_00-00-00", "raw_x", "pipeline_a", "2026-01-04_00-00-00"),
            ]
        )
        mock_asset_basics.return_value = pd.DataFrame(
            [
                {"name": "derived_ecephys_old_2026-01-01_00-00-00", "modalities": "ecephys"},
                {"name": "derived_ecephys_new_2026-01-03_00-00-00", "modalities": "ecephys"},
                {"name": "derived_behavior_2026-01-04_00-00-00", "modalities": "behavior"},
            ]
        )

        result = raw_to_derived("raw_x", latest=True, modality="ecephys")

        self.assertEqual(result, ["derived_ecephys_new_2026-01-03_00-00-00"])
        self.assertNotIn("derived_ecephys_old_2026-01-01_00-00-00", result)
        self.assertNotIn("derived_behavior_2026-01-04_00-00-00", result)

    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.asset_basics")
    @patch("zombie_squirrel.acorn_helpers.raw_to_derived.source_data")
    def test_modality_no_match_returns_empty(self, mock_source_data, mock_asset_basics):
        """Test modality filter returns empty list when no derived assets match."""
        mock_source_data.return_value = self._make_df(
            [
                ("derived_behavior_2026-01-01_00-00-00", "raw_x", "pipeline_a", "2026-01-01_00-00-00"),
            ]
        )
        mock_asset_basics.return_value = pd.DataFrame(
            [
                {"name": "derived_behavior_2026-01-01_00-00-00", "modalities": "behavior"},
            ]
        )

        result = raw_to_derived("raw_x", modality="ecephys")

        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()

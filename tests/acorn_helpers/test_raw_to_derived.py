"""Unit tests for raw_to_derived helper."""

import unittest
from unittest.mock import patch

import pandas as pd

from zombie_squirrel.acorn_helpers.raw_to_derived import raw_to_derived


class TestRawToDerived(unittest.TestCase):
    """Tests for raw_to_derived helper function."""

    def _make_df(self, rows):
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
    def test_force_update_passed_through(self, mock_source_data):
        """Test force_update is passed through to source_data."""
        mock_source_data.return_value = self._make_df([])

        raw_to_derived("raw_x", force_update=True)

        mock_source_data.assert_called_once_with(force_update=True)

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


if __name__ == "__main__":
    unittest.main()

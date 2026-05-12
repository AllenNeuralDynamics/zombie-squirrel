"""Unit tests for metadata_upgrade acorn."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.acorn_helpers.metadata_upgrade import metadata_upgrade


def _make_raw_upgrade_df(rows):
    return pd.DataFrame(rows, columns=["v1_id", "upgrader_version", "status", "v2_id", "last_modified", "upgrade_datetime"])


def _make_basics_df():
    return pd.DataFrame({"_id": [], "name": [], "project_name": [], "data_level": []})


class TestMetadataUpgradeCacheHit(unittest.TestCase):
    """Tests for cache-hit path."""

    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.TREE")
    def test_returns_cached_df_when_not_empty(self, mock_tree):
        cached = pd.DataFrame({"_id": ["a"], "0.1.0": ["success"]})
        mock_tree.scurry.return_value = cached
        result = metadata_upgrade(force_update=False)
        pd.testing.assert_frame_equal(result, cached)

    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.TREE")
    def test_raises_when_cache_empty_and_no_force(self, mock_tree):
        mock_tree.scurry.return_value = pd.DataFrame()
        with self.assertRaises(ValueError):
            metadata_upgrade(force_update=False)


class TestMetadataUpgradeVersionColumns(unittest.TestCase):
    """Tests that version columns are created correctly."""

    def _patch_acorns(self, mock_tree, mock_custom, mock_registry, cached_df, raw_rows):
        mock_tree.scurry.return_value = cached_df
        mock_custom.return_value = _make_raw_upgrade_df(raw_rows)
        mock_registry.__getitem__ = MagicMock(return_value=lambda: _make_basics_df())

    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.ACORN_REGISTRY")
    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.custom")
    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.TREE")
    def test_single_version_creates_version_column(self, mock_tree, mock_custom, mock_registry):
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_custom.return_value = _make_raw_upgrade_df([
            {"v1_id": "a", "upgrader_version": "0.1.0", "status": "success", "v2_id": None, "last_modified": None, "upgrade_datetime": None},
        ])
        mock_registry.__getitem__ = MagicMock(return_value=lambda: _make_basics_df())

        result = metadata_upgrade(force_update=True)

        self.assertIn("0.1.0", result.columns)
        self.assertEqual(result.loc[result["_id"] == "a", "0.1.0"].iloc[0], "success")

    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.ACORN_REGISTRY")
    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.custom")
    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.TREE")
    def test_multiple_versions_in_raw_creates_all_columns(self, mock_tree, mock_custom, mock_registry):
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_custom.return_value = _make_raw_upgrade_df([
            {"v1_id": "a", "upgrader_version": "0.1.0", "status": "success", "v2_id": None, "last_modified": None, "upgrade_datetime": None},
            {"v1_id": "b", "upgrader_version": "0.2.0", "status": "failed", "v2_id": None, "last_modified": None, "upgrade_datetime": None},
        ])
        mock_registry.__getitem__ = MagicMock(return_value=lambda: _make_basics_df())

        result = metadata_upgrade(force_update=True)

        self.assertIn("0.1.0", result.columns)
        self.assertIn("0.2.0", result.columns)
        row_a = result[result["_id"] == "a"].iloc[0]
        row_b = result[result["_id"] == "b"].iloc[0]
        self.assertEqual(row_a["0.1.0"], "success")
        self.assertTrue(pd.isna(row_a["0.2.0"]))
        self.assertEqual(row_b["0.2.0"], "failed")
        self.assertTrue(pd.isna(row_b["0.1.0"]))


class TestMetadataUpgradeVersionColumnPreservation(unittest.TestCase):
    """Tests that cached version columns are never lost on update."""

    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.ACORN_REGISTRY")
    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.custom")
    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.TREE")
    def test_old_version_column_preserved_when_new_version_arrives(self, mock_tree, mock_custom, mock_registry):
        cached_df = pd.DataFrame({
            "_id": ["a", "b"],
            "0.1.0": ["success", "failed"],
        })
        mock_tree.scurry.return_value = cached_df
        mock_custom.return_value = _make_raw_upgrade_df([
            {"v1_id": "a", "upgrader_version": "0.2.0", "status": "success", "v2_id": None, "last_modified": None, "upgrade_datetime": None},
            {"v1_id": "b", "upgrader_version": "0.2.0", "status": "success", "v2_id": None, "last_modified": None, "upgrade_datetime": None},
        ])
        mock_registry.__getitem__ = MagicMock(return_value=lambda: _make_basics_df())

        result = metadata_upgrade(force_update=True)

        self.assertIn("0.1.0", result.columns, "Old version column must be preserved")
        self.assertIn("0.2.0", result.columns, "New version column must be present")
        row_a = result[result["_id"] == "a"].iloc[0]
        row_b = result[result["_id"] == "b"].iloc[0]
        self.assertEqual(row_a["0.1.0"], "success")
        self.assertEqual(row_b["0.1.0"], "failed")
        self.assertEqual(row_a["0.2.0"], "success")
        self.assertEqual(row_b["0.2.0"], "success")

    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.ACORN_REGISTRY")
    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.custom")
    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.TREE")
    def test_old_version_values_not_overwritten_by_na(self, mock_tree, mock_custom, mock_registry):
        cached_df = pd.DataFrame({
            "_id": ["a", "b"],
            "0.1.0": ["success", "failed"],
        })
        mock_tree.scurry.return_value = cached_df
        mock_custom.return_value = _make_raw_upgrade_df([
            {"v1_id": "a", "upgrader_version": "0.1.0", "status": "success", "v2_id": None, "last_modified": None, "upgrade_datetime": None},
        ])
        mock_registry.__getitem__ = MagicMock(return_value=lambda: _make_basics_df())

        result = metadata_upgrade(force_update=True)

        self.assertIn("0.1.0", result.columns)
        row_b = result[result["_id"] == "b"].iloc[0]
        self.assertEqual(
            row_b["0.1.0"],
            "failed",
            "Row 'b' was not in the new pull; its cached 0.1.0 value must not be overwritten with NaN",
        )

    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.ACORN_REGISTRY")
    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.custom")
    @patch("zombie_squirrel.acorn_helpers.metadata_upgrade.acorns.TREE")
    def test_new_row_added_when_not_in_cache(self, mock_tree, mock_custom, mock_registry):
        cached_df = pd.DataFrame({
            "_id": ["a"],
            "0.1.0": ["success"],
        })
        mock_tree.scurry.return_value = cached_df
        mock_custom.return_value = _make_raw_upgrade_df([
            {"v1_id": "c", "upgrader_version": "0.2.0", "status": "success", "v2_id": None, "last_modified": None, "upgrade_datetime": None},
        ])
        mock_registry.__getitem__ = MagicMock(return_value=lambda: _make_basics_df())

        result = metadata_upgrade(force_update=True)

        self.assertIn("a", result["_id"].values, "Cached row must still exist")
        self.assertIn("c", result["_id"].values, "New row must be added")
        self.assertIn("0.1.0", result.columns)
        self.assertIn("0.2.0", result.columns)


if __name__ == "__main__":
    unittest.main()

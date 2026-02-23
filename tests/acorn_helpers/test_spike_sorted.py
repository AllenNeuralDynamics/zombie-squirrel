"""Unit tests for spike sorted acorn."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

import zombie_squirrel.acorns as acorns
from zombie_squirrel.acorn_helpers.spike_sorted import (
    _fetch_subject_spike_sorted,
    spike_sorted,
)
from zombie_squirrel.forest import MemoryTree

RAW_NAME = "ecephys_800779_2025-09-26_12-57-44"
DERIVED_SS_NAME = "ecephys_800779_2025-09-26_12-57-44_sorted_2025-09-27_00-00-00"
DERIVED_OTHER_NAME = "ecephys_800779_2025-09-26_12-57-44_other_2025-09-27_00-00-00"

UNITS_DF = pd.DataFrame({"unit_id": [1, 2], "asset_name": [DERIVED_SS_NAME, DERIVED_SS_NAME], "subject_id": ["800779", "800779"]})
SPIKES_DF = pd.DataFrame({"unit_id": [1, 1, 2], "spike_time": [0.1, 0.2, 0.3], "asset_name": [DERIVED_SS_NAME, DERIVED_SS_NAME, DERIVED_SS_NAME], "subject_id": ["800779", "800779", "800779"]})


def _basics_df(subject_id="800779"):
    return pd.DataFrame({
        "name": [RAW_NAME],
        "data_level": ["raw"],
        "subject_id": [subject_id],
    })


class TestFetchSubjectSpikeSorted(unittest.TestCase):
    """Tests for _fetch_subject_spike_sorted."""

    def setUp(self):
        acorns.TREE = MemoryTree()

    @patch("zombie_squirrel.acorn_helpers.spike_sorted._extract_from_nwb")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.raw_to_derived")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.asset_basics")
    def test_caches_spike_sorted_asset(self, mock_basics, mock_r2d, mock_client_class, mock_extract):
        """Test that a spike-sorted derived asset is cached."""
        mock_basics.return_value = _basics_df()
        mock_r2d.return_value = [DERIVED_SS_NAME]
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.retrieve_docdb_records.return_value = [
            {
                "_id": "asset-001",
                "name": DERIVED_SS_NAME,
                "location": "s3://bucket/prefix/",
                "subject": {"subject_id": "800779"},
                "processing": {"data_processes": [{"process_type": "Spike sorting"}]},
            }
        ]
        mock_extract.return_value = (UNITS_DF, SPIKES_DF)

        _fetch_subject_spike_sorted("800779")

        mock_r2d.assert_called_once_with(RAW_NAME, latest=True)
        mock_extract.assert_called_once()

    @patch("zombie_squirrel.acorn_helpers.spike_sorted.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.raw_to_derived")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.asset_basics")
    def test_no_raw_assets_returns_early(self, mock_basics, mock_r2d, mock_client_class):
        """Test that no raw assets causes early return without querying DB."""
        mock_basics.return_value = pd.DataFrame({"name": [], "data_level": [], "subject_id": []})

        _fetch_subject_spike_sorted("800779")

        mock_r2d.assert_not_called()
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.spike_sorted.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.raw_to_derived")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.asset_basics")
    def test_no_derived_assets_returns_early(self, mock_basics, mock_r2d, mock_client_class):
        """Test that no derived assets from raw_to_derived causes early return."""
        mock_basics.return_value = _basics_df()
        mock_r2d.return_value = []

        _fetch_subject_spike_sorted("800779")

        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.spike_sorted._extract_from_nwb")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.raw_to_derived")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.asset_basics")
    def test_non_spike_sorted_derived_skipped(self, mock_basics, mock_r2d, mock_client_class, mock_extract):
        """Test that derived assets without spike sorting are not extracted."""
        mock_basics.return_value = _basics_df()
        mock_r2d.return_value = [DERIVED_OTHER_NAME]
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.retrieve_docdb_records.return_value = [
            {
                "_id": "asset-002",
                "name": DERIVED_OTHER_NAME,
                "location": "s3://bucket/prefix/",
                "subject": {"subject_id": "800779"},
                "processing": {"data_processes": [{"process_type": "Other"}]},
            }
        ]

        _fetch_subject_spike_sorted("800779")

        mock_extract.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.spike_sorted.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.raw_to_derived")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.asset_basics")
    def test_db_returns_empty_records(self, mock_basics, mock_r2d, mock_client_class):
        """Test handling when DB returns no records for the derived names."""
        mock_basics.return_value = _basics_df()
        mock_r2d.return_value = [DERIVED_SS_NAME]
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.retrieve_docdb_records.return_value = []

        _fetch_subject_spike_sorted("800779")

    @patch("zombie_squirrel.acorn_helpers.spike_sorted._extract_from_nwb")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.raw_to_derived")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.asset_basics")
    def test_raw_to_derived_called_per_raw_asset(self, mock_basics, mock_r2d, mock_client_class, mock_extract):
        """Test that raw_to_derived is called once per raw asset."""
        mock_basics.return_value = pd.DataFrame({
            "name": ["raw_a", "raw_b"],
            "data_level": ["raw", "raw"],
            "subject_id": ["800779", "800779"],
        })
        mock_r2d.side_effect = lambda name, latest: [f"{name}_sorted"] if name == "raw_a" else []
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        mock_client.retrieve_docdb_records.return_value = [
            {
                "_id": "asset-001",
                "name": "raw_a_sorted",
                "location": "s3://bucket/prefix/",
                "subject": {"subject_id": "800779"},
                "processing": {"data_processes": [{"process_type": "Spike sorting"}]},
            }
        ]
        mock_extract.return_value = (UNITS_DF, SPIKES_DF)

        _fetch_subject_spike_sorted("800779")

        self.assertEqual(mock_r2d.call_count, 2)
        mock_client.retrieve_docdb_records.assert_called_once()
        filter_used = mock_client.retrieve_docdb_records.call_args[1]["filter_query"]
        self.assertEqual(filter_used, {"name": {"$in": ["raw_a_sorted"]}})

    @patch("zombie_squirrel.acorn_helpers.spike_sorted._extract_from_nwb")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.raw_to_derived")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted.asset_basics")
    def test_only_raw_assets_for_subject_used(self, mock_basics, mock_r2d, mock_client_class, mock_extract):
        """Test that only raw assets matching the subject_id are looked up."""
        mock_basics.return_value = pd.DataFrame({
            "name": [RAW_NAME, "other_subject_raw"],
            "data_level": ["raw", "raw"],
            "subject_id": ["800779", "999999"],
        })
        mock_r2d.return_value = []
        mock_client_class.return_value = MagicMock()

        _fetch_subject_spike_sorted("800779")

        mock_r2d.assert_called_once_with(RAW_NAME, latest=True)


class TestSpikeSortedPublic(unittest.TestCase):
    """Tests for the public spike_sorted function."""

    def setUp(self):
        acorns.TREE = MemoryTree()

    def test_empty_cache_returns_empty_dicts(self):
        """Test that empty cache returns empty DataFrames without force_update."""
        result = spike_sorted("nonexistent-subject", force_update=False)

        self.assertIsInstance(result, dict)
        self.assertTrue(result["units"].empty)
        self.assertTrue(result["spikes"].empty)

    @patch("zombie_squirrel.acorn_helpers.spike_sorted._fetch_subject_spike_sorted")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted._scurry_partitioned")
    def test_force_update_calls_fetch(self, mock_scurry, mock_fetch):
        """Test that force_update=True calls _fetch_subject_spike_sorted."""
        mock_scurry.return_value = pd.DataFrame()

        spike_sorted("800779", force_update=True)

        mock_fetch.assert_called_once_with("800779")

    @patch("zombie_squirrel.acorn_helpers.spike_sorted._fetch_subject_spike_sorted")
    @patch("zombie_squirrel.acorn_helpers.spike_sorted._scurry_partitioned")
    def test_no_force_update_skips_fetch(self, mock_scurry, mock_fetch):
        """Test that force_update=False does not call _fetch_subject_spike_sorted."""
        mock_scurry.return_value = pd.DataFrame()

        spike_sorted("800779", force_update=False)

        mock_fetch.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.spike_sorted._scurry_partitioned")
    def test_asset_names_string_filter(self, mock_scurry):
        """Test filtering by a single asset name string."""
        units = pd.DataFrame({"asset_name": ["a", "b"], "subject_id": ["800779", "800779"]})
        spikes = pd.DataFrame({"asset_name": ["a", "b"], "subject_id": ["800779", "800779"]})
        mock_scurry.side_effect = lambda table, _: units if "units" in table else spikes

        result = spike_sorted("800779", asset_names="a")

        self.assertEqual(len(result["units"]), 1)
        self.assertEqual(result["units"].iloc[0]["asset_name"], "a")
        self.assertEqual(len(result["spikes"]), 1)

    @patch("zombie_squirrel.acorn_helpers.spike_sorted._scurry_partitioned")
    def test_asset_names_list_filter(self, mock_scurry):
        """Test filtering by a list of asset names."""
        units = pd.DataFrame({"asset_name": ["a", "b", "c"], "subject_id": ["800779"] * 3})
        spikes = pd.DataFrame({"asset_name": ["a", "b", "c"], "subject_id": ["800779"] * 3})
        mock_scurry.side_effect = lambda table, _: units if "units" in table else spikes

        result = spike_sorted("800779", asset_names=["a", "c"])

        self.assertCountEqual(result["units"]["asset_name"].tolist(), ["a", "c"])
        self.assertNotIn("b", result["units"]["asset_name"].values)

    @patch("zombie_squirrel.acorn_helpers.spike_sorted._scurry_partitioned")
    def test_asset_names_missing_returns_empty(self, mock_scurry):
        """Test filtering by a non-existent asset name returns empty DataFrames."""
        units = pd.DataFrame({"asset_name": ["a"], "subject_id": ["800779"]})
        spikes = pd.DataFrame({"asset_name": ["a"], "subject_id": ["800779"]})
        mock_scurry.side_effect = lambda table, _: units if "units" in table else spikes

        result = spike_sorted("800779", asset_names="nonexistent")

        self.assertTrue(result["units"].empty)
        self.assertTrue(result["spikes"].empty)

    @patch("zombie_squirrel.acorn_helpers.spike_sorted._scurry_partitioned")
    def test_lazy_returns_paths(self, mock_scurry):
        """Test lazy=True returns glob path strings."""
        result = spike_sorted("800779", lazy=True)

        self.assertIsInstance(result["units"], str)
        self.assertIsInstance(result["spikes"], str)
        mock_scurry.assert_not_called()


if __name__ == "__main__":
    unittest.main()

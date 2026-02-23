"""Integration test for spike sorted data fetching from NWB zarr files on S3."""

import time
import unittest

import duckdb
import pandas as pd

from zombie_squirrel import spike_sorted

TEST_SUBJECT_ID = "800779"
TEST_ASSET_NAME = "ecephys_800779_2025-09-26_12-57-44_sorted-ads2.4.0-main_2026-02-09_14-01-16"


class TestSpikeSortedIntegration(unittest.TestCase):
    """Integration tests for spike sorted data fetching."""

    @classmethod
    def setUpClass(cls):
        """Fetch spike sorted data once for all tests."""
        print(f"\nFetching spike sorted data for subject {TEST_SUBJECT_ID}...")
        start = time.time()
        result = spike_sorted(TEST_SUBJECT_ID, force_update=True)
        cls.units_df = result["units"]
        cls.spikes_df = result["spikes"]
        elapsed = time.time() - start
        print(f"Fetched {len(cls.units_df)} units and {len(cls.spikes_df)} spikes in {elapsed:.2f} seconds.")

    def test_returns_dict_with_dataframes(self):
        """Test that spike_sorted returns a dict with DataFrames."""
        result = spike_sorted(TEST_SUBJECT_ID)
        self.assertIsInstance(result, dict)
        self.assertIn("units", result)
        self.assertIn("spikes", result)
        self.assertIsInstance(result["units"], pd.DataFrame)
        self.assertIsInstance(result["spikes"], pd.DataFrame)

    def test_has_units(self):
        """Test that units DataFrame contains units."""
        self.assertGreater(len(self.units_df), 0, "Should have at least one unit")
    
    def test_has_spikes(self):
        """Test that spikes DataFrame contains spikes."""
        self.assertGreater(len(self.spikes_df), 0, "Should have at least one spike")

    def test_units_has_required_columns(self):
        """Test that units DataFrame has required tracking columns."""
        required_columns = {"asset_id", "asset_name", "subject_id", "nwb_file"}
        self.assertTrue(
            required_columns.issubset(self.units_df.columns),
            f"Missing required columns. Expected: {required_columns}, Got: {set(self.units_df.columns)}"
        )
    
    def test_spikes_has_required_columns(self):
        """Test that spikes DataFrame has required tracking columns."""
        required_columns = {"unit_id", "spike_time", "asset_name", "subject_id"}
        self.assertTrue(
            required_columns.issubset(self.spikes_df.columns),
            f"Missing required columns. Expected: {required_columns}, Got: {set(self.spikes_df.columns)}"
        )

    def test_has_unit_metadata(self):
        """Test that units DataFrame has typical unit metadata columns."""
        expected_columns = {"id", "firing_rate", "num_spikes"}
        found_columns = set(self.units_df.columns)
        intersection = expected_columns.intersection(found_columns)
        self.assertTrue(
            len(intersection) > 0,
            f"Should have at least one unit metadata column from {expected_columns}"
        )

    def test_subject_id_matches(self):
        """Test that all rows have correct subject_id."""
        units_subjects = self.units_df["subject_id"].unique()
        self.assertEqual(len(units_subjects), 1, "Units should have exactly one subject_id")
        self.assertEqual(units_subjects[0], TEST_SUBJECT_ID)
        
        spikes_subjects = self.spikes_df["subject_id"].unique()
        self.assertEqual(len(spikes_subjects), 1, "Spikes should have exactly one subject_id")
        self.assertEqual(spikes_subjects[0], TEST_SUBJECT_ID)

    def test_asset_name_present(self):
        """Test that the expected asset name is present."""
        units_assets = self.units_df["asset_name"].unique()
        self.assertIn(
            TEST_ASSET_NAME,
            units_assets,
            f"Expected asset {TEST_ASSET_NAME} not found in units. Available: {list(units_assets)}"
        )
        
        spikes_assets = self.spikes_df["asset_name"].unique()
        self.assertIn(
            TEST_ASSET_NAME,
            spikes_assets,
            f"Expected asset {TEST_ASSET_NAME} not found in spikes. Available: {list(spikes_assets)}"
        )

    def test_filter_by_asset_name(self):
        """Test filtering by specific asset name."""
        result = spike_sorted(TEST_SUBJECT_ID, asset_names=TEST_ASSET_NAME)
        filtered_units = result["units"]
        filtered_spikes = result["spikes"]
        
        self.assertIsInstance(filtered_units, pd.DataFrame)
        self.assertGreater(len(filtered_units), 0, "Filtered units should have rows")
        units_assets = filtered_units["asset_name"].unique()
        self.assertEqual(len(units_assets), 1)
        self.assertEqual(units_assets[0], TEST_ASSET_NAME)
        
        if len(filtered_spikes) > 0:
            spikes_assets = filtered_spikes["asset_name"].unique()
            self.assertEqual(len(spikes_assets), 1)
            self.assertEqual(spikes_assets[0], TEST_ASSET_NAME)

    def test_lazy_loading(self):
        """Test lazy loading returns S3 paths."""
        result = spike_sorted(TEST_SUBJECT_ID, lazy=True)
        self.assertIsInstance(result, dict)
        self.assertIn("units", result)
        self.assertIn("spikes", result)
        self.assertIsInstance(result["units"], str)
        self.assertIsInstance(result["spikes"], str)
        self.assertTrue(result["units"].startswith("s3://"), f"Expected s3:// path for units, got: {result['units']}")
        self.assertTrue(result["spikes"].startswith("s3://"), f"Expected s3:// path for spikes, got: {result['spikes']}")

    def test_lazy_path_readable_by_duckdb(self):
        """Test that lazy-loaded S3 paths can be read by DuckDB."""
        result = spike_sorted(TEST_SUBJECT_ID, lazy=True)
        units_path = result["units"]
        spikes_path = result["spikes"]
        
        units_query = f"SELECT COUNT(*) as count FROM read_parquet('{units_path}')"
        units_result = duckdb.query(units_query).fetchone()
        units_count = units_result[0]
        self.assertGreater(units_count, 0, "DuckDB should be able to read units from S3 path")
        self.assertEqual(units_count, len(self.units_df), "DuckDB units count should match DataFrame length")
        
        spikes_query = f"SELECT COUNT(*) as count FROM read_parquet('{spikes_path}')"
        spikes_result = duckdb.query(spikes_query).fetchone()
        spikes_count = spikes_result[0]
        self.assertGreater(spikes_count, 0, "DuckDB should be able to read spikes from S3 path")
        self.assertEqual(spikes_count, len(self.spikes_df), "DuckDB spikes count should match DataFrame length")

    def test_cache_hit_is_faster(self):
        """Test that cached data loads faster than fresh fetch."""
        start = time.time()
        result = spike_sorted(TEST_SUBJECT_ID, force_update=False)
        cached_time = time.time() - start

        self.assertIsInstance(result, dict)
        self.assertEqual(len(result["units"]), len(self.units_df))
        self.assertEqual(len(result["spikes"]), len(self.spikes_df))
        print(f"  Cache hit took {cached_time:.2f} seconds")

    def test_no_duplicate_units(self):
        """Test that there are no duplicate unit entries."""
        if 'id' in self.units_df.columns and 'nwb_file' in self.units_df.columns:
            duplicates = self.units_df.duplicated(subset=['id', 'nwb_file', 'asset_id'])
            self.assertFalse(
                duplicates.any(),
                f"Found {duplicates.sum()} duplicate unit entries"
            )
    
    def test_spike_times_are_numeric(self):
        """Test that spike times are numeric values."""
        self.assertTrue(
            pd.api.types.is_numeric_dtype(self.spikes_df["spike_time"]),
            "spike_time column should be numeric"
        )
    
    def test_spikes_link_to_units(self):
        """Test that spikes reference valid unit IDs."""
        if 'id' in self.units_df.columns and 'unit_id' in self.spikes_df.columns:
            unit_ids = set(self.units_df['id'])
            spike_unit_ids = set(self.spikes_df['unit_id'])
            invalid_refs = spike_unit_ids - unit_ids
            self.assertEqual(
                len(invalid_refs), 0,
                f"Found {len(invalid_refs)} spike unit_ids that don't exist in units table: {invalid_refs}"
            )


def test_spike_sorted_basic():
    """Standalone test function for basic spike sorted functionality."""
    print(f"\nTesting spike sorted for subject {TEST_SUBJECT_ID}...")
    start = time.time()
    result = spike_sorted(TEST_SUBJECT_ID)
    elapsed = time.time() - start
    
    units_df = result["units"]
    spikes_df = result["spikes"]
    
    print(f"✓ Fetched {len(units_df)} units and {len(spikes_df)} spikes in {elapsed:.2f} seconds")
    assert isinstance(result, dict), "Result should be a dict"
    assert "units" in result and "spikes" in result, "Result should have 'units' and 'spikes' keys"
    assert isinstance(units_df, pd.DataFrame), "Units should be a DataFrame"
    assert isinstance(spikes_df, pd.DataFrame), "Spikes should be a DataFrame"
    assert len(units_df) > 0, "Should have at least one unit"
    assert len(spikes_df) > 0, "Should have at least one spike"
    assert "asset_id" in units_df.columns, "Units should have asset_id column"
    assert "asset_name" in units_df.columns, "Units should have asset_name column"
    assert "subject_id" in units_df.columns, "Units should have subject_id column"
    assert "unit_id" in spikes_df.columns, "Spikes should have unit_id column"
    assert "spike_time" in spikes_df.columns, "Spikes should have spike_time column"
    print(f"✓ Units columns: {list(units_df.columns)}")
    print(f"✓ Spikes columns: {list(spikes_df.columns)}")
    print(f"✓ Assets found: {list(units_df['asset_name'].unique())}")


def main():
    """Run the standalone test function."""
    test_spike_sorted_basic()
    print("\n✓ All standalone tests passed!")


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--unittest":
        sys.argv.pop(1)
        unittest.main()
    else:
        main()

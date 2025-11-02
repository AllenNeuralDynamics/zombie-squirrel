"""Unit tests for zombie_squirrel.squirrels module.

Tests for squirrel functions, caching, and registry mechanism."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.acorns import MemoryAcorn
from zombie_squirrel.squirrels import (
    NAMES,
    SQUIRREL_REGISTRY,
    asset_basics,
    raw_to_derived,
    source_data,
    unique_project_names,
    unique_subject_ids,
)


class TestSquirrelRegistration(unittest.TestCase):
    """Tests for squirrel registration mechanism."""

    def test_squirrel_registry_contains_all_functions(self):
        """Test that all squirrel functions are registered."""
        self.assertIn(NAMES["upn"], SQUIRREL_REGISTRY)
        self.assertIn(NAMES["usi"], SQUIRREL_REGISTRY)
        self.assertIn(NAMES["basics"], SQUIRREL_REGISTRY)
        self.assertIn(NAMES["d2r"], SQUIRREL_REGISTRY)
        self.assertIn(NAMES["r2d"], SQUIRREL_REGISTRY)

    def test_registry_values_are_callable(self):
        """Test that registry values are callable functions."""
        for name, func in SQUIRREL_REGISTRY.items():
            self.assertTrue(callable(func), f"{name} is not callable")

    def test_names_dict_completeness(self):
        """Test that NAMES dict has expected keys."""
        expected_keys = ["upn", "usi", "basics", "d2r", "r2d"]
        for key in expected_keys:
            self.assertIn(key, NAMES)


class TestUniqueProjectNames(unittest.TestCase):
    """Tests for unique_project_names squirrel."""

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_unique_project_names_cache_hit(self, mock_client_class, mock_acorn):
        """Test returning cached project names."""
        cached_df = pd.DataFrame({"project_name": ["proj1", "proj2", "proj3"]})
        mock_acorn.hide(NAMES["upn"], cached_df)

        result = unique_project_names()

        self.assertEqual(result, ["proj1", "proj2", "proj3"])
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_unique_project_names_cache_miss(self, mock_client_class, mock_acorn):
        """Test fetching project names when cache is empty."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.aggregate_docdb_records.return_value = [
            {"project_name": "proj1"},
            {"project_name": "proj2"},
        ]

        result = unique_project_names()

        self.assertEqual(result, ["proj1", "proj2"])
        mock_client_class.assert_called_once()
        mock_client_instance.aggregate_docdb_records.assert_called_once()

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_unique_project_names_force_update(self, mock_client_class, mock_acorn):
        """Test force_update bypasses cache."""
        cached_df = pd.DataFrame({"project_name": ["old_proj"]})
        mock_acorn.hide(NAMES["upn"], cached_df)

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.aggregate_docdb_records.return_value = [{"project_name": "new_proj"}]

        result = unique_project_names(force_update=True)

        self.assertEqual(result, ["new_proj"])
        mock_client_instance.aggregate_docdb_records.assert_called_once()


class TestUniqueSubjectIds(unittest.TestCase):
    """Tests for unique_subject_ids squirrel."""

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_unique_subject_ids_cache_hit(self, mock_client_class, mock_acorn):
        """Test returning cached subject IDs."""
        cached_df = pd.DataFrame({"subject_id": ["sub001", "sub002"]})
        mock_acorn.hide(NAMES["usi"], cached_df)

        result = unique_subject_ids()

        self.assertEqual(result, ["sub001", "sub002"])
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_unique_subject_ids_cache_miss(self, mock_client_class, mock_acorn):
        """Test fetching subject IDs when cache is empty."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.aggregate_docdb_records.return_value = [
            {"subject_id": "sub001"},
            {"subject_id": "sub002"},
        ]

        result = unique_subject_ids()

        self.assertEqual(result, ["sub001", "sub002"])
        mock_client_class.assert_called_once()

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_unique_subject_ids_force_update(self, mock_client_class, mock_acorn):
        """Test force_update bypasses cache."""
        cached_df = pd.DataFrame({"subject_id": ["old_sub"]})
        mock_acorn.hide(NAMES["usi"], cached_df)

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.aggregate_docdb_records.return_value = [{"subject_id": "new_sub"}]

        result = unique_subject_ids(force_update=True)

        self.assertEqual(result, ["new_sub"])


class TestAssetBasics(unittest.TestCase):
    """Tests for asset_basics squirrel."""

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_asset_basics_cache_hit(self, mock_client_class, mock_acorn):
        """Test returning cached asset basics."""
        cached_df = pd.DataFrame(
            {
                "_id": ["id1", "id2"],
                "_last_modified": ["2023-01-01", "2023-01-02"],
                "modalities": ["imaging", "electrophysiology"],
                "project_name": ["proj1", "proj2"],
                "data_level": ["raw", "derived"],
                "subject_id": ["sub001", "sub002"],
                "acquisition_start_time": [
                    "2023-01-01T10:00:00",
                    "2023-01-02T10:00:00",
                ],
                "acquisition_end_time": [
                    "2023-01-01T11:00:00",
                    "2023-01-02T11:00:00",
                ],
            }
        )
        mock_acorn.hide(NAMES["basics"], cached_df)

        result = asset_basics()

        self.assertEqual(len(result), 2)
        self.assertListEqual(list(result["_id"]), ["id1", "id2"])
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_asset_basics_cache_miss(self, mock_client_class, mock_acorn):
        """Test fetching asset basics when cache is empty."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "id1",
                "_last_modified": "2023-01-01",
                "data_description": {
                    "modalities": [{"abbreviation": "img"}],
                    "project_name": "proj1",
                    "data_level": "raw",
                },
                "subject": {"subject_id": "sub001"},
                "acquisition": {
                    "acquisition_start_time": "2023-01-01T10:00:00",
                    "acquisition_end_time": "2023-01-01T11:00:00",
                },
            }
        ]

        result = asset_basics()

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["_id"], "id1")
        self.assertEqual(result.iloc[0]["modalities"], "img")
        self.assertEqual(result.iloc[0]["project_name"], "proj1")

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_asset_basics_incremental_update(self, mock_client_class, mock_acorn):
        """Test incremental cache update with partial data refresh."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.side_effect = [
            [
                {"_id": "id1", "_last_modified": "2023-01-01"},
                {"_id": "id2", "_last_modified": "2023-01-02"},
            ],  # First call: shows id2 is new
            [
                {
                    "_id": "id2",
                    "_last_modified": "2023-01-02",
                    "data_description": {
                        "modalities": [{"abbreviation": "elec"}],
                        "project_name": "proj2",
                        "data_level": "derived",
                    },
                    "subject": {"subject_id": "sub002"},
                    "acquisition": {
                        "acquisition_start_time": "2023-01-02T10:00:00",
                        "acquisition_end_time": "2023-01-02T11:00:00",
                    },
                }
            ],  # Second call: batch fetch for new record
        ]

        result = asset_basics()

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["_id"], "id2")


class TestSourceData(unittest.TestCase):
    """Tests for source_data squirrel."""

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_source_data_cache_hit(self, mock_client_class, mock_acorn):
        """Test returning cached source data."""
        cached_df = pd.DataFrame(
            {
                "_id": ["id1", "id2"],
                "source_data": ["source1, source2", "source3"],
            }
        )
        mock_acorn.hide(NAMES["d2r"], cached_df)

        result = source_data()

        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["source_data"], "source1, source2")
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_source_data_cache_miss(self, mock_client_class, mock_acorn):
        """Test fetching source data when cache is empty."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [
            {
                "_id": "id1",
                "data_description": {"source_data": ["src1", "src2"]},
            },
            {"_id": "id2", "data_description": {"source_data": []}},
        ]

        result = source_data()

        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["source_data"], "src1, src2")
        self.assertEqual(result.iloc[1]["source_data"], "")

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_source_data_force_update(self, mock_client_class, mock_acorn):
        """Test force_update bypasses cache."""
        cached_df = pd.DataFrame(
            {
                "_id": ["old_id"],
                "source_data": ["old_source"],
            }
        )
        mock_acorn.hide(NAMES["d2r"], cached_df)

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


class TestRawToDerived(unittest.TestCase):
    """Tests for raw_to_derived squirrel."""

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_raw_to_derived_cache_hit(self, mock_client_class, mock_acorn):
        """Test returning cached raw to derived mapping."""
        cached_df = pd.DataFrame(
            {
                "_id": ["raw1", "raw2"],
                "derived_records": ["derived1, derived2", "derived3"],
            }
        )
        mock_acorn.hide(NAMES["r2d"], cached_df)

        result = raw_to_derived()

        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["derived_records"], "derived1, derived2")
        mock_client_class.assert_not_called()

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_raw_to_derived_cache_miss(self, mock_client_class, mock_acorn):
        """Test fetching raw to derived mapping when cache is empty."""
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

        result = raw_to_derived()

        self.assertEqual(len(result), 2)
        raw1_row = result[result["_id"] == "raw1"]
        raw2_row = result[result["_id"] == "raw2"]
        self.assertEqual(raw1_row.iloc[0]["derived_records"], "derived1, derived2")
        self.assertEqual(raw2_row.iloc[0]["derived_records"], "derived2")

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_raw_to_derived_no_derived(self, mock_client_class, mock_acorn):
        """Test raw records with no derived data."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance

        mock_client_instance.retrieve_docdb_records.side_effect = [
            [{"_id": "raw1"}],  # Raw records
            [],  # No derived records
        ]

        result = raw_to_derived()

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["derived_records"], "")

    @patch("zombie_squirrel.squirrels.ACORN", new_callable=MemoryAcorn)
    @patch("zombie_squirrel.squirrels.MetadataDbClient")
    def test_raw_to_derived_force_update(self, mock_client_class, mock_acorn):
        """Test force_update bypasses cache."""
        cached_df = pd.DataFrame(
            {
                "_id": ["old_raw"],
                "derived_records": ["old_derived"],
            }
        )
        mock_acorn.hide(NAMES["r2d"], cached_df)

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

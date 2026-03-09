"""Unit tests for behavior_trials acorn."""

import unittest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from zombie_squirrel.acorn_helpers.behavior_trials import (
    _read_trials_from_nwb_zarr,
    _update_behavior_index,
    behavior_trials,
    behavior_trials_columns,
)


class TestReadTrialsFromNwbZarr(unittest.TestCase):
    """Tests for _read_trials_from_nwb_zarr."""

    @patch("s3fs.S3FileSystem")
    def test_returns_none_when_zarr_not_found(self, mock_fs_class):
        """Test returns None when behavior.nwb.zarr does not exist."""
        mock_fs = MagicMock()
        mock_fs_class.return_value = mock_fs
        mock_fs.exists.return_value = False

        result = _read_trials_from_nwb_zarr("s3://bucket/asset_name")

        self.assertIsNone(result)
        mock_fs.exists.assert_called_once_with("bucket/asset_name/behavior.nwb.zarr")

    @patch("zarr.open_group")
    @patch("s3fs.S3Map")
    @patch("s3fs.S3FileSystem")
    def test_returns_none_when_no_intervals_trials(self, mock_fs_class, mock_s3map, mock_open_group):
        """Test returns None when intervals/trials group is absent."""
        mock_fs = MagicMock()
        mock_fs_class.return_value = mock_fs
        mock_fs.exists.return_value = True

        mock_root = MagicMock()
        mock_root.__getitem__ = MagicMock(side_effect=KeyError("intervals/trials"))
        mock_open_group.return_value = mock_root

        result = _read_trials_from_nwb_zarr("s3://bucket/asset_name")

        self.assertIsNone(result)

    @patch("zarr.open_group")
    @patch("s3fs.S3Map")
    @patch("s3fs.S3FileSystem")
    def test_reads_trials_from_zarr_arrays(self, mock_fs_class, mock_s3map, mock_open_group):
        """Test reads trials table from zarr Array columns (hasattr shape)."""
        mock_fs = MagicMock()
        mock_fs_class.return_value = mock_fs
        mock_fs.exists.return_value = True

        mock_trials = MagicMock()
        mock_trials.attrs = {"colnames": ["start_time", "stop_time"]}

        def make_array_col(values):
            col = MagicMock()
            col.shape = (len(values),)
            col.__getitem__ = lambda self, key: np.array(values)
            return col

        id_array = make_array_col([0, 1, 2])
        start_array = make_array_col([0.0, 1.0, 2.0])
        stop_array = make_array_col([0.5, 1.5, 2.5])

        def trials_getitem(key):
            return {"id": id_array, "start_time": start_array, "stop_time": stop_array}[key]

        mock_trials.__contains__ = lambda self, key: key in ["id", "start_time", "stop_time"]
        mock_trials.__getitem__ = MagicMock(side_effect=trials_getitem)

        mock_root = MagicMock()
        mock_root.__getitem__ = MagicMock(return_value=mock_trials)
        mock_open_group.return_value = mock_root

        result = _read_trials_from_nwb_zarr("s3://bucket/asset_name")

        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    @patch("zarr.open_group")
    @patch("s3fs.S3Map")
    @patch("s3fs.S3FileSystem")
    def test_reads_trials_from_zarr_group_data(self, mock_fs_class, mock_s3map, mock_open_group):
        """Test reads trials from VectorData-style zarr Groups (with data child)."""
        mock_fs = MagicMock()
        mock_fs_class.return_value = mock_fs
        mock_fs.exists.return_value = True

        mock_trials = MagicMock()
        mock_trials.attrs = {"colnames": ["start_time"]}

        def make_group_col(values):
            col = MagicMock(spec=["__contains__", "__getitem__"])
            data_inner = MagicMock()
            data_inner.__getitem__ = lambda self, key: np.array(values)
            col.__contains__ = lambda self, key: key == "data"
            col.__getitem__ = MagicMock(return_value=data_inner)
            return col

        id_col = make_group_col([0, 1])
        start_col = make_group_col([0.0, 1.0])

        def trials_getitem(key):
            return {"id": id_col, "start_time": start_col}[key]

        mock_trials.__contains__ = lambda self, key: key in ["id", "start_time"]
        mock_trials.__getitem__ = MagicMock(side_effect=trials_getitem)

        mock_root = MagicMock()
        mock_root.__getitem__ = MagicMock(return_value=mock_trials)
        mock_open_group.return_value = mock_root

        result = _read_trials_from_nwb_zarr("s3://bucket/asset_name")

        self.assertIsNotNone(result)
        self.assertIsInstance(result, pd.DataFrame)

    @patch("zarr.open_group")
    @patch("s3fs.S3Map")
    @patch("s3fs.S3FileSystem")
    def test_returns_none_when_open_group_raises(self, mock_fs_class, mock_s3map, mock_open_group):
        """Test returns None when zarr.open_group raises an exception."""
        mock_fs = MagicMock()
        mock_fs_class.return_value = mock_fs
        mock_fs.exists.return_value = True
        mock_open_group.side_effect = Exception("zarr error")

        result = _read_trials_from_nwb_zarr("s3://bucket/asset_name")

        self.assertIsNone(result)


class TestUpdateBehaviorIndex(unittest.TestCase):
    """Tests for _update_behavior_index."""

    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_empty_rows_does_nothing(self, mock_tree):
        """Test that empty new_rows list does nothing."""
        _update_behavior_index([])
        mock_tree.scurry.assert_not_called()
        mock_tree.hide.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_creates_new_index_when_existing_empty(self, mock_tree):
        """Test creates index from scratch when no existing index."""
        mock_tree.scurry.return_value = pd.DataFrame()
        new_rows = [
            {"asset_name": "asset1", "has_behavior": True},
            {"asset_name": "asset2", "has_behavior": False},
        ]

        _update_behavior_index(new_rows)

        mock_tree.hide.assert_called_once()
        saved_df = mock_tree.hide.call_args[0][1]
        self.assertEqual(len(saved_df), 2)
        self.assertIn("asset1", saved_df["asset_name"].values)
        self.assertIn("asset2", saved_df["asset_name"].values)

    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_merges_with_existing_index(self, mock_tree):
        """Test merges new rows, replacing entries for existing asset names."""
        existing = pd.DataFrame([{"asset_name": "asset1", "has_behavior": False}])
        mock_tree.scurry.return_value = existing

        new_rows = [{"asset_name": "asset1", "has_behavior": True}]
        _update_behavior_index(new_rows)

        saved_df = mock_tree.hide.call_args[0][1]
        self.assertEqual(len(saved_df), 1)
        row = saved_df[saved_df["asset_name"] == "asset1"].iloc[0]
        self.assertTrue(row["has_behavior"])

    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_preserves_unaffected_existing_rows(self, mock_tree):
        """Test existing rows for different asset names are preserved."""
        existing = pd.DataFrame(
            [
                {"asset_name": "asset1", "has_behavior": True},
                {"asset_name": "asset2", "has_behavior": False},
            ]
        )
        mock_tree.scurry.return_value = existing

        new_rows = [{"asset_name": "asset3", "has_behavior": True}]
        _update_behavior_index(new_rows)

        saved_df = mock_tree.hide.call_args[0][1]
        self.assertEqual(len(saved_df), 3)


class TestBehaviorTrials(unittest.TestCase):
    """Tests for behavior_trials acorn function."""

    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_cache_hit_returns_cached_data(self, mock_tree):
        """Test returns cached data when available and no force_update."""
        cached_df = pd.DataFrame(
            {
                "asset_name": ["asset1"],
                "subject_id": ["sub001"],
                "id": [0],
                "start_time": [0.0],
                "stop_time": [1.0],
            }
        )
        mock_tree.scurry.return_value = cached_df

        result = behavior_trials("sub001", force_update=False)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["asset_name"], "asset1")

    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_empty_cache_no_force_update_returns_empty(self, mock_tree):
        """Test returns empty df with warning when cache is empty and no force_update."""
        mock_tree.scurry.return_value = pd.DataFrame()

        result = behavior_trials("sub001", force_update=False)

        self.assertTrue(result.empty)

    @patch("zombie_squirrel.acorn_helpers.behavior_trials._read_trials_from_nwb_zarr")
    @patch("zombie_squirrel.acorn_helpers.behavior_trials.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_force_update_fetches_from_db(self, mock_tree, mock_client_class, mock_read_zarr):
        """Test force_update queries DocDB and reads zarr."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [
            {"_id": "id1", "name": "asset1", "location": "s3://bucket/asset1"}
        ]
        mock_read_zarr.return_value = pd.DataFrame(
            {"id": [0], "start_time": [0.0], "stop_time": [1.0]}
        )

        result = behavior_trials("sub001", force_update=True)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["asset_name"], "asset1")
        self.assertEqual(result.iloc[0]["subject_id"], "sub001")

    @patch("zombie_squirrel.acorn_helpers.behavior_trials._read_trials_from_nwb_zarr")
    @patch("zombie_squirrel.acorn_helpers.behavior_trials.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_nwb_not_found_sets_has_behavior_false(self, mock_tree, mock_client_class, mock_read_zarr):
        """Test has_behavior=False in index when zarr not found."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [
            {"_id": "id1", "name": "asset1", "location": "s3://bucket/asset1"}
        ]
        mock_read_zarr.return_value = None

        behavior_trials("sub001", force_update=True)

        hide_calls = mock_tree.hide.call_args_list
        index_call = next(c for c in hide_calls if c[0][0] == "behavior_trials_index")
        index_df = index_call[0][1]
        row = index_df[index_df["asset_name"] == "asset1"].iloc[0]
        self.assertFalse(row["has_behavior"])

    @patch("zombie_squirrel.acorn_helpers.behavior_trials._read_trials_from_nwb_zarr")
    @patch("zombie_squirrel.acorn_helpers.behavior_trials.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_asset_with_no_location_sets_has_behavior_false(self, mock_tree, mock_client_class, mock_read_zarr):
        """Test has_behavior=False when asset has no location field."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = [
            {"_id": "id1", "name": "asset1", "location": ""}
        ]

        behavior_trials("sub001", force_update=True)

        mock_read_zarr.assert_not_called()
        hide_calls = mock_tree.hide.call_args_list
        index_call = next(c for c in hide_calls if c[0][0] == "behavior_trials_index")
        index_df = index_call[0][1]
        row = index_df[index_df["asset_name"] == "asset1"].iloc[0]
        self.assertFalse(row["has_behavior"])

    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_lazy_mode_returns_s3_path(self, mock_tree):
        """Test lazy=True returns the S3 path string."""
        mock_tree.get_location.return_value = "s3://bucket/path/behavior_trials/sub001.pqt"
        mock_tree.scurry.return_value = pd.DataFrame()

        result = behavior_trials("sub001", lazy=True)

        self.assertIsInstance(result, str)
        self.assertIn("sub001", result)

    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_asset_names_filter_string(self, mock_tree):
        """Test filtering by a single asset name string."""
        cached_df = pd.DataFrame(
            {
                "asset_name": ["asset1", "asset2"],
                "subject_id": ["sub001", "sub001"],
                "id": [0, 1],
            }
        )
        mock_tree.scurry.return_value = cached_df

        result = behavior_trials("sub001", asset_names="asset1", force_update=False)

        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["asset_name"], "asset1")

    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_asset_names_filter_list(self, mock_tree):
        """Test filtering by a list of asset names."""
        cached_df = pd.DataFrame(
            {
                "asset_name": ["asset1", "asset2", "asset3"],
                "subject_id": ["sub001", "sub001", "sub001"],
                "id": [0, 1, 2],
            }
        )
        mock_tree.scurry.return_value = cached_df

        result = behavior_trials("sub001", asset_names=["asset1", "asset3"], force_update=False)

        self.assertEqual(len(result), 2)
        self.assertListEqual(sorted(result["asset_name"].tolist()), ["asset1", "asset3"])

    @patch("zombie_squirrel.acorn_helpers.behavior_trials._read_trials_from_nwb_zarr")
    @patch("zombie_squirrel.acorn_helpers.behavior_trials.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_empty_result_when_no_records(self, mock_tree, mock_client_class, mock_read_zarr):
        """Test returns empty DataFrame when no records found in DocDB."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = []

        result = behavior_trials("sub001", force_update=True)

        self.assertTrue(result.empty)
        mock_read_zarr.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.behavior_trials._read_trials_from_nwb_zarr")
    @patch("zombie_squirrel.acorn_helpers.behavior_trials.MetadataDbClient")
    @patch("zombie_squirrel.acorn_helpers.behavior_trials.acorns.TREE")
    def test_db_query_uses_correct_filter(self, mock_tree, mock_client_class, mock_read_zarr):
        """Test DocDB query filters for derived behavior assets."""
        mock_tree.scurry.return_value = pd.DataFrame()
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_client_instance.retrieve_docdb_records.return_value = []

        behavior_trials("sub001", force_update=True)

        call_kwargs = mock_client_instance.retrieve_docdb_records.call_args[1]
        fq = call_kwargs["filter_query"]
        self.assertEqual(fq["subject.subject_id"], "sub001")
        self.assertEqual(fq["data_description.data_level"], "derived")
        self.assertIn("data_description.modalities", fq)


class TestBehaviorTrialsColumns(unittest.TestCase):
    """Tests for behavior_trials_columns."""

    def test_returns_list_of_columns(self):
        """Test returns non-empty list of Column objects."""
        cols = behavior_trials_columns()
        self.assertIsInstance(cols, list)
        self.assertGreater(len(cols), 0)

    def test_contains_required_columns(self):
        """Test that core columns are present."""
        cols = behavior_trials_columns()
        col_names = {c.name for c in cols}
        for required in ("asset_name", "subject_id"):
            self.assertIn(required, col_names)


if __name__ == "__main__":
    unittest.main()

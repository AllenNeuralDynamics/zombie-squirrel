"""Unit tests for zombie_squirrel.sync module."""

import json
import unittest
from unittest.mock import MagicMock, call, patch

import pandas as pd

from zombie_squirrel.sync import hide_acorns, publish_squirrel_metadata


class TestHideAcorns(unittest.TestCase):
    def _make_registry(self, mock_upn, mock_usi, mock_basics, mock_d2r, mock_r2d, mock_qc):
        return {
            "unique_project_names": mock_upn,
            "unique_subject_ids": mock_usi,
            "asset_basics": mock_basics,
            "source_data": mock_d2r,
            "raw_to_derived": mock_r2d,
            "quality_control": mock_qc,
        }

    @patch("zombie_squirrel.sync.publish_squirrel_metadata")
    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_all_acorns_called_with_force_update(self, mock_registry, mock_publish):
        df_basics = pd.DataFrame({"subject_id": ["sub1"]})
        mock_basics = MagicMock(return_value=df_basics)
        mock_upn = MagicMock()
        mock_usi = MagicMock()
        mock_d2r = MagicMock()
        mock_r2d = MagicMock()
        mock_qc = MagicMock()
        mock_registry.__getitem__.side_effect = self._make_registry(
            mock_upn, mock_usi, mock_basics, mock_d2r, mock_r2d, mock_qc
        ).__getitem__

        hide_acorns()

        mock_upn.assert_called_once_with(force_update=True)
        mock_usi.assert_called_once_with(force_update=True)
        mock_basics.assert_called_once_with(force_update=True)
        mock_d2r.assert_called_once_with(force_update=True)
        mock_r2d.assert_not_called()

    @patch("zombie_squirrel.sync.publish_squirrel_metadata")
    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_qc_called_per_subject(self, mock_registry, mock_publish):
        df_basics = pd.DataFrame({"subject_id": ["sub1", "sub2", None]})
        mock_basics = MagicMock(return_value=df_basics)
        mock_qc = MagicMock()
        mock_registry.__getitem__.side_effect = self._make_registry(
            MagicMock(), MagicMock(), mock_basics, MagicMock(), MagicMock(), mock_qc
        ).__getitem__

        hide_acorns()

        mock_qc.assert_has_calls(
            [
                call(subject_id="sub1", force_update=True),
                call(subject_id="sub2", force_update=True),
            ],
            any_order=True,
        )
        self.assertEqual(mock_qc.call_count, 2)

    @patch("zombie_squirrel.sync.publish_squirrel_metadata")
    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_qc_skipped_when_no_subjects(self, mock_registry, mock_publish):
        df_basics = pd.DataFrame({"subject_id": [None, None]})
        mock_basics = MagicMock(return_value=df_basics)
        mock_qc = MagicMock()
        mock_registry.__getitem__.side_effect = self._make_registry(
            MagicMock(), MagicMock(), mock_basics, MagicMock(), MagicMock(), mock_qc
        ).__getitem__

        hide_acorns()

        mock_qc.assert_not_called()

    @patch("zombie_squirrel.sync.publish_squirrel_metadata")
    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_publish_metadata_called_at_end(self, mock_registry, mock_publish):
        df_basics = pd.DataFrame({"subject_id": ["sub1"]})
        mock_basics = MagicMock(return_value=df_basics)
        mock_registry.__getitem__.side_effect = self._make_registry(
            MagicMock(), MagicMock(), mock_basics, MagicMock(), MagicMock(), MagicMock()
        ).__getitem__

        hide_acorns()

        mock_publish.assert_called_once()

    @patch("zombie_squirrel.sync.publish_squirrel_metadata")
    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_exception_from_acorn_propagates(self, mock_registry, mock_publish):
        mock_upn = MagicMock(side_effect=Exception("Update failed"))
        mock_registry.__getitem__.side_effect = self._make_registry(
            mock_upn, MagicMock(), MagicMock(), MagicMock(), MagicMock(), MagicMock()
        ).__getitem__

        with self.assertRaises(Exception) as ctx:
            hide_acorns()

        self.assertEqual(str(ctx.exception), "Update failed")


class TestPublishSquirrelMetadata(unittest.TestCase):
    @patch("zombie_squirrel.sync.TREE")
    def test_plant_called_with_squirrel_json_key(self, mock_tree):
        mock_tree.get_location.return_value = "s3://bucket/path"

        publish_squirrel_metadata()

        mock_tree.plant.assert_called_once()
        key = mock_tree.plant.call_args[0][0]
        self.assertEqual(key, "squirrel.json")

    @patch("zombie_squirrel.sync.TREE")
    def test_published_json_contains_six_acorns(self, mock_tree):
        mock_tree.get_location.return_value = "s3://bucket/path"

        publish_squirrel_metadata()

        payload = json.loads(mock_tree.plant.call_args[0][1])
        self.assertIn("acorns", payload)
        self.assertEqual(len(payload["acorns"]), 6)

    @patch("zombie_squirrel.sync.TREE")
    def test_published_json_acorn_names(self, mock_tree):
        mock_tree.get_location.return_value = "s3://bucket/path"

        publish_squirrel_metadata()

        payload = json.loads(mock_tree.plant.call_args[0][1])
        names = {a["name"] for a in payload["acorns"]}
        self.assertIn("unique_project_names", names)
        self.assertIn("unique_subject_ids", names)
        self.assertIn("asset_basics", names)
        self.assertIn("source_data", names)
        self.assertIn("raw_to_derived", names)
        self.assertIn("quality_control", names)

    @patch("zombie_squirrel.sync.TREE")
    def test_qc_acorn_is_partitioned(self, mock_tree):
        mock_tree.get_location.return_value = "s3://bucket/path"

        publish_squirrel_metadata()

        payload = json.loads(mock_tree.plant.call_args[0][1])
        qc = next(a for a in payload["acorns"] if a["name"] == "quality_control")
        self.assertTrue(qc["partitioned"])
        self.assertEqual(qc["partition_key"], "subject_id")
        self.assertEqual(qc["type"], "asset")

    @patch("zombie_squirrel.sync.TREE")
    def test_non_qc_acorns_are_metadata_type(self, mock_tree):
        mock_tree.get_location.return_value = "s3://bucket/path"

        publish_squirrel_metadata()

        payload = json.loads(mock_tree.plant.call_args[0][1])
        for acorn in payload["acorns"]:
            if acorn["name"] != "quality_control":
                self.assertEqual(acorn["type"], "metadata")
                self.assertFalse(acorn["partitioned"])

    @patch("zombie_squirrel.sync.TREE")
    def test_get_location_called_for_each_acorn(self, mock_tree):
        mock_tree.get_location.return_value = "s3://bucket/path"

        publish_squirrel_metadata()

        self.assertEqual(mock_tree.get_location.call_count, 6)

    @patch("zombie_squirrel.sync.TREE")
    def test_qc_location_uses_partitioned_flag(self, mock_tree):
        mock_tree.get_location.return_value = "s3://bucket/path"

        publish_squirrel_metadata()

        partitioned_call = call("qc", partitioned=True)
        self.assertIn(partitioned_call, mock_tree.get_location.call_args_list)

    @patch("zombie_squirrel.sync.TREE")
    def test_acorns_have_columns(self, mock_tree):
        mock_tree.get_location.return_value = "s3://bucket/path"

        publish_squirrel_metadata()

        payload = json.loads(mock_tree.plant.call_args[0][1])
        for acorn in payload["acorns"]:
            self.assertIsInstance(acorn["columns"], list)
            if acorn["name"] != "raw_to_derived":
                self.assertGreater(len(acorn["columns"]), 0)


if __name__ == "__main__":
    unittest.main()


    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_calls_all_acorns(self, mock_registry):
        """Test that hide_acorns calls all registered acorns with force_update."""
        mock_upn = MagicMock()
        mock_usi = MagicMock()
        mock_basics = MagicMock()
        mock_d2r = MagicMock()
        mock_r2d = MagicMock()
        mock_qc = MagicMock()

        # Create mock dataframe with subject IDs for QC
        mock_df = pd.DataFrame({"subject_id": ["subject1", "subject2"]})
        mock_basics.return_value = mock_df

        mock_registry.__getitem__.side_effect = lambda x: {
            "unique_project_names": mock_upn,
            "unique_subject_ids": mock_usi,
            "asset_basics": mock_basics,
            "source_data": mock_d2r,
            "raw_to_derived": mock_r2d,
            "quality_control": mock_qc,
        }[x]

        hide_acorns()

        mock_upn.assert_called_once_with(force_update=True)
        mock_usi.assert_called_once_with(force_update=True)
        mock_basics.assert_called_once_with(force_update=True)
        mock_d2r.assert_called_once_with(force_update=True)
        mock_r2d.assert_called_once_with(force_update=True)
        assert mock_qc.call_count == 2
        mock_qc.assert_any_call(subject_id="subject1", force_update=True, write_metadata=True)
        mock_qc.assert_any_call(subject_id="subject2", force_update=True, write_metadata=False)

    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_empty_registry(self, mock_registry):
        """Test hide_acorns with no subject IDs in asset_basics."""
        mock_upn = MagicMock()
        mock_usi = MagicMock()
        mock_basics = MagicMock()
        mock_d2r = MagicMock()
        mock_r2d = MagicMock()
        mock_qc = MagicMock()

        # Create mock dataframe with no subject IDs
        mock_df = pd.DataFrame({"subject_id": []})
        mock_basics.return_value = mock_df

        mock_registry.__getitem__.side_effect = lambda x: {
            "unique_project_names": mock_upn,
            "unique_subject_ids": mock_usi,
            "asset_basics": mock_basics,
            "source_data": mock_d2r,
            "raw_to_derived": mock_r2d,
            "quality_control": mock_qc,
        }[x]

        hide_acorns()

        mock_upn.assert_called_once_with(force_update=True)
        mock_usi.assert_called_once_with(force_update=True)
        mock_basics.assert_called_once_with(force_update=True)
        mock_d2r.assert_called_once_with(force_update=True)
        mock_r2d.assert_called_once_with(force_update=True)
        mock_qc.assert_not_called()

    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_single_acorn(self, mock_registry):
        """Test hide_acorns with a single subject ID."""
        mock_upn = MagicMock()
        mock_usi = MagicMock()
        mock_basics = MagicMock()
        mock_d2r = MagicMock()
        mock_r2d = MagicMock()
        mock_qc = MagicMock()

        # Create mock dataframe with single subject ID
        mock_df = pd.DataFrame({"subject_id": ["subject1"]})
        mock_basics.return_value = mock_df

        mock_registry.__getitem__.side_effect = lambda x: {
            "unique_project_names": mock_upn,
            "unique_subject_ids": mock_usi,
            "asset_basics": mock_basics,
            "source_data": mock_d2r,
            "raw_to_derived": mock_r2d,
            "quality_control": mock_qc,
        }[x]

        hide_acorns()

        mock_qc.assert_called_once_with(subject_id="subject1", force_update=True, write_metadata=True)

    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_acorn_order_independent(self, mock_registry):
        """Test that hide_acorns processes all subject IDs."""
        mock_upn = MagicMock()
        mock_usi = MagicMock()
        mock_basics = MagicMock()
        mock_d2r = MagicMock()
        mock_r2d = MagicMock()
        mock_qc = MagicMock()

        # Create mock dataframe with multiple subject IDs
        mock_df = pd.DataFrame({"subject_id": ["sub1", "sub2", "sub3", "sub4", "sub5"]})
        mock_basics.return_value = mock_df

        mock_registry.__getitem__.side_effect = lambda x: {
            "unique_project_names": mock_upn,
            "unique_subject_ids": mock_usi,
            "asset_basics": mock_basics,
            "source_data": mock_d2r,
            "raw_to_derived": mock_r2d,
            "quality_control": mock_qc,
        }[x]

        hide_acorns()

        assert mock_qc.call_count == 5
        mock_qc.assert_any_call(subject_id="sub1", force_update=True, write_metadata=True)
        for sub_id in ["sub2", "sub3", "sub4", "sub5"]:
            mock_qc.assert_any_call(subject_id=sub_id, force_update=True, write_metadata=False)

    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_propagates_exceptions(self, mock_registry):
        """Test that exceptions from acorns are propagated."""
        mock_upn = MagicMock(side_effect=Exception("Update failed"))
        mock_usi = MagicMock()
        mock_basics = MagicMock()
        mock_d2r = MagicMock()
        mock_r2d = MagicMock()
        mock_qc = MagicMock()

        mock_registry.__getitem__.side_effect = lambda x: {
            "unique_project_names": mock_upn,
            "unique_subject_ids": mock_usi,
            "asset_basics": mock_basics,
            "source_data": mock_d2r,
            "raw_to_derived": mock_r2d,
            "quality_control": mock_qc,
        }[x]

        with self.assertRaises(Exception) as context:
            hide_acorns()

        self.assertEqual(str(context.exception), "Update failed")
        mock_upn.assert_called_once_with(force_update=True)


if __name__ == "__main__":
    unittest.main()

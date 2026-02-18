"""Unit tests for zombie_squirrel.sync module.

Tests for cache synchronization functions."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.sync import hide_acorns


class TestHideAcorns(unittest.TestCase):
    """Tests for the hide_acorns function."""

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

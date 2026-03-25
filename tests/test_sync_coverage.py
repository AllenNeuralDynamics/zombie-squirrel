"""Additional tests for sync module coverage."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.sync import hide_acorns


class TestHideAcornsExceptionHandling(unittest.TestCase):
    """Tests for hide_acorns exception handling."""

    @patch("zombie_squirrel.sync.publish_squirrel_metadata")
    @patch("zombie_squirrel.sync.as_completed")
    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_fallback_sequential_on_concurrent_failure(
        self, mock_registry, mock_as_completed, mock_publish
    ):
        """Test hide_acorns falls back to sequential execution when future.result() fails."""
        df_basics = pd.DataFrame({"subject_id": ["sub1", "sub2"]})
        mock_basics = MagicMock(return_value=df_basics)
        mock_upn = MagicMock()
        mock_usi = MagicMock()
        mock_d2r = MagicMock()

        call_count = [0]

        def qc_side_effect(*args, **kwargs):
            """Track QC call count and return empty DataFrame."""
            call_count[0] += 1
            return pd.DataFrame()

        mock_qc = MagicMock(side_effect=qc_side_effect)

        registry_dict = {
            "unique_project_names": mock_upn,
            "unique_subject_ids": mock_usi,
            "asset_basics": mock_basics,
            "source_data": mock_d2r,
            "quality_control": mock_qc,
            "assets_smartspim": MagicMock(),
        }
        mock_registry.__getitem__.side_effect = registry_dict.__getitem__

        failed_future = MagicMock()
        failed_future.result.side_effect = RuntimeError("Executor failed")
        mock_as_completed.return_value = [failed_future]

        hide_acorns()

        self.assertGreaterEqual(call_count[0], 2)
        mock_publish.assert_called_once()


if __name__ == "__main__":
    unittest.main()

"""Unit tests for zombie_squirrel.sync module.

Tests for cache synchronization functions."""

import unittest
from unittest.mock import MagicMock, patch

from zombie_squirrel.sync import hide_acorns


class TestHideAcorns(unittest.TestCase):
    """Tests for the hide_acorns function."""

    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_calls_all_acorns(self, mock_registry):
        """Test that hide_acorns calls all registered acorns with force_update."""
        mock_acorn1 = MagicMock()
        mock_acorn2 = MagicMock()
        mock_acorn3 = MagicMock()

        mock_registry.values.return_value = [
            mock_acorn1,
            mock_acorn2,
            mock_acorn3,
        ]

        hide_acorns()

        mock_acorn1.assert_called_once_with(force_update=True)
        mock_acorn2.assert_called_once_with(force_update=True)
        mock_acorn3.assert_called_once_with(force_update=True)

    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_empty_registry(self, mock_registry):
        """Test hide_acorns with empty registry."""
        mock_registry.values.return_value = []

        # Should not raise any exception
        hide_acorns()

        mock_registry.values.assert_called_once()

    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_single_acorn(self, mock_registry):
        """Test hide_acorns with a single acorn."""
        mock_acorn = MagicMock()
        mock_registry.values.return_value = [mock_acorn]

        hide_acorns()

        mock_acorn.assert_called_once_with(force_update=True)

    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_acorn_order_independent(self, mock_registry):
        """Test that hide_acorns calls all acorns regardless of order."""
        mock_acorns = [MagicMock() for _ in range(5)]
        mock_registry.values.return_value = mock_acorns

        hide_acorns()

        # All acorns should be called with force_update=True
        for acorn in mock_acorns:
            acorn.assert_called_once_with(force_update=True)

    @patch("zombie_squirrel.sync.ACORN_REGISTRY")
    def test_hide_acorns_propagates_exceptions(self, mock_registry):
        """Test that exceptions from acorns are propagated."""
        mock_acorn_ok = MagicMock()
        mock_acorn_error = MagicMock(side_effect=Exception("Update failed"))

        mock_registry.values.return_value = [
            mock_acorn_ok,
            mock_acorn_error,
        ]

        with self.assertRaises(Exception) as context:
            hide_acorns()

        self.assertEqual(str(context.exception), "Update failed")
        # First acorn should have been called
        mock_acorn_ok.assert_called_once_with(force_update=True)


if __name__ == "__main__":
    unittest.main()

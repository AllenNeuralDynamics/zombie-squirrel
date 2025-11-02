"""Unit tests for zombie_squirrel.sync module.

Tests for cache synchronization functions."""

import unittest
from unittest.mock import MagicMock, patch

from zombie_squirrel.sync import hide_acorns


class TestHideAcorns(unittest.TestCase):
    """Tests for the hide_acorns function."""

    @patch("zombie_squirrel.sync.SQUIRREL_REGISTRY")
    def test_hide_acorns_calls_all_squirrels(self, mock_registry):
        """Test that hide_acorns calls all registered squirrels with force_update."""
        mock_squirrel1 = MagicMock()
        mock_squirrel2 = MagicMock()
        mock_squirrel3 = MagicMock()

        mock_registry.values.return_value = [
            mock_squirrel1,
            mock_squirrel2,
            mock_squirrel3,
        ]

        hide_acorns()

        mock_squirrel1.assert_called_once_with(force_update=True)
        mock_squirrel2.assert_called_once_with(force_update=True)
        mock_squirrel3.assert_called_once_with(force_update=True)

    @patch("zombie_squirrel.sync.SQUIRREL_REGISTRY")
    def test_hide_acorns_empty_registry(self, mock_registry):
        """Test hide_acorns with empty registry."""
        mock_registry.values.return_value = []

        # Should not raise any exception
        hide_acorns()

        mock_registry.values.assert_called_once()

    @patch("zombie_squirrel.sync.SQUIRREL_REGISTRY")
    def test_hide_acorns_single_squirrel(self, mock_registry):
        """Test hide_acorns with a single squirrel."""
        mock_squirrel = MagicMock()
        mock_registry.values.return_value = [mock_squirrel]

        hide_acorns()

        mock_squirrel.assert_called_once_with(force_update=True)

    @patch("zombie_squirrel.sync.SQUIRREL_REGISTRY")
    def test_hide_acorns_squirrel_order_independent(self, mock_registry):
        """Test that hide_acorns calls all squirrels regardless of order."""
        mock_squirrels = [MagicMock() for _ in range(5)]
        mock_registry.values.return_value = mock_squirrels

        hide_acorns()

        # All squirrels should be called with force_update=True
        for squirrel in mock_squirrels:
            squirrel.assert_called_once_with(force_update=True)

    @patch("zombie_squirrel.sync.SQUIRREL_REGISTRY")
    def test_hide_acorns_propagates_exceptions(self, mock_registry):
        """Test that exceptions from squirrels are propagated."""
        mock_squirrel_ok = MagicMock()
        mock_squirrel_error = MagicMock(side_effect=Exception("Update failed"))

        mock_registry.values.return_value = [
            mock_squirrel_ok,
            mock_squirrel_error,
        ]

        with self.assertRaises(Exception) as context:
            hide_acorns()

        self.assertEqual(str(context.exception), "Update failed")
        # First squirrel should have been called
        mock_squirrel_ok.assert_called_once_with(force_update=True)


if __name__ == "__main__":
    unittest.main()

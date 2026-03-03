"""Unit tests for custom acorn."""

import unittest
from unittest.mock import patch

import pandas as pd

from zombie_squirrel.acorn_helpers.custom import custom


class TestCustomAcorn(unittest.TestCase):
    """Tests for custom acorn."""

    @patch("zombie_squirrel.acorn_helpers.custom.acorns.TREE")
    def test_force_update_stores_and_returns_df(self, mock_tree):
        """Test that force_update stores the DataFrame and returns it."""
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = custom(name="my_acorn", force_update=True, df=df)
        mock_tree.hide.assert_called_once_with("my_acorn", df)
        pd.testing.assert_frame_equal(result, df)

    @patch("zombie_squirrel.acorn_helpers.custom.acorns.TREE")
    def test_force_update_without_df_raises(self, mock_tree):
        """Test that force_update=True without df raises ValueError."""
        with self.assertRaises(ValueError, msg="df must be provided when force_update=True."):
            custom(name="my_acorn", force_update=True)
        mock_tree.hide.assert_not_called()

    @patch("zombie_squirrel.acorn_helpers.custom.acorns.TREE")
    def test_retrieval_returns_cached_df(self, mock_tree):
        """Test that retrieval returns the cached DataFrame."""
        cached_df = pd.DataFrame({"x": [10, 20]})
        mock_tree.scurry.return_value = cached_df
        result = custom(name="my_acorn")
        mock_tree.scurry.assert_called_once_with("my_acorn")
        pd.testing.assert_frame_equal(result, cached_df)

    @patch("zombie_squirrel.acorn_helpers.custom.acorns.TREE")
    def test_retrieval_empty_cache_raises(self, mock_tree):
        """Test that empty cache raises ValueError without force_update."""
        mock_tree.scurry.return_value = pd.DataFrame()
        with self.assertRaises(ValueError):
            custom(name="my_acorn")

    @patch("zombie_squirrel.acorn_helpers.custom.acorns.TREE")
    def test_different_names_are_independent(self, mock_tree):
        """Test that different names retrieve from different cache keys."""
        df1 = pd.DataFrame({"a": [1]})
        df2 = pd.DataFrame({"b": [2]})
        mock_tree.scurry.side_effect = lambda name: df1 if name == "acorn_a" else df2
        result1 = custom(name="acorn_a")
        result2 = custom(name="acorn_b")
        pd.testing.assert_frame_equal(result1, df1)
        pd.testing.assert_frame_equal(result2, df2)


if __name__ == "__main__":
    unittest.main()

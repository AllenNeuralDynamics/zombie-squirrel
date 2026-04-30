"""Unit tests for zombie_squirrel.utils module.

Tests for utility functions.
"""

import unittest

from unittest.mock import MagicMock, patch

import zombie_squirrel.acorns as acorns
from zombie_squirrel.forest import MemoryTree
from zombie_squirrel.squirrel import Squirrel
from zombie_squirrel.utils import get_s3_cache_path, get_squirrel_info, prefix_table_name


class TestPrefixTableName(unittest.TestCase):
    """Tests for the prefix_table_name function."""

    def test_prefix_table_name_basic(self):
        """Test that prefix_table_name adds 'zs_' prefix and '.pqt' ext."""
        result = prefix_table_name("my_table")
        self.assertEqual(result, "zs_my_table.pqt")

    def test_prefix_table_name_empty_string(self):
        """Test with empty string."""
        result = prefix_table_name("")
        self.assertEqual(result, "zs_.pqt")

    def test_prefix_table_name_single_char(self):
        """Test with single character."""
        result = prefix_table_name("a")
        self.assertEqual(result, "zs_a.pqt")

    def test_prefix_table_name_with_underscores(self):
        """Test with table name containing underscores."""
        result = prefix_table_name("my_long_table_name")
        self.assertEqual(result, "zs_my_long_table_name.pqt")

    def test_prefix_table_name_with_numbers(self):
        """Test with table name containing numbers."""
        result = prefix_table_name("table123")
        self.assertEqual(result, "zs_table123.pqt")


class TestGetS3CachePath(unittest.TestCase):
    """Tests for the get_s3_cache_path function."""

    def test_get_s3_cache_path_basic(self):
        """Test that get_s3_cache_path constructs correct S3 path."""
        result = get_s3_cache_path("zs_test.pqt")
        self.assertEqual(result, "data-asset-cache/zs_test.pqt")

    def test_get_s3_cache_path_various_names(self):
        """Test with various filenames."""
        result = get_s3_cache_path("zs_my_data.pqt")
        self.assertEqual(result, "data-asset-cache/zs_my_data.pqt")


class TestGetSquirrelInfo(unittest.TestCase):
    """Tests for get_squirrel_info function."""

    def test_get_squirrel_info(self):
        tree = MemoryTree()
        squirrel = Squirrel(acorns=[])
        tree.plant("squirrel.json", squirrel.model_dump_json())
        acorns.TREE = tree

        result = get_squirrel_info()

        self.assertIsInstance(result, Squirrel)
        self.assertEqual(result.acorns, [])


if __name__ == "__main__":
    unittest.main()

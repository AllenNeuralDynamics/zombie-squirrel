"""Unit tests for zombie_squirrel.utils module.

Tests for utility functions."""

import unittest

from zombie_squirrel.utils import prefix_table_name


class TestPrefixTableName(unittest.TestCase):
    """Tests for the prefix_table_name function."""

    def test_prefix_table_name_basic(self):
        """Test that prefix_table_name correctly adds 'zs_' prefix."""
        result = prefix_table_name("my_table")
        self.assertEqual(result, "zs_my_table")

    def test_prefix_table_name_empty_string(self):
        """Test with empty string."""
        result = prefix_table_name("")
        self.assertEqual(result, "zs_")

    def test_prefix_table_name_single_char(self):
        """Test with single character."""
        result = prefix_table_name("a")
        self.assertEqual(result, "zs_a")

    def test_prefix_table_name_with_underscores(self):
        """Test with table name containing underscores."""
        result = prefix_table_name("my_long_table_name")
        self.assertEqual(result, "zs_my_long_table_name")

    def test_prefix_table_name_with_numbers(self):
        """Test with table name containing numbers."""
        result = prefix_table_name("table123")
        self.assertEqual(result, "zs_table123")


if __name__ == "__main__":
    unittest.main()

"""Unit tests for zombie_squirrel.utils module.

Tests for utility functions."""

import json
import unittest
from unittest.mock import MagicMock, patch

from zombie_squirrel.utils import get_s3_cache_path, load_columns_from_metadata, prefix_table_name


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
        self.assertEqual(result, "application-caches/zs_test.pqt")

    def test_get_s3_cache_path_various_names(self):
        """Test with various filenames."""
        result = get_s3_cache_path("zs_my_data.pqt")
        self.assertEqual(result, "application-caches/zs_my_data.pqt")


class TestLoadColumnsFromMetadata(unittest.TestCase):
    """Tests for the load_columns_from_metadata function."""

    @patch("zombie_squirrel.utils.boto3.client")
    def test_load_columns_regular_table(self, mock_boto3_client):
        """Test loading columns from regular table metadata."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_response = {"Body": MagicMock(read=lambda: json.dumps({"columns": ["col1", "col2", "col3"]}).encode())}
        mock_s3.get_object.return_value = mock_response

        result = load_columns_from_metadata("unique_project_names")

        self.assertEqual(result, ["col1", "col2", "col3"])
        mock_s3.get_object.assert_called_once_with(
            Bucket="aind-scratch-data", Key="application-caches/zs_unique_project_names.json"
        )

    @patch("zombie_squirrel.utils.boto3.client")
    def test_load_columns_qc_table(self, mock_boto3_client):
        """Test loading columns from QC table metadata."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_response = {"Body": MagicMock(read=lambda: json.dumps({"columns": ["qc1", "qc2"]}).encode())}
        mock_s3.get_object.return_value = mock_response

        result = load_columns_from_metadata("qc/test")

        self.assertEqual(result, ["qc1", "qc2"])
        mock_s3.get_object.assert_called_once_with(Bucket="aind-scratch-data", Key="application-caches/zs_qc.json")


if __name__ == "__main__":
    unittest.main()

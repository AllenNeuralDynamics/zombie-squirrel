"""Unit tests for zombie_squirrel.trees module.

Tests for abstract base class, memory backend, and S3 backend
for caching functionality."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from zombie_squirrel.forest import (
    Tree,
    MemoryTree,
    S3Tree,
)


class TestTreeAbstractClass(unittest.TestCase):
    """Tests for Tree abstract base class."""

    def test_tree_cannot_be_instantiated(self):
        """Test that Tree abstract class cannot be instantiated."""
        with self.assertRaises(TypeError):
            Tree()

    def test_tree_subclass_must_implement_hide(self):
        """Test that subclasses must implement hide method."""

        class IncompleteTree(Tree):
            """Incomplete Tree subclass missing hide method."""

            def scurry(self, table_name: str) -> pd.DataFrame:  # pragma: no cover
                """Fetch records from the cache."""
                return pd.DataFrame()

        with self.assertRaises(TypeError):
            IncompleteTree()

    def test_tree_subclass_must_implement_scurry(self):
        """Test that subclasses must implement scurry method."""

        class IncompleteTree(Tree):
            """Incomplete Tree subclass missing scurry method."""

            def hide(self, table_name: str, data: pd.DataFrame) -> None:  # pragma: no cover
                """Store records in the cache."""
                pass

        with self.assertRaises(TypeError):
            IncompleteTree()


class TestMemoryTree(unittest.TestCase):
    """Tests for MemoryTree implementation."""

    def setUp(self):
        """Initialize a fresh MemoryTree for each test."""
        self.tree = MemoryTree()

    def test_hide_and_scurry_basic(self):
        """Test basic hide and scurry operations."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        self.tree.hide("test_table", df)

        retrieved = self.tree.scurry("test_table")
        pd.testing.assert_frame_equal(df, retrieved)

    def test_scurry_empty_table(self):
        """Test scurrying a table that doesn't exist returns empty DataFrame."""
        result = self.tree.scurry("nonexistent_table")
        self.assertTrue(result.empty)
        self.assertIsInstance(result, pd.DataFrame)

    def test_hide_overwrites_existing(self):
        """Test that hiding data overwrites existing data."""
        df1 = pd.DataFrame({"col1": [1, 2, 3]})
        df2 = pd.DataFrame({"col1": [4, 5, 6]})

        self.tree.hide("table", df1)
        self.tree.hide("table", df2)

        retrieved = self.tree.scurry("table")
        pd.testing.assert_frame_equal(df2, retrieved)

    def test_multiple_tables(self):
        """Test managing multiple tables."""
        df1 = pd.DataFrame({"col1": [1, 2]})
        df2 = pd.DataFrame({"col2": ["a", "b"]})

        self.tree.hide("table1", df1)
        self.tree.hide("table2", df2)

        retrieved1 = self.tree.scurry("table1")
        retrieved2 = self.tree.scurry("table2")

        pd.testing.assert_frame_equal(df1, retrieved1)
        pd.testing.assert_frame_equal(df2, retrieved2)

    def test_hide_empty_dataframe(self):
        """Test hiding an empty DataFrame."""
        df = pd.DataFrame()
        self.tree.hide("empty_table", df)

        retrieved = self.tree.scurry("empty_table")
        pd.testing.assert_frame_equal(df, retrieved)


class TestS3Tree(unittest.TestCase):
    """Tests for S3Tree implementation with mocking."""

    @patch("zombie_squirrel.forest.boto3.client")
    def test_s3_acorn_initialization(self, mock_boto3_client):
        """Test S3Tree initialization."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        acorn = S3Tree()

        self.assertEqual(acorn.bucket, "aind-scratch-data")
        self.assertEqual(acorn.s3_client, mock_s3_client)
        mock_boto3_client.assert_called_once_with("s3")

    @patch("zombie_squirrel.forest.boto3.client")
    def test_s3_hide(self, mock_boto3_client):
        """Test S3Tree.hide method writes to S3."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        acorn = S3Tree()
        df = pd.DataFrame({"col1": [1, 2, 3]})

        acorn.hide("test_table", df)

        mock_s3_client.put_object.assert_called_once()
        call_kwargs = mock_s3_client.put_object.call_args[1]
        self.assertEqual(call_kwargs["Bucket"], "aind-scratch-data")
        self.assertEqual(
            call_kwargs["Key"], "application-caches/zs_test_table.pqt"
        )
        self.assertIsInstance(call_kwargs["Body"], bytes)

    @patch("zombie_squirrel.forest.duckdb.query")
    @patch("zombie_squirrel.forest.boto3.client")
    def test_s3_scurry(self, mock_boto3_client, mock_duckdb_query):
        """Test S3Tree.scurry method reads from S3 using DuckDB."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client

        expected_df = pd.DataFrame({"col1": [1, 2, 3]})
        mock_result = MagicMock()
        mock_result.to_df.return_value = expected_df
        mock_duckdb_query.return_value = mock_result

        acorn = S3Tree()
        result = acorn.scurry("test_table")

        # Verify DuckDB was called with correct S3 path
        mock_duckdb_query.assert_called_once()
        query_call = mock_duckdb_query.call_args[0][0]
        self.assertIn(
            "s3://aind-scratch-data/application-caches/zs_test_table.pqt",
            query_call,
        )
        pd.testing.assert_frame_equal(result, expected_df)

    @patch("zombie_squirrel.forest.duckdb.query")
    @patch("zombie_squirrel.forest.boto3.client")
    def test_s3_scurry_handles_error(
        self, mock_boto3_client, mock_duckdb_query
    ):
        """Test S3Tree.scurry returns empty DataFrame on error."""
        mock_s3_client = MagicMock()
        mock_boto3_client.return_value = mock_s3_client
        mock_duckdb_query.side_effect = Exception("S3 access error")

        acorn = S3Tree()
        result = acorn.scurry("nonexistent_table")

        self.assertTrue(result.empty)
        self.assertIsInstance(result, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()

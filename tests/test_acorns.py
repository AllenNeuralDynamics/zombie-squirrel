"""Unit tests for zombie_squirrel.acorns module.

Tests for abstract base class, memory backend, and Redshift backend
for caching functionality."""

import os
import unittest
from unittest.mock import MagicMock, Mock, patch

import pandas as pd

from zombie_squirrel.acorns import (
    Acorn,
    MemoryAcorn,
    RedshiftAcorn,
    rds_get_handle_empty,
)


class TestAcornAbstractClass(unittest.TestCase):
    """Tests for Acorn abstract base class."""

    def test_acorn_cannot_be_instantiated(self):
        """Test that Acorn abstract class cannot be instantiated."""
        with self.assertRaises(TypeError):
            Acorn()

    def test_acorn_subclass_must_implement_hide(self):
        """Test that subclasses must implement hide method."""

        class IncompleteAcorn(Acorn):
            """Incomplete Acorn subclass missing hide method."""

            def scurry(self, table_name: str) -> pd.DataFrame:  # pragma: no cover
                """Fetch records from the cache."""
                return pd.DataFrame()

        with self.assertRaises(TypeError):
            IncompleteAcorn()

    def test_acorn_subclass_must_implement_scurry(self):
        """Test that subclasses must implement scurry method."""

        class IncompleteAcorn(Acorn):
            """Incomplete Acorn subclass missing scurry method."""

            def hide(self, table_name: str, data: pd.DataFrame) -> None:  # pragma: no cover
                """Store records in the cache."""
                pass

        with self.assertRaises(TypeError):
            IncompleteAcorn()


class TestMemoryAcorn(unittest.TestCase):
    """Tests for MemoryAcorn implementation."""

    def setUp(self):
        """Initialize a fresh MemoryAcorn for each test."""
        self.acorn = MemoryAcorn()

    def test_hide_and_scurry_basic(self):
        """Test basic hide and scurry operations."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
        self.acorn.hide("test_table", df)

        retrieved = self.acorn.scurry("test_table")
        pd.testing.assert_frame_equal(df, retrieved)

    def test_scurry_empty_table(self):
        """Test scurrying a table that doesn't exist returns empty DataFrame."""
        result = self.acorn.scurry("nonexistent_table")
        self.assertTrue(result.empty)
        self.assertIsInstance(result, pd.DataFrame)

    def test_hide_overwrites_existing(self):
        """Test that hiding data overwrites existing data."""
        df1 = pd.DataFrame({"col1": [1, 2, 3]})
        df2 = pd.DataFrame({"col1": [4, 5, 6]})

        self.acorn.hide("table", df1)
        self.acorn.hide("table", df2)

        retrieved = self.acorn.scurry("table")
        pd.testing.assert_frame_equal(df2, retrieved)

    def test_multiple_tables(self):
        """Test managing multiple tables."""
        df1 = pd.DataFrame({"col1": [1, 2]})
        df2 = pd.DataFrame({"col2": ["a", "b"]})

        self.acorn.hide("table1", df1)
        self.acorn.hide("table2", df2)

        retrieved1 = self.acorn.scurry("table1")
        retrieved2 = self.acorn.scurry("table2")

        pd.testing.assert_frame_equal(df1, retrieved1)
        pd.testing.assert_frame_equal(df2, retrieved2)

    def test_hide_empty_dataframe(self):
        """Test hiding an empty DataFrame."""
        df = pd.DataFrame()
        self.acorn.hide("empty_table", df)

        retrieved = self.acorn.scurry("empty_table")
        pd.testing.assert_frame_equal(df, retrieved)


class TestRedshiftAcorn(unittest.TestCase):
    """Tests for RedshiftAcorn implementation with mocking."""

    @patch("zombie_squirrel.acorns.RDSCredentials")
    @patch("zombie_squirrel.acorns.Client")
    def test_redshift_acorn_initialization(self, mock_client_class, mock_credentials_class):
        """Test RedshiftAcorn initialization."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_credentials_instance = MagicMock()
        mock_credentials_class.return_value = mock_credentials_instance

        acorn = RedshiftAcorn()

        self.assertEqual(acorn.rds_client, mock_client_instance)
        mock_client_class.assert_called_once()

    @patch("zombie_squirrel.acorns.RDSCredentials")
    @patch("zombie_squirrel.acorns.Client")
    def test_redshift_hide(self, mock_client_class, mock_credentials_class):
        """Test RedshiftAcorn.hide method."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_credentials_instance = MagicMock()
        mock_credentials_class.return_value = mock_credentials_instance

        acorn = RedshiftAcorn()
        df = pd.DataFrame({"col1": [1, 2, 3]})

        acorn.hide("test_table", df)

        mock_client_instance.overwrite_table_with_df.assert_called_once()
        call_args = mock_client_instance.overwrite_table_with_df.call_args
        pd.testing.assert_frame_equal(call_args[1]["df"], df)
        self.assertEqual(call_args[1]["table_name"], "zs_test_table")

    @patch("zombie_squirrel.acorns.RDSCredentials")
    @patch("zombie_squirrel.acorns.Client")
    def test_redshift_scurry(self, mock_client_class, mock_credentials_class):
        """Test RedshiftAcorn.scurry method."""
        mock_client_instance = MagicMock()
        expected_df = pd.DataFrame({"col1": [1, 2, 3]})
        mock_client_instance.read_table.return_value = expected_df
        mock_client_class.return_value = mock_client_instance
        mock_credentials_instance = MagicMock()
        mock_credentials_class.return_value = mock_credentials_instance

        acorn = RedshiftAcorn()
        result = acorn.scurry("test_table")

        mock_client_instance.read_table.assert_called_once_with(table_name="zs_test_table")
        pd.testing.assert_frame_equal(result, expected_df)

    @patch.dict("os.environ", {}, clear=False)
    @patch("zombie_squirrel.acorns.RDSCredentials")
    @patch("zombie_squirrel.acorns.Client")
    def test_redshift_default_secrets_path(self, mock_client_class, mock_credentials_class):
        """Test RedshiftAcorn uses default secrets path."""
        if "REDSHIFT_SECRETS" in os.environ:  # pragma: no cover
            del os.environ["REDSHIFT_SECRETS"]  # pragma: no cover

        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        mock_credentials_instance = MagicMock()
        mock_credentials_class.return_value = mock_credentials_instance

        RedshiftAcorn()

        mock_client_class.assert_called_once()
        call_args = mock_client_class.call_args
        self.assertIsNotNone(call_args)


class TestRdsGetHandleEmpty(unittest.TestCase):
    """Tests for rds_get_handle_empty helper function."""

    def test_rds_get_handle_empty_success(self):
        """Test successful retrieval from acorn."""
        acorn = MemoryAcorn()
        df = pd.DataFrame({"col1": [1, 2, 3]})
        acorn.hide("test_table", df)

        result = rds_get_handle_empty(acorn, "test_table")

        pd.testing.assert_frame_equal(result, df)

    def test_rds_get_handle_empty_missing_table(self):
        """Test returns empty DataFrame when table is missing."""
        acorn = MemoryAcorn()

        result = rds_get_handle_empty(acorn, "nonexistent_table")

        self.assertTrue(result.empty)
        self.assertIsInstance(result, pd.DataFrame)

    def test_rds_get_handle_empty_exception(self):
        """Test returns empty DataFrame when acorn raises exception."""
        acorn = Mock(spec=["scurry"])
        acorn.scurry.side_effect = Exception("Connection error")

        result = rds_get_handle_empty(acorn, "test_table")

        self.assertTrue(result.empty)
        self.assertIsInstance(result, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()

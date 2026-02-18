"""Unit tests for acorn columns functions.

Minimal tests to verify columns functions work correctly."""

import json
import unittest
from unittest.mock import MagicMock, patch

from zombie_squirrel.acorn_helpers.asset_basics import asset_basics_columns
from zombie_squirrel.acorn_helpers.qc import qc_columns
from zombie_squirrel.acorn_helpers.raw_to_derived import raw_to_derived_columns
from zombie_squirrel.acorn_helpers.source_data import source_data_columns
from zombie_squirrel.acorn_helpers.unique_project_names import unique_project_names_columns
from zombie_squirrel.acorn_helpers.unique_subject_ids import unique_subject_ids_columns


class TestColumnsFunction(unittest.TestCase):
    """Tests for columns functions."""

    @patch("zombie_squirrel.utils.boto3.client")
    def test_unique_project_names_columns(self, mock_boto3_client):
        """Test unique_project_names_columns returns column list."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_response = {"Body": MagicMock(read=lambda: json.dumps({"columns": ["project_name"]}).encode())}
        mock_s3.get_object.return_value = mock_response

        result = unique_project_names_columns()

        self.assertEqual(result, ["project_name"])

    @patch("zombie_squirrel.utils.boto3.client")
    def test_unique_subject_ids_columns(self, mock_boto3_client):
        """Test unique_subject_ids_columns returns column list."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_response = {"Body": MagicMock(read=lambda: json.dumps({"columns": ["subject_id"]}).encode())}
        mock_s3.get_object.return_value = mock_response

        result = unique_subject_ids_columns()

        self.assertEqual(result, ["subject_id"])

    @patch("zombie_squirrel.utils.boto3.client")
    def test_asset_basics_columns(self, mock_boto3_client):
        """Test asset_basics_columns returns column list."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_response = {
            "Body": MagicMock(read=lambda: json.dumps({"columns": ["_id", "modalities", "project_name"]}).encode())
        }
        mock_s3.get_object.return_value = mock_response

        result = asset_basics_columns()

        self.assertEqual(result, ["_id", "modalities", "project_name"])

    @patch("zombie_squirrel.utils.boto3.client")
    def test_qc_columns(self, mock_boto3_client):
        """Test qc_columns returns column list."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_response = {"Body": MagicMock(read=lambda: json.dumps({"columns": ["name", "stage"]}).encode())}
        mock_s3.get_object.return_value = mock_response

        result = qc_columns()

        self.assertEqual(result, ["name", "stage"])

    @patch("zombie_squirrel.utils.boto3.client")
    def test_source_data_columns(self, mock_boto3_client):
        """Test source_data_columns returns column list."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_response = {"Body": MagicMock(read=lambda: json.dumps({"columns": ["_id", "source_data"]}).encode())}
        mock_s3.get_object.return_value = mock_response

        result = source_data_columns()

        self.assertEqual(result, ["_id", "source_data"])

    @patch("zombie_squirrel.utils.boto3.client")
    def test_raw_to_derived_columns(self, mock_boto3_client):
        """Test raw_to_derived_columns returns column list."""
        mock_s3 = MagicMock()
        mock_boto3_client.return_value = mock_s3
        mock_response = {"Body": MagicMock(read=lambda: json.dumps({"columns": ["name", "derived_records"]}).encode())}
        mock_s3.get_object.return_value = mock_response

        result = raw_to_derived_columns()

        self.assertEqual(result, ["name", "derived_records"])


if __name__ == "__main__":
    unittest.main()

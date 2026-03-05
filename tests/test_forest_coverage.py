"""Additional tests for forest module coverage."""

import unittest
from unittest.mock import MagicMock, patch

from zombie_squirrel.forest import S3Tree


class TestS3TreeGetLocation(unittest.TestCase):
    """Tests for S3Tree.get_location method."""

    @patch("zombie_squirrel.forest.boto3")
    def test_get_location_partitioned(self, mock_boto3):
        """Test get_location returns partitioned path when partitioned=True."""
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client

        tree = S3Tree()
        result = tree.get_location("my_table", partitioned=True)

        self.assertIn("application-caches/zs_my_table/", result)
        self.assertTrue(result.startswith("s3://"))

    @patch("zombie_squirrel.forest.boto3")
    def test_get_location_not_partitioned(self, mock_boto3):
        """Test get_location returns single file path when partitioned=False."""
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client

        tree = S3Tree()
        result = tree.get_location("my_table", partitioned=False)

        self.assertIn("application-caches/", result)
        self.assertTrue(result.startswith("s3://"))
        self.assertNotIn("zs_my_table/", result)


if __name__ == "__main__":
    unittest.main()

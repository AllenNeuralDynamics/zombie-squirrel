"""Additional tests for forest module coverage."""

import unittest
from unittest.mock import MagicMock, patch

from zombie_squirrel.forest import MemoryTree, S3Tree


class TestMemoryTreeGetLocation(unittest.TestCase):
    """Tests for MemoryTree.get_location method."""

    def test_memory_tree_get_location_partitioned(self):
        """Test MemoryTree.get_location with partitioned=True."""
        tree = MemoryTree()
        result = tree.get_location("qc", partitioned=True)
        self.assertEqual(result, "qc/")

    def test_memory_tree_plant(self):
        """Test MemoryTree.plant stores data in json store."""
        tree = MemoryTree()
        tree.plant("test.json", '{"key": "value"}')
        self.assertIn("test.json", tree._json_store)
        self.assertEqual(tree._json_store["test.json"], '{"key": "value"}')


class TestS3TreeGetLocation(unittest.TestCase):
    """Tests for S3Tree.get_location method."""

    @patch("zombie_squirrel.forest.boto3")
    def test_get_location_partitioned(self, mock_boto3):
        """Test get_location returns partitioned path when partitioned=True."""
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client

        tree = S3Tree()
        result = tree.get_location("my_table", partitioned=True)

        self.assertIn("zombie-squirrel/zs_my_table/", result)
        self.assertTrue(result.startswith("s3://"))

    @patch("zombie_squirrel.forest.boto3")
    def test_get_location_not_partitioned(self, mock_boto3):
        """Test get_location returns single file path when partitioned=False."""
        mock_s3_client = MagicMock()
        mock_boto3.client.return_value = mock_s3_client

        tree = S3Tree()
        result = tree.get_location("my_table", partitioned=False)

        self.assertIn("zombie-squirrel/", result)
        self.assertTrue(result.startswith("s3://"))
        self.assertNotIn("zs_my_table/", result)


if __name__ == "__main__":
    unittest.main()

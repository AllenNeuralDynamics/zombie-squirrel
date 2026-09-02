"""Additional tests for backend module coverage."""

from unittest.mock import MagicMock, patch

from biodata_cache.backend import MemoryBackend, S3Backend
from biodata_cache.utils import BDC_VERSION

_VF = f"bdc-v{BDC_VERSION}"


def test_memory_backend_get_location_partitioned():
    backend = MemoryBackend()
    result = backend.get_location("qc", partitioned=True)
    assert result == f"{_VF}/qc/"


def test_memory_backend_put_json():
    backend = MemoryBackend()
    backend.put_json("test.json", '{"key": "value"}')
    assert f"{_VF}/test.json" in backend._json_store
    assert backend._json_store[f"{_VF}/test.json"] == '{"key": "value"}'


@patch("biodata_cache.backend.boto3")
def test_s3_get_location_partitioned(mock_boto3):
    mock_boto3.client.return_value = MagicMock()
    backend = S3Backend()
    result = backend.get_location("my_table", partitioned=True)
    assert f"data-asset-cache/{_VF}/my_table/" in result
    assert result.startswith("s3://")


@patch("biodata_cache.backend.boto3")
def test_s3_get_location_not_partitioned(mock_boto3):
    mock_boto3.client.return_value = MagicMock()
    backend = S3Backend()
    result = backend.get_location("my_table", partitioned=False)
    assert f"data-asset-cache/{_VF}/my_table.pqt" in result
    assert result.startswith("s3://")

"""Unit tests for biodata_cache.backend module."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from biodata_cache.backend import Backend, MemoryBackend, S3Backend
from biodata_cache.utils import BDC_VERSION

_VF = f"bdc-v{BDC_VERSION}"


def _not_found_error() -> ClientError:
    """Build a 404 ClientError as boto3 head_object raises for a missing key."""
    return ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")



# --- Backend abstract class ---


def test_backend_cannot_be_instantiated():
    with pytest.raises(TypeError):
        Backend()


def test_backend_subclass_must_implement_write():
    class IncompleteBackend(Backend):
        def read(self, table_name: str) -> pd.DataFrame:  # pragma: no cover
            return pd.DataFrame()

    with pytest.raises(TypeError):
        IncompleteBackend()


def test_backend_subclass_must_implement_read():
    class IncompleteBackend(Backend):
        def write(self, table_name: str, data: pd.DataFrame) -> None:  # pragma: no cover
            pass

    with pytest.raises(TypeError):
        IncompleteBackend()


# --- MemoryBackend ---


@pytest.fixture
def backend():
    return MemoryBackend()


def test_write_and_read_basic(backend):
    df = pd.DataFrame({"col1": [1, 2, 3], "col2": ["a", "b", "c"]})
    backend.write("test_table", df)
    pd.testing.assert_frame_equal(df, backend.read("test_table"))


def test_read_empty_table(backend):
    result = backend.read("nonexistent_table")
    assert result.empty
    assert isinstance(result, pd.DataFrame)


def test_write_overwrites_existing(backend):
    backend.write("table", pd.DataFrame({"col1": [1, 2, 3]}))
    df2 = pd.DataFrame({"col1": [4, 5, 6]})
    backend.write("table", df2)
    pd.testing.assert_frame_equal(df2, backend.read("table"))


def test_multiple_tables(backend):
    df1 = pd.DataFrame({"col1": [1, 2]})
    df2 = pd.DataFrame({"col2": ["a", "b"]})
    backend.write("table1", df1)
    backend.write("table2", df2)
    pd.testing.assert_frame_equal(df1, backend.read("table1"))
    pd.testing.assert_frame_equal(df2, backend.read("table2"))


def test_write_empty_dataframe(backend):
    df = pd.DataFrame()
    backend.write("empty_table", df)
    pd.testing.assert_frame_equal(df, backend.read("empty_table"))


def test_read_multiple_tables(backend):
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    df2 = pd.DataFrame({"col1": [3, 4], "col2": ["c", "d"]})
    backend.write("table1", df1)
    backend.write("table2", df2)
    result = backend.read(["table1", "table2"])
    assert len(result) == 4
    assert "asset_name" in result.columns
    assert result[result["col1"] == 1].iloc[0]["asset_name"] == "table1"
    assert result[result["col1"] == 3].iloc[0]["asset_name"] == "table2"


def test_read_multiple_with_missing_table(backend):
    backend.write("table1", pd.DataFrame({"col1": [1, 2]}))
    result = backend.read(["table1", "nonexistent"])
    assert len(result) == 2
    assert "asset_name" in result.columns
    assert (result["asset_name"] == "table1").all()


def test_read_multiple_all_missing(backend):
    result = backend.read(["missing1", "missing2"])
    assert result.empty
    assert isinstance(result, pd.DataFrame)


# --- S3Backend ---


@patch("biodata_cache.backend.boto3.client")
def test_s3_backend_initialization(mock_boto3_client):
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    backend = S3Backend()
    assert backend.bucket == "allen-data-views"
    assert backend.s3_client == mock_s3_client
    mock_boto3_client.assert_called_once_with("s3")


@patch("biodata_cache.backend.boto3.client")
def test_s3_write(mock_boto3_client):
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    backend = S3Backend()
    backend.write("test_table", pd.DataFrame({"col1": [1, 2, 3]}))
    assert mock_s3_client.put_object.call_count == 2
    parquet_call = mock_s3_client.put_object.call_args_list[0][1]
    assert parquet_call["Bucket"] == "allen-data-views"
    assert parquet_call["Key"] == f"data-asset-cache/{_VF}/test_table.pqt"
    assert isinstance(parquet_call["Body"], bytes)
    json_call = mock_s3_client.put_object.call_args_list[1][1]
    assert json_call["Bucket"] == "allen-data-views"
    assert json_call["Key"] == f"data-asset-cache/{_VF}/test_table.json"
    assert "columns" in json_call["Body"]


@patch("biodata_cache.backend.boto3.client")
def test_s3_write_qc_metadata(mock_boto3_client):
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    backend = S3Backend()
    backend.write("qc/subject123", pd.DataFrame({"metric": ["value1", "value2"]}))
    assert mock_s3_client.put_object.call_count == 2
    parquet_call = mock_s3_client.put_object.call_args_list[0][1]
    assert parquet_call["Key"] == f"data-asset-cache/{_VF}/qc/subject_id=subject123/data.pqt"
    json_call = mock_s3_client.put_object.call_args_list[1][1]
    assert json_call["Bucket"] == "allen-data-views"
    assert json_call["Key"] == f"data-asset-cache/{_VF}/qc.json"
    assert "columns" in json_call["Body"]
    assert "metric" in json_call["Body"]


@patch("biodata_cache.backend.boto3.client")
def test_s3_write_platform_qc_metadata(mock_boto3_client):
    mock_s3_client = MagicMock()
    mock_boto3_client.return_value = mock_s3_client
    backend = S3Backend()
    backend.write("platform_qc/spim", pd.DataFrame({"tag": ["a"]}))
    parquet_call = mock_s3_client.put_object.call_args_list[0][1]
    assert parquet_call["Key"] == f"data-asset-cache/{_VF}/platform_qc/platform=spim/data.pqt"
    json_call = mock_s3_client.put_object.call_args_list[1][1]
    assert json_call["Key"] == f"data-asset-cache/{_VF}/platform_qc.json"


@patch("biodata_cache.backend.duckdb_query")
@patch("biodata_cache.backend.boto3.client")
def test_s3_read(mock_boto3_client, mock_duckdb_query):
    mock_boto3_client.return_value = MagicMock()
    expected_df = pd.DataFrame({"col1": [1, 2, 3]})
    mock_duckdb_query.return_value = expected_df
    backend = S3Backend()
    result = backend.read("test_table")
    mock_duckdb_query.assert_called_once()
    assert f"data-asset-cache/{_VF}/test_table.pqt" in mock_duckdb_query.call_args[0][0]
    pd.testing.assert_frame_equal(result, expected_df)


@patch("biodata_cache.backend.duckdb_query")
@patch("biodata_cache.backend.boto3.client")
def test_s3_read_partitioned_table(mock_boto3_client, mock_duckdb_query):
    mock_boto3_client.return_value = MagicMock()
    expected_df = pd.DataFrame({"metric": ["a"]})
    mock_duckdb_query.return_value = expected_df
    result = S3Backend().read("qc/subject123")
    assert f"data-asset-cache/{_VF}/qc/subject_id=subject123/data*.pqt" in mock_duckdb_query.call_args[0][0]
    pd.testing.assert_frame_equal(result, expected_df)


@patch("biodata_cache.backend.boto3.client")
def test_s3_get_location_single_partition(mock_boto3_client):
    mock_boto3_client.return_value = MagicMock()
    backend = S3Backend()
    result = backend.get_location("qc/subject123")
    assert result == f"s3://allen-data-views/data-asset-cache/{_VF}/qc/subject_id=subject123/data.pqt"


@patch("biodata_cache.backend.boto3.client")
def test_s3_get_location_platform_qc_partition(mock_boto3_client):
    mock_boto3_client.return_value = MagicMock()
    backend = S3Backend()
    result = backend.get_location("platform_qc/spim")
    assert result == f"s3://allen-data-views/data-asset-cache/{_VF}/platform_qc/platform=spim/data.pqt"


@patch("biodata_cache.backend.duckdb_query")
@patch("biodata_cache.backend.boto3.client")
def test_s3_read_missing_object_returns_empty(mock_boto3_client, mock_duckdb_query):
    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = _not_found_error()
    mock_boto3_client.return_value = mock_s3
    mock_duckdb_query.side_effect = Exception("read failed")
    result = S3Backend().read("nonexistent_table")
    assert result.empty
    assert isinstance(result, pd.DataFrame)


@patch("biodata_cache.backend.duckdb_query")
@patch("biodata_cache.backend.boto3.client")
def test_s3_read_read_error_raises(mock_boto3_client, mock_duckdb_query):
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"ContentLength": 1}
    mock_boto3_client.return_value = mock_s3
    mock_duckdb_query.side_effect = Exception("Connection reset by peer")
    with pytest.raises(Exception, match="Connection reset"):
        S3Backend().read("some_table")


@patch("biodata_cache.backend.duckdb_query")
@patch("biodata_cache.backend.boto3.client")
def test_s3_read_multiple_tables(mock_boto3_client, mock_duckdb_query):
    mock_boto3_client.return_value = MagicMock()
    expected_df = pd.DataFrame(
        {"col1": [1, 2, 3, 4], "col2": ["a", "b", "c", "d"], "asset_name": ["table1", "table1", "table2", "table2"]}
    )
    mock_duckdb_query.return_value = expected_df
    result = S3Backend().read(["table1", "table2"])
    mock_duckdb_query.assert_called_once()
    query_call = mock_duckdb_query.call_args[0][0]
    assert "UNION ALL" in query_call
    assert "'table1' as asset_name" in query_call
    assert "'table2' as asset_name" in query_call
    pd.testing.assert_frame_equal(result, expected_df)


@patch("biodata_cache.backend.duckdb_query")
@patch("biodata_cache.backend.boto3.client")
def test_s3_read_multiple_missing_returns_empty(mock_boto3_client, mock_duckdb_query):
    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = _not_found_error()
    mock_boto3_client.return_value = mock_s3
    mock_duckdb_query.side_effect = Exception("read failed")
    result = S3Backend().read(["table1", "table2"])
    assert result.empty
    assert isinstance(result, pd.DataFrame)


@patch("biodata_cache.backend.duckdb_query")
@patch("biodata_cache.backend.boto3.client")
def test_s3_read_multiple_read_error_raises(mock_boto3_client, mock_duckdb_query):
    mock_s3 = MagicMock()
    mock_s3.head_object.return_value = {"ContentLength": 1}
    mock_boto3_client.return_value = mock_s3
    mock_duckdb_query.side_effect = Exception("Merge error")
    with pytest.raises(Exception, match="Merge error"):
        S3Backend().read(["table1", "table2"])


@patch("biodata_cache.backend.duckdb_query")
@patch("biodata_cache.backend.boto3.client")
def test_s3_read_partitioned_missing_returns_empty(mock_boto3_client, mock_duckdb_query):
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"KeyCount": 0}
    mock_boto3_client.return_value = mock_s3
    mock_duckdb_query.side_effect = Exception("read failed")
    result = S3Backend().read("qc/subject1")
    assert result.empty
    assert isinstance(result, pd.DataFrame)


@patch("biodata_cache.backend.duckdb_query")
@patch("biodata_cache.backend.boto3.client")
def test_s3_read_partitioned_read_error_raises(mock_boto3_client, mock_duckdb_query):
    mock_s3 = MagicMock()
    mock_s3.list_objects_v2.return_value = {"KeyCount": 1}
    mock_boto3_client.return_value = mock_s3
    mock_duckdb_query.side_effect = Exception("read failed")
    with pytest.raises(Exception, match="read failed"):
        S3Backend().read("qc/subject1")


@patch("biodata_cache.backend.duckdb_query")
@patch("biodata_cache.backend.boto3.client")
def test_s3_read_non_404_head_error_raises(mock_boto3_client, mock_duckdb_query):
    mock_s3 = MagicMock()
    mock_s3.head_object.side_effect = ClientError(
        {"Error": {"Code": "500", "Message": "Internal Error"}}, "HeadObject"
    )
    mock_boto3_client.return_value = mock_s3
    mock_duckdb_query.side_effect = Exception("read failed")
    with pytest.raises(ClientError):
        S3Backend().read("some_table")


# --- column sidecar writes ------------------------------------------------------
#
# For a partitioned table the <table>.json sidecar describes the table, not the
# partition, so it must not be re-PUT once per partition: that is one wasted S3
# round trip per partition in a job that is entirely round-trip bound.


def _sidecar_backend():
    """Return an S3Backend with a mocked S3 client."""
    from unittest.mock import MagicMock

    from biodata_cache.backend import S3Backend

    with patch("boto3.client", return_value=MagicMock()):
        backend = S3Backend()
    return backend


def _sidecar_puts(backend):
    """Return the keys of every .json sidecar PUT issued so far."""
    return [
        call.kwargs["Key"]
        for call in backend.s3_client.put_object.call_args_list
        if call.kwargs["Key"].endswith(".json")
    ]


def test_repeated_partition_writes_put_the_sidecar_once():
    backend = _sidecar_backend()
    frame = pd.DataFrame({"a": [1], "b": [2]})
    for value in ("p1", "p2", "p3"):
        backend.write(f"platform_pophys/{value}", frame)

    assert len(_sidecar_puts(backend)) == 1
    # ...while every partition's parquet object is still written.
    parquet_puts = [
        call.kwargs["Key"]
        for call in backend.s3_client.put_object.call_args_list
        if call.kwargs["Key"].endswith(".pqt")
    ]
    assert len(parquet_puts) == 3


def test_sidecar_is_rewritten_when_the_columns_change():
    backend = _sidecar_backend()
    backend.write("platform_ecephys_units/p1", pd.DataFrame({"a": [1]}))
    backend.write("platform_ecephys_units/p2", pd.DataFrame({"a": [1], "extra": [2]}))
    backend.write("platform_ecephys_units/p3", pd.DataFrame({"a": [1], "extra": [2]}))

    # One write for each distinct column list, not one per partition.
    assert len(_sidecar_puts(backend)) == 2


def test_sidecars_for_different_tables_are_tracked_separately():
    backend = _sidecar_backend()
    frame = pd.DataFrame({"a": [1]})
    backend.write("platform_pophys/p1", frame)
    backend.write("platform_ecephys_units/p1", frame)

    assert sorted(_sidecar_puts(backend)) == sorted(
        [
            f"data-asset-cache/bdc-v{BDC_VERSION}/platform_ecephys_units.json",
            f"data-asset-cache/bdc-v{BDC_VERSION}/platform_pophys.json",
        ]
    )


def test_a_failed_sidecar_put_is_retried_on_the_next_write():
    backend = _sidecar_backend()
    frame = pd.DataFrame({"a": [1]})

    def _fail_on_json(**kwargs):
        if kwargs["Key"].endswith(".json"):
            raise RuntimeError("boom")

    backend.s3_client.put_object.side_effect = _fail_on_json
    with pytest.raises(RuntimeError):
        backend.write("platform_pophys/p1", frame)

    # The key must not stay marked as written, or the sidecar is lost for good.
    backend.s3_client.put_object.side_effect = None
    backend.write("platform_pophys/p2", frame)
    assert len(_sidecar_puts(backend)) == 2


def test_write_chunk_uses_the_same_sidecar_dedup():
    backend = _sidecar_backend()
    frame = pd.DataFrame({"a": [1]})
    for chunk in range(3):
        backend.write_chunk("platform_pophys/p1", frame, chunk)

    assert len(_sidecar_puts(backend)) == 1

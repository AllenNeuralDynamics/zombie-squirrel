"""Unit tests for the shared CloudWatch operations engine."""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import biodata_cache.cache_table_helpers.shared.cloudwatch_utils as cw

_PIPELINE = "aind-fiber-photometry-pipeline"


def _row(timestamp, acq, process, event_type, message, level="INFO", exc=None, ingest="2026-07-27 10:00:00.000"):
    record = {
        "timestamp": timestamp,
        "level": level,
        "message": message,
        "acquisition_name": acq,
        "process_name": process,
        "event_type": event_type,
        "pipeline_name": _PIPELINE,
    }
    if exc is not None:
        record["exc_info"] = exc
    return [
        {"field": "@timestamp", "value": ingest},
        {"field": "@message", "value": json.dumps(record)},
        {"field": "@logStream", "value": "nf-stream/default/abc123"},
    ]


def test_query_string_includes_filters_and_event_types():
    q = cw.query_string(_PIPELINE)
    assert f'pipeline_name = "{_PIPELINE}"' in q
    assert 'event_type in ["stage_start", "stage_complete", "stage_error"]' in q
    assert 'acquisition_name != ""' in q
    assert "acquisition_name =" not in q


def test_query_string_single_asset():
    q = cw.query_string(_PIPELINE, "behavior_1_2026-01-01_00-00-00")
    assert 'filter acquisition_name = "behavior_1_2026-01-01_00-00-00"' in q


def test_query_string_pipeline_is_parameterized():
    q = cw.query_string("dynamic-foraging-processing-pipeline")
    assert 'pipeline_name = "dynamic-foraging-processing-pipeline"' in q


def test_query_string_all_registered_pipelines():
    with patch.dict(cw._OPERATIONS_PIPELINES, {"pipe-a": "k_a", "pipe-b": "k_b"}, clear=True):
        q = cw.query_string()
    assert 'pipeline_name in ["pipe-a", "pipe-b"]' in q
    assert "pipeline_name =" not in q


def test_register_operations_pipeline():
    with patch.dict(cw._OPERATIONS_PIPELINES, {}, clear=True):
        cw.register_operations_pipeline("pipe-x", "key_x")
        assert cw._OPERATIONS_PIPELINES == {"pipe-x": "key_x"}


def test_parse_row_valid():
    row = _row("2026-07-27T10:00:00+00:00", "acq1", "aind-fip-dff", "stage_complete", "done")
    parsed = cw.parse_row(row)
    assert parsed["asset_name"] == "acq1"
    assert parsed["process_name"] == "aind-fip-dff"
    assert parsed["event_type"] == "stage_complete"
    assert parsed["error_info"] is None
    assert parsed["cloudwatch_url"].startswith("https://us-west-2.console.aws.amazon.com/cloudwatch/home")
    assert "log-events" in parsed["cloudwatch_url"]


def test_parse_row_error_keeps_exc_info():
    row = _row("2026-07-27T10:00:00+00:00", "acq1", "aind-fip-dff", "stage_error", "failed", level="ERROR", exc="Traceback...")
    parsed = cw.parse_row(row)
    assert parsed["error_info"] == "Traceback..."
    assert parsed["level"] == "ERROR"


def test_parse_row_malformed_message_returns_none():
    row = [{"field": "@timestamp", "value": "x"}, {"field": "@message", "value": "not json"}]
    assert cw.parse_row(row) is None


def test_parse_row_missing_acquisition_returns_none():
    row = [
        {"field": "@timestamp", "value": "x"},
        {"field": "@message", "value": json.dumps({"process_name": "p", "event_type": "stage_start"})},
    ]
    assert cw.parse_row(row) is None


def test_events_dataframe_parses_timestamps():
    rows = [
        _row("2026-07-27T10:00:00+00:00", "acq1", "aind-fip-dff", "stage_start", "a"),
        _row("2026-07-27T10:05:00+00:00", "acq1", "aind-fip-dff", "stage_complete", "b"),
    ]
    df = cw.events_dataframe(rows)
    assert len(df) == 2
    assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])
    assert pd.api.types.is_datetime64_any_dtype(df["ingest_ts"])


def test_events_dataframe_empty():
    df = cw.events_dataframe([])
    assert df.empty
    assert "event_type" in df.columns


def test_collect_results_bisects_on_cap():
    client = MagicMock()
    big = [_row("2026-07-27T10:00:00+00:00", "acq1", "p", "stage_start", "m")] * cw.MAX_QUERY_RESULTS
    small = [_row("2026-07-27T10:00:00+00:00", "acq1", "p", "stage_start", "m")]

    def fake_run(_client, start, end, _q):
        if end - start > 4_000:
            return big
        return small

    with patch.object(cw, "run_query", side_effect=fake_run):
        results = cw.collect_results(client, 0, 8_000, "q")
    assert len(results) == 2


def test_fetch_all_operations_writes_one_partition_per_acquisition():
    rows = [
        _row("2026-07-27T10:00:00+00:00", "acq1", "aind-fip-dff", "stage_start", "a"),
        _row("2026-07-27T10:05:00+00:00", "acq1", "aind-fip-dff", "stage_complete", "b"),
        _row("2026-07-27T11:00:00+00:00", "acq2", "aind-fip-qc-raw", "stage_error", "c", exc="boom"),
    ]
    with (
        patch.object(cw, "logs_client", return_value=MagicMock()),
        patch.object(cw, "collect_results", return_value=rows),
        patch.object(cw, "read_last_scan", return_value=None),
        patch.object(cw.registry, "BACKEND") as backend,
    ):
        written = cw.fetch_all_operations("fib_operations", _PIPELINE)

    assert set(written) == {"acq1", "acq2"}
    written_partitions = {c[0][0] for c in backend.write.call_args_list}
    assert written_partitions == {
        "platform_fib_operations/acq1",
        "platform_fib_operations/acq2",
    }


def test_fetch_all_operations_empty_returns_empty_list():
    with (
        patch.object(cw, "logs_client", return_value=MagicMock()),
        patch.object(cw, "collect_results", return_value=[]),
        patch.object(cw, "read_last_scan", return_value=None),
        patch.object(cw.registry, "BACKEND") as backend,
    ):
        assert cw.fetch_all_operations("fib_operations", _PIPELINE) == []
    backend.write.assert_not_called()


def test_fetch_all_operations_filters_by_pipeline():
    rows = [
        _row("2026-07-27T10:00:00+00:00", "acq1", "p", "stage_start", "a"),
        _row("2026-07-27T11:00:00+00:00", "acq2", "p", "stage_start", "b"),
    ]
    other = json.loads(rows[1][1]["value"])
    other["pipeline_name"] = "other-pipeline"
    rows[1][1]["value"] = json.dumps(other)
    with (
        patch.object(cw, "logs_client", return_value=MagicMock()),
        patch.object(cw, "collect_results", return_value=rows),
        patch.object(cw, "read_last_scan", return_value=None),
        patch.object(cw.registry, "BACKEND"),
    ):
        written = cw.fetch_all_operations("fib_operations", _PIPELINE)
    assert written == ["acq1"]


def test_fetch_all_operations_initial_scan_overwrites_and_advances_sidecar():
    rows = [_row("2026-07-27T10:00:00+00:00", "acq1", "p", "stage_start", "a", ingest="2026-07-27 10:00:00.000")]
    with (
        patch.object(cw, "logs_client", return_value=MagicMock()),
        patch.object(cw, "collect_results", return_value=rows),
        patch.object(cw, "read_last_scan", return_value=None),
        patch.object(cw, "write_last_scan") as write_scan,
        patch.object(cw.registry, "BACKEND") as backend,
    ):
        written = cw.fetch_all_operations("fib_operations", _PIPELINE)

    assert written == ["acq1"]
    # initial scan overwrites the partition (no chunked append)
    backend.write.assert_called_once()
    backend.write_chunk.assert_not_called()
    write_scan.assert_called_once()
    assert write_scan.call_args[0][0] == "platform_fib_operations"


def test_fetch_all_operations_incremental_appends_chunk_and_filters_by_ingest():
    last_scan = datetime(2026, 7, 27, 10, 0, 0, tzinfo=timezone.utc)
    rows = [
        # already ingested at/prior to last_scan -> filtered out
        _row("2026-07-27T09:00:00+00:00", "acq1", "p", "stage_start", "old", ingest="2026-07-27 10:00:00.000"),
        # ingested after last_scan -> kept
        _row("2026-07-27T11:00:00+00:00", "acq2", "p", "stage_complete", "new", ingest="2026-07-27 11:00:00.000"),
    ]
    with (
        patch.object(cw, "logs_client", return_value=MagicMock()),
        patch.object(cw, "collect_results", return_value=rows),
        patch.object(cw, "read_last_scan", return_value=last_scan),
        patch.object(cw, "write_last_scan"),
        patch.object(cw.registry, "BACKEND") as backend,
    ):
        written = cw.fetch_all_operations("fib_operations", _PIPELINE)

    assert written == ["acq2"]
    # incremental run appends chunks rather than overwriting
    backend.write.assert_not_called()
    backend.write_chunk.assert_called_once()


def test_build_all_operations_pulls_once_and_routes():
    rows = [
        _row("2026-07-27T10:00:00+00:00", "acq1", "p", "stage_start", "a"),
        _row("2026-07-27T11:00:00+00:00", "acq2", "p", "stage_start", "b"),
    ]
    df_record = json.loads(rows[1][1]["value"])
    df_record["pipeline_name"] = "dynamic-foraging-processing-pipeline"
    rows[1][1]["value"] = json.dumps(df_record)

    with (
        patch.dict(
            cw._OPERATIONS_PIPELINES,
            {_PIPELINE: "fib_operations", "dynamic-foraging-processing-pipeline": "df_operations"},
            clear=True,
        ),
        patch.object(cw, "logs_client", return_value=MagicMock()),
        patch.object(cw, "collect_results", return_value=rows) as collect,
        patch.object(cw, "read_last_scan", return_value=None),
        patch.object(cw, "write_last_scan"),
        patch.object(cw.registry, "BACKEND") as backend,
    ):
        written = cw.build_all_operations()

    collect.assert_called_once()
    assert written == {"fib_operations": ["acq1"], "df_operations": ["acq2"]}
    written_partitions = {c[0][0] for c in backend.write.call_args_list}
    assert written_partitions == {
        "platform_fib_operations/acq1",
        "platform_df_operations/acq2",
    }


def test_build_all_operations_uses_shared_operations_sidecar():
    with (
        patch.dict(cw._OPERATIONS_PIPELINES, {_PIPELINE: "fib_operations"}, clear=True),
        patch.object(cw, "logs_client", return_value=MagicMock()),
        patch.object(cw, "collect_results", return_value=[]),
        patch.object(cw, "read_last_scan", return_value=None) as read_scan,
        patch.object(cw, "write_last_scan") as write_scan,
        patch.object(cw.registry, "BACKEND"),
    ):
        cw.build_all_operations()

    read_scan.assert_called_once_with("operations")
    assert write_scan.call_args[0][0] == "operations"



def test_platform_operations_reads_cache():
    df = pd.DataFrame({"event_type": ["stage_start"]})
    with patch.object(cw.registry, "BACKEND") as backend:
        backend.read.return_value = df
        out = cw.platform_operations("fib_operations", _PIPELINE, "acq1", False, False)
    pd.testing.assert_frame_equal(out, df)


def test_platform_operations_empty_cache_raises():
    with patch.object(cw.registry, "BACKEND") as backend:
        backend.read.return_value = pd.DataFrame()
        with pytest.raises(ValueError, match="Cache is empty"):
            cw.platform_operations("fib_operations", _PIPELINE, "acq1", False, False)


def test_platform_operations_lazy_returns_location():
    with patch.object(cw.registry, "BACKEND") as backend:
        backend.get_location.return_value = "s3://bucket/loc"
        out = cw.platform_operations("fib_operations", _PIPELINE, "acq1", False, True)
    assert out == "s3://bucket/loc"


def test_platform_operations_force_update_calls_fetch():
    with patch.object(cw, "fetch_asset_operations", return_value=pd.DataFrame()) as fetch:
        out = cw.platform_operations("fib_operations", _PIPELINE, "acq1", True, False)
    fetch.assert_called_once_with("fib_operations", _PIPELINE, "acq1")
    assert out.empty


def test_platform_operations_force_update_no_asset_rebuilds_all():
    with patch.object(cw, "fetch_all_operations", return_value=["a", "b"]) as fetch_all:
        out = cw.platform_operations("fib_operations", _PIPELINE, None, True, False)
    fetch_all.assert_called_once_with("fib_operations", _PIPELINE)
    assert out.empty


def test_platform_operations_read_without_asset_raises():
    with pytest.raises(ValueError, match="asset_name is required"):
        cw.platform_operations("fib_operations", _PIPELINE, None, False, False)


def test_operations_columns_names():
    names = {c.name for c in cw.operations_columns()}
    assert {"timestamp", "ingest_ts", "process_name", "event_type", "error_info", "cloudwatch_url"} <= names

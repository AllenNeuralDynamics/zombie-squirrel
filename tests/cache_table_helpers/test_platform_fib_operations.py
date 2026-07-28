"""Unit tests for platform_fib_operations cache table."""

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

import biodata_cache.cache_table_helpers.platform_fib_operations as fib_ops
from biodata_cache.cache_table_helpers.platform_fib_operations import (
    _collect_results,
    _events_dataframe,
    _parse_row,
    _query_string,
    fetch_all_fib_operations,
    platform_fib_operations,
    platform_fib_operations_columns,
)


def _row(timestamp, acq, process, event_type, message, level="INFO", exc=None, ingest="2026-07-27 10:00:00.000"):
    import json

    record = {
        "timestamp": timestamp,
        "level": level,
        "message": message,
        "acquisition_name": acq,
        "process_name": process,
        "event_type": event_type,
        "pipeline_name": "aind-fiber-photometry-pipeline",
    }
    if exc is not None:
        record["exc_info"] = exc
    return [
        {"field": "@timestamp", "value": ingest},
        {"field": "@message", "value": json.dumps(record)},
    ]


def test_query_string_includes_filters_and_event_types():
    q = _query_string()
    assert 'pipeline_name = "aind-fiber-photometry-pipeline"' in q
    assert 'event_type in ["stage_start", "stage_complete", "stage_error"]' in q
    assert 'acquisition_name != ""' in q
    assert "acquisition_name =" not in q


def test_query_string_single_asset():
    q = _query_string("behavior_1_2026-01-01_00-00-00")
    assert 'filter acquisition_name = "behavior_1_2026-01-01_00-00-00"' in q


def test_parse_row_valid():
    row = _row("2026-07-27T10:00:00+00:00", "acq1", "aind-fip-dff", "stage_complete", "done")
    parsed = _parse_row(row)
    assert parsed["asset_name"] == "acq1"
    assert parsed["process_name"] == "aind-fip-dff"
    assert parsed["event_type"] == "stage_complete"
    assert parsed["error_info"] is None


def test_parse_row_error_keeps_exc_info():
    row = _row("2026-07-27T10:00:00+00:00", "acq1", "aind-fip-dff", "stage_error", "failed", level="ERROR", exc="Traceback...")
    parsed = _parse_row(row)
    assert parsed["error_info"] == "Traceback..."
    assert parsed["level"] == "ERROR"


def test_parse_row_malformed_message_returns_none():
    row = [{"field": "@timestamp", "value": "x"}, {"field": "@message", "value": "not json"}]
    assert _parse_row(row) is None


def test_parse_row_missing_acquisition_returns_none():
    import json

    row = [
        {"field": "@timestamp", "value": "x"},
        {"field": "@message", "value": json.dumps({"process_name": "p", "event_type": "stage_start"})},
    ]
    assert _parse_row(row) is None


def test_events_dataframe_parses_timestamps():
    rows = [
        _row("2026-07-27T10:00:00+00:00", "acq1", "aind-fip-dff", "stage_start", "a"),
        _row("2026-07-27T10:05:00+00:00", "acq1", "aind-fip-dff", "stage_complete", "b"),
    ]
    df = _events_dataframe(rows)
    assert len(df) == 2
    assert pd.api.types.is_datetime64_any_dtype(df["timestamp"])
    assert pd.api.types.is_datetime64_any_dtype(df["ingest_ts"])


def test_events_dataframe_empty():
    df = _events_dataframe([])
    assert df.empty
    assert "event_type" in df.columns


def test_collect_results_bisects_on_cap():
    client = MagicMock()
    big = [_row("2026-07-27T10:00:00+00:00", "acq1", "p", "stage_start", "m")] * fib_ops._MAX_QUERY_RESULTS
    small = [_row("2026-07-27T10:00:00+00:00", "acq1", "p", "stage_start", "m")]

    def fake_run(_client, start, end, _q):
        # Full window returns the cap; halves return small results.
        if end - start > 4_000:
            return big
        return small

    with patch.object(fib_ops, "_run_query", side_effect=fake_run):
        results = _collect_results(client, 0, 8_000, "q")
    assert len(results) == 2


def test_fetch_all_writes_one_partition_per_acquisition():
    rows = [
        _row("2026-07-27T10:00:00+00:00", "acq1", "aind-fip-dff", "stage_start", "a"),
        _row("2026-07-27T10:05:00+00:00", "acq1", "aind-fip-dff", "stage_complete", "b"),
        _row("2026-07-27T11:00:00+00:00", "acq2", "aind-fip-qc-raw", "stage_error", "c", exc="boom"),
    ]
    with (
        patch.object(fib_ops, "_logs_client", return_value=MagicMock()),
        patch.object(fib_ops, "_collect_results", return_value=rows),
        patch.object(fib_ops.registry, "BACKEND") as backend,
    ):
        written = fetch_all_fib_operations()

    assert set(written) == {"acq1", "acq2"}
    written_partitions = {c[0][0] for c in backend.write.call_args_list}
    assert written_partitions == {
        "platform_fib_operations/acq1",
        "platform_fib_operations/acq2",
    }


def test_fetch_all_empty_returns_empty_list():
    with (
        patch.object(fib_ops, "_logs_client", return_value=MagicMock()),
        patch.object(fib_ops, "_collect_results", return_value=[]),
        patch.object(fib_ops.registry, "BACKEND") as backend,
    ):
        assert fetch_all_fib_operations() == []
    backend.write.assert_not_called()


def test_platform_fib_operations_reads_cache():
    df = pd.DataFrame({"event_type": ["stage_start"]})
    with patch.object(fib_ops.registry, "BACKEND") as backend:
        backend.read.return_value = df
        out = platform_fib_operations("acq1")
    pd.testing.assert_frame_equal(out, df)


def test_platform_fib_operations_empty_cache_raises():
    with patch.object(fib_ops.registry, "BACKEND") as backend:
        backend.read.return_value = pd.DataFrame()
        with pytest.raises(ValueError, match="Cache is empty"):
            platform_fib_operations("acq1")


def test_platform_fib_operations_lazy_returns_location():
    with patch.object(fib_ops.registry, "BACKEND") as backend:
        backend.get_location.return_value = "s3://bucket/loc"
        out = platform_fib_operations("acq1", lazy=True)
    assert out == "s3://bucket/loc"


def test_platform_fib_operations_force_update_calls_fetch():
    with patch.object(fib_ops, "_fetch_asset_fib_operations", return_value=pd.DataFrame()) as fetch:
        out = platform_fib_operations("acq1", force_update=True)
    fetch.assert_called_once_with("acq1")
    assert out.empty


def test_platform_fib_operations_force_update_no_asset_rebuilds_all():
    with patch.object(fib_ops, "fetch_all_fib_operations", return_value=["a", "b"]) as fetch_all:
        out = platform_fib_operations(force_update=True)
    fetch_all.assert_called_once_with()
    assert out.empty


def test_platform_fib_operations_read_without_asset_raises():
    with pytest.raises(ValueError, match="asset_name is required"):
        platform_fib_operations()


def test_columns_names():
    names = {c.name for c in platform_fib_operations_columns()}
    assert {"timestamp", "ingest_ts", "process_name", "event_type", "error_info"} <= names

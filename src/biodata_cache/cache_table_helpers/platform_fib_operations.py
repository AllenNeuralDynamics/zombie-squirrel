"""Fiber photometry processing-status cache table (partitioned by asset_name).

Reconstructs the processing lifecycle of each fiber photometry acquisition from
the structured pipeline logs emitted to CloudWatch by the
``aind-fiber-photometry-pipeline``. Every log record is a JSON document carrying
the acquisition it belongs to, the pipeline stage (``process_name``), and an
``event_type`` (``stage_start`` / ``stage_complete`` / ``stage_error``). One row is
stored per lifecycle event; ``asset_name`` (the acquisition name) is the partition
key and joins to ``asset_basics``.

A single CloudWatch Logs Insights query fetches every event across all
acquisitions in a lookback window; the events are then grouped locally and one
partition is written per acquisition. This is far cheaper than one query per
asset (each Insights query scans the whole log group).
"""

import json
import logging
import time
import urllib.parse

import boto3
import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

_LOG_GROUP = "/codeocean/pipelines"
_PIPELINE_NAME = "aind-fiber-photometry-pipeline"
_KEEP_EVENT_TYPES = ("stage_start", "stage_complete", "stage_error")
_DEFAULT_LOOKBACK_DAYS = 400
# CloudWatch Logs Insights returns at most 10000 rows per query; when a time
# window hits this cap it is split in half and re-queried so no events are lost.
_MAX_QUERY_RESULTS = 10000
_QUERY_POLL_SECONDS = 2.0
_QUERY_TIMEOUT_SECONDS = 120.0
_REGION = "us-west-2"


def _cloudwatch_url(log_stream: str | None) -> str | None:
    """Build a CloudWatch console deep link to a log stream, or None.

    Links to the ``/codeocean/pipelines`` log group's stream that emitted the
    event, so the full surrounding logs (and traceback) can be inspected in the
    console. The log-group and stream path segments are URL-encoded twice, as the
    CloudWatch console hash router requires.
    """
    if not log_stream:
        return None
    group = urllib.parse.quote(urllib.parse.quote(_LOG_GROUP, safe=""), safe="")
    stream = urllib.parse.quote(urllib.parse.quote(log_stream, safe=""), safe="")
    return (
        f"https://{_REGION}.console.aws.amazon.com/cloudwatch/home?region={_REGION}"
        f"#logsV2:log-groups/log-group/{group}/log-events/{stream}"
    )


def _log(message: str) -> None:
    """Emit a structured cache log message for the fib_operations table."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES["fib_operations"],
            message=message,
        ).to_json()
    )


def _logs_client():
    """Return a boto3 CloudWatch Logs client."""
    return boto3.client("logs")


def _query_string(acquisition_name: str | None = None) -> str:
    """Build the Logs Insights query string for the fiber photometry pipeline.

    Restricts to the lifecycle event types and non-empty acquisition names; when
    ``acquisition_name`` is given, only that acquisition's events are returned.
    """
    event_list = ", ".join(f'"{e}"' for e in _KEEP_EVENT_TYPES)
    lines = [
        "fields @timestamp, @message, @logStream",
        f'| filter pipeline_name = "{_PIPELINE_NAME}"',
        f"| filter event_type in [{event_list}]",
        '| filter acquisition_name != ""',
    ]
    if acquisition_name is not None:
        lines.append(f'| filter acquisition_name = "{acquisition_name}"')
    lines.append("| sort @timestamp asc")
    return "\n".join(lines)


def _run_query(client, start_ms: int, end_ms: int, query_string: str) -> list[list[dict]]:
    """Run one Logs Insights query and return its raw result rows.

    Blocks until the query completes (or times out). Each row is the list of
    ``{"field": ..., "value": ...}`` dicts returned by the API.
    """
    query_id = client.start_query(
        logGroupName=_LOG_GROUP,
        startTime=start_ms // 1000,
        endTime=end_ms // 1000,
        queryString=query_string,
        limit=_MAX_QUERY_RESULTS,
    )["queryId"]
    deadline = time.monotonic() + _QUERY_TIMEOUT_SECONDS
    while True:
        resp = client.get_query_results(queryId=query_id)
        if resp["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            if resp["status"] != "Complete":
                raise RuntimeError(f"CloudWatch query {query_id} ended with status {resp['status']}")
            return resp["results"]
        if time.monotonic() > deadline:
            raise TimeoutError(f"CloudWatch query {query_id} did not complete within {_QUERY_TIMEOUT_SECONDS}s")
        time.sleep(_QUERY_POLL_SECONDS)


def _collect_results(client, start_ms: int, end_ms: int, query_string: str) -> list[list[dict]]:
    """Return all result rows for a window, splitting it if the row cap is hit.

    Logs Insights caps results at ``_MAX_QUERY_RESULTS``; when a window returns
    that many rows it is bisected in time and each half re-queried, recursively,
    so every event is retrieved.
    """
    results = _run_query(client, start_ms, end_ms, query_string)
    if len(results) < _MAX_QUERY_RESULTS or end_ms - start_ms <= 1000:
        return results
    mid = start_ms + (end_ms - start_ms) // 2
    return _collect_results(client, start_ms, mid, query_string) + _collect_results(client, mid, end_ms, query_string)


def _parse_row(row: list[dict]) -> dict | None:
    """Parse one Insights result row into a normalized event dict, or None.

    ``@message`` is a JSON log record; ``@timestamp`` is CloudWatch's ingestion
    time (kept separately as ``ingest_ts``). Rows whose message is not JSON, or
    that lack an acquisition name, are skipped.
    """
    fields = {f["field"]: f["value"] for f in row}
    message = fields.get("@message")
    if not message:
        return None
    try:
        record = json.loads(message)
    except (json.JSONDecodeError, TypeError):
        return None
    asset_name = record.get("acquisition_name")
    if not asset_name:
        return None
    return {
        "asset_name": asset_name,
        "timestamp": record.get("timestamp"),
        "ingest_ts": fields.get("@timestamp"),
        "process_name": record.get("process_name"),
        "event_type": record.get("event_type"),
        "level": record.get("level"),
        "message": record.get("message"),
        "error_info": record.get("exc_info"),
        "cloudwatch_url": _cloudwatch_url(fields.get("@logStream")),
    }


def _events_dataframe(rows: list[list[dict]]) -> pd.DataFrame:
    """Build a normalized events DataFrame from raw Insights result rows."""
    records = [r for r in (_parse_row(row) for row in rows) if r is not None]
    columns = [
        "asset_name",
        "timestamp",
        "ingest_ts",
        "process_name",
        "event_type",
        "level",
        "message",
        "error_info",
        "cloudwatch_url",
    ]
    df = pd.DataFrame(records, columns=columns)
    if df.empty:
        return df
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="ISO8601", utc=True, errors="coerce")
    df["ingest_ts"] = pd.to_datetime(df["ingest_ts"], utc=True, errors="coerce")
    return df


def _window_ms(lookback_days: int) -> tuple[int, int]:
    """Return the ``(start_ms, end_ms)`` epoch-millisecond query window."""
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - lookback_days * 86_400_000
    return start_ms, end_ms


def _write_partition(asset_name: str, df: pd.DataFrame) -> None:
    """Sort one acquisition's events by time and write its partition."""
    cache_key = f"{registry.NAMES['fib_operations']}/{asset_name}"
    registry.BACKEND.clear_partition(cache_key)
    ordered = df.drop(columns=["asset_name"]).sort_values("timestamp").reset_index(drop=True)
    registry.BACKEND.write(cache_key, ordered)


def _fetch_asset_fib_operations(asset_name: str, lookback_days: int = _DEFAULT_LOOKBACK_DAYS) -> pd.DataFrame:
    """Fetch and cache the processing events for a single acquisition.

    Queries CloudWatch for only this acquisition's lifecycle events. Returns an
    empty DataFrame; callers should read back from the backend.
    """
    setup_logging()
    _log(f"Updating cache for asset {asset_name}")
    client = _logs_client()
    start_ms, end_ms = _window_ms(lookback_days)
    rows = _collect_results(client, start_ms, end_ms, _query_string(asset_name))
    df = _events_dataframe(rows)
    if df.empty:
        _log(f"No processing events found for asset {asset_name}")
        return pd.DataFrame()
    _write_partition(asset_name, df)
    _log(f"Cached {len(df)} processing events for asset {asset_name}")
    return pd.DataFrame()


def fetch_all_fib_operations(lookback_days: int = _DEFAULT_LOOKBACK_DAYS) -> list[str]:
    """Fetch every acquisition's events in one bulk query and write all partitions.

    Runs a single Logs Insights query (bisected if it hits the row cap) covering
    the whole lookback window, groups the events by acquisition, and overwrites one
    partition per acquisition. Returns the list of acquisition names written.
    """
    setup_logging()
    client = _logs_client()
    start_ms, end_ms = _window_ms(lookback_days)
    _log(f"Querying fiber photometry pipeline events over the last {lookback_days} days")
    rows = _collect_results(client, start_ms, end_ms, _query_string())
    df = _events_dataframe(rows)
    if df.empty:
        _log("No fiber photometry processing events found")
        return []
    written: list[str] = []
    for asset_name, group in df.groupby("asset_name", sort=False):
        _write_partition(asset_name, group)
        written.append(asset_name)
    _log(f"Cached processing events for {len(written)} acquisitions ({len(df)} events)")
    return written


@registry.register_table(registry.NAMES["fib_operations"])
def platform_fib_operations(
    asset_name: str | None = None,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    """Return fiber photometry processing events, or rebuild the whole table.

    One row per pipeline lifecycle event (``stage_start`` / ``stage_complete`` /
    ``stage_error``) found in the CloudWatch pipeline logs, across every pipeline
    stage (``process_name``). Data is cached per asset_name partition. Group by
    asset_name for a per-acquisition processing-status view.

    A ``force_update`` with no ``asset_name`` rebuilds every partition from a single
    bulk CloudWatch query (the normal sync path); this table is not built one asset
    at a time. Passing ``asset_name`` targets a single acquisition (ad-hoc refresh
    or read).

    Args:
        asset_name: Acquisition name to read/refresh (joins to asset_basics
            ``name``). If None, a ``force_update`` rebuilds the whole table and a
            read is not supported.
        force_update: If True, re-query CloudWatch and write the cache (the whole
            table when ``asset_name`` is None, otherwise just that acquisition). An
            empty DataFrame is returned; read again without force_update.
        lazy: If True, return the partition's storage location string (for DuckDB)
            instead of loading the DataFrame. Requires ``asset_name``.
        location: Unused; accepted for a uniform sync interface.

    Returns:
        DataFrame of processing events; the partition location string if lazy=True;
        or an empty DataFrame if force_update=True (data is written to the cache).

    Raises:
        ValueError: If ``asset_name`` is None on a read, or the cache is empty for
            the asset and force_update is False.
    """
    if asset_name is None:
        if not force_update:
            raise ValueError(
                "asset_name is required to read platform_fib_operations. Call with "
                "force_update=True (no asset_name) to rebuild the whole table."
            )
        fetch_all_fib_operations()
        return pd.DataFrame()

    cache_key = f"{registry.NAMES['fib_operations']}/{asset_name}"

    if lazy:
        if force_update:
            _fetch_asset_fib_operations(asset_name)
        return registry.BACKEND.get_location(cache_key)

    if force_update:
        return _fetch_asset_fib_operations(asset_name)

    df = registry.BACKEND.read(cache_key)
    if df.empty:
        raise ValueError(
            f"Cache is empty for asset {asset_name}. Use force_update=True to fetch data from CloudWatch."
        )
    return df


def platform_fib_operations_columns() -> list[Column]:
    """Return platform_fib_operations cache table column definitions."""
    return [
        Column(name="timestamp", description="Event time (UTC) from the pipeline log record"),
        Column(name="ingest_ts", description="CloudWatch log ingestion time (UTC), kept separately from event time"),
        Column(name="process_name", description="Pipeline stage that emitted the event (e.g. aind-fip-dff)"),
        Column(name="event_type", description="Lifecycle event: stage_start, stage_complete, or stage_error"),
        Column(name="level", description="Log level of the record (e.g. INFO, ERROR)"),
        Column(name="message", description="Human-readable log message"),
        Column(name="error_info", description="Exception traceback for stage_error events; null otherwise"),
        Column(name="cloudwatch_url", description="Deep link to the CloudWatch console log stream for this event"),
    ]

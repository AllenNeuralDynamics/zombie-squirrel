"""Unit tests for platform_df_operations cache table wiring."""

from unittest.mock import patch

import pandas as pd

import biodata_cache.cache_table_helpers.platform_df_operations as df_ops
from biodata_cache.cache_table_helpers.platform_df_operations import (
    fetch_all_df_operations,
    platform_df_operations,
    platform_df_operations_columns,
)


def test_fetch_all_delegates_to_shared():
    with patch.object(df_ops.cw, "fetch_all_operations", return_value=["a"]) as fetch_all:
        assert fetch_all_df_operations() == ["a"]
    fetch_all.assert_called_once_with("df_operations", df_ops._PIPELINE_NAME, df_ops.cw.DEFAULT_LOOKBACK_DAYS)


def test_platform_df_operations_delegates_to_shared():
    df = pd.DataFrame({"event_type": ["stage_start"]})
    with patch.object(df_ops.cw, "platform_operations", return_value=df) as op:
        out = platform_df_operations("acq1", force_update=True, lazy=False)
    op.assert_called_once_with("df_operations", df_ops._PIPELINE_NAME, "acq1", True, False)
    pd.testing.assert_frame_equal(out, df)


def test_pipeline_name():
    assert df_ops._PIPELINE_NAME == "dynamic-foraging-processing-pipeline"


def test_columns_names():
    names = {c.name for c in platform_df_operations_columns()}
    assert {"timestamp", "ingest_ts", "process_name", "event_type", "error_info", "cloudwatch_url"} <= names

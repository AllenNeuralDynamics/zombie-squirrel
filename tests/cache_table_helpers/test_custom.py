"""Unit tests for custom cache table."""

from unittest.mock import patch

import pandas as pd
import pytest

from biodata_cache.cache_table_helpers.custom import custom


@patch("biodata_cache.cache_table_helpers.custom.registry.BACKEND")
def test_df_provided_stores_and_returns_df(mock_backend):
    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    result = custom(name="my_table", df=df)
    mock_backend.write.assert_called_once_with("my_table", df)
    pd.testing.assert_frame_equal(result, df)


@patch("biodata_cache.cache_table_helpers.custom.registry.BACKEND")
def test_retrieval_returns_cached_df(mock_backend):
    cached_df = pd.DataFrame({"x": [10, 20]})
    mock_backend.read.return_value = cached_df
    result = custom(name="my_table")
    mock_backend.read.assert_called_once_with("my_table")
    pd.testing.assert_frame_equal(result, cached_df)


@patch("biodata_cache.cache_table_helpers.custom.registry.BACKEND")
def test_retrieval_empty_cache_raises(mock_backend):
    mock_backend.read.return_value = pd.DataFrame()
    with pytest.raises(ValueError):
        custom(name="my_table")


@patch("biodata_cache.cache_table_helpers.custom.registry.BACKEND")
def test_different_names_are_independent(mock_backend):
    df1 = pd.DataFrame({"a": [1]})
    df2 = pd.DataFrame({"b": [2]})
    mock_backend.read.side_effect = lambda name: df1 if name == "table_a" else df2
    pd.testing.assert_frame_equal(custom(name="table_a"), df1)
    pd.testing.assert_frame_equal(custom(name="table_b"), df2)

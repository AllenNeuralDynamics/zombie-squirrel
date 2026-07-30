"""Unit tests for the swdb_2025 cache tables."""

from unittest.mock import patch

import pandas as pd
import pytest

from biodata_cache.cache_table_helpers.platform_swdb import (
    BCI_PROBLEM_ASSETS,
    _build_bci_dataframe,
    _build_v1dd_dataframe,
    swdb_2025_bci,
    swdb_2025_bci_columns,
    swdb_2025_v1dd,
    swdb_2025_v1dd_columns,
)


def _bci_records():
    return [
        {
            "_id": "id-1",
            "name": "single-plane-ophys_731015_2025-01-28_00-00-00_processed_2025-08-10",
            "project_name": "Brain Computer Interface",
            "session_type": "BCI single neuron stim",
            "subject_id": "731015",
            "genotype": "Slc17a6-IRES-Cre/wt",
            "virus": "pAAV-hSyn1-RiboL1-GCaMP8s-WPRE",
            "date_of_birth": "2024-03-14",
            "sex": "Female",
            "modality": "Planar optical physiology",
            "session_time": "2025-01-28T17:40:57.996000",
            "targeted_structure": "Primary Motor Cortex",
            "ophys_fov": "FOV_04",
            "session_number": 22.0,
        },
        {
            "_id": "id-2",
            "name": BCI_PROBLEM_ASSETS[0],
            "project_name": "Brain Computer Interface",
            "session_type": "BCI single neuron stim",
            "subject_id": "731015",
            "genotype": "Slc17a6-IRES-Cre/wt",
            "virus": "pAAV-hSyn1-RiboL1-GCaMP8s-WPRE",
            "date_of_birth": "2024-03-14",
            "sex": "Female",
            "modality": "Planar optical physiology",
            "session_time": "2025-01-28T17:40:57.996000",
            "targeted_structure": "Primary Motor Cortex",
            "ophys_fov": "FOV_04",
            "session_number": 22.0,
        },
    ]


def _v1dd_records():
    return [
        {
            "_id": "id-1",
            "name": "v1dd_409828_2023-01-01",
            "project_name": "V1 Deep Dive",
            "subject_id": "409828",
            "genotype": "wt",
            "date_of_birth": "2022-01-01",
            "sex": "Male",
            "modality": ["Planar optical physiology"],
            "session_time": "2023-01-31T10:20:30.000000",
            "column": "column 3",
            "volume": "volume 5",
        },
        {
            "_id": "id-2",
            "name": "v1dd_111111_2023-02-01",
            "project_name": "V1 Deep Dive",
            "subject_id": "111111",
            "genotype": "wt",
            "date_of_birth": "2022-01-01",
            "sex": "Female",
            "modality": ["Planar optical physiology"],
            "session_time": "2023-02-01T10:20:30.000000",
            "column": "column 1",
            "volume": "volume 2",
        },
    ]


def test_build_bci_dataframe_computes_derived_columns_and_drops_problem_assets():
    df = _build_bci_dataframe(_bci_records())

    assert len(df) == 1
    row = df.iloc[0]
    assert row["name"] not in BCI_PROBLEM_ASSETS
    assert str(row["session_date"]) == "2025-01-28"
    assert str(row["session_time"]) == "17:40:57.996000"
    assert str(row["date_of_birth"]) == "2024-03-14"
    assert row["age"] == 320
    assert list(df.columns) == [c.name for c in swdb_2025_bci_columns()]


def test_build_v1dd_dataframe_computes_derived_columns():
    df = _build_v1dd_dataframe(_v1dd_records())

    assert len(df) == 2
    assert list(df.columns) == [c.name for c in swdb_2025_v1dd_columns()]
    golden = df[df["subject_id"] == "409828"].iloc[0]
    assert golden["golden_mouse"]
    assert golden["column"] == 3
    assert golden["volume"] == 5
    assert str(golden["session_date"]) == "2023-01-31"
    assert not df[df["subject_id"] == "111111"].iloc[0]["golden_mouse"]


@patch("biodata_cache.cache_table_helpers.platform_swdb.registry.BACKEND")
def test_bci_cache_hit(mock_backend):
    cached = pd.DataFrame({"name": ["asset-1"]})
    mock_backend.read.return_value = cached
    result = swdb_2025_bci(force_update=False)
    assert list(result["name"]) == ["asset-1"]
    mock_backend.write.assert_not_called()


@patch("biodata_cache.cache_table_helpers.platform_swdb.registry.BACKEND")
def test_bci_empty_cache_raises_error(mock_backend):
    mock_backend.read.return_value = pd.DataFrame()
    with pytest.raises(ValueError, match="Cache is empty"):
        swdb_2025_bci(force_update=False)


@patch("biodata_cache.cache_table_helpers.platform_swdb._fetch_bci_records")
@patch("biodata_cache.cache_table_helpers.platform_swdb.registry.BACKEND")
def test_bci_cache_miss_fetches_and_writes(mock_backend, mock_fetch):
    mock_backend.read.return_value = pd.DataFrame()
    mock_fetch.return_value = _bci_records()

    result = swdb_2025_bci(force_update=True)

    assert len(result) == 1
    mock_backend.write.assert_called_once()


@patch("biodata_cache.cache_table_helpers.platform_swdb.registry.BACKEND")
def test_v1dd_empty_cache_raises_error(mock_backend):
    mock_backend.read.return_value = pd.DataFrame()
    with pytest.raises(ValueError, match="Cache is empty"):
        swdb_2025_v1dd(force_update=False)


@patch("biodata_cache.cache_table_helpers.platform_swdb._fetch_v1dd_records")
@patch("biodata_cache.cache_table_helpers.platform_swdb.registry.BACKEND")
def test_v1dd_cache_miss_fetches_and_writes(mock_backend, mock_fetch):
    mock_backend.read.return_value = pd.DataFrame()
    mock_fetch.return_value = _v1dd_records()

    result = swdb_2025_v1dd(force_update=True)

    assert len(result) == 2
    mock_backend.write.assert_called_once()

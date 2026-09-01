"""Unit tests for biodata_cache.sync module."""

import json
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest

from biodata_cache.sync import (
    CELL_BY_EVERYTHING_SOURCE_JOBS,
    JOBS,
    PARALLEL_JOBS,
    publish_cache_registry,
    publish_registry_fragment,
    run_sync_job,
    update_all_tables,
)


def _make_registry(basics_df=None, sessions_df=None):
    """Build a TABLE_REGISTRY-like dict of MagicMocks for every table name."""
    if basics_df is None:
        basics_df = pd.DataFrame({"subject_id": ["sub1"]})
    if sessions_df is None:
        sessions_df = pd.DataFrame({"subject_id": []})
    mocks = {
        "unique_project_names": MagicMock(),
        "unique_subject_ids": MagicMock(),
        "unique_genotypes": MagicMock(),
        "asset_basics": MagicMock(return_value=basics_df),
        "source_data": MagicMock(),
        "raw_to_derived": MagicMock(),
        "quality_control": MagicMock(),
        "platform_smartspim": MagicMock(),
        "platform_exaspim": MagicMock(),
        "metadata_upgrade": MagicMock(),
        "platform_fib": MagicMock(),
        "platform_fib_traces": MagicMock(),
        "platform_pophys": MagicMock(),
        "platform_visual_coding_ophys": MagicMock(),
        "platform_visual_learning_cell_gene": MagicMock(),
        "platform_visual_learning_coreg": MagicMock(),
        "platform_fib_operations": MagicMock(),
        "platform_df_operations": MagicMock(),
        "platform_ecephys_spikes": MagicMock(),
        "platform_ecephys_units": MagicMock(),
        "platform_mouselight": MagicMock(),
        "platform_dynamic_foraging_sessions": MagicMock(return_value=sessions_df),
        "platform_dynamic_foraging_trials": MagicMock(),
        "platform_dynamic_foraging_events": MagicMock(),
        "behavior_curriculum": MagicMock(),
        "platform_qc": MagicMock(),
        "time_to_qc": MagicMock(),
        "storage_lens": MagicMock(),
    }
    return mocks


def _mock_backend():
    """A backend mock that satisfies CacheTable construction and partition checks."""
    backend = MagicMock()
    backend.get_location.return_value = "s3://bucket/path"
    backend.partition_exists.return_value = False
    return backend


# --- run_sync_job dispatch ---------------------------------------------------


def test_run_sync_job_uses_env_var(monkeypatch):
    spy = MagicMock()
    monkeypatch.setenv("BIODATA_CACHE_SYNC_JOB", "asset_basics")
    with patch.dict("biodata_cache.sync.JOBS", {"asset_basics": spy}):
        run_sync_job()
    spy.assert_called_once_with()


def test_run_sync_job_explicit_arg_beats_env(monkeypatch):
    spy = MagicMock()
    monkeypatch.setenv("BIODATA_CACHE_SYNC_JOB", "asset_basics")
    with patch.dict("biodata_cache.sync.JOBS", {"fast": spy}):
        run_sync_job("fast")
    spy.assert_called_once_with()


def test_run_sync_job_no_job_raises(monkeypatch):
    monkeypatch.delenv("BIODATA_CACHE_SYNC_JOB", raising=False)
    with pytest.raises(ValueError, match="No sync job specified"):
        run_sync_job()


def test_run_sync_job_unknown_job_raises(monkeypatch):
    monkeypatch.delenv("BIODATA_CACHE_SYNC_JOB", raising=False)
    with pytest.raises(ValueError, match="Unknown sync job"):
        run_sync_job("does_not_exist")


def test_parallel_jobs_excludes_asset_basics():
    assert "asset_basics" not in PARALLEL_JOBS
    # cell-by-everything is also excluded: it projects the tables the parallel
    # jobs build, so it runs as a final stage rather than alongside them.
    assert "cell-by-everything" not in PARALLEL_JOBS
    assert set(PARALLEL_JOBS) | {"asset_basics", "cell-by-everything"} == set(JOBS)


# --- asset_basics job --------------------------------------------------------


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_asset_basics_job_registers_and_publishes(mock_registry, mock_backend):
    mock_registry.__getitem__.side_effect = _make_registry().__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"

    run_sync_job("asset_basics")

    # The registry must NOT be cleared: a failed job would otherwise drop its
    # table from visibility even though its parquet data is intact.
    mock_backend.clear_registry.assert_not_called()
    mock_backend.register_version.assert_called_once()
    mock_registry["asset_basics"].assert_called_once_with(force_update=True)
    mock_registry["source_data"].assert_called_once_with(force_update=True)
    published = {c[0][0] for c in mock_backend.put_registry_fragment.call_args_list}
    assert published == {"asset_basics", "source_data"}


# --- fast job ----------------------------------------------------------------


@patch("biodata_cache.sync.PLATFORMS", ["p1", "p2"])
@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_fast_job_builds_all_fast_tables(mock_registry, mock_backend):
    reg = _make_registry()
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"

    run_sync_job("fast")

    for name in ("unique_project_names", "unique_subject_ids", "unique_genotypes",
                 "metadata_upgrade", "platform_fib", "platform_mouselight"):
        reg[name].assert_called_once_with(force_update=True)
    reg["platform_qc"].assert_has_calls(
        [call(platform="p1", force_update=True), call(platform="p2", force_update=True)]
    )
    published = {c[0][0] for c in mock_backend.put_registry_fragment.call_args_list}
    assert published == {
        "unique_project_names", "unique_subject_ids", "unique_genotypes",
        "metadata_upgrade", "platform_fib", "platform_mouselight", "platform_qc",
    }
    # fast job never touches asset_basics or source_data (built by the asset_basics job)
    reg["asset_basics"].assert_not_called()
    reg["source_data"].assert_not_called()


# --- qc job ------------------------------------------------------------------


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_qc_job_called_per_subject(mock_registry, mock_backend):
    reg = _make_registry(basics_df=pd.DataFrame({"subject_id": ["sub1", "sub2", None]}))
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"

    run_sync_job("qc")

    reg["quality_control"].assert_has_calls(
        [call(subject_id="sub1", force_update=True), call(subject_id="sub2", force_update=True)],
        any_order=True,
    )
    assert reg["quality_control"].call_count == 2
    # asset_basics read from cache, not force-updated
    reg["asset_basics"].assert_called_once_with()
    assert mock_backend.put_registry_fragment.call_args[0][0] == "quality_control"


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_qc_job_no_subjects_still_publishes(mock_registry, mock_backend):
    reg = _make_registry(basics_df=pd.DataFrame({"subject_id": [None, None]}))
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"

    run_sync_job("qc")

    reg["quality_control"].assert_not_called()
    mock_backend.put_registry_fragment.assert_called_once()


# --- df job ------------------------------------------------------------------


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_df_job_sessions_then_per_subject(mock_registry, mock_backend):
    reg = _make_registry(sessions_df=pd.DataFrame({"subject_id": ["s1", "s2"]}))
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"

    run_sync_job("df")

    reg["platform_dynamic_foraging_sessions"].assert_called_once_with(force_update=True)
    assert reg["platform_dynamic_foraging_trials"].call_count == 2
    assert reg["platform_dynamic_foraging_events"].call_count == 2
    published = {c[0][0] for c in mock_backend.put_registry_fragment.call_args_list}
    assert published == {
        "platform_dynamic_foraging_sessions",
        "platform_dynamic_foraging_trials",
        "platform_dynamic_foraging_events",
    }


# --- fib_traces / ecephys jobs ----------------------------------------------


def _fib_basics():
    return pd.DataFrame(
        {
            "subject_id": ["sub1", "sub2"],
            "name": ["asset1", "asset2"],
            "location": ["s3://bucket/asset1", "s3://bucket/asset2"],
            "modalities": [["fib"], ["behavior"]],
            "data_level": ["derived", "derived"],
        }
    )


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_fib_traces_job_per_derived_fib_asset(mock_registry, mock_backend):
    reg = _make_registry(basics_df=_fib_basics())
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"
    mock_backend.partition_exists.return_value = False

    run_sync_job("fib_traces")

    reg["platform_fib_traces"].assert_called_once_with(
        asset_name="asset1", location="s3://bucket/asset1", force_update=True
    )
    assert mock_backend.put_registry_fragment.call_args[0][0] == "platform_fib_traces"


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_fib_traces_job_skips_existing_partitions(mock_registry, mock_backend):
    reg = _make_registry(basics_df=_fib_basics())
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"
    mock_backend.partition_exists.return_value = True

    run_sync_job("fib_traces")

    reg["platform_fib_traces"].assert_not_called()
    # still publishes the registry fragment
    mock_backend.put_registry_fragment.assert_called_once()


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_fib_traces_job_isolates_asset_failures(mock_registry, mock_backend):
    df = pd.DataFrame(
        {
            "name": ["asset1", "asset2"],
            "location": ["s3://bucket/asset1", "s3://bucket/asset2"],
            "modalities": [["fib"], ["fib"]],
            "data_level": ["derived", "derived"],
        }
    )
    reg = _make_registry(basics_df=df)
    reg["platform_fib_traces"].side_effect = [RuntimeError("unsupported"), None]
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"
    mock_backend.partition_exists.return_value = False

    run_sync_job("fib_traces")

    assert reg["platform_fib_traces"].call_count == 2
    assert mock_backend.put_registry_fragment.call_args[0][0] == "platform_fib_traces"


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.build_all_operations")
def test_operations_job_single_pull(mock_build, mock_backend):
    mock_backend.get_location.return_value = "s3://bucket/path"

    run_sync_job("operations")

    mock_build.assert_called_once_with()
    published = {c[0][0] for c in mock_backend.put_registry_fragment.call_args_list}
    assert {"platform_fib_operations", "platform_df_operations"} <= published


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_ecephys_jobs_run_over_ecephys_assets(mock_registry, mock_backend):
    df = pd.DataFrame(
        {
            "name": ["ec1", "other"],
            "location": ["s3://bucket/ec1", "s3://bucket/other"],
            "modalities": [["ecephys"], ["fib"]],
            "data_level": ["derived", "derived"],
        }
    )
    reg = _make_registry(basics_df=df)
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"
    mock_backend.partition_exists.return_value = False

    run_sync_job("ecephys_spikes")
    run_sync_job("ecephys_units")

    reg["platform_ecephys_spikes"].assert_called_once_with(
        asset_name="ec1", location="s3://bucket/ec1", force_update=True
    )
    reg["platform_ecephys_units"].assert_called_once_with(
        asset_name="ec1", location="s3://bucket/ec1", force_update=True
    )


def _pophys_basics():
    return pd.DataFrame(
        {
            "name": [
                "multiplane-ophys_asset_processed_1",
                "legacy_filtered_asset",
                "single-plane-ophys_bci_asset",
                "other",
            ],
            "location": [
                "s3://bucket/processed",
                "s3://bucket/filtered",
                "s3://bucket/bci",
                "s3://bucket/other",
            ],
            "modalities": [["pophys"], ["pophys"], ["ophys"], ["behavior"]],
            "data_level": ["derived", "derived", "derived", "derived"],
        }
    )


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_pophys_job_processes_all_derived_pophys_names(mock_registry, mock_backend):
    reg = _make_registry(basics_df=_pophys_basics())
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"
    mock_backend.partition_exists.return_value = False

    run_sync_job("pophys")

    assert [c.kwargs["asset_name"] for c in reg["platform_pophys"].call_args_list] == [
        "multiplane-ophys_asset_processed_1",
        "legacy_filtered_asset",
        "single-plane-ophys_bci_asset",
    ]


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_visual_coding_ophys_job_owns_canonical_visual_coding_assets(mock_registry, mock_backend):
    basics = pd.DataFrame(
        {
            "name": ["vco_asset", "ordinary_pophys"],
            "location": ["s3://bucket/vco", "s3://bucket/ordinary"],
            "project_name": ["Allen Brain Observatory - Visual Coding Ophys", "Other"],
            "modalities": [["pophys"], ["pophys"]],
            "data_level": ["derived", "derived"],
        }
    )
    reg = _make_registry(basics_df=basics)
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"
    mock_backend.partition_exists.return_value = False

    run_sync_job("visual_coding_ophys")

    reg["platform_visual_coding_ophys"].assert_called_once_with(
        asset_name="vco_asset", location="s3://bucket/vco", force_update=True
    )


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_visual_learning_job_builds_both_public_lookup_tables(mock_registry, mock_backend):
    reg = _make_registry()
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"

    run_sync_job("visual_learning")

    reg["platform_visual_learning_cell_gene"].assert_called_once_with(force_update=True)
    reg["platform_visual_learning_coreg"].assert_called_once_with(force_update=True)
    published = {call_args[0][0] for call_args in mock_backend.put_registry_fragment.call_args_list}
    assert published == {"platform_visual_learning_cell_gene", "platform_visual_learning_coreg"}


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.build_cell_by_everything")
def test_cell_by_everything_job_builds_and_publishes_all_three(mock_build, mock_backend):
    mock_backend.get_location.return_value = "s3://bucket/path"

    run_sync_job("cell-by-everything")

    mock_build.assert_called_once_with()
    published = {call_args[0][0] for call_args in mock_backend.put_registry_fragment.call_args_list}
    assert published == {"cell_index", "cell_properties", "cell_genes"}


def test_cell_by_everything_sources_are_real_jobs():
    """The documented upstream dependencies must stay valid job names."""
    assert set(CELL_BY_EVERYTHING_SOURCE_JOBS) <= set(JOBS)


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_pophys_job_retries_child_from_processable_source(mock_registry, mock_backend):
    basics = pd.DataFrame(
        {
            "name": ["legacy_filtered_asset", "canonical_nwb_asset"],
            "location": ["s3://bucket/filtered", "s3://bucket/nwb"],
            "modalities": [["pophys"], ["pophys"]],
            "data_level": ["derived", "derived"],
        }
    )
    reg = _make_registry(basics_df=basics)
    reg["source_data"].return_value = pd.DataFrame(
        {"name": ["legacy_filtered_asset"], "source_data": ["canonical_nwb_asset"]}
    )
    reg["platform_pophys"].side_effect = [None, None]
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"
    mock_backend.partition_exists.side_effect = [False, False, True]

    run_sync_job("pophys")

    reg["platform_pophys"].assert_has_calls(
        [
            call(
                asset_name="legacy_filtered_asset",
                location="s3://bucket/filtered",
                raw_name="canonical_nwb_asset",
                force_update=True,
            ),
            call(
                asset_name="legacy_filtered_asset",
                location="s3://bucket/nwb",
                raw_name="canonical_nwb_asset",
                force_update=True,
            ),
        ]
    )


# --- update_all_tables -------------------------------------------------------


@patch("biodata_cache.sync.run_sync_job")
def test_update_all_tables_runs_every_job(mock_run):
    update_all_tables()
    ran = [c[0][0] for c in mock_run.call_args_list]
    assert ran[0] == "asset_basics"
    assert set(ran) == set(JOBS)


@patch("biodata_cache.sync.run_sync_job")
def test_update_all_tables_fast_only(mock_run):
    update_all_tables(fast=True, slow=False)
    ran = [c[0][0] for c in mock_run.call_args_list]
    assert ran == ["asset_basics", "fast"]


@patch("biodata_cache.sync.run_sync_job")
def test_update_all_tables_slow_only(mock_run):
    update_all_tables(fast=False, slow=True)
    ran = [c[0][0] for c in mock_run.call_args_list]
    assert ran[0] == "asset_basics"
    assert "fast" not in ran
    assert "qc" in ran and "time_to_qc" in ran


@patch("biodata_cache.sync.BACKEND")
@patch("biodata_cache.sync.TABLE_REGISTRY")
def test_update_all_tables_propagates_exceptions(mock_registry, mock_backend):
    reg = _make_registry()
    reg["asset_basics"] = MagicMock(side_effect=Exception("Update failed"))
    mock_registry.__getitem__.side_effect = reg.__getitem__
    mock_backend.get_location.return_value = "s3://bucket/path"

    with pytest.raises(Exception, match="Update failed"):
        update_all_tables()


# --- registry fragments ------------------------------------------------------


@patch("biodata_cache.sync.BACKEND")
def test_publish_cache_registry_writes_a_fragment_per_table(mock_backend):
    mock_backend.get_location.return_value = "s3://bucket/path"
    publish_cache_registry()
    assert mock_backend.put_registry_fragment.call_count == 43


@patch("biodata_cache.sync.BACKEND")
def test_publish_cache_registry_fragment_names(mock_backend):
    mock_backend.get_location.return_value = "s3://bucket/path"
    publish_cache_registry()
    names = {c[0][0] for c in mock_backend.put_registry_fragment.call_args_list}
    for expected in (
        "unique_project_names", "unique_subject_ids", "unique_genotypes", "asset_basics",
        "source_data", "quality_control", "platform_smartspim", "metadata_upgrade",
        "platform_fib", "platform_qc", "platform_dynamic_foraging_sessions",
        "platform_dynamic_foraging_trials", "platform_dynamic_foraging_events",
        "platform_visual_learning_cell_gene", "platform_visual_learning_coreg",
    ):
        assert expected in names


@patch("biodata_cache.sync.BACKEND")
def test_publish_registry_fragment_payload_is_valid_cache_table(mock_backend):
    mock_backend.get_location.return_value = "s3://bucket/path"
    publish_registry_fragment("quality_control")
    name, payload = mock_backend.put_registry_fragment.call_args[0]
    assert name == "quality_control"
    parsed = json.loads(payload)
    assert parsed["name"] == "quality_control"
    assert parsed["partitioned"] is True
    assert parsed["partition_key"] == "subject_id"
    assert parsed["type"] == "asset"
    assert len(parsed["columns"]) > 0

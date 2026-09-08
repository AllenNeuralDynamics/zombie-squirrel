"""Tests for the published table specification manifest."""

from biodata_cache.registry import TABLE_REGISTRY
from biodata_cache.sync import JOBS
from biodata_cache.table_specs import NAMES, PARTITION_KEYS, TABLE_SPECS, TABLE_SPECS_BY_NAME


def test_table_specs_cover_registered_tables():
    spec_names = {spec.name for spec in TABLE_SPECS}
    assert spec_names == set(TABLE_REGISTRY)
    assert spec_names == set(NAMES.values()) - {"raw_to_derived"}
    assert set(TABLE_SPECS_BY_NAME) == spec_names


def test_table_specs_have_unique_keys_and_names():
    assert len(TABLE_SPECS) == len({spec.key for spec in TABLE_SPECS})
    assert len(TABLE_SPECS) == len({spec.name for spec in TABLE_SPECS})


def test_table_specs_preserve_storage_contracts():
    quality_control = TABLE_SPECS_BY_NAME["quality_control"]
    assert quality_control.storage_name == "qc"
    assert quality_control.partition_key == "subject_id"
    assert PARTITION_KEYS["qc"] == "subject_id"

    metadata_core = TABLE_SPECS_BY_NAME["metadata_core"]
    assert metadata_core.sync_job == "fast"
    assert metadata_core.lifecycle == "nightly"

    assert TABLE_SPECS_BY_NAME["cell_properties"].partition_key == "asset_name"
    assert TABLE_SPECS_BY_NAME["cell_genes"].partition_key == "subject_id"
    virtual = TABLE_SPECS_BY_NAME["platform_ecephys_virtual"]
    assert virtual.storage_name == "platform_ecephys_virtual_index"
    assert virtual.partition_key == "asset_name"

    manual_tables = [spec for spec in TABLE_SPECS if spec.lifecycle == "manual"]
    assert manual_tables
    assert all(spec.sync_job is None for spec in manual_tables)


def test_table_spec_jobs_are_known_sync_jobs():
    scheduled_jobs = {spec.sync_job for spec in TABLE_SPECS if spec.sync_job is not None}
    assert scheduled_jobs <= set(JOBS)

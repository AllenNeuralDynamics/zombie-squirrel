"""Unit tests for biodata_cache.models module."""

import json

from biodata_cache.cache_table_helpers.asset_basics import asset_basics_columns
from biodata_cache.cache_table_helpers.qc import qc_columns
from biodata_cache.cache_table_helpers.source_data import source_data_columns
from biodata_cache.cache_table_helpers.unique_genotypes import unique_genotypes_columns
from biodata_cache.cache_table_helpers.unique_project_names import unique_project_names_columns
from biodata_cache.cache_table_helpers.unique_subject_ids import unique_subject_ids_columns
from biodata_cache.models import CacheRegistry, CacheTable, CacheTableType, Column


def _make_cache_table(**kwargs):
    defaults = {
        "name": "test_table",
        "description": "A test cache table",
        "location": "s3://bucket/path/file.pqt",
        "partitioned": False,
        "type": CacheTableType.metadata,
        "columns": [Column(name="col1", description=""), Column(name="col2", description="")],
    }
    defaults.update(kwargs)
    return CacheTable(**defaults)


# --- CacheTableType ---


def test_metadata_value():
    assert CacheTableType.metadata.value == "metadata"


def test_asset_value():
    assert CacheTableType.asset.value == "asset"


def test_event_value():
    assert CacheTableType.event.value == "event"


def test_realtime_value():
    assert CacheTableType.realtime.value == "realtime"


def test_all_types_count():
    assert len(CacheTableType) == 5


# --- CacheTable ---


def test_cache_table_basic_creation():
    cache_table = _make_cache_table()
    assert cache_table.name == "test_table"
    assert cache_table.location == "s3://bucket/path/file.pqt"
    assert cache_table.partitioned is False
    assert cache_table.partition_key is None
    assert cache_table.type == CacheTableType.metadata
    assert cache_table.columns == [Column(name="col1", description=""), Column(name="col2", description="")]


def test_cache_table_partitioned_with_key():
    cache_table = _make_cache_table(partitioned=True, partition_key="subject_id", type=CacheTableType.asset)
    assert cache_table.partitioned is True
    assert cache_table.partition_key == "subject_id"


def test_cache_table_partition_key_defaults_none():
    assert _make_cache_table().partition_key is None


def test_cache_table_asset_type():
    assert _make_cache_table(type=CacheTableType.asset).type == CacheTableType.asset


def test_cache_table_event_type():
    assert _make_cache_table(type=CacheTableType.event).type == CacheTableType.event


def test_cache_table_realtime_type():
    assert _make_cache_table(type=CacheTableType.realtime).type == CacheTableType.realtime


def test_cache_table_serialization_includes_type_value():
    data = json.loads(_make_cache_table(type=CacheTableType.metadata).model_dump_json())
    assert data["type"] == "metadata"


def test_cache_table_serialization_includes_all_fields():
    data = json.loads(_make_cache_table().model_dump_json())
    for field in ("name", "description", "location", "partitioned", "partition_key", "type", "columns"):
        assert field in data


def test_cache_table_columns_preserved():
    cols = [
        Column(name="_id", description=""),
        Column(name="_last_modified", description=""),
        Column(name="subject_id", description=""),
    ]
    assert _make_cache_table(columns=cols).columns == cols


# --- CacheRegistry ---


def _make_registry_table(name="a"):
    return CacheTable(
        name=name,
        description="A test cache table",
        location="s3://bucket/path.pqt",
        partitioned=False,
        type=CacheTableType.metadata,
        columns=[Column(name="col1", description="")],
    )


def test_registry_empty_tables():
    assert CacheRegistry(tables=[]).tables == []


def test_registry_single_table():
    assert len(CacheRegistry(tables=[_make_registry_table()]).tables) == 1


def test_registry_multiple_tables():
    assert len(CacheRegistry(tables=[_make_registry_table(name=f"table_{i}") for i in range(3)]).tables) == 3


def test_registry_serialization_top_level_key():
    data = json.loads(CacheRegistry(tables=[_make_registry_table()]).model_dump_json())
    assert "tables" in data
    assert len(data["tables"]) == 1


def test_registry_serialization_roundtrip():
    cache_table = CacheTable(
        name="quality_control",
        description="QC data per subject",
        location="s3://bucket/qc/",
        partitioned=True,
        partition_key="subject_id",
        type=CacheTableType.asset,
        columns=[Column(name="name", description=""), Column(name="stage", description="")],
    )
    data = json.loads(CacheRegistry(tables=[cache_table]).model_dump_json())
    restored = CacheRegistry.model_validate(data)
    assert restored.tables[0].name == "quality_control"
    assert restored.tables[0].partition_key == "subject_id"
    assert restored.tables[0].partitioned is True


# --- Column helpers ---


def test_unique_project_names_columns():
    cols = unique_project_names_columns()
    assert isinstance(cols, list)
    assert "project_name" in [c.name for c in cols]


def test_unique_subject_ids_columns():
    cols = unique_subject_ids_columns()
    assert isinstance(cols, list)
    assert "subject_id" in [c.name for c in cols]


def test_unique_genotypes_columns():
    cols = unique_genotypes_columns()
    assert isinstance(cols, list)
    assert "genotype" in [c.name for c in cols]


def test_asset_basics_columns():
    names = [c.name for c in asset_basics_columns()]
    for expected in ("_id", "_last_modified", "subject_id", "modalities", "project_name"):
        assert expected in names


def test_source_data_columns():
    assert "source_data" in [c.name for c in source_data_columns()]


def test_qc_columns():
    names = [c.name for c in qc_columns()]
    for expected in ("name", "stage", "status", "asset_name"):
        assert expected in names

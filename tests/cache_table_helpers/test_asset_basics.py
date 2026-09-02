"""Unit tests for asset_basics cache table."""

from unittest.mock import MagicMock, patch

import pandas as pd

from biodata_cache.cache_table_helpers.asset_basics import asset_basics


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_cache_hit(mock_backend, mock_client_class):
    cached_df = pd.DataFrame(
        {
            "_id": ["id1", "id2"],
            "_last_modified": ["2023-01-01", "2023-01-02"],
            "modalities": ["imaging", "electrophysiology"],
            "project_name": ["proj1", "proj2"],
            "data_level": ["raw", "derived"],
            "subject_id": ["sub001", "sub002"],
            "acquisition_start_time": ["2023-01-01T10:00:00", "2023-01-02T10:00:00"],
            "acquisition_end_time": ["2023-01-01T11:00:00", "2023-01-02T11:00:00"],
        }
    )
    mock_backend.read.return_value = cached_df
    result = asset_basics(force_update=False)
    assert len(result) == 2
    assert list(result["_id"]) == ["id1", "id2"]
    mock_client_class.assert_not_called()


@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_filtered_read_uses_backend_pushdown(mock_backend):
    expected = pd.DataFrame({"name": ["asset[1]"]})
    mock_backend.cache_exists.return_value = True
    mock_backend.read_filtered.return_value = expected

    result = asset_basics(
        subject_id="subject-1",
        project_name="Project",
        modality="ECEPHYS",
        name="asset[1]",
        acquisition_start_after="2023-01-01T00:00:00Z",
        columns=["name"],
        limit=1,
    )

    pd.testing.assert_frame_equal(result, expected)
    mock_backend.read.assert_not_called()
    mock_backend.read_filtered.assert_called_once()
    call_kwargs = mock_backend.read_filtered.call_args.kwargs
    assert [(predicate.column, predicate.operator, predicate.value) for predicate in call_kwargs["filters"]] == [
        ("subject_id", "eq", "subject-1"),
        ("project_name", "eq", "Project"),
        ("modalities", "contains", "ECEPHYS"),
        ("name", "eq", "asset[1]"),
        ("acquisition_start_time", "gte", "2023-01-01T00:00:00Z"),
    ]
    assert call_kwargs["columns"] == ["name"]
    assert call_kwargs["limit"] == 1


@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_filtered_read_uses_lightweight_default_projection(mock_backend):
    mock_backend.cache_exists.return_value = True
    mock_backend.read_filtered.return_value = pd.DataFrame({"name": ["asset"]})

    asset_basics(name="asset")

    projected_columns = mock_backend.read_filtered.call_args.kwargs["columns"]
    assert "name" in projected_columns
    assert "experimenters" not in projected_columns
    assert "investigators" not in projected_columns
    assert "code_ocean" not in projected_columns


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_empty_cache_fetches_from_db(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.side_effect = [
        [{"_id": "id1", "_last_modified": "2023-01-01"}],
        [
            {
                "_id": "id1",
                "_last_modified": "2023-01-01",
                "data_description": {
                    "modalities": [{"abbreviation": "img"}],
                    "project_name": "proj1",
                    "data_level": "raw",
                },
                "subject": {"subject_id": "sub001"},
                "acquisition": {
                    "acquisition_start_time": "2023-01-01T10:00:00",
                    "acquisition_end_time": "2023-01-01T11:00:00",
                },
            }
        ],
    ]
    result = asset_basics(force_update=False)
    assert len(result) == 1
    assert result.iloc[0]["_id"] == "id1"


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_cache_miss(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {"modalities": [{"abbreviation": "img"}], "project_name": "proj1", "data_level": "raw"},
            "subject": {"subject_id": "sub001"},
            "acquisition": {
                "acquisition_start_time": "2023-01-01T10:00:00",
                "acquisition_end_time": "2023-01-01T11:00:00",
            },
        }
    ]
    result = asset_basics(force_update=True)
    assert len(result) == 1
    assert result.iloc[0]["_id"] == "id1"
    assert result.iloc[0]["modalities"] == ["img"]
    assert result.iloc[0]["project_name"] == "proj1"


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_with_data_processes(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {"modalities": [{"abbreviation": "img"}], "project_name": "proj1", "data_level": "raw"},
            "subject": {"subject_id": "sub001"},
            "acquisition": {
                "acquisition_start_time": "2023-01-01T10:00:00",
                "acquisition_end_time": "2023-01-01T11:00:00",
            },
            "processing": {
                "data_processes": [
                    {"start_date_time": "2023-01-15T14:30:00"},
                    {"start_date_time": "2023-01-20T09:15:00"},
                ]
            },
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["process_date"] == "2023-01-20"


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_incremental_update(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.side_effect = [
        [{"_id": "id1", "_last_modified": "2023-01-01"}, {"_id": "id2", "_last_modified": "2023-01-02"}],
        [
            {
                "_id": "id2",
                "_last_modified": "2023-01-02",
                "data_description": {
                    "modalities": [{"abbreviation": "elec"}],
                    "project_name": "proj2",
                    "data_level": "derived",
                },
                "subject": {"subject_id": "sub002"},
                "acquisition": {
                    "acquisition_start_time": "2023-01-02T10:00:00",
                    "acquisition_end_time": "2023-01-02T11:00:00",
                },
            }
        ],
    ]
    result = asset_basics(force_update=True)
    assert len(result) == 1
    assert result.iloc[0]["_id"] == "id2"


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_with_other_identifiers_no_code_ocean(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {"modalities": [{"abbreviation": "img"}], "project_name": "proj1", "data_level": "raw"},
            "subject": {"subject_id": "sub001"},
            "acquisition": {
                "acquisition_start_time": "2023-01-01T10:00:00",
                "acquisition_end_time": "2023-01-01T11:00:00",
            },
            "other_identifiers": {"Some Other Field": "value123"},
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["code_ocean"] is None


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_with_code_ocean_identifier(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {"modalities": [{"abbreviation": "img"}], "project_name": "proj1", "data_level": "raw"},
            "subject": {"subject_id": "sub001"},
            "acquisition": {
                "acquisition_start_time": "2023-01-01T10:00:00",
                "acquisition_end_time": "2023-01-01T11:00:00",
            },
            "other_identifiers": {"Code Ocean": ["df429003-91a0-45d2-8457-66b156ad8bfa"]},
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["code_ocean"] == ["df429003-91a0-45d2-8457-66b156ad8bfa"]


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_age_calculated_from_date_of_birth(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {},
            "acquisition": {
                "acquisition_start_time": "2023-06-01T00:00:00",
                "subject_details": {"date_of_birth": "2023-01-01"},
            },
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["age"] == 151


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_age_calculated_from_year_of_birth(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {},
            "acquisition": {
                "acquisition_start_time": "2023-06-01T00:00:00",
                "subject_details": {"year_of_birth": 2023},
            },
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["age"] == 151


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_age_none_when_no_birth_info(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {},
            "acquisition": {"acquisition_start_time": "2023-06-01T00:00:00"},
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["age"] is None


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_acquisition_type_stored(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {},
            "acquisition": {"acquisition_type": "multiplane-2photon"},
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["acquisition_type"] == "multiplane-2photon"


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_experimenters_stored_as_list(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {},
            "acquisition": {"experimenters": ["huy.nguyen", "jane.doe"]},
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["experimenters"] == ["huy.nguyen", "jane.doe"]


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_experimenters_stored_as_list_dicts(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {},
            "acquisition": {
                "experimenters": [
                    {"name": "Jane Doe", "object_type": "Person"},
                    {"name": "John Smith", "object_type": "Person"},
                ]
            },
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["experimenters"] == ["Jane Doe", "John Smith"]


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_experimenters_empty_when_missing(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {},
            "acquisition": {},
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["experimenters"] == []


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_instrument_id_stored(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {},
            "acquisition": {"instrument_id": "4A"},
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["instrument_id"] == "4A"


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_instrument_id_none_when_missing(mock_backend, mock_client_class):
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-01",
            "data_description": {},
            "acquisition": {},
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["instrument_id"] is None


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_created_from_docdb_created(mock_backend, mock_client_class):
    """The upload-time column mirrors the record's DocDB _created timestamp."""
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-05",
            "_created": "2023-01-02T08:30:00Z",
            "data_description": {"modalities": [{"abbreviation": "img"}], "project_name": "proj1", "data_level": "raw"},
            "subject": {"subject_id": "sub001"},
            "acquisition": {
                "acquisition_start_time": "2023-01-01T10:00:00",
                "acquisition_end_time": "2023-01-01T11:00:00",
            },
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["created"] == "2023-01-02T08:30:00Z"


@patch("aind_data_access_api.document_db.MetadataDbClient")
@patch("biodata_cache.cache_table_helpers.asset_basics.registry.BACKEND")
def test_asset_basics_created_missing_is_none(mock_backend, mock_client_class):
    """Records without _created still produce a row, with a null upload time."""
    mock_backend.read.return_value = pd.DataFrame()
    mock_client_instance = MagicMock()
    mock_client_class.return_value = mock_client_instance
    mock_client_instance.retrieve_docdb_records.return_value = [
        {
            "_id": "id1",
            "_last_modified": "2023-01-05",
            "data_description": {"modalities": [{"abbreviation": "img"}], "project_name": "proj1", "data_level": "raw"},
            "subject": {"subject_id": "sub001"},
            "acquisition": {
                "acquisition_start_time": "2023-01-01T10:00:00",
                "acquisition_end_time": "2023-01-01T11:00:00",
            },
        }
    ]
    result = asset_basics(force_update=True)
    assert result.iloc[0]["created"] is None

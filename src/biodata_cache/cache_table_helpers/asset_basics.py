"""Asset basics cache table."""

import logging

import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.backend import Predicate
from biodata_cache.models import Column
from biodata_cache.utils import (
    CacheLogMessage,
    apply_first_name_map,
    build_first_name_map,
    normalize_experimenters,
    normalize_instrument_id,
    setup_logging,
)


def _load_asset_basics(force_update: bool = False) -> pd.DataFrame:
    """Load the complete asset basics table, refreshing it when necessary.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        Complete DataFrame with basic asset metadata.

    """
    df = registry.BACKEND.read(registry.NAMES["basics"])

    FIELDS = [
        "_created",
        "data_description.modalities",
        "data_description.project_name",
        "data_description.data_level",
        "subject.subject_id",
        "acquisition.acquisition_start_time",
        "acquisition.acquisition_end_time",
        "acquisition.acquisition_type",
        "acquisition.subject_details.date_of_birth",
        "acquisition.subject_details.year_of_birth",
        "processing.data_processes.start_date_time",
        "subject.subject_details.genotype",
        "other_identifiers",
        "location",
        "name",
        "acquisition.experimenters",
        "acquisition.instrument_id",
        "data_description.investigators",
    ]

    if df.empty or force_update:
        setup_logging()
        logging.info(
            CacheLogMessage(
                backend=registry.BACKEND.__class__.__name__, table=registry.NAMES["basics"], message="Updating cache"
            ).to_json()
        )
        df = pd.DataFrame(
            columns=[
                "_id",
                "_last_modified",
                "created",
                "modalities",
                "project_name",
                "data_level",
                "subject_id",
                "acquisition_start_time",
                "acquisition_end_time",
                "code_ocean",
                "process_date",
                "genotype",
                "age",
                "acquisition_type",
                "location",
                "name",
                "experimenters",
                "experimenters_normalized",
                "instrument_id",
                "instrument_id_normalized",
                "investigators",
                "investigators_normalized",
            ]
        )
        from aind_data_access_api.document_db import MetadataDbClient
        client = MetadataDbClient(
            host=registry.API_GATEWAY_HOST,
            version="v2",
        )
        # It's a bit complex to get multiple fields that aren't indexed in a database
        # as large as DocDB. We'll also try to limit ourselves to only updating fields
        # that are necessary
        record_ids = client.retrieve_docdb_records(
            filter_query={},
            projection={"_id": 1, "_last_modified": 1},
            limit=0,
        )
        keep_ids = []
        # Drop all _ids where _last_modified matches cache
        cached_last_modified = dict(zip(df["_id"], df["_last_modified"], strict=False))
        for record in record_ids:
            if cached_last_modified.get(record["_id"]) != record["_last_modified"]:
                keep_ids.append(record["_id"])

        # Now batch by 100 IDs at a time to avoid overloading server, and fetch all the fields
        BATCH_SIZE = 100
        asset_records = []
        for i in range(0, len(keep_ids), BATCH_SIZE):
            logging.info(
                CacheLogMessage(
                    backend=registry.BACKEND.__class__.__name__,
                    table=registry.NAMES["basics"],
                    message=f"Fetching batch {i // BATCH_SIZE + 1}",
                ).to_json()
            )
            batch_ids = keep_ids[i : i + BATCH_SIZE]
            batch_records = client.retrieve_docdb_records(
                filter_query={"_id": {"$in": batch_ids}},
                projection={field: 1 for field in FIELDS + ["_id", "_last_modified"]},
                limit=0,
            )
            asset_records.extend(batch_records)

        # Unwrap nested fields
        records = []
        for record in asset_records:
            modalities = record.get("data_description", {}).get("modalities", [])
            modality_abbreviations = [modality["abbreviation"] for modality in modalities if "abbreviation" in modality]

            # Get the process date, convert to YYYY-MM-DD if present
            data_processes = record.get("processing", {}).get("data_processes", [])
            if data_processes:
                latest_process = data_processes[-1]
                process_datetime = latest_process.get("start_date_time", None)
                process_date = process_datetime.split("T")[0]
            else:
                process_date = None

            # Get the CO asset ID
            other_identifiers = record.get("other_identifiers", {})
            if other_identifiers:
                code_ocean = other_identifiers.get("Code Ocean", None)
            else:
                code_ocean = None

            # Calculate age in days from acquisition_start_time and date_of_birth or year_of_birth
            acquisition_start = record.get("acquisition", {}).get("acquisition_start_time", None)
            acq_subject_details = record.get("acquisition", {}).get("subject_details", {}) or {}
            date_of_birth = acq_subject_details.get("date_of_birth", None)
            year_of_birth = acq_subject_details.get("year_of_birth", None)
            age = None
            if acquisition_start and (date_of_birth or year_of_birth):
                try:
                    acq_date = pd.to_datetime(acquisition_start)
                    if date_of_birth:
                        dob = pd.to_datetime(date_of_birth)
                    else:
                        dob = pd.Timestamp(int(year_of_birth), 1, 1)
                    age = (acq_date - dob).days
                except Exception:
                    age = None

            flat_record = {
                "_id": record["_id"],
                "_last_modified": record.get("_last_modified", None),
                # DocDB record creation time — the asset's metadata record is
                # written when the asset finishes uploading, so this is the
                # closest available proxy for upload time.
                "created": record.get("_created", None),
                "modalities": modality_abbreviations,
                "project_name": record.get("data_description", {}).get("project_name", None),
                "data_level": record.get("data_description", {}).get("data_level", None),
                "subject_id": record.get("subject", {}).get("subject_id", None),
                "acquisition_start_time": record.get("acquisition", {}).get("acquisition_start_time", None),
                "acquisition_end_time": record.get("acquisition", {}).get("acquisition_end_time", None),
                "code_ocean": code_ocean,
                "process_date": process_date,
                "genotype": record.get("subject", {}).get("subject_details", {}).get("genotype", None),
                "age": age,
                "acquisition_type": record.get("acquisition", {}).get("acquisition_type", None),
                "location": record.get("location", None),
                "name": record.get("name", None),
                "experimenters": [
                    e if isinstance(e, str) else e.get("name", "")
                    for e in (record.get("acquisition", {}).get("experimenters", []) or [])
                ],
                "experimenters_normalized": normalize_experimenters(
                    [
                        e if isinstance(e, str) else e.get("name", "")
                        for e in (record.get("acquisition", {}).get("experimenters", []) or [])
                    ]
                ),
                "instrument_id": record.get("acquisition", {}).get("instrument_id", None),
                "instrument_id_normalized": normalize_instrument_id(
                    record.get("acquisition", {}).get("instrument_id", None)
                ),
                "investigators": [
                    i.get("name", "") for i in (record.get("data_description", {}).get("investigators", []) or [])
                ],
                "investigators_normalized": normalize_experimenters(
                    [i.get("name", "") for i in (record.get("data_description", {}).get("investigators", []) or [])]
                ),
            }
            records.append(flat_record)

        # Combine new records with the old df and store in cache
        new_df = pd.DataFrame(records)
        df = pd.concat([df[~df["_id"].isin(keep_ids)], new_df], ignore_index=True)

        def _iter_names(cell):
            """Helper to iterate over names in a cell that may be a list or a single string."""
            if hasattr(cell, "__iter__") and not isinstance(cell, str):
                return list(cell)
            return []

        all_names = [
            name
            for col in ("experimenters_normalized", "investigators_normalized")
            for cell in df[col]
            for name in _iter_names(cell)
        ]
        first_name_map = build_first_name_map(list(dict.fromkeys(all_names)))
        if first_name_map:
            df["experimenters_normalized"] = df["experimenters_normalized"].apply(
                lambda x: apply_first_name_map(_iter_names(x), first_name_map)
            )
            df["investigators_normalized"] = df["investigators_normalized"].apply(
                lambda x: apply_first_name_map(_iter_names(x), first_name_map)
            )

        registry.BACKEND.write(registry.NAMES["basics"], df)

    return df


_FILTERED_DEFAULT_COLUMNS = (
    "_id",
    "_last_modified",
    "created",
    "name",
    "modalities",
    "project_name",
    "data_level",
    "subject_id",
    "acquisition_start_time",
    "acquisition_end_time",
    "process_date",
    "genotype",
    "age",
    "acquisition_type",
    "location",
    "instrument_id",
    "instrument_id_normalized",
)


def _validate_timestamp(value: str, parameter_name: str) -> None:
    """Reject an invalid date filter consistently across both backends."""
    try:
        pd.to_datetime(value, utc=True, errors="raise")
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{parameter_name} must be a valid ISO timestamp") from exc


def _asset_basics_predicates(
    *,
    subject_id: str | None,
    project_name: str | None,
    modality: str | None,
    data_level: str | None,
    name: str | None,
    name_contains: str | None,
    acquisition_start_before: str | None,
    acquisition_start_after: str | None,
) -> list[Predicate]:
    """Translate the typed asset filters into backend predicates."""
    predicates = []
    if subject_id is not None:
        predicates.append(Predicate("subject_id", "eq", str(subject_id)))
    if project_name is not None:
        predicates.append(Predicate("project_name", "eq", project_name))
    if modality is not None:
        predicates.append(Predicate("modalities", "contains", modality))
    if data_level is not None:
        predicates.append(Predicate("data_level", "eq", data_level))
    if name is not None:
        predicates.append(Predicate("name", "eq", name))
    if name_contains is not None:
        predicates.append(Predicate("name", "contains", name_contains))
    if acquisition_start_before is not None:
        _validate_timestamp(acquisition_start_before, "acquisition_start_before")
        predicates.append(Predicate("acquisition_start_time", "lt", acquisition_start_before))
    if acquisition_start_after is not None:
        _validate_timestamp(acquisition_start_after, "acquisition_start_after")
        predicates.append(Predicate("acquisition_start_time", "gte", acquisition_start_after))
    return predicates


@registry.register_table(registry.NAMES["basics"])
def asset_basics(
    force_update: bool = False,
    *,
    subject_id: str | None = None,
    project_name: str | None = None,
    modality: str | None = None,
    data_level: str | None = None,
    name: str | None = None,
    name_contains: str | None = None,
    acquisition_start_before: str | None = None,
    acquisition_start_after: str | None = None,
    columns: list[str] | None = None,
    limit: int | None = 100,
    offset: int = 0,
    include_total: bool = False,
) -> pd.DataFrame | tuple[pd.DataFrame, int]:
    """Fetch asset metadata, optionally using a pushed-down filtered read.

    Calling this function without query options retains the historical behavior:
    it returns the complete cached table and performs incremental refreshes. A
    filtered call reads only the requested columns and page from the published
    cache. When ``include_total`` is true, the return value is ``(page, total)``.

    ``name`` is an exact match; ``name_contains`` is a case-insensitive literal
    substring match. Date filters use the same half-open interval semantics as
    the MCP API: before is strict and after is inclusive.
    """
    predicates = _asset_basics_predicates(
        subject_id=subject_id,
        project_name=project_name,
        modality=modality,
        data_level=data_level,
        name=name,
        name_contains=name_contains,
        acquisition_start_before=acquisition_start_before,
        acquisition_start_after=acquisition_start_after,
    )
    filtered = bool(predicates) or columns is not None or offset != 0 or include_total or limit != 100
    if not filtered:
        return _load_asset_basics(force_update=force_update)

    table_name = registry.NAMES["basics"]
    if force_update or not registry.BACKEND.cache_exists(table_name):
        _load_asset_basics(force_update=force_update)

    projected_columns = _FILTERED_DEFAULT_COLUMNS if columns is None else columns
    return registry.BACKEND.read_filtered(
        table_name,
        filters=predicates,
        columns=projected_columns,
        order_by="name",
        limit=limit,
        offset=offset,
        include_total=include_total,
    )


def asset_basics_columns() -> list[Column]:
    """Return asset basics cache table column definitions."""
    return [
        Column(name="_id", description="DocDB record ID for the asset"),
        Column(name="_last_modified", description="DocDB last modified timestamp for the asset record"),
        Column(
            name="created",
            description=(
                "DocDB record creation timestamp (_created), used as the asset's upload time — the metadata "
                "record is written when upload completes"
            ),
        ),
        Column(name="modalities", description="Modalities present in the asset as a list of abbreviations"),
        Column(name="project_name", description="Project name associated with the asset"),
        Column(name="data_level", description="Data level of the asset (e.g. raw, derived)"),
        Column(name="subject_id", description="Subject ID"),
        Column(name="acquisition_start_time", description="Acquisition start time in ISO format"),
        Column(name="acquisition_end_time", description="Acquisition end time in ISO format"),
        Column(name="code_ocean", description="Code Ocean asset ID if available"),
        Column(name="process_date", description="Date of latest processing in YYYY-MM-DD format"),
        Column(name="genotype", description="Genotype information for the subject if available"),
        Column(
            name="age",
            description="Age of the subject in days at time of acquisition, derived from date_of_birth or year_of_birth",
        ),
        Column(name="acquisition_type", description="Acquisition type (e.g. multiplane-2photon)"),
        Column(name="location", description="Location of the asset in S3"),
        Column(name="name", description="Asset name"),
        Column(name="experimenters", description="Acquisition experimenters as a list of raw names"),
        Column(
            name="experimenters_normalized",
            description="Normalized, deduplicated list of experimenter display names in original order",
        ),
        Column(name="instrument_id", description="Instrument ID used for the acquisition"),
        Column(
            name="instrument_id_normalized", description="Normalized short instrument name derived from instrument_id"
        ),
        Column(name="investigators", description="Investigators from data_description as a list of names"),
        Column(
            name="investigators_normalized",
            description="Normalized, deduplicated list of investigator display names in original order",
        ),
    ]

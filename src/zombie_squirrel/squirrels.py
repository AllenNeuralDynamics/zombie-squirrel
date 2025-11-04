"""Squirrels: functions to fetch and cache data from MongoDB."""

import logging
import os
from collections.abc import Callable
from typing import Any

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

from zombie_squirrel.acorns import (
    MemoryAcorn,
    RedshiftAcorn,
    rds_get_handle_empty,
)

# --- Backend setup ---------------------------------------------------

API_GATEWAY_HOST = "api.allenneuraldynamics.org"

tree_type = os.getenv("TREE_SPECIES", "memory").lower()

if tree_type == "redshift":  # pragma: no cover
    logging.info("Using Redshift acorn for caching")
    ACORN = RedshiftAcorn()
else:
    logging.info("Using in-memory acorn for caching")
    ACORN = MemoryAcorn()

# --- Squirrel registry -----------------------------------------------------

SQUIRREL_REGISTRY: dict[str, Callable[[], Any]] = {}


def register_squirrel(name: str):
    """Decorator for registering new squirrels."""

    def decorator(func):
        """Register function in squirrel registry."""
        SQUIRREL_REGISTRY[name] = func
        return func

    return decorator


# --- Squirrels -----------------------------------------------------

NAMES = {
    "upn": "unique_project_names",
    "usi": "unique_subject_ids",
    "basics": "asset_basics",
    "d2r": "source_data",
    "r2d": "raw_to_derived",
}


@register_squirrel(NAMES["upn"])
def unique_project_names(force_update: bool = False) -> list[str]:
    """Fetch unique project names from metadata database.

    Returns cached results if available, fetches from database if cache is empty
    or force_update is True.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        List of unique project names."""
    df = rds_get_handle_empty(ACORN, NAMES["upn"])

    if df.empty or force_update:
        # If cache is missing, fetch data
        logging.info("Updating cache for unique project names")
        client = MetadataDbClient(
            host=API_GATEWAY_HOST,
            version="v2",
        )
        unique_project_names = client.aggregate_docdb_records(
            pipeline=[
                {"$group": {"_id": "$data_description.project_name"}},
                {"$project": {"project_name": "$_id", "_id": 0}},
            ]
        )
        df = pd.DataFrame(unique_project_names)
        ACORN.hide(NAMES["upn"], df)

    return df["project_name"].tolist()


@register_squirrel(NAMES["usi"])
def unique_subject_ids(force_update: bool = False) -> list[str]:
    """Fetch unique subject IDs from metadata database.

    Returns cached results if available, fetches from database if cache is empty
    or force_update is True.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        List of unique subject IDs."""
    df = rds_get_handle_empty(ACORN, NAMES["usi"])

    if df.empty or force_update:
        # If cache is missing, fetch data
        logging.info("Updating cache for unique subject IDs")
        client = MetadataDbClient(
            host=API_GATEWAY_HOST,
            version="v2",
        )
        unique_subject_ids = client.aggregate_docdb_records(
            pipeline=[
                {"$group": {"_id": "$subject.subject_id"}},
                {"$project": {"subject_id": "$_id", "_id": 0}},
            ]
        )
        df = pd.DataFrame(unique_subject_ids)
        ACORN.hide(NAMES["usi"], df)

    return df["subject_id"].tolist()


@register_squirrel(NAMES["basics"])
def asset_basics(force_update: bool = False) -> pd.DataFrame:
    """Fetch basic asset metadata including modalities, projects, and subject info.

    Returns a DataFrame with columns: _id, _last_modified, modalities,
    project_name, data_level, subject_id, acquisition_start_time, and
    acquisition_end_time. Uses incremental updates based on _last_modified
    timestamps to avoid re-fetching unchanged records.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with basic asset metadata."""
    df = rds_get_handle_empty(ACORN, NAMES["basics"])

    FIELDS = [
        "data_description.modalities",
        "data_description.project_name",
        "data_description.data_level",
        "subject.subject_id",
        "acquisition.acquisition_start_time",
        "acquisition.acquisition_end_time",
    ]

    if df.empty or force_update:
        logging.info("Updating cache for asset basics")
        df = pd.DataFrame(
            columns=[
                "_id",
                "_last_modified",
                "modalities",
                "project_name",
                "data_level",
                "subject_id",
                "acquisition_start_time",
                "acquisition_end_time",
            ]
        )
        client = MetadataDbClient(
            host=API_GATEWAY_HOST,
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
        for record in record_ids:
            cached_row = df[df["_id"] == record["_id"]]
            if cached_row.empty or cached_row["_last_modified"].values[0] != record["_last_modified"]:
                keep_ids.append(record["_id"])

        # Now batch by 100 IDs at a time to avoid overloading server, and fetch all the fields
        BATCH_SIZE = 100
        asset_records = []
        for i in range(0, len(keep_ids), BATCH_SIZE):
            logging.info(f"Fetching asset basics batch {i // BATCH_SIZE + 1}...")
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
            modality_abbreviations_str = ", ".join(modality_abbreviations)
            flat_record = {
                "_id": record["_id"],
                "_last_modified": record.get("_last_modified", None),
                "modalities": modality_abbreviations_str,
                "project_name": record.get("data_description", {}).get("project_name", None),
                "data_level": record.get("data_description", {}).get("data_level", None),
                "subject_id": record.get("subject", {}).get("subject_id", None),
                "acquisition_start_time": record.get("acquisition", {}).get("acquisition_start_time", None),
                "acquisition_end_time": record.get("acquisition", {}).get("acquisition_end_time", None),
            }
            records.append(flat_record)

        # Combine new records with the old df and store in cache
        new_df = pd.DataFrame(records)
        df = pd.concat([df[~df["_id"].isin(keep_ids)], new_df], ignore_index=True)

        ACORN.hide(NAMES["basics"], df)

    return df


@register_squirrel(NAMES["d2r"])
def source_data(force_update: bool = False) -> pd.DataFrame:
    """Fetch source data references for derived records.

    Returns a DataFrame mapping record IDs to their upstream source data
    dependencies as comma-separated lists.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with _id and source_data columns."""
    df = rds_get_handle_empty(ACORN, NAMES["d2r"])

    if df.empty or force_update:
        logging.info("Updating cache for source data")
        client = MetadataDbClient(
            host=API_GATEWAY_HOST,
            version="v2",
        )
        records = client.retrieve_docdb_records(
            filter_query={},
            projection={"_id": 1, "data_description.source_data": 1},
            limit=0,
        )
        data = []
        for record in records:
            source_data_list = record.get("data_description", {}).get("source_data", [])
            source_data_str = ", ".join(source_data_list) if source_data_list else ""
            data.append(
                {
                    "_id": record["_id"],
                    "source_data": source_data_str,
                }
            )

        df = pd.DataFrame(data)
        ACORN.hide(NAMES["d2r"], df)

    return df


@register_squirrel(NAMES["r2d"])
def raw_to_derived(force_update: bool = False) -> pd.DataFrame:
    """Fetch mapping of raw records to their derived records.

    Returns a DataFrame mapping raw record IDs to lists of derived record IDs
    that depend on them as source data.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with _id and derived_records columns."""
    df = rds_get_handle_empty(ACORN, NAMES["r2d"])

    if df.empty or force_update:
        logging.info("Updating cache for raw to derived mapping")
        client = MetadataDbClient(
            host=API_GATEWAY_HOST,
            version="v2",
        )

        # Get all raw record IDs
        raw_records = client.retrieve_docdb_records(
            filter_query={"data_description.data_level": "raw"},
            projection={"_id": 1},
            limit=0,
        )
        raw_ids = {record["_id"] for record in raw_records}

        # Get all derived records with their _id and source_data
        derived_records = client.retrieve_docdb_records(
            filter_query={"data_description.data_level": "derived"},
            projection={"_id": 1, "data_description.source_data": 1},
            limit=0,
        )

        # Build mapping: raw_id -> list of derived _ids
        raw_to_derived_map = {raw_id: [] for raw_id in raw_ids}
        for derived_record in derived_records:
            source_data_list = derived_record.get("data_description", {}).get("source_data", [])
            derived_id = derived_record["_id"]
            # Add this derived record to each raw record it depends on
            for source_id in source_data_list:
                if source_id in raw_to_derived_map:
                    raw_to_derived_map[source_id].append(derived_id)

        # Convert to DataFrame
        data = []
        for raw_id, derived_ids in raw_to_derived_map.items():
            derived_ids_str = ", ".join(derived_ids)
            data.append(
                {
                    "_id": raw_id,
                    "derived_records": derived_ids_str,
                }
            )

        df = pd.DataFrame(data)
        ACORN.hide(NAMES["r2d"], df)

    return df

"""Squirrels: functions to fetch and cache data from MongoDB."""
import pandas as pd
from typing import Any, Callable
from zombie_squirrel.acorns import RedshiftAcorn, MemoryAcorn, rds_get_handle_empty
from aind_data_access_api.document_db import MetadataDbClient
import os
import logging

# --- Backend setup ---------------------------------------------------

API_GATEWAY_HOST = "api.allenneuraldynamics.org"

tree_type = os.getenv("TREE_SPECIES", "memory").lower()

if tree_type == "redshift":
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
        SQUIRREL_REGISTRY[name] = func
        return func
    return decorator


# --- Squirrels -----------------------------------------------------

NAMES = {
    "upn": "unique_project_names",
    "usi": "unique_subject_ids",
    "basics": "asset_basics",
}


@register_squirrel(NAMES["upn"])
def unique_project_names(force_update: bool = False) -> list[str]:
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
    """Basic asset metadata.

    _id, _last_modified,
    modalities, project names, data_level, subject_id, acquisition_start and _end
    """
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
        df = pd.DataFrame(columns=["_id", "_last_modified", "modalities", "project_name",
                                   "data_level", "subject_id",
                                   "acquisition_start_time", "acquisition_end_time"])
        client = MetadataDbClient(
            host=API_GATEWAY_HOST,
            version="v2",
        )
        # It's a bit complex to get multiple fields that aren't indexed in a database
        # as large as DocDB. We'll also try to limit ourselves to only updating fields
        # that are necessary
        record_ids = client.retrieve_docdb_records(
            filter_query={}, projection={"_id": 1, "_last_modified": 1}, limit=0,
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
            batch_ids = keep_ids[i:i + BATCH_SIZE]
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
        df = pd.concat([df[df["_id"].isin(keep_ids) == False], new_df], ignore_index=True)

        ACORN.hide(NAMES["basics"], df)

    return df

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
    "upn": "unique-project-names",
    "usi": "unique-subject-ids",
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

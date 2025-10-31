"""Squirrels: functions to fetch and cache data from MongoDB."""
import pandas as pd
from typing import Any, Callable, Optional
from zombie_squirrel.acorns import RedshiftAcorn, MemoryAcorn
from aind_data_access_api.document_db import MetadataDbClient
import os

# --- Backend setup ---------------------------------------------------

API_GATEWAY_HOST = "api.allenneuraldynamics.org"

tree_type = os.getenv("TREE_SPECIES", "memory").lower()

if tree_type == "redshift":
    ACORN = RedshiftAcorn()
else:
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


@register_squirrel("unique-project-names")
def unique_project_names(force_update: bool = False) -> list[str]:
    df = ACORN.scurry("unique-project-names")

    if df.empty or force_update:
        # If cache is missing, fetch data
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
        ACORN.hide("unique-project-names", df)

    return df["project_name"].tolist()


@register_squirrel("unique-subject-ids")
def unique_subject_ids(force_update: bool = False) -> list[str]:
    df = ACORN.scurry("unique-subject-ids")

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
        ACORN.hide("unique-subject-ids", df)

    return df["subject_id"].tolist()

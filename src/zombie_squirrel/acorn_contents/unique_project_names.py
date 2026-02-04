"""Unique project names acorn."""

import logging

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.utils import setup_logging, SquirrelMessage


@acorns.register_acorn(acorns.NAMES["upn"])
def unique_project_names(force_update: bool = False) -> list[str]:
    """Fetch unique project names from metadata database.

    Returns cached results if available, fetches from database if cache is empty
    or force_update is True.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        List of unique project names."""
    df = acorns.TREE.scurry(acorns.NAMES["upn"])

    if df.empty and not force_update:
        raise ValueError(
            "Cache is empty. Use force_update=True to fetch data from database."
        )

    if df.empty or force_update:
        setup_logging()
        logging.info(SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["upn"],
            message="Updating cache"
        ).to_json())
        client = MetadataDbClient(
            host=acorns.API_GATEWAY_HOST,
            version="v2",
        )
        unique_project_names = client.aggregate_docdb_records(
            pipeline=[
                {"$group": {"_id": "$data_description.project_name"}},
                {"$project": {"project_name": "$_id", "_id": 0}},
            ]
        )
        df = pd.DataFrame(unique_project_names)
        acorns.TREE.hide(acorns.NAMES["upn"], df)

    return df["project_name"].tolist()

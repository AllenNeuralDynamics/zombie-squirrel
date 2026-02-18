"""Unique subject IDs acorn."""

import logging

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.utils import (
    SquirrelMessage,
    load_columns_from_metadata,
    setup_logging,
)


@acorns.register_acorn(acorns.NAMES["usi"])
def unique_subject_ids(force_update: bool = False) -> list[str]:
    """Fetch unique subject IDs from metadata database.

    Returns cached results if available, fetches from database if cache is empty
    or force_update is True.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        List of unique subject IDs."""
    df = acorns.TREE.scurry(acorns.NAMES["usi"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from database.")

    if df.empty or force_update:
        setup_logging()
        logging.info(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__, acorn=acorns.NAMES["usi"], message="Updating cache"
            ).to_json()
        )
        client = MetadataDbClient(
            host=acorns.API_GATEWAY_HOST,
            version="v2",
        )
        unique_subject_ids = client.aggregate_docdb_records(
            pipeline=[
                {"$group": {"_id": "$subject.subject_id"}},
                {"$project": {"subject_id": "$_id", "_id": 0}},
            ]
        )
        df = pd.DataFrame(unique_subject_ids)
        acorns.TREE.hide(acorns.NAMES["usi"], df)

    return df["subject_id"].tolist()


def unique_subject_ids_columns() -> list[str]:
    """Get column names from unique subject IDs metadata.

    Returns:
        List of column names from the cached metadata."""
    return load_columns_from_metadata(acorns.NAMES["usi"])

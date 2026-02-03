"""Unique subject IDs acorn."""

import logging

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns


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

    if df.empty or force_update:
        # If cache is missing, fetch data
        logging.info("Updating cache for unique subject IDs")
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

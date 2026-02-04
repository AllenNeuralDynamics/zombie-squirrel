"""Source data acorn."""

import logging

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns


@acorns.register_acorn(acorns.NAMES["d2r"])
def source_data(force_update: bool = False) -> pd.DataFrame:
    """Fetch source data references for derived records.

    Returns a DataFrame mapping record IDs to their upstream source data
    dependencies as comma-separated lists.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with _id and source_data columns."""
    df = acorns.TREE.scurry(acorns.NAMES["d2r"])

    if df.empty and not force_update:
        raise ValueError(
            "Cache is empty. Use force_update=True to fetch data from database."
        )

    if df.empty or force_update:
        logging.info("Updating cache for source data")
        client = MetadataDbClient(
            host=acorns.API_GATEWAY_HOST,
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
        acorns.TREE.hide(acorns.NAMES["d2r"], df)

    return df

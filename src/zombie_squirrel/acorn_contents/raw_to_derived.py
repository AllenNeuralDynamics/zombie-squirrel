"""Raw to derived mapping acorn."""

import logging

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns


@acorns.register_acorn(acorns.NAMES["r2d"])
def raw_to_derived(force_update: bool = False) -> pd.DataFrame:
    """Fetch mapping of raw records to their derived records.

    Returns a DataFrame mapping raw record IDs to lists of derived record IDs
    that depend on them as source data.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with _id and derived_records columns."""
    df = acorns.TREE.scurry(acorns.NAMES["r2d"])

    if df.empty and not force_update:
        raise ValueError(
            "Cache is empty. Use force_update=True to fetch data from database."
        )

    if df.empty or force_update:
        logging.info("Updating cache for raw to derived mapping")
        client = MetadataDbClient(
            host=acorns.API_GATEWAY_HOST,
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
            if source_data_list:
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
        acorns.TREE.hide(acorns.NAMES["r2d"], df)

    return df

"""Raw to derived mapping acorn."""

import logging

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.utils import SquirrelMessage, setup_logging


@acorns.register_acorn(acorns.NAMES["r2d"])
def raw_to_derived(force_update: bool = False) -> pd.DataFrame:
    """Fetch mapping of raw records to their derived records.

    Returns a DataFrame mapping raw record names to lists of derived record names
    that depend on them as source data.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with name and derived_records columns."""
    df = acorns.TREE.scurry(acorns.NAMES["r2d"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from database.")

    if df.empty or force_update:
        setup_logging()
        logging.info(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__, acorn=acorns.NAMES["r2d"], message="Updating cache"
            ).to_json()
        )
        client = MetadataDbClient(
            host=acorns.API_GATEWAY_HOST,
            version="v2",
        )

        # Get all raw record names
        raw_records = client.retrieve_docdb_records(
            filter_query={"data_description.data_level": "raw"},
            projection={"name": 1},
            limit=0,
        )

        # Initialize mapping: raw_name -> list of derived_names
        raw_to_derived_map = {record["name"]: [] for record in raw_records}

        # Get all derived records with their name and source_data
        derived_records = client.retrieve_docdb_records(
            filter_query={"data_description.data_level": "derived"},
            projection={"name": 1, "data_description.source_data": 1},
            limit=0,
        )

        # Build mapping by iterating through derived records
        for derived_record in derived_records:
            source_data_list = derived_record.get("data_description", {}).get("source_data", [])
            derived_name = derived_record["name"]
            if source_data_list:
                for source_name in source_data_list:
                    if source_name in raw_to_derived_map:
                        raw_to_derived_map[source_name].append(derived_name)

        # Convert to DataFrame
        data = []
        for raw_name, derived_names in raw_to_derived_map.items():
            derived_names_str = ", ".join(derived_names)
            data.append(
                {
                    "name": raw_name,
                    "derived_records": derived_names_str,
                }
            )
            logging.info(
                SquirrelMessage(
                    tree=acorns.TREE.__class__.__name__,
                    acorn=acorns.NAMES["r2d"],
                    message=f"Processed raw record {raw_name} with derived records: {derived_names_str}",
                ).to_json()
            )

        df = pd.DataFrame(data)
        acorns.TREE.hide(acorns.NAMES["r2d"], df)

    return df

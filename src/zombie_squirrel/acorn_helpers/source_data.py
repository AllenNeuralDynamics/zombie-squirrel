"""Source data acorn - unified derived asset table."""

import logging
import re

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.squirrel import Column
from zombie_squirrel.utils import (
    SquirrelMessage,
    setup_logging,
)

_DATETIME_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2})$")


def _extract_processing_time(name: str) -> str:
    """Extract processing time from asset name using regex pattern."""
    match = _DATETIME_PATTERN.search(name)
    return match.group(1) if match else ""


@acorns.register_acorn(acorns.NAMES["d2r"])
def source_data(force_update: bool = False) -> pd.DataFrame:
    """Fetch derived asset table with one row per derived asset per source.

    Returns a DataFrame with one row per derived asset per source data entry,
    including the pipeline name and processing time extracted from the asset name.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with name, source_data, pipeline_name, and processing_time columns.

    """
    df = acorns.TREE.scurry(acorns.NAMES["d2r"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from database.")

    if df.empty or force_update:
        setup_logging()
        logging.info(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__, acorn=acorns.NAMES["d2r"], message="Updating cache"
            ).to_json()
        )
        client = MetadataDbClient(
            host=acorns.API_GATEWAY_HOST,
            version="v2",
        )
        records = client.retrieve_docdb_records(
            filter_query={"data_description.data_level": "derived"},
            projection={"name": 1, "data_description.source_data": 1, "processing.pipelines": 1},
            limit=0,
        )
        data = []
        for record in records:
            name = record["name"]
            source_data_list = record.get("data_description", {}).get("source_data", []) or []
            pipelines = record.get("processing", {}).get("pipelines", []) or []
            pipeline_name = pipelines[0].get("name", "") if pipelines else ""
            processing_time = _extract_processing_time(name)
            if source_data_list:
                for source_name in source_data_list:
                    data.append(
                        {
                            "name": name,
                            "source_data": source_name,
                            "pipeline_name": pipeline_name,
                            "processing_time": processing_time,
                        }
                    )
            else:
                data.append(
                    {
                        "name": name,
                        "source_data": "",
                        "pipeline_name": pipeline_name,
                        "processing_time": processing_time,
                    }
                )

        df = pd.DataFrame(data)
        acorns.TREE.hide(acorns.NAMES["d2r"], df)

    return df


def source_data_columns() -> list[Column]:
    """Return source data acorn column definitions."""
    return [
        Column(name="name", description="Asset name"),
        Column(name="source_data", description="Asset name that this derived asset was generated from, if available"),
        Column(name="pipeline_name", description="Pipeline that created this asset"),
        Column(name="processing_time", description="Timestamp this asset was processed"),
    ]

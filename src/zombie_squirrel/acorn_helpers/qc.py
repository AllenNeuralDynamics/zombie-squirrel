"""Quality control data acorn."""

import json
import logging
from typing import Union

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.utils import setup_logging, SquirrelMessage


def encode_dict_value(value):
    if isinstance(value, dict):
        return f"json:{json.dumps(value)}"
    return value


def decode_dict_value(value):
    if isinstance(value, str) and value.startswith("json:"):
        return json.loads(value[5:])
    return value


@acorns.register_acorn(acorns.NAMES["qc"])
def qc(
    asset_names: Union[str, list[str]], force_update: bool = False
) -> pd.DataFrame:
    """Fetch quality control metrics for one or more assets.

    Returns a DataFrame with columns from the quality_control metrics
    including: name, stage, object_type, modality, value, tags, status,
    and status_history. Dict values are stored as JSON strings with
    'json:' prefix for cleaner dataframe storage.

    When multiple assets are provided, merges all QC data into a single
    DataFrame with an 'asset_name' column to differentiate sources.

    Args:
        asset_names: Single asset name or list of asset names.
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with quality control metrics for the asset(s)."""
    if isinstance(asset_names, str):
        return _qc_single(asset_names, force_update)
    return _qc_multiple(asset_names, force_update)


def _qc_single(asset_name: str, force_update: bool) -> pd.DataFrame:
    """Fetch QC data for a single asset."""
    cache_key = f"qc/{asset_name}"
    df = acorns.TREE.scurry(cache_key)

    if df.empty and not force_update:
        raise ValueError(
            f"Cache is empty for {asset_name}. "
            "Use force_update=True to fetch data from database."
        )

    if df.empty or force_update:
        setup_logging()
        logging.info(SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["qc"],
            message=f"Updating cache for {asset_name}"
        ).to_json())

        client = MetadataDbClient(
            host=acorns.API_GATEWAY_HOST,
            version="v2",
        )

        records = client.retrieve_docdb_records(
            filter_query={"name": asset_name},
            projection={
                "_id": 1,
                "name": 1,
                "quality_control": 1,
            },
            limit=1,
        )

        if not records:
            logging.warning(SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["qc"],
                message=f"No record found for {asset_name}"
            ).to_json())
            return pd.DataFrame()

        record = records[0]
        quality_control = record.get("quality_control", {})

        if not quality_control or "metrics" not in quality_control:
            logging.warning(SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["qc"],
                message=f"No quality_control metrics found for {asset_name}"
            ).to_json())
            return pd.DataFrame()

        metrics_copy = []
        for metric in quality_control["metrics"]:
            metric_copy = metric.copy()
            metric_copy["value"] = encode_dict_value(metric_copy.get("value"))
            metric_copy["tags"] = encode_dict_value(
                metric_copy.get("tags", {})
            )
            metrics_copy.append(metric_copy)

        df = pd.DataFrame.from_records(metrics_copy)

        acorns.TREE.hide(cache_key, df)

        logging.info(SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["qc"],
            message=f"Cached QC data for {asset_name}"
        ).to_json())

    return df


def _qc_multiple(
    asset_names: list[str], force_update: bool
) -> pd.DataFrame:
    """Fetch and merge QC data for multiple assets."""
    cache_keys = [f"qc/{name}" for name in asset_names]

    if force_update:
        setup_logging()
        for name in asset_names:
            _qc_single(name, force_update=True)
    else:
        df_check = acorns.TREE.scurry(cache_keys[0])
        if df_check.empty:
            raise ValueError(
                f"Cache is empty for {asset_names[0]}. "
                "Use force_update=True to fetch data from database."
            )

    df = acorns.TREE.scurry(cache_keys)

    if df.empty:
        logging.warning(SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["qc"],
            message=f"No QC data found for assets: {asset_names}"
        ).to_json())

    return df

"""Quality control data acorn."""

import json
import logging

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.utils import SquirrelMessage, setup_logging


def encode_dict_value(value):
    """Encode dict or list values as JSON strings with a prefix for DataFrame storage."""
    if isinstance(value, (dict, list)):
        return f"json:{json.dumps(value)}"
    if value is not None and not isinstance(value, str):
        return str(value)
    return value


def decode_dict_value(value):
    """Decode JSON strings with prefix back to dict or list."""
    if isinstance(value, str) and value.startswith("json:"):
        return json.loads(value[5:])
    return value


@acorns.register_acorn(acorns.NAMES["qc"])
def qc(subject_id: str, asset_names: str | list[str] | None = None, force_update: bool = False) -> pd.DataFrame:
    """Fetch quality control metrics for assets belonging to a subject.

    Returns a DataFrame with columns from the quality_control metrics
    including: name, stage, object_type, modality, value, tags, status,
    status_history, and asset_name. Dict values are stored as JSON strings
    with 'json:' prefix for cleaner dataframe storage.

    Data is cached per subject_id. All assets for the subject are stored
    in cache, but can be filtered using the asset_names parameter.

    Args:
        subject_id: Subject ID to fetch QC data for (from subject.subject_id field).
        asset_names: Optional asset name or list of asset names to filter to.
                    If None, returns QC data for all assets of the subject.
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with quality control metrics for the subject's asset(s).

    Raises:
        ValueError: If requested asset_names are not found in the subject's cache."""
    cache_key = f"qc/{subject_id}"
    df = acorns.TREE.scurry(cache_key)

    if df.empty and not force_update:
        logging.error(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["qc"],
                message=f"Cache is empty for subject {subject_id}. Use force_update=True to fetch data from database.",
            ).to_json()
        )

    if force_update:
        df = _fetch_subject_qc(subject_id)

    if asset_names is not None:
        df = _filter_by_asset_names(df, asset_names, subject_id)

    return df


def _fetch_subject_qc(subject_id: str) -> pd.DataFrame:
    """Fetch QC data for all assets belonging to a subject."""
    setup_logging()
    cache_key = f"qc/{subject_id}"

    logging.info(
        SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["qc"],
            message=f"Updating cache for subject {subject_id}",
        ).to_json()
    )

    client = MetadataDbClient(
        host=acorns.API_GATEWAY_HOST,
        version="v2",
    )

    records = client.retrieve_docdb_records(
        filter_query={"subject.subject_id": subject_id},
        projection={
            "_id": 1,
            "name": 1,
            "quality_control": 1,
        },
        limit=100,
    )

    if not records:
        logging.warning(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["qc"],
                message=f"No records found for subject {subject_id}",
            ).to_json()
        )
        return pd.DataFrame()

    all_metrics = []
    for record in records:
        asset_name = record.get("name", "")
        quality_control = record.get("quality_control", {})

        if not quality_control or "metrics" not in quality_control:
            continue

        for metric in quality_control["metrics"]:
            metric_copy = metric.copy()
            for key, value in metric_copy.items():
                metric_copy[key] = encode_dict_value(value)
            metric_copy["asset_name"] = asset_name
            all_metrics.append(metric_copy)

    if not all_metrics:
        logging.warning(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["qc"],
                message=f"No quality_control metrics found for subject {subject_id}",
            ).to_json()
        )
        return pd.DataFrame()

    df = pd.DataFrame.from_records(all_metrics)
    acorns.TREE.hide(cache_key, df)

    logging.info(
        SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["qc"],
            message=f"Cached QC data for subject {subject_id} ({len(records)} assets, {len(all_metrics)} metrics)",
        ).to_json()
    )

    return df


def _filter_by_asset_names(df: pd.DataFrame, asset_names: str | list[str], subject_id: str) -> pd.DataFrame:
    """Filter QC DataFrame to specific asset names and validate they exist."""
    if df.empty:
        return df

    if isinstance(asset_names, str):
        asset_names = [asset_names]

    available_assets = df["asset_name"].unique().tolist()
    missing_assets = [name for name in asset_names if name not in available_assets]

    if missing_assets:
        logging.warning(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["qc"],
                message=f"Requested asset(s) {missing_assets} not found in cache for subject {subject_id}. "
                f"Available assets: {available_assets}",
            ).to_json()
        )

    return df[df["asset_name"].isin(asset_names)].reset_index(drop=True)

"""Behavior trials acorn - extracts NWB trials table from derived behavior assets."""

import logging

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.squirrel import Column
from zombie_squirrel.utils import (
    SquirrelMessage,
    setup_logging,
)


def _parse_s3_location(location: str) -> tuple[str, str]:
    """Parse an s3:// URI into (bucket, key_prefix)."""
    without_scheme = location.removeprefix("s3://")
    bucket, _, prefix = without_scheme.partition("/")
    return bucket, prefix


def _read_trials_from_nwb_zarr(location: str) -> pd.DataFrame | None:
    """Read the trials table from a behavior.nwb.zarr store on S3.

    Args:
        location: S3 URI of the asset folder, e.g. s3://bucket/asset_name.

    Returns:
        DataFrame of trials, or None if the zarr or table is not found.

    """
    import s3fs
    import zarr

    bucket, prefix = _parse_s3_location(location)
    zarr_path = f"{bucket}/{prefix}/behavior.nwb.zarr"

    fs = s3fs.S3FileSystem(anon=False)
    if not fs.exists(zarr_path):
        return None

    store = s3fs.S3Map(zarr_path, s3=fs)
    try:
        root = zarr.open_group(store, mode="r")
    except Exception:
        return None

    try:
        trials_group = root["intervals/trials"]
    except KeyError:
        return None

    attrs = dict(trials_group.attrs)
    colnames = list(attrs.get("colnames", []))
    all_cols = ["id"] + colnames

    data: dict[str, list] = {}
    for col_name in all_cols:
        if col_name not in trials_group:
            continue
        col = trials_group[col_name]
        if hasattr(col, "shape"):
            data[col_name] = col[:]
        elif "data" in col:
            data[col_name] = col["data"][:]

    if not data:
        return None

    return pd.DataFrame(data)


@acorns.register_acorn(acorns.NAMES["behavior"])
def behavior_trials(
    subject_id: str,
    asset_names: str | list[str] | None = None,
    force_update: bool = False,
    lazy: bool = False,
) -> pd.DataFrame | str:
    """Fetch NWB trials table for derived behavior assets of a subject.

    For each derived asset with behavior modality belonging to the subject,
    reads the trials table from behavior.nwb.zarr on S3 and stores the
    combined result as a parquet cache partitioned by subject_id.

    Args:
        subject_id: Subject ID to fetch behavior trials for.
        asset_names: Optional asset name or list of asset names to filter to.
        force_update: If True, bypass cache and fetch fresh data from S3.
        lazy: If True, return the S3 path to the parquet file instead of loading.

    Returns:
        DataFrame with trials data for the subject's behavior assets, or
        string path to the S3 parquet file if lazy=True.

    """
    cache_key = f"behavior_trials/{subject_id}"

    if lazy:
        if force_update:
            _fetch_subject_behavior_trials(subject_id)
        return acorns.TREE.get_location(cache_key)

    df = acorns.TREE.scurry(cache_key)

    if df.empty and not force_update:
        logging.warning(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["behavior"],
                message=f"Cache empty for subject {subject_id}. Use force_update=True.",
            ).to_json()
        )

    if force_update:
        df = _fetch_subject_behavior_trials(subject_id)

    if asset_names is not None:
        if isinstance(asset_names, str):
            asset_names = [asset_names]
        df = df[df["asset_name"].isin(asset_names)]

    return df


def _fetch_subject_behavior_trials(subject_id: str) -> pd.DataFrame:
    """Fetch behavior trials for all derived+behavior assets of a subject."""
    setup_logging()
    cache_key = f"behavior_trials/{subject_id}"

    logging.info(
        SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["behavior"],
            message=f"Updating cache for subject {subject_id}",
        ).to_json()
    )

    client = MetadataDbClient(
        host=acorns.API_GATEWAY_HOST,
        version="v2",
    )

    records = client.retrieve_docdb_records(
        filter_query={
            "subject.subject_id": subject_id,
            "data_description.data_level": "derived",
            "data_description.modalities": {"$elemMatch": {"abbreviation": "behavior"}},
        },
        projection={
            "_id": 1,
            "name": 1,
            "location": 1,
        },
        limit=0,
    )

    all_trials: list[pd.DataFrame] = []
    index_rows: list[dict] = []

    for record in records:
        asset_name = record.get("name", "")
        location = record.get("location", "")
        if not location:
            index_rows.append({"asset_name": asset_name, "has_behavior": False})
            continue

        trials_df = _read_trials_from_nwb_zarr(location)
        if trials_df is not None and not trials_df.empty:
            trials_df["asset_name"] = asset_name
            trials_df["subject_id"] = subject_id
            all_trials.append(trials_df)
            index_rows.append({"asset_name": asset_name, "has_behavior": True})
        else:
            index_rows.append({"asset_name": asset_name, "has_behavior": False})

    if all_trials:
        df = pd.concat(all_trials, ignore_index=True)
    else:
        df = pd.DataFrame(columns=["asset_name", "subject_id"])

    acorns.TREE.hide(cache_key, df)

    _update_behavior_index(index_rows)

    return df


def _update_behavior_index(new_rows: list[dict]) -> None:
    """Merge new_rows into the behavior_trials_index cache."""
    if not new_rows:
        return

    existing = acorns.TREE.scurry(acorns.NAMES["behavior_index"])
    new_df = pd.DataFrame(new_rows)

    if existing.empty:
        merged = new_df
    else:
        new_asset_names = set(new_df["asset_name"])
        merged = pd.concat(
            [existing[~existing["asset_name"].isin(new_asset_names)], new_df],
            ignore_index=True,
        )

    acorns.TREE.hide(acorns.NAMES["behavior_index"], merged)


def behavior_trials_columns() -> list[Column]:
    """Return behavior_trials acorn column definitions."""
    return [
        Column(name="asset_name", description="Asset name the trial belongs to"),
        Column(name="subject_id", description="Subject ID"),
        Column(name="id", description="Trial ID"),
        Column(name="start_time", description="Trial start time in seconds"),
        Column(name="stop_time", description="Trial stop time in seconds"),
    ]

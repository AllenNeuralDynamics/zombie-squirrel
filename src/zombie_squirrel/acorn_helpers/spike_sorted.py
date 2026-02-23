"""Spike sorted data acorn."""

import logging

import numpy as np
import pandas as pd
import s3fs
import zarr
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.acorn_helpers.asset_basics import asset_basics
from zombie_squirrel.acorn_helpers.raw_to_derived import raw_to_derived
from zombie_squirrel.utils import (
    SquirrelMessage,
    setup_logging,
)


@acorns.register_acorn(acorns.NAMES["ss"])
def spike_sorted(
    subject_id: str,
    asset_names: str | list[str] | None = None,
    force_update: bool = False,
    lazy: bool = False,
) -> dict[str, pd.DataFrame] | dict[str, str]:
    """Fetch spike sorted data for a subject.

    Returns a dictionary with two keys:
    - 'units': DataFrame with per-unit metadata (firing_rate, quality metrics, etc)
    - 'spikes': DataFrame with individual spike times (unit_id, spike_time, etc)

    Data is cached per asset. Each spike sorted asset for the subject is stored
    as a separate parquet file, partitioned by asset_name. Can be filtered using
    the asset_names parameter.

    Args:
        subject_id: Subject ID to fetch spike sorted assets for (from subject.subject_id field).
        asset_names: Optional asset name or list of asset names to filter to.
                    If None, returns data for all spike sorted assets of the subject.
        force_update: If True, bypass cache and fetch fresh data from database.
        lazy: If True, return S3 glob paths instead of loaded DataFrames.
              Default False. Paths are suitable for use with DuckDB.

    Returns:
        Dictionary with 'units' and 'spikes' keys, each containing a DataFrame
        or string glob path to S3 parquet files if lazy=True.

    Raises:
        ValueError: If requested asset_names are not found in the subject's cache.
    """
    if force_update:
        _fetch_subject_spike_sorted(subject_id)

    if lazy:
        units_path = acorns.TREE.get_location("spike_sorted_units", partitioned=True)
        spikes_path = acorns.TREE.get_location("spike_sorted_spikes", partitioned=True)
        return {"units": units_path, "spikes": spikes_path}

    units_df = _scurry_partitioned("spike_sorted_units", subject_id)
    spikes_df = _scurry_partitioned("spike_sorted_spikes", subject_id)

    if units_df.empty and not force_update:
        logging.error(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["ss"],
                message=f"Cache is empty for subject {subject_id}. Use force_update=True to fetch data from database.",
            ).to_json()
        )

    if asset_names is not None:
        if isinstance(asset_names, str):
            asset_names = [asset_names]
        units_df = units_df[units_df["asset_name"].isin(asset_names)].reset_index(drop=True)
        spikes_df = spikes_df[spikes_df["asset_name"].isin(asset_names)].reset_index(drop=True)

    return {"units": units_df, "spikes": spikes_df}


def _scurry_partitioned(table: str, subject_id: str) -> pd.DataFrame:
    """Read all per-asset parquet files for a subject using DuckDB glob."""
    import duckdb

    location = acorns.TREE.get_location(table, partitioned=True)
    glob_path = location.rstrip("/") + "/*.pqt"

    try:
        return duckdb.query(
            f"SELECT * FROM read_parquet('{glob_path}') WHERE subject_id = '{subject_id}'"
        ).to_df()
    except Exception:
        return pd.DataFrame()


def _fetch_subject_spike_sorted(subject_id: str) -> None:
    """Fetch spike sorted assets for a subject and cache each as a separate parquet file.

    Only caches the latest derived asset per pipeline for each raw asset of the subject,
    preventing stale/old versions of assets from being cached.

    Args:
        subject_id: Subject ID to fetch spike sorted assets for.
    """
    setup_logging()

    logging.info(
        SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["ss"],
            message=f"Updating cache for subject {subject_id}",
        ).to_json()
    )

    basics_df = asset_basics()
    raw_names = basics_df[
        (basics_df["subject_id"] == subject_id) & (basics_df["data_level"] == "raw")
    ]["name"].tolist()

    if not raw_names:
        logging.warning(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["ss"],
                message=f"No raw assets found for subject {subject_id}",
            ).to_json()
        )
        return

    derived_names: list[str] = []
    for raw_name in raw_names:
        derived_names.extend(raw_to_derived(raw_name, latest=True))

    if not derived_names:
        logging.warning(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["ss"],
                message=f"No derived assets found for subject {subject_id}",
            ).to_json()
        )
        return

    client = MetadataDbClient(
        host=acorns.API_GATEWAY_HOST,
        version="v2",
    )

    records = client.retrieve_docdb_records(
        filter_query={"name": {"$in": derived_names}},
        projection={
            "_id": 1,
            "name": 1,
            "location": 1,
            "subject.subject_id": 1,
            "processing.data_processes": 1,
        },
        limit=len(derived_names),
    )

    if not records:
        logging.warning(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["ss"],
                message=f"No records found for subject {subject_id}",
            ).to_json()
        )
        return

    assets_cached = 0
    for record in records:
        asset_id = record.get("_id", "")
        asset_name = record.get("name", "")
        subject = record.get("subject", {})
        processing = record.get("processing", {})
        location = record.get("location", "")

        subject_id_value = subject.get("subject_id", "")

        data_processes = processing.get("data_processes", [])
        has_spike_sorting = any(
            process.get("process_type") == "Spike sorting"
            for process in data_processes
        )

        if has_spike_sorting and location:
            try:
                units_df, spikes_df = _extract_from_nwb(location, asset_id, asset_name, subject_id_value)
                if not units_df.empty:
                    acorns.TREE.hide(f"spike_sorted_units/{asset_name}", units_df)
                    acorns.TREE.hide(f"spike_sorted_spikes/{asset_name}", spikes_df)
                    assets_cached += 1
            except Exception as e:
                logging.warning(
                    SquirrelMessage(
                        tree=acorns.TREE.__class__.__name__,
                        acorn=acorns.NAMES["ss"],
                        message=f"Failed to extract data from asset {asset_name}: {str(e)}",
                    ).to_json()
                )

    if assets_cached == 0:
        logging.warning(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["ss"],
                message=f"No spike sorted units found for subject {subject_id}",
            ).to_json()
        )

    logging.info(
        SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["ss"],
            message=f"Cached {assets_cached} spike sorted asset(s) for subject {subject_id}",
        ).to_json()
    )


def _extract_from_nwb(
    s3_location: str,
    asset_id: str,
    asset_name: str,
    subject_id: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Extract units and spikes tables from NWB zarr files on S3.
    
    Args:
        s3_location: S3 URI (e.g., s3://bucket/prefix/)
        asset_id: Asset ID for tracking
        asset_name: Asset name for tracking
        subject_id: Subject ID for tracking
        
    Returns:
        Tuple of (units_df, spikes_df)
    """
    if not s3_location.startswith("s3://"):
        raise ValueError(f"Expected s3:// location, got: {s3_location}")
    
    s3_path = s3_location.replace("s3://", "")
    bucket, prefix = s3_path.split("/", 1)
    
    if not prefix.endswith("/"):
        prefix = prefix + "/"
    
    s3 = s3fs.S3FileSystem(anon=False)
    
    nwb_prefix = f"{bucket}/{prefix}nwb/"
    try:
        nwb_dirs = s3.ls(nwb_prefix)
    except Exception as e:
        logging.warning(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["ss"],
                message=f"Failed to list NWB directories at {nwb_prefix}: {str(e)}",
            ).to_json()
        )
        return pd.DataFrame(), pd.DataFrame()
    
    all_units = []
    all_spikes = []
    
    for nwb_dir in nwb_dirs:
        if not nwb_dir.endswith(".nwb") and not nwb_dir.endswith(".nwb/"):
            continue
            
        nwb_name = nwb_dir.split("/")[-1].replace(".nwb/", "").replace(".nwb", "")
        
        try:
            store = s3fs.S3Map(root=nwb_dir, s3=s3, check=False)
            zgroup = zarr.open_consolidated(store, mode="r")
            
            if "units" not in zgroup:
                continue
            
            units_group = zgroup["units"]
            
            unit_ids = units_group["id"][:]
            num_units = len(unit_ids)
            
            units_data = {}
            spike_times_array = None
            spike_times_index_array = None
            
            for key in units_group.keys():
                if key.startswith("."):
                    continue
                    
                try:
                    arr = units_group[key]
                    
                    if key == "spike_times":
                        spike_times_array = arr[:]
                    elif key == "spike_times_index":
                        spike_times_index_array = arr[:]
                    elif arr.ndim == 1 and arr.shape[0] == num_units:
                        data = arr[:]
                        units_data[key] = data.tolist() if hasattr(data, "tolist") else list(data)
                    # skip ragged arrays (electrodes etc) and multi-dim arrays (waveforms etc)
                except Exception as e:
                    logging.debug(f"Skipping column {key}: {str(e)}")
                    continue
            units_data["asset_id"] = [asset_id] * num_units
            units_data["asset_name"] = [asset_name] * num_units
            units_data["subject_id"] = [subject_id] * num_units
            units_data["nwb_file"] = [nwb_name] * num_units
            
            if num_units > 0:
                units_df = pd.DataFrame(units_data)
                all_units.append(units_df)
            
            if spike_times_array is not None and spike_times_index_array is not None:
                counts = np.diff(np.concatenate([[0], spike_times_index_array]))
                repeated_unit_ids = np.repeat(unit_ids, counts)
                spikes_df = pd.DataFrame({
                    "unit_id": repeated_unit_ids,
                    "spike_time": spike_times_array,
                    "asset_name": asset_name,
                    "subject_id": subject_id,
                })
                all_spikes.append(spikes_df)
                
        except Exception as e:
            logging.warning(
                SquirrelMessage(
                    tree=acorns.TREE.__class__.__name__,
                    acorn=acorns.NAMES["ss"],
                    message=f"Failed to read NWB file {nwb_name}: {str(e)}",
                ).to_json()
            )
            continue
    
    units_result = pd.concat(all_units, ignore_index=True) if all_units else pd.DataFrame()
    spikes_result = pd.concat(all_spikes, ignore_index=True) if all_spikes else pd.DataFrame()
    
    return units_result, spikes_result


def spike_sorted_columns() -> dict[str, list[str]]:
    """Get known column names from spike sorted tables.

    Returns:
        Dictionary with 'units' and 'spikes' keys containing their guaranteed columns.
        Note: Units table includes additional columns extracted dynamically from NWB files.
    """
    return {
        "units": ["asset_id", "asset_name", "subject_id", "nwb_file", "id"],
        "spikes": ["unit_id", "spike_time", "asset_name", "subject_id"],
    }

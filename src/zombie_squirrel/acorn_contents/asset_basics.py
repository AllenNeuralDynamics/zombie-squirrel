"""Asset basics acorn."""

import logging

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.utils import setup_logging, SquirrelMessage


@acorns.register_acorn(acorns.NAMES["basics"])
def asset_basics(force_update: bool = False) -> pd.DataFrame:
    """Fetch basic asset metadata including modalities, projects, and subject info.

    Returns a DataFrame with columns: _id, _last_modified, modalities,
    project_name, data_level, subject_id, acquisition_start_time, and
    acquisition_end_time. Uses incremental updates based on _last_modified
    timestamps to avoid re-fetching unchanged records.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with basic asset metadata."""
    df = acorns.TREE.scurry(acorns.NAMES["basics"])

    FIELDS = [
        "data_description.modalities",
        "data_description.project_name",
        "data_description.data_level",
        "subject.subject_id",
        "acquisition.acquisition_start_time",
        "acquisition.acquisition_end_time",
        "processing.data_processes.start_date_time",
        "subject.subject_details.genotype",
        "other_identifiers",
        "location",
        "name",
    ]

    if df.empty and not force_update:
        raise ValueError(
            "Cache is empty. Use force_update=True to fetch data from database."
        )

    if df.empty or force_update:
        setup_logging()
        logging.info(SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["basics"],
            message="Updating cache"
        ).to_json())
        df = pd.DataFrame(
            columns=[
                "_id",
                "_last_modified",
                "modalities",
                "project_name",
                "data_level",
                "subject_id",
                "acquisition_start_time",
                "acquisition_end_time",
                "code_ocean",
                "process_date",
                "genotype",
                "location",
                "name",
            ]
        )
        client = MetadataDbClient(
            host=acorns.API_GATEWAY_HOST,
            version="v2",
        )
        # It's a bit complex to get multiple fields that aren't indexed in a database
        # as large as DocDB. We'll also try to limit ourselves to only updating fields
        # that are necessary
        record_ids = client.retrieve_docdb_records(
            filter_query={},
            projection={"_id": 1, "_last_modified": 1},
            limit=0,
        )
        keep_ids = []
        # Drop all _ids where _last_modified matches cache
        for record in record_ids:
            cached_row = df[df["_id"] == record["_id"]]
            if cached_row.empty or cached_row["_last_modified"].values[0] != record["_last_modified"]:
                keep_ids.append(record["_id"])

        # Now batch by 100 IDs at a time to avoid overloading server, and fetch all the fields
        BATCH_SIZE = 100
        asset_records = []
        for i in range(0, len(keep_ids), BATCH_SIZE):
            logging.info(SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["basics"],
                message=f"Fetching batch {i // BATCH_SIZE + 1}"
            ).to_json())
            batch_ids = keep_ids[i: i + BATCH_SIZE]
            batch_records = client.retrieve_docdb_records(
                filter_query={"_id": {"$in": batch_ids}},
                projection={field: 1 for field in FIELDS + ["_id", "_last_modified"]},
                limit=0,
            )
            asset_records.extend(batch_records)

        # Unwrap nested fields
        records = []
        for record in asset_records:
            modalities = record.get("data_description", {}).get("modalities", [])
            modality_abbreviations = [modality["abbreviation"] for modality in modalities if "abbreviation" in modality]
            modality_abbreviations_str = ", ".join(modality_abbreviations)

            # Get the process date, convert to YYYY-MM-DD if present
            data_processes = record.get("processing", {}).get("data_processes", [])
            if data_processes:
                latest_process = data_processes[-1]
                process_datetime = latest_process.get("start_date_time", None)
                process_date = process_datetime.split("T")[0]
            else:
                process_date = None

            # Get the CO asset ID
            other_identifiers = record.get("other_identifiers", {})
            if other_identifiers:
                code_ocean = other_identifiers.get("Code Ocean", None)
            else:
                code_ocean = None

            flat_record = {
                "_id": record["_id"],
                "_last_modified": record.get("_last_modified", None),
                "modalities": modality_abbreviations_str,
                "project_name": record.get("data_description", {}).get("project_name", None),
                "data_level": record.get("data_description", {}).get("data_level", None),
                "subject_id": record.get("subject", {}).get("subject_id", None),
                "acquisition_start_time": record.get("acquisition", {}).get("acquisition_start_time", None),
                "acquisition_end_time": record.get("acquisition", {}).get("acquisition_end_time", None),
                "code_ocean": code_ocean,
                "process_date": process_date,
                "genotype": record.get("subject", {}).get("subject_details", {}).get("genotype", None),
                "location": record.get("location", None),
                "name": record.get("name", None),
            }
            records.append(flat_record)

        # Combine new records with the old df and store in cache
        new_df = pd.DataFrame(records)
        df = pd.concat([df[~df["_id"].isin(keep_ids)], new_df], ignore_index=True)

        acorns.TREE.hide(acorns.NAMES["basics"], df)

    return df

"""Upgrade status acorn."""

import logging
import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.acorn_helpers.custom import custom
from zombie_squirrel.squirrel import Column
from zombie_squirrel.utils import (
    SquirrelMessage,
    setup_logging,
)

@acorns.register_acorn("metadata_upgrade")
def metadata_upgrade(force_update: bool = False) -> pd.DataFrame:
    """Fetch and calculate metadata upgrade status.

    Pulls raw upgrade results from the 'metadata_upgrade_status_prod' custom table
    and merges them with v1 asset basics. Maintains historical version columns.

    Args:
        force_update: If True, bypass cache and re-calculate status.

    Returns:
        DataFrame with upgrade status.
    """
    df = acorns.TREE.scurry("metadata_upgrade")

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data.")

    if df.empty or force_update:
        setup_logging()
        logging.info(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__, acorn="metadata_upgrade", message="Updating upgrade status"
            ).to_json()
        )

        # Get the raw upgrade status from Redshift/Custom storage
        try:
            raw_upgrade_df = custom("metadata_upgrade_status_prod")
        except ValueError:
            logging.warning("Custom table 'metadata_upgrade_status_prod' not found. Returning empty upgrade status.")
            return pd.DataFrame()

        # Use the upgrader version from the custom table itself
        if "upgrader_version" not in raw_upgrade_df.columns:
            logging.warning("Column 'upgrader_version' not found in custom table. Cannot version columns.")
            return pd.DataFrame()

        upgrade_data = raw_upgrade_df.copy()
        upgrade_data.rename(columns={"v1_id": "_id"}, inplace=True)

        unique_versions = [str(v) for v in upgrade_data["upgrader_version"].unique()]
        for version in unique_versions:
            mask = upgrade_data["upgrader_version"].astype(str) == version
            upgrade_data[version] = upgrade_data["status"].where(mask, other=pd.NA)

        if not df.empty:
            drop_cols = ["name", "project_name", "data_level", "v2_id", "upgrader_version", "last_modified", "status", "upgrade_datetime"]
            existing_versions = df.drop(columns=[c for c in drop_cols if c in df.columns], errors='ignore')
            df_merged = existing_versions.merge(upgrade_data, on="_id", how="outer", suffixes=('', '_new'))
            for version in unique_versions:
                new_col = f"{version}_new"
                if new_col in df_merged.columns:
                    if version in df_merged.columns:
                        df_merged[version] = df_merged[new_col].combine_first(df_merged[version])
                    else:
                        df_merged[version] = df_merged[new_col]
                    df_merged.drop(columns=[new_col], inplace=True)
            df_final = df_merged
        else:
            df_final = upgrade_data

        # For successful upgrades, pull name/project_name/data_level from V2 via v2_id
        # (already available in basics_df). For the rest, fetch from V1 DocDB.
        basics_df = acorns.ACORN_REGISTRY[acorns.NAMES["basics"]]()
        v2_lookup = basics_df[["_id", "name", "project_name", "data_level"]].rename(columns={"_id": "v2_id"})

        successful = df_final[df_final["v2_id"].notna()].copy()
        failed = df_final[df_final["v2_id"].isna()].copy()

        successful = successful.merge(v2_lookup, on="v2_id", how="left")

        if not failed.empty:
            v1_client = MetadataDbClient(
                host=acorns.API_GATEWAY_HOST,
                version="v1",
            )
            v1_ids = failed["_id"].tolist()
            v1_records = []
            BATCH_SIZE = 100
            for i in range(0, len(v1_ids), BATCH_SIZE):
                logging.info(
                    SquirrelMessage(
                        tree=acorns.TREE.__class__.__name__,
                        acorn="metadata_upgrade",
                        message=f"Fetching V1 batch {i // BATCH_SIZE + 1}",
                    ).to_json()
                )
                batch = v1_ids[i : i + BATCH_SIZE]
                batch_records = v1_client.retrieve_docdb_records(
                    filter_query={"_id": {"$in": batch}},
                    projection={"_id": 1, "name": 1, "data_description.data_level": 1, "data_description.project_name": 1},
                    limit=0,
                )
                v1_records.extend(batch_records)

            for record in v1_records:
                dd = record.pop("data_description", {}) or {}
                record["data_level"] = dd.get("data_level")
                record["project_name"] = dd.get("project_name")

            v1_df = pd.DataFrame(v1_records) if v1_records else pd.DataFrame(columns=["_id", "name", "project_name", "data_level"])
            failed = failed.merge(v1_df[["_id", "name", "project_name", "data_level"]], on="_id", how="left")

        final_df = pd.concat([successful, failed], ignore_index=True)

        acorns.TREE.hide("metadata_upgrade", final_df)
        return final_df

    return df

def metadata_upgrade_columns() -> list[Column]:
    """Return metadata upgrade acorn column definitions."""
    return [
        Column(name="_id", description="DocDB record ID (v1)"),
        Column(name="name", description="Asset name"),
        Column(name="project_name", description="Project name"),
        Column(name="data_level", description="Data level"),
        Column(name="v2_id", description="DocDB record ID (v2)"),
        Column(name="upgrader_version", description="Version of the upgrader used for the latest run"),
        Column(name="last_modified", description="DocDB last modified timestamp"),
        Column(name="status", description="Status of the latest upgrade run"),
        Column(name="upgrade_datetime", description="Timestamp of the upgrade run"),
    ]

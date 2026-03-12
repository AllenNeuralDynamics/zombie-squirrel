"""SmartSPIM assets acorn."""

import logging

import boto3
import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.acorn_helpers.asset_basics import asset_basics
from zombie_squirrel.acorn_helpers.source_data import source_data
from zombie_squirrel.utils import SquirrelMessage, setup_logging

NEUROGLANCER_BASE = "https://neuroglancer-demo.appspot.com/#!"
AIND_OPEN_DATA_BUCKET = "aind-open-data"


def _stitched_link(location: str) -> str:
    return f"{NEUROGLANCER_BASE}{location}/neuroglancer_config.json"


def _segmentation_link(location: str, channel: str) -> str:
    return f"{NEUROGLANCER_BASE}{location}/image_cell_segmentation/{channel}/visualization/neuroglancer_config.json"


def _quantification_link(location: str, channel: str) -> str:
    return f"{NEUROGLANCER_BASE}{location}/image_cell_quantification/{channel}/visualization/neuroglancer_config.json"


def _list_channels(location: str) -> list[str]:
    """List channel subfolders under image_cell_segmentation/ for a given asset location."""
    s3_client = boto3.client("s3")
    prefix = location.replace(f"s3://{AIND_OPEN_DATA_BUCKET}/", "") + "/image_cell_segmentation/"
    result = s3_client.list_objects_v2(
        Bucket=AIND_OPEN_DATA_BUCKET,
        Prefix=prefix,
        Delimiter="/",
    )
    return [cp["Prefix"].rstrip("/").split("/")[-1] for cp in result.get("CommonPrefixes", [])]


def _fetch_stitched_metadata(stitched_names: list[str]) -> dict[str, dict]:
    """Fetch metadata for stitched assets from the document DB in batches of 100."""
    client = MetadataDbClient(
        host=acorns.API_GATEWAY_HOST,
        version="v2",
    )
    fields = [
        "name",
        "subject.subject_id",
        "subject.subject_details.genotype",
        "data_description.institution",
        "acquisition.acquisition_start_time",
        "processing.data_processes",
        "location",
    ]
    projection = {field: 1 for field in fields + ["_id"]}
    BATCH_SIZE = 100
    all_records = []
    for i in range(0, len(stitched_names), BATCH_SIZE):
        batch = stitched_names[i : i + BATCH_SIZE]
        batch_records = client.retrieve_docdb_records(
            filter_query={"name": {"$in": batch}},
            projection=projection,
            limit=0,
        )
        all_records.extend(batch_records)
    return {record["name"]: record for record in all_records}


def _build_rows(stitched_names: list[str], metadata: dict[str, dict]) -> list[dict]:
    """Build one row per (stitched_asset, channel) from metadata and S3 channel listing."""
    rows = []
    for stitched_name in stitched_names:
        record = metadata.get(stitched_name, {})
        location = record.get("location", "")

        subject = record.get("subject", {})
        subject_id = subject.get("subject_id", None)
        genotype = subject.get("subject_details", {}).get("genotype", None)

        institution = record.get("data_description", {}).get("institution", {})
        institution_abbrev = institution.get("abbreviation", None) if institution else None

        acquisition_start_time = record.get("acquisition", {}).get("acquisition_start_time", None)

        data_processes = record.get("processing", {}).get("data_processes", []) or []
        processing_end_time = data_processes[-1].get("end_date_time", None) if data_processes else None

        stitch_link = _stitched_link(location) if location else None

        channels = _list_channels(location) if location else []

        if not channels:
            rows.append(
                {
                    "subject_id": subject_id,
                    "genotype": genotype,
                    "institution": institution_abbrev,
                    "acquisition_start_time": acquisition_start_time,
                    "processing_end_time": processing_end_time,
                    "stitched_link": stitch_link,
                    "channel": None,
                    "segmentation_link": None,
                    "quantification_link": None,
                    "name": stitched_name,
                }
            )
        else:
            for channel in channels:
                rows.append(
                    {
                        "subject_id": subject_id,
                        "genotype": genotype,
                        "institution": institution_abbrev,
                        "acquisition_start_time": acquisition_start_time,
                        "processing_end_time": processing_end_time,
                        "stitched_link": stitch_link,
                        "channel": channel,
                        "segmentation_link": _segmentation_link(location, channel),
                        "quantification_link": _quantification_link(location, channel),
                        "name": stitched_name,
                    }
                )
    return rows


@acorns.register_acorn(acorns.NAMES["smartspim"])
def assets_smartspim(force_update: bool = False) -> pd.DataFrame:
    """Build a DataFrame of SmartSPIM stitched assets for dashboard use.

    Fetches raw SPIM assets from asset_basics, finds the latest stitched derived
    asset for each via raw_to_derived, then enriches with metadata and S3 channel
    links from image_cell_segmentation/. Results are cached.

    Args:
        force_update: If True, bypass cache and rebuild from database and S3.

    Returns:
        DataFrame with one row per (stitched_asset, channel) and columns:
        subject_id, genotype, institution, acquisition_start_time,
        processing_end_time, stitched_link, channel, segmentation_link,
        quantification_link, name.
    """
    df = acorns.TREE.scurry(acorns.NAMES["smartspim"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from database.")

    if df.empty or force_update:
        setup_logging()
        logging.info(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["smartspim"],
                message="Updating cache",
            ).to_json()
        )

        basics = asset_basics()
        raw_spim = basics[
            (basics["data_level"] == "raw") & (basics["modalities"].str.contains("SPIM", na=False))
        ]
        raw_spim_names = set(raw_spim["name"].dropna())

        sd = source_data()
        stitched_candidates = sd[
            sd["source_data"].isin(raw_spim_names) & sd["name"].str.contains("stitched", case=False, na=False)
        ].copy()
        stitched_candidates = (
            stitched_candidates.sort_values("processing_time", ascending=False)
            .groupby("source_data", as_index=False)
            .first()
        )
        stitched_names = list(stitched_candidates["name"].unique())

        if not stitched_names:
            df = pd.DataFrame(columns=assets_smartspim_columns())
            acorns.TREE.hide(acorns.NAMES["smartspim"], df)
            return df

        logging.info(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["smartspim"],
                message=f"Fetching metadata for {len(stitched_names)} stitched assets",
            ).to_json()
        )

        metadata = _fetch_stitched_metadata(stitched_names)
        rows = _build_rows(stitched_names, metadata)
        df = pd.DataFrame(rows)

        acorns.TREE.hide(acorns.NAMES["smartspim"], df)

    return df


def assets_smartspim_columns() -> list[str]:
    return [
        "subject_id",
        "genotype",
        "institution",
        "acquisition_start_time",
        "processing_end_time",
        "stitched_link",
        "channel",
        "segmentation_link",
        "quantification_link",
        "name",
    ]

"""SmartSPIM assets acorn."""

import logging

import boto3
import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.acorn_helpers.asset_basics import asset_basics
from zombie_squirrel.acorn_helpers.source_data import source_data
from zombie_squirrel.squirrel import Column
from zombie_squirrel.utils import SquirrelMessage, setup_logging

NEUROGLANCER_BASE = "https://allen.neuroglass.io/new#!"
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


def _fetch_asset_metadata(asset_names: list[str]) -> dict[str, dict]:
    """Fetch metadata for assets (raw or stitched) from the document DB in batches of 100."""
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
    for i in range(0, len(asset_names), BATCH_SIZE):
        batch = asset_names[i : i + BATCH_SIZE]
        batch_records = client.retrieve_docdb_records(
            filter_query={"name": {"$in": batch}},
            projection=projection,
            limit=0,
        )
        all_records.extend(batch_records)
    return {record["name"]: record for record in all_records}


MAX_CHANNELS = 3


def _build_rows(raw_to_stitched: dict[str, str | None], metadata: dict[str, dict]) -> list[dict]:
    """Build one row per raw asset with up to 3 channel columns.

    For raw assets with a stitched derived asset, metadata and neuroglancer links
    are pulled from the stitched record. For raw assets without one, metadata is
    pulled from the raw record and all link/channel columns are None.
    """
    rows = []
    for raw_name, stitched_name in raw_to_stitched.items():
        processed = stitched_name is not None
        lookup_name = stitched_name if processed else raw_name
        record = metadata.get(lookup_name, {})

        subject = record.get("subject", {})
        subject_id = subject.get("subject_id", None)
        genotype = subject.get("subject_details", {}).get("genotype", None)

        institution = record.get("data_description", {}).get("institution", {})
        institution_abbrev = institution.get("abbreviation", None) if institution else None

        acquisition_start_time = record.get("acquisition", {}).get("acquisition_start_time", None)

        if processed:
            location = record.get("location", "")
            data_processes = record.get("processing", {}).get("data_processes", []) or []
            processing_end_time = data_processes[-1].get("end_date_time", None) if data_processes else None
            stitch_link = _stitched_link(location) if location else None
            channels = _list_channels(location) if location else []
        else:
            processing_end_time = None
            stitch_link = None
            channels = []

        row = {
            "subject_id": subject_id,
            "genotype": genotype,
            "institution": institution_abbrev,
            "acquisition_start_time": acquisition_start_time,
            "processing_end_time": processing_end_time,
            "stitched_link": stitch_link,
            "processed": processed,
            "name": stitched_name if processed else raw_name,
        }
        for i in range(1, MAX_CHANNELS + 1):
            channel = channels[i - 1] if i <= len(channels) else None
            row[f"channel_{i}"] = channel
            row[f"segmentation_link_{i}"] = _segmentation_link(location, channel) if (processed and channel) else None
            row[f"quantification_link_{i}"] = _quantification_link(location, channel) if (processed and channel) else None
        rows.append(row)
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
        raw_spim_names = list(raw_spim["name"].dropna())

        sd = source_data()
        stitched_candidates = sd[
            sd["source_data"].isin(raw_spim_names) & sd["name"].str.contains("stitched", case=False, na=False)
        ].copy()
        stitched_candidates = (
            stitched_candidates.sort_values("processing_time", ascending=False)
            .groupby("source_data", as_index=False)
            .first()
        )
        raw_to_stitched_series = stitched_candidates.set_index("source_data")["name"]
        raw_to_stitched = {name: raw_to_stitched_series.get(name) for name in raw_spim_names}

        stitched_names = [v for v in raw_to_stitched.values() if v is not None]
        raw_without_stitched = [k for k, v in raw_to_stitched.items() if v is None]
        all_to_fetch = stitched_names + raw_without_stitched

        logging.info(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["smartspim"],
                message=f"Fetching metadata for {len(stitched_names)} stitched and {len(raw_without_stitched)} unprocessed assets",
            ).to_json()
        )

        metadata = _fetch_asset_metadata(all_to_fetch)
        rows = _build_rows(raw_to_stitched, metadata)
        df = pd.DataFrame(rows)

        acorns.TREE.hide(acorns.NAMES["smartspim"], df)

    return df


def assets_smartspim_columns() -> list[Column]:
    return [
        Column(name="subject_id", description="Subject for the asset"),
        Column(name="genotype", description="Subject genotype"),
        Column(name="institution", description="Institution abbreviation"),
        Column(name="acquisition_start_time", description="Acquisition start time"),
        Column(name="processing_end_time", description="Processing end time for stitched asset"),
        Column(name="stitched_link", description="Neuroglancer link to stitched asset"),
        Column(name="processed", description="Whether a stitched derived asset exists"),
        Column(name="name", description="Asset name (stitched if available, otherwise raw)"),
        Column(name="channel_1", description="First channel name"),
        Column(name="segmentation_link_1", description="Neuroglancer segmentation link for channel 1"),
        Column(name="quantification_link_1", description="Neuroglancer quantification link for channel 1"),
        Column(name="channel_2", description="Second channel name"),
        Column(name="segmentation_link_2", description="Neuroglancer segmentation link for channel 2"),
        Column(name="quantification_link_2", description="Neuroglancer quantification link for channel 2"),
        Column(name="channel_3", description="Third channel name"),
        Column(name="segmentation_link_3", description="Neuroglancer segmentation link for channel 3"),
        Column(name="quantification_link_3", description="Neuroglancer quantification link for channel 3"),
    ]

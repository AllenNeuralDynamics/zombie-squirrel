"""SWDB 2025 metadata cache tables.

Builds the standalone SWDB 2025 metadata tables, each mirroring a metadata notebook
from the SWDB 2025 DataIntro repo:

===================  ==================================================
``swdb_2025_bci``    one row per curated derived Brain Computer Interface
                     single-neuron-stim session (``bci_metadata.ipynb``)
``swdb_2025_v1dd``   one row per V1 Deep Dive asset (``V1DD_metadata.ipynb``)
===================  ==================================================

The rows come from DocDB aggregations over the ``metadata_index`` collection rather
than from ``asset_basics``, so these tables are self-contained and are built by a
standalone script (see ``scripts/build_swdb.py``); they are deliberately not part
of the nightly sync pipeline.
"""

import logging
from datetime import datetime

import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.swdb_public_assets import SWDB_2026_DERIVED_ASSETS_BY_COLLECTION
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

DATABASE = "metadata_index"
COLLECTION = "data_assets"


def _fetch_records(pipeline: list[dict]) -> list[dict]:
    """Run an aggregation pipeline against the metadata_index collection."""
    from aind_data_access_api.document_db import MetadataDbClient

    client = MetadataDbClient(
        host=registry.API_GATEWAY_HOST,
        version="v2",
    )
    return client.aggregate_docdb_records(pipeline=pipeline)


# ---------------------------------------------------------------------------
# swdb_2025_bci
# ---------------------------------------------------------------------------

# Assets with known-bad metadata that the notebook removes by hand.
BCI_PROBLEM_ASSETS = [
    "single-plane-ophys_731015_2025-01-28_17-40-57_processed_2025-08-04_04-38-08",
    "single-plane-ophys_772414_2025-02-04_13-21-29_processed_2025-08-12_06-14-42",
]

BCI_COLUMN_ORDER = [
    "project_name",
    "session_type",
    "_id",
    "name",
    "subject_id",
    "genotype",
    "virus",
    "date_of_birth",
    "sex",
    "modality",
    "session_date",
    "age",
    "session_time",
    "targeted_structure",
    "ophys_fov",
    "session_number",
]

BCI_PIPELINE = [
    {
        "$match": {
            "session.session_type": "BCI single neuron stim",
            "data_description.data_level": "derived",
            "processing.processing_pipeline.data_processes.start_date_time": {"$gte": "2025-08-03"},
        }
    },
    {
        "$project": {
            "name": 1,
            "subject_id": "$data_description.subject_id",
            "genotype": "$subject.genotype",
            "virus": "$procedures.subject_procedures.procedures.injection_materials.name",
            "date_of_birth": "$subject.date_of_birth",
            "sex": "$subject.sex",
            "session_type": "$session.session_type",
            "session_time": "$session.session_start_time",
            "stimulus_epochs": "$session.stimulus_epochs.stimulus_name",
            "project_name": "$data_description.project_name",
            "modality": "$data_description.modality.name",
            "targeted_structure": "$session.data_streams.stack_parameters.targeted_structure",
            "session_number": {
                "$filter": {
                    "input": "$session.stimulus_epochs",
                    "as": "epoch",
                    "cond": {"$eq": ["$$epoch.stimulus_name", "single neuron BCI conditioning"]},
                }
            },
            "ophys_fov": {
                "$map": {
                    "input": "$session.data_streams",
                    "as": "stream",
                    "in": {
                        "$map": {
                            "input": "$$stream.ophys_fovs",
                            "as": "fov",
                            "in": "$$fov.notes",
                        }
                    },
                }
            },
        }
    },
    {
        "$project": {
            "name": 1,
            "subject_id": 1,
            "genotype": 1,
            "virus": 1,
            "date_of_birth": 1,
            "sex": 1,
            "session_type": 1,
            "session_time": 1,
            "stimulus_epochs": 1,
            "project_name": 1,
            "modality": 1,
            "targeted_structure": 1,
            "session_number": {"$arrayElemAt": ["$session_number.session_number", 0]},
            "ophys_fov": 1,
        }
    },
    {"$unwind": {"path": "$ophys_fov", "preserveNullAndEmptyArrays": False}},
    {"$unwind": {"path": "$ophys_fov", "preserveNullAndEmptyArrays": False}},
    {"$unwind": {"path": "$virus", "preserveNullAndEmptyArrays": False}},
    {"$unwind": {"path": "$virus", "preserveNullAndEmptyArrays": False}},
    {"$unwind": {"path": "$virus", "preserveNullAndEmptyArrays": False}},
    {"$unwind": {"path": "$modality", "preserveNullAndEmptyArrays": False}},
    {"$unwind": {"path": "$targeted_structure", "preserveNullAndEmptyArrays": False}},
]


def _fetch_bci_records() -> list[dict]:
    """Run the BCI metadata aggregation against the metadata_index collection."""
    return _fetch_records(BCI_PIPELINE)


def _build_bci_dataframe(records: list[dict]) -> pd.DataFrame:
    """Shape aggregation records into the ordered BCI metadata table."""
    df = pd.DataFrame(records)
    df = df.drop_duplicates(subset="name")

    df["session_date"] = df.apply(lambda x: datetime.fromisoformat(x["session_time"]).date(), axis=1)
    df["session_time"] = df.apply(lambda x: datetime.fromisoformat(x["session_time"]).time(), axis=1)
    df["date_of_birth"] = df.apply(lambda x: datetime.strptime(x["date_of_birth"], "%Y-%m-%d").date(), axis=1)
    df["age"] = df.apply(lambda x: (x["session_date"] - x["date_of_birth"]).days, axis=1)

    df = df[BCI_COLUMN_ORDER]
    df = df[~df["name"].isin(BCI_PROBLEM_ASSETS)]
    return df.reset_index(drop=True)


@registry.register_table(registry.NAMES["swdb_2025_bci"])
def swdb_2025_bci(force_update: bool = False) -> pd.DataFrame:
    """Return the SWDB 2025 BCI metadata table, building it on demand."""
    df = registry.BACKEND.read(registry.NAMES["swdb_2025_bci"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from database.")

    if df.empty or force_update:
        setup_logging()
        logging.info(
            CacheLogMessage(
                backend=registry.BACKEND.__class__.__name__,
                table=registry.NAMES["swdb_2025_bci"],
                message="Updating cache",
            ).to_json()
        )

        records = _fetch_bci_records()
        df = _build_bci_dataframe(records)

        registry.BACKEND.write(registry.NAMES["swdb_2025_bci"], df)

    return df


def swdb_2025_bci_columns() -> list[Column]:
    return [
        Column(name="project_name", description="Project name from data_description"),
        Column(name="session_type", description="Session type (BCI single neuron stim)"),
        Column(name="_id", description="DocDB record id"),
        Column(name="name", description="Derived asset name"),
        Column(name="subject_id", description="Subject id"),
        Column(name="genotype", description="Subject genotype"),
        Column(name="virus", description="Injection material name"),
        Column(name="date_of_birth", description="Subject date of birth"),
        Column(name="sex", description="Subject sex"),
        Column(name="modality", description="Data modality name"),
        Column(name="session_date", description="Session date"),
        Column(name="age", description="Subject age in days at session"),
        Column(name="session_time", description="Session start time of day"),
        Column(name="targeted_structure", description="Targeted brain structure"),
        Column(name="ophys_fov", description="Ophys FOV notes"),
        Column(name="session_number", description="BCI conditioning session number"),
    ]


# ---------------------------------------------------------------------------
# swdb_2025_v1dd
# ---------------------------------------------------------------------------

# Subject id of the reference ("golden") mouse in the V1 Deep Dive dataset.
V1DD_GOLDEN_MOUSE_SUBJECT_ID = "409828"

V1DD_COLUMN_ORDER = [
    "project_name",
    "_id",
    "name",
    "subject_id",
    "golden_mouse",
    "genotype",
    "date_of_birth",
    "sex",
    "modality",
    "session_date",
    "age",
    "session_time",
    "column",
    "volume",
]

V1DD_PIPELINE = [
    {"$match": {"data_description.project_name": "V1 Deep Dive"}},
    {
        "$project": {
            "name": 1,
            "subject_id": "$data_description.subject_id",
            "genotype": "$subject.subject_details.genotype",
            "date_of_birth": "$subject.subject_details.date_of_birth",
            "sex": "$subject.subject_details.sex",
            "session_time": "$acquisition.acquisition_start_time",
            "project_name": "$data_description.project_name",
            "modality": "$data_description.modalities.name",
            "column": {"$arrayElemAt": ["$data_description.tags", 0]},
            "volume": {"$arrayElemAt": ["$data_description.tags", 1]},
        }
    },
]


def _fetch_v1dd_records() -> list[dict]:
    """Run the V1 Deep Dive metadata aggregation against the metadata_index collection."""
    return _fetch_records(V1DD_PIPELINE)


def _build_v1dd_dataframe(records: list[dict]) -> pd.DataFrame:
    """Shape aggregation records into the ordered V1 Deep Dive metadata table."""
    df = pd.DataFrame(records)

    df["session_date"] = df.apply(lambda x: datetime.fromisoformat(x["session_time"]).date(), axis=1)
    df["session_time"] = df.apply(lambda x: datetime.fromisoformat(x["session_time"]).time(), axis=1)
    df["date_of_birth"] = df.apply(lambda x: datetime.strptime(x["date_of_birth"], "%Y-%m-%d").date(), axis=1)
    df["age"] = df.apply(lambda x: (x["session_date"] - x["date_of_birth"]).days, axis=1)

    df["column"] = df.apply(lambda x: int(x["column"].split(" ")[-1]), axis=1)
    df["volume"] = df.apply(lambda x: int(x["volume"].split(" ")[-1]), axis=1)

    df["golden_mouse"] = df["subject_id"] == V1DD_GOLDEN_MOUSE_SUBJECT_ID

    df = df[V1DD_COLUMN_ORDER]
    return df.reset_index(drop=True)


@registry.register_table(registry.NAMES["swdb_2025_v1dd"])
def swdb_2025_v1dd(force_update: bool = False) -> pd.DataFrame:
    """Return the SWDB 2025 V1 Deep Dive metadata table, building it on demand."""
    df = registry.BACKEND.read(registry.NAMES["swdb_2025_v1dd"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from database.")

    if df.empty or force_update:
        setup_logging()
        logging.info(
            CacheLogMessage(
                backend=registry.BACKEND.__class__.__name__,
                table=registry.NAMES["swdb_2025_v1dd"],
                message="Updating cache",
            ).to_json()
        )

        records = _fetch_v1dd_records()
        df = _build_v1dd_dataframe(records)

        registry.BACKEND.write(registry.NAMES["swdb_2025_v1dd"], df)

    return df


def swdb_2025_v1dd_columns() -> list[Column]:
    return [
        Column(name="project_name", description="Project name from data_description"),
        Column(name="_id", description="DocDB record id"),
        Column(name="name", description="Asset name"),
        Column(name="subject_id", description="Subject id"),
        Column(name="golden_mouse", description="Whether this is the reference 'golden' mouse"),
        Column(name="genotype", description="Subject genotype"),
        Column(name="date_of_birth", description="Subject date of birth"),
        Column(name="sex", description="Subject sex"),
        Column(name="modality", description="Data modality names"),
        Column(name="session_date", description="Acquisition date"),
        Column(name="age", description="Subject age in days at acquisition"),
        Column(name="session_time", description="Acquisition start time of day"),
        Column(name="column", description="V1DD column index"),
        Column(name="volume", description="V1DD volume index"),
    ]


# ---------------------------------------------------------------------------
# SWDB 2026 public Code Ocean collection membership
# ---------------------------------------------------------------------------

# This is the public SWDB 2026 collection, not a DocDB project query.  These
# rows are deliberately kept as cache inputs so the dashboard reflects the
# public collection even when a data asset has no ordinary metadata-index row.
SWDB_2026_COLLECTION_ID = "23ffb03b-1f54-4369-95b2-a8cea1a9ab33"
SWDB_2026_COLLECTION_URL = (
    f"https://codeocean.allenneuraldynamics.org/collections/{SWDB_2026_COLLECTION_ID}"
)

SWDB_2026_PUBLIC_COLLECTIONS = {
    "swdb_2026_bci": [
        {"name": "brain-computer-interface-v2", "data_asset_id": "4bf7c961-8bf8-42a9-8221-c0185e41d2c5", "prefix": "brain-computer-interface-v2"},
    ],
    "swdb_2026_v1dd": [
        {"name": "409828_V1DD_Filtered", "data_asset_id": "2e02e089-3c5d-4de6-aa79-6d4723ebb663", "prefix": "409828_V1DD_Filtered"},
        {"name": "427836_V1DD_Filtered", "data_asset_id": "bc489ad7-96d0-4292-a0f1-3d1ba8bac057", "prefix": "427836_V1DD_Filtered"},
        {"name": "438833_V1DD_Filtered", "data_asset_id": "476412fd-5312-4d5e-ad29-64025b0bc247", "prefix": "438833_V1DD_Filtered"},
        {"name": "416296_V1DD_Filtered", "data_asset_id": "8ece62d1-3228-4d4d-a842-0df83577f819", "prefix": "416296_V1DD_Filtered"},
    ],
    "swdb_2026_visual_learning": [
        {"name": "Visual-Learning-SWDB", "data_asset_id": "6efb73c3-ebb9-432d-a225-203759356027", "prefix": "Visual-Learning-SWDB"},
    ],
    "swdb_2026_visual_coding_neuropixels": [
        {"name": "visual_coding_neuropixels", "data_asset_id": "caa3ab37-9fda-460d-8cce-0a43bff6cd7b", "prefix": "visual_coding_neuropixels"},
    ],
    "swdb_2026_visual_coding_ophys": [
        {"name": "visual_coding_ophys", "data_asset_id": "17494011-7e38-48ee-931b-6afa364131f3", "prefix": "visual_coding_ophys"},
    ],
    "swdb_2026_dynamic_routing": [
        {"name": "swdb_dynamicrouting_datacube", "data_asset_id": "a0a476ca-a855-4c18-a363-c2dcba4d8478", "prefix": "swdb_dynamicrouting_datacube"},
    ],
    "swdb_2026_neuropixels_opto": [
        {"name": "Neuropixels_Opto_ecephys_nwb_combined", "data_asset_id": "7469cd5a-046d-4c94-8234-8a6a1c83b73d", "prefix": "Neuropixels_Opto_ecephys_nwb_combined"},
    ],
}

SWDB_2026_COLUMN_ORDER = [
    "collection_id",
    "collection_url",
    "data_asset_id",
    "collection_asset_name",
    "name",
]


def _build_swdb_2026_public_table(table_name: str, force_update: bool = False) -> pd.DataFrame:
    """Build one table from the assets inside the public Code Ocean collections."""
    df = registry.BACKEND.read(registry.NAMES[table_name])
    if df.empty or force_update:
        derived_assets = {
            collection["name"]: SWDB_2026_DERIVED_ASSETS_BY_COLLECTION.get(collection["name"], [])
            for collection in SWDB_2026_PUBLIC_COLLECTIONS[table_name]
        }
        rows = [
            {
                "collection_id": SWDB_2026_COLLECTION_ID,
                "collection_url": SWDB_2026_COLLECTION_URL,
                "data_asset_id": collection["data_asset_id"],
                "collection_asset_name": collection["name"],
                "name": name,
            }
            for collection in SWDB_2026_PUBLIC_COLLECTIONS[table_name]
            for name in derived_assets[collection["name"]]
        ]
        if not rows:
            raise RuntimeError(f"No derived assets found in public SWDB collection table {table_name}")
        df = pd.DataFrame(rows, columns=SWDB_2026_COLUMN_ORDER)
        registry.BACKEND.write(registry.NAMES[table_name], df)
    return df


def _swdb_2026_columns() -> list[Column]:
    return [
        Column(name="collection_id", description="Public Code Ocean collection ID"),
        Column(name="collection_url", description="Public Code Ocean collection URL"),
        Column(name="data_asset_id", description="Code Ocean data asset ID containing the derived assets"),
        Column(name="collection_asset_name", description="Public Code Ocean data asset name containing the derived assets"),
        Column(name="name", description="Derived asset name inside the public data asset"),
    ]


def swdb_2026_bci(force_update: bool = False) -> pd.DataFrame:
    return _build_swdb_2026_public_table("swdb_2026_bci", force_update)


def swdb_2026_v1dd(force_update: bool = False) -> pd.DataFrame:
    return _build_swdb_2026_public_table("swdb_2026_v1dd", force_update)


def swdb_2026_visual_learning(force_update: bool = False) -> pd.DataFrame:
    return _build_swdb_2026_public_table("swdb_2026_visual_learning", force_update)


def swdb_2026_visual_coding_neuropixels(force_update: bool = False) -> pd.DataFrame:
    return _build_swdb_2026_public_table("swdb_2026_visual_coding_neuropixels", force_update)


def swdb_2026_visual_coding_ophys(force_update: bool = False) -> pd.DataFrame:
    return _build_swdb_2026_public_table("swdb_2026_visual_coding_ophys", force_update)


def swdb_2026_dynamic_routing(force_update: bool = False) -> pd.DataFrame:
    return _build_swdb_2026_public_table("swdb_2026_dynamic_routing", force_update)


def swdb_2026_neuropixels_opto(force_update: bool = False) -> pd.DataFrame:
    return _build_swdb_2026_public_table("swdb_2026_neuropixels_opto", force_update)


for _swdb_2026_name, _swdb_2026_builder in {
    "swdb_2026_bci": swdb_2026_bci,
    "swdb_2026_v1dd": swdb_2026_v1dd,
    "swdb_2026_visual_learning": swdb_2026_visual_learning,
    "swdb_2026_visual_coding_neuropixels": swdb_2026_visual_coding_neuropixels,
    "swdb_2026_visual_coding_ophys": swdb_2026_visual_coding_ophys,
    "swdb_2026_dynamic_routing": swdb_2026_dynamic_routing,
    "swdb_2026_neuropixels_opto": swdb_2026_neuropixels_opto,
}.items():
    registry.register_table(registry.NAMES[_swdb_2026_name])(_swdb_2026_builder)


def swdb_2026_bci_columns() -> list[Column]:
    return _swdb_2026_columns()


def swdb_2026_v1dd_columns() -> list[Column]:
    return _swdb_2026_columns()


def swdb_2026_visual_learning_columns() -> list[Column]:
    return _swdb_2026_columns()


def swdb_2026_visual_coding_neuropixels_columns() -> list[Column]:
    return _swdb_2026_columns()


def swdb_2026_visual_coding_ophys_columns() -> list[Column]:
    return _swdb_2026_columns()


def swdb_2026_dynamic_routing_columns() -> list[Column]:
    return _swdb_2026_columns()


def swdb_2026_neuropixels_opto_columns() -> list[Column]:
    return _swdb_2026_columns()

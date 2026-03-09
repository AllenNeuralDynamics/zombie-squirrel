"""Synchronization utilities for updating all cached data."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from .acorn_helpers.asset_basics import asset_basics_columns
from .acorn_helpers.behavior_trials import behavior_trials_columns
from .acorn_helpers.qc import qc_columns
from .acorn_helpers.source_data import source_data_columns
from .acorn_helpers.unique_project_names import unique_project_names_columns
from .acorn_helpers.unique_subject_ids import unique_subject_ids_columns
from .acorns import ACORN_REGISTRY, NAMES, TREE
from .squirrel import Acorn, AcornType, Squirrel


def publish_squirrel_metadata() -> None:
    """Build and publish a Squirrel metadata JSON to the cache root.

    Collects column and location information for all registered acorns,
    constructs a Squirrel model, and writes it as JSON via the active Tree.
    """
    acorn_list = [
        Acorn(
            name=NAMES["upn"],
            description="Unique project names across all assets",
            location=TREE.get_location(NAMES["upn"]),
            partitioned=False,
            type=AcornType.metadata,
            columns=unique_project_names_columns(),
        ),
        Acorn(
            name=NAMES["usi"],
            description="Unique subject_ids across all assets",
            location=TREE.get_location(NAMES["usi"]),
            partitioned=False,
            type=AcornType.metadata,
            columns=unique_subject_ids_columns(),
        ),
        Acorn(
            name=NAMES["basics"],
            description="Commonly used asset metadata, one row per data asset",
            location=TREE.get_location(NAMES["basics"]),
            partitioned=False,
            type=AcornType.metadata,
            columns=asset_basics_columns(),
        ),
        Acorn(
            name=NAMES["d2r"],
            description="Mapping from derived asset names to their source raw asset names",
            location=TREE.get_location(NAMES["d2r"]),
            partitioned=False,
            type=AcornType.metadata,
            columns=source_data_columns(),
        ),
        Acorn(
            name=NAMES["r2d"],
            description="Mapping from raw asset names to their derived asset names",
            location=TREE.get_location(NAMES["r2d"]),
            partitioned=False,
            type=AcornType.metadata,
        ),
        Acorn(
            name=NAMES["qc"],
            description="Quality control table with one row per QC metric, partitioned by subject_id",
            location=TREE.get_location("qc", partitioned=True),
            partitioned=True,
            partition_key="subject_id",
            type=AcornType.asset,
            columns=qc_columns(),
        ),
        Acorn(
            name=NAMES["behavior"],
            description="NWB trials table for derived behavior assets, partitioned by subject_id",
            location=TREE.get_location("behavior_trials", partitioned=True),
            partitioned=True,
            partition_key="subject_id",
            type=AcornType.asset,
            columns=behavior_trials_columns(),
        ),
    ]
    squirrel = Squirrel(acorns=acorn_list)
    TREE.plant("squirrel.json", squirrel.model_dump_json())


def hide_acorns():
    """Trigger force update of all registered acorn functions.

    Updates each acorn individually. For the QC and behavior acorns, fetches
    unique subject IDs from asset_basics and updates each individually,
    using parallelization when multiple subjects are available.
    After behavior trials are synced, the asset_basics cache is updated with
    the acorn:behavior column. After all updates, publishes Squirrel metadata
    JSON to the cache root.
    """
    ACORN_REGISTRY[NAMES["upn"]](force_update=True)
    ACORN_REGISTRY[NAMES["usi"]](force_update=True)

    df_basics = ACORN_REGISTRY[NAMES["basics"]](force_update=True)

    ACORN_REGISTRY[NAMES["d2r"]](force_update=True)

    subject_ids = df_basics["subject_id"].dropna().unique()

    if len(subject_ids) > 0:
        qc_acorn = ACORN_REGISTRY[NAMES["qc"]]
        try:
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(qc_acorn, subject_id=subject_id, force_update=True) for subject_id in subject_ids
                ]
                for future in as_completed(futures):
                    future.result()
        except Exception:
            for subject_id in subject_ids:
                qc_acorn(subject_id=subject_id, force_update=True)

    behavior_subject_ids: list = []
    if "data_level" in df_basics.columns and "modalities" in df_basics.columns:
        behavior_assets = df_basics[
            (df_basics["data_level"] == "derived")
            & df_basics["modalities"].str.contains("behavior", na=False)
        ]
        behavior_subject_ids = list(behavior_assets["subject_id"].dropna().unique())

    if behavior_subject_ids:
        behavior_acorn = ACORN_REGISTRY[NAMES["behavior"]]
        for subject_id in behavior_subject_ids:
            behavior_acorn(subject_id=subject_id, force_update=True)

        cached_basics = TREE.scurry(NAMES["basics"])
        behavior_index = TREE.scurry(NAMES["behavior_index"])
        if not behavior_index.empty and not cached_basics.empty:
            behavior_set = set(behavior_index[behavior_index["has_behavior"] == True]["asset_name"])  # noqa: E712
            cached_basics["acorn:behavior"] = cached_basics["name"].isin(behavior_set)
            TREE.hide(NAMES["basics"], cached_basics)

    publish_squirrel_metadata()

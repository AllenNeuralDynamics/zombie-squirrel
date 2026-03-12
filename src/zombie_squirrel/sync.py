"""Synchronization utilities for updating all cached data."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from .acorn_helpers.asset_basics import asset_basics_columns
from .acorn_helpers.assets_smartspim import assets_smartspim_columns
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
            name=NAMES["smartspim"],
            location=TREE.get_location(NAMES["smartspim"]),
            partitioned=False,
            type=AcornType.metadata,
            columns=assets_smartspim_columns(),
        ),
    ]
    squirrel = Squirrel(acorns=acorn_list)
    TREE.plant("squirrel.json", squirrel.model_dump_json())


def hide_acorns():
    """Trigger force update of all registered acorn functions.

    Updates each acorn individually. For the QC acorn, fetches
    unique subject IDs from asset_basics and updates each individually,
    using parallelization when multiple subjects are available.
    After all updates, publishes Squirrel metadata JSON to the cache root.
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

    ACORN_REGISTRY[NAMES["smartspim"]](force_update=True)

    publish_squirrel_metadata()

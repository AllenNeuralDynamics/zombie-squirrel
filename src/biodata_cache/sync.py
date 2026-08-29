"""Synchronization utilities for updating cached data.

Each cache table (or logical group of tables) is built by an independent *sync
job*. Jobs are dispatched by name so that a single Code Ocean capsule image can
be cloned once per job and selected at run time through the
``BIODATA_CACHE_SYNC_JOB`` environment variable. This lets the jobs run as
separate capsules in a Nextflow pipeline: ``asset_basics`` first (it builds
``asset_basics`` and ``source_data``, the prerequisites for every other job), then
all remaining jobs in parallel.

Every job writes its own per-table registry fragment (``cache_registry/<name>.json``)
as soon as it finishes, rather than one process writing the whole registry at the
end. Parallel jobs therefore never contend on a single JSON object. The
``asset_basics`` job additionally resets the fragment directory and registers the
version folder in ``cache_versions.json`` before any other job runs.

See ``PIPELINE.md`` for the capsule/pipeline layout and the version-bump procedure.
"""

import logging
import os
from collections.abc import Callable

from .cache_table_helpers.asset_basics import asset_basics_columns
from .cache_table_helpers.behavior_curriculum import behavior_curriculum_columns
from .cache_table_helpers.metadata_upgrade import metadata_upgrade_columns
from .cache_table_helpers.platform_df import (
    platform_dynamic_foraging_events_columns,
    platform_dynamic_foraging_sessions_columns,
    platform_dynamic_foraging_trials_columns,
)
from .cache_table_helpers.platform_df_operations import platform_df_operations_columns
from .cache_table_helpers.platform_ecephys_spikes import platform_ecephys_spikes_columns
from .cache_table_helpers.platform_ecephys_units import platform_ecephys_units_columns
from .cache_table_helpers.platform_exaspim import platform_exaspim_columns
from .cache_table_helpers.platform_fib import platform_fib_columns
from .cache_table_helpers.platform_fib_operations import (
    platform_fib_operations_columns,
)
from .cache_table_helpers.platform_fib_traces import platform_fib_traces_columns
from .cache_table_helpers.platform_mouselight import platform_mouselight_columns
from .cache_table_helpers.platform_pophys import platform_pophys_columns
from .cache_table_helpers.platform_qc import PLATFORMS, platform_qc_columns
from .cache_table_helpers.platform_smartspim import assets_smartspim_columns
from .cache_table_helpers.platform_swdb import (
    swdb_2025_bci_columns,
    swdb_2025_v1dd_columns,
    swdb_2026_bci_columns,
    swdb_2026_dynamic_routing_columns,
    swdb_2026_neuropixels_opto_columns,
    swdb_2026_v1dd_columns,
    swdb_2026_visual_coding_neuropixels_columns,
    swdb_2026_visual_coding_ophys_columns,
    swdb_2026_visual_learning_columns,
)
from .cache_table_helpers.platform_swdb_dr_switch import (
    platform_swdb_dr_switch_columns,
    platform_swdb_dr_switch_markers_columns,
)
from .cache_table_helpers.platform_video_frame_times import platform_video_frame_times_columns
from .cache_table_helpers.platform_visual_coding_neuropixels_units import (
    platform_visual_coding_neuropixels_units_columns,
)
from .cache_table_helpers.platform_visual_coding_ophys import platform_visual_coding_ophys_columns
from .cache_table_helpers.platform_visual_learning import (
    platform_visual_learning_cell_gene_columns,
    platform_visual_learning_coreg_columns,
)
from .cache_table_helpers.qc import qc_columns
from .cache_table_helpers.shared.cloudwatch_utils import build_all_operations
from .cache_table_helpers.source_data import source_data_columns
from .cache_table_helpers.storage_lens import storage_lens_columns
from .cache_table_helpers.time_to_qc import time_to_qc_columns
from .cache_table_helpers.unique_genotypes import unique_genotypes_columns
from .cache_table_helpers.unique_project_names import unique_project_names_columns
from .cache_table_helpers.unique_subject_ids import unique_subject_ids_columns
from .models import CacheTable, CacheTableType
from .registry import BACKEND, NAMES, TABLE_REGISTRY

# Environment variable read by a capsule to decide which job it runs.
SYNC_JOB_ENV = "BIODATA_CACHE_SYNC_JOB"


# --- Registry fragment builders ----------------------------------------------
#
# One builder per cache table, keyed by the table's registry name (== CacheTable.name
# == the fragment filename). A job publishes only the fragments for the tables it
# builds, so the set of jobs collectively covers every entry below.


def _entry_builders() -> dict[str, Callable[[], CacheTable]]:
    """Return the per-table registry-entry factories, keyed by table name."""
    return {
        NAMES["upn"]: lambda: CacheTable(
            name=NAMES["upn"],
            description="Unique project names across all assets",
            location=BACKEND.get_location(NAMES["upn"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=unique_project_names_columns(),
        ),
        NAMES["usi"]: lambda: CacheTable(
            name=NAMES["usi"],
            description="Unique subject_ids across all assets",
            location=BACKEND.get_location(NAMES["usi"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=unique_subject_ids_columns(),
        ),
        NAMES["ugt"]: lambda: CacheTable(
            name=NAMES["ugt"],
            description="Unique genotypes across all assets where subject.subject_details.genotype is present",
            location=BACKEND.get_location(NAMES["ugt"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=unique_genotypes_columns(),
        ),
        NAMES["basics"]: lambda: CacheTable(
            name=NAMES["basics"],
            description="Commonly used asset metadata, one row per data asset",
            location=BACKEND.get_location(NAMES["basics"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=asset_basics_columns(),
        ),
        NAMES["d2r"]: lambda: CacheTable(
            name=NAMES["d2r"],
            description="Mapping from derived asset names to their source raw asset names",
            location=BACKEND.get_location(NAMES["d2r"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=source_data_columns(),
        ),
        NAMES["qc"]: lambda: CacheTable(
            name=NAMES["qc"],
            description="Quality control table with one row per QC metric, partitioned by subject_id",
            location=BACKEND.get_location("qc", partitioned=True),
            partitioned=True,
            partition_key="subject_id",
            type=CacheTableType.asset,
            columns=qc_columns(),
        ),
        NAMES["smartspim"]: lambda: CacheTable(
            name=NAMES["smartspim"],
            description="SmartSPIM assets including processing status and neuroglancer links",
            location=BACKEND.get_location(NAMES["smartspim"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=assets_smartspim_columns(),
        ),
        NAMES["exaspim"]: lambda: CacheTable(
            name=NAMES["exaspim"],
            description="ExaSPIM assets including processing status and neuroglancer links",
            location=BACKEND.get_location(NAMES["exaspim"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=platform_exaspim_columns(),
        ),
        NAMES["upgrade"]: lambda: CacheTable(
            name=NAMES["upgrade"],
            description="Metadata upgrade status for each asset across versions",
            location=BACKEND.get_location(NAMES["upgrade"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=metadata_upgrade_columns(),
        ),
        NAMES["fib"]: lambda: CacheTable(
            name=NAMES["fib"],
            description="Fiber photometry assets with per-fiber targeted structure and intended channel measurement",
            location=BACKEND.get_location(NAMES["fib"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=platform_fib_columns(),
        ),
        NAMES["fib_traces"]: lambda: CacheTable(
            name=NAMES["fib_traces"],
            description="Processed fiber photometry dF/F traces (one row per sample), partitioned by asset_name",
            location=BACKEND.get_location("platform_fib_traces", partitioned=True),
            partitioned=True,
            partition_key="asset_name",
            type=CacheTableType.platform,
            columns=platform_fib_traces_columns(),
        ),
        NAMES["fib_operations"]: lambda: CacheTable(
            name=NAMES["fib_operations"],
            description="Fiber photometry pipeline processing events (one row per lifecycle event), partitioned by asset_name",
            location=BACKEND.get_location("platform_fib_operations", partitioned=True),
            partitioned=True,
            partition_key="asset_name",
            type=CacheTableType.platform,
            columns=platform_fib_operations_columns(),
        ),
        NAMES["df_operations"]: lambda: CacheTable(
            name=NAMES["df_operations"],
            description="Dynamic foraging pipeline processing events (one row per lifecycle event), partitioned by asset_name",
            location=BACKEND.get_location("platform_df_operations", partitioned=True),
            partitioned=True,
            partition_key="asset_name",
            type=CacheTableType.platform,
            columns=platform_df_operations_columns(),
        ),
        NAMES["ecephys_spikes"]: lambda: CacheTable(
            name=NAMES["ecephys_spikes"],
            description="Sorted ecephys spike times (one row per spike), partitioned by asset_name",
            location=BACKEND.get_location("platform_ecephys_spikes", partitioned=True),
            partitioned=True,
            partition_key="asset_name",
            type=CacheTableType.platform,
            columns=platform_ecephys_spikes_columns(),
        ),
        NAMES["ecephys_units"]: lambda: CacheTable(
            name=NAMES["ecephys_units"],
            description="Sorted ecephys units with quality/waveform metrics (one row per unit), partitioned by asset_name",
            location=BACKEND.get_location("platform_ecephys_units", partitioned=True),
            partitioned=True,
            partition_key="asset_name",
            type=CacheTableType.platform,
            columns=platform_ecephys_units_columns(),
        ),
        NAMES["pophys"]: lambda: CacheTable(
            name=NAMES["pophys"],
            description="Population physiology (multiplane-ophys) ROI contours and metadata (one row per ROI), partitioned by asset_name",
            location=BACKEND.get_location("platform_pophys", partitioned=True),
            partitioned=True,
            partition_key="asset_name",
            type=CacheTableType.platform,
            columns=platform_pophys_columns(),
        ),
        NAMES["visual_coding_ophys"]: lambda: CacheTable(
            name=NAMES["visual_coding_ophys"],
            description="Visual Coding Ophys sparse ROI contours and projection metadata, partitioned by asset_name",
            location=BACKEND.get_location("platform_visual_coding_ophys", partitioned=True),
            partitioned=True,
            partition_key="asset_name",
            type=CacheTableType.platform,
            columns=platform_visual_coding_ophys_columns(),
        ),
        NAMES["visual_learning_cell_gene"]: lambda: CacheTable(
            name=NAMES["visual_learning_cell_gene"],
            description="Visual Learning HCR cell-by-gene counts and annotated cell types, partitioned by subject_id",
            location=BACKEND.get_location("platform_visual_learning_cell_gene", partitioned=True),
            partitioned=True,
            partition_key="subject_id",
            type=CacheTableType.platform,
            columns=platform_visual_learning_cell_gene_columns(),
        ),
        NAMES["visual_learning_coreg"]: lambda: CacheTable(
            name=NAMES["visual_learning_coreg"],
            description="Visual Learning imaging ROI to HCR cell co-registration, partitioned by subject_id",
            location=BACKEND.get_location("platform_visual_learning_coreg", partitioned=True),
            partitioned=True,
            partition_key="subject_id",
            type=CacheTableType.platform,
            columns=platform_visual_learning_coreg_columns(),
        ),
        NAMES["swdb_2025_bci"]: lambda: CacheTable(
            name=NAMES["swdb_2025_bci"],
            description="SWDB 2025 BCI single-neuron-stim session metadata (one row per curated derived asset)",
            location=BACKEND.get_location(NAMES["swdb_2025_bci"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=swdb_2025_bci_columns(),
        ),
        NAMES["swdb_2025_v1dd"]: lambda: CacheTable(
            name=NAMES["swdb_2025_v1dd"],
            description="SWDB 2025 V1 Deep Dive metadata (one row per asset)",
            location=BACKEND.get_location(NAMES["swdb_2025_v1dd"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=swdb_2025_v1dd_columns(),
        ),
        NAMES["swdb_2026_bci"]: lambda: CacheTable(
            name=NAMES["swdb_2026_bci"],
            description="SWDB 2026 public Code Ocean collection BCI data asset membership",
            location=BACKEND.get_location(NAMES["swdb_2026_bci"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=swdb_2026_bci_columns(),
        ),
        NAMES["swdb_2026_v1dd"]: lambda: CacheTable(
            name=NAMES["swdb_2026_v1dd"],
            description="SWDB 2026 public Code Ocean collection V1DD data asset membership",
            location=BACKEND.get_location(NAMES["swdb_2026_v1dd"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=swdb_2026_v1dd_columns(),
        ),
        NAMES["swdb_2026_visual_learning"]: lambda: CacheTable(
            name=NAMES["swdb_2026_visual_learning"],
            description="SWDB 2026 public Code Ocean collection Visual Learning data asset membership",
            location=BACKEND.get_location(NAMES["swdb_2026_visual_learning"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=swdb_2026_visual_learning_columns(),
        ),
        NAMES["swdb_2026_visual_coding_neuropixels"]: lambda: CacheTable(
            name=NAMES["swdb_2026_visual_coding_neuropixels"],
            description="SWDB 2026 public Code Ocean collection Visual Coding Neuropixels membership",
            location=BACKEND.get_location(NAMES["swdb_2026_visual_coding_neuropixels"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=swdb_2026_visual_coding_neuropixels_columns(),
        ),
        NAMES["swdb_2026_visual_coding_ophys"]: lambda: CacheTable(
            name=NAMES["swdb_2026_visual_coding_ophys"],
            description="SWDB 2026 public Code Ocean collection Visual Coding Ophys membership",
            location=BACKEND.get_location(NAMES["swdb_2026_visual_coding_ophys"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=swdb_2026_visual_coding_ophys_columns(),
        ),
        NAMES["swdb_2026_dynamic_routing"]: lambda: CacheTable(
            name=NAMES["swdb_2026_dynamic_routing"],
            description="SWDB 2026 public Code Ocean collection Dynamic Routing membership",
            location=BACKEND.get_location(NAMES["swdb_2026_dynamic_routing"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=swdb_2026_dynamic_routing_columns(),
        ),
        NAMES["swdb_2026_neuropixels_opto"]: lambda: CacheTable(
            name=NAMES["swdb_2026_neuropixels_opto"],
            description="SWDB 2026 public Code Ocean collection Neuropixels Opto membership",
            location=BACKEND.get_location(NAMES["swdb_2026_neuropixels_opto"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=swdb_2026_neuropixels_opto_columns(),
        ),
        NAMES["visual_coding_neuropixels_units"]: lambda: CacheTable(
            name=NAMES["visual_coding_neuropixels_units"],
            description="CCF unit locations for every Visual Coding Neuropixels session (one small unpartitioned table for the SWDB neuron-locations overview)",
            location=BACKEND.get_location(NAMES["visual_coding_neuropixels_units"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=platform_visual_coding_neuropixels_units_columns(),
        ),
        NAMES["swdb_dr_switch"]: lambda: CacheTable(
            name=NAMES["swdb_dr_switch"],
            description="Dynamic Routing block-switch firing rate per QC-passing unit, averaged across every switch of each direction (one small unpartitioned table for the SWDB neuron-locations replay)",
            location=BACKEND.get_location(NAMES["swdb_dr_switch"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=platform_swdb_dr_switch_columns(),
        ),
        NAMES["swdb_dr_switch_markers"]: lambda: CacheTable(
            name=NAMES["swdb_dr_switch_markers"],
            description="Representative trial-boundary times per Dynamic Routing block-switch direction (one small unpartitioned table for the SWDB neuron-locations replay's trial axis)",
            location=BACKEND.get_location(NAMES["swdb_dr_switch_markers"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=platform_swdb_dr_switch_markers_columns(),
        ),
        NAMES["video_frame_times"]: lambda: CacheTable(
            name=NAMES["video_frame_times"],
            description="Behavior-camera per-frame times from the camstim NI-DAQ sync file (one row per camera frame), partitioned by raw asset_name",
            location=BACKEND.get_location(NAMES["video_frame_times"], partitioned=True),
            partitioned=True,
            partition_key="asset_name",
            type=CacheTableType.platform,
            columns=platform_video_frame_times_columns(),
        ),
        NAMES["df_sessions"]: lambda: CacheTable(
            name=NAMES["df_sessions"],
            description="Dynamic foraging session table (one row per session); mirrors upstream aind-dynamic-foraging-database",
            location=BACKEND.get_location(NAMES["df_sessions"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=platform_dynamic_foraging_sessions_columns(),
        ),
        NAMES["df_trials"]: lambda: CacheTable(
            name=NAMES["df_trials"],
            description="Dynamic foraging trial table (one row per trial), partitioned by subject_id; mirrors upstream aind-dynamic-foraging-database",
            location=BACKEND.get_location(NAMES["df_trials"], partitioned=True),
            partitioned=True,
            partition_key="subject_id",
            type=CacheTableType.platform,
            columns=platform_dynamic_foraging_trials_columns(),
        ),
        NAMES["df_events"]: lambda: CacheTable(
            name=NAMES["df_events"],
            description="Dynamic foraging event table (one row per behavioral event), partitioned by subject_id; mirrors upstream aind-dynamic-foraging-database",
            location=BACKEND.get_location(NAMES["df_events"], partitioned=True),
            partitioned=True,
            partition_key="subject_id",
            type=CacheTableType.platform,
            columns=platform_dynamic_foraging_events_columns(),
        ),
        NAMES["curriculum"]: lambda: CacheTable(
            name=NAMES["curriculum"],
            description="Behavior assets with curriculum name and stage from trainer_state.json",
            location=BACKEND.get_location(NAMES["curriculum"]),
            partitioned=False,
            type=CacheTableType.asset,
            columns=behavior_curriculum_columns(),
        ),
        NAMES["platform_qc"]: lambda: CacheTable(
            name=NAMES["platform_qc"],
            description="Tag-level QC statuses per platform, one row per asset/tag combination",
            location=BACKEND.get_location("platform_qc", partitioned=True),
            partitioned=True,
            partition_key="platform",
            type=CacheTableType.platform,
            columns=platform_qc_columns(),
        ),
        NAMES["time_to_qc"]: lambda: CacheTable(
            name=NAMES["time_to_qc"],
            description="Time from processing completion to QC completion for derived assets",
            location=BACKEND.get_location(NAMES["time_to_qc"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=time_to_qc_columns(),
        ),
        NAMES["mouselight"]: lambda: CacheTable(
            name=NAMES["mouselight"],
            description="Janelia MouseLight neuron list (one row per neuron) with label, soma region and tracing UUIDs",
            location=BACKEND.get_location(NAMES["mouselight"]),
            partitioned=False,
            type=CacheTableType.platform,
            columns=platform_mouselight_columns(),
        ),
        NAMES["storage_lens"]: lambda: CacheTable(
            name=NAMES["storage_lens"],
            description="Weekly S3 Storage Lens report (one row per prefix/storage class), sourced from RDS",
            location=BACKEND.get_location(NAMES["storage_lens"]),
            partitioned=False,
            type=CacheTableType.metadata,
            columns=storage_lens_columns(),
        ),
    }


def publish_registry_fragment(name: str) -> None:
    """Build and publish the registry fragment for a single cache table.

    Overwrites the existing fragment if one is present (idempotent re-run).
    """
    entry = _entry_builders()[name]()
    BACKEND.put_registry_fragment(name, entry.model_dump_json())


def publish_cache_registry() -> None:
    """Publish a registry fragment for every registered cache table.

    Convenience for local/full runs; the pipeline instead has each job publish
    only its own fragment(s) as it completes.
    """
    for name in _entry_builders():
        publish_registry_fragment(name)


# --- Shared helpers -----------------------------------------------------------


def _load_basics():
    """Return the cached asset_basics DataFrame (built by the asset_basics job).

    In the pipeline every non-basics job reads asset_basics from the shared cache
    rather than recomputing it. Calling the helper without ``force_update`` returns
    the already-cached table.
    """
    return TABLE_REGISTRY[NAMES["basics"]]()


def _derived_asset_names(df_basics, modality_substr: str) -> list:
    """Return derived asset names whose modalities contain ``modality_substr``."""
    if "modalities" not in df_basics.columns or "data_level" not in df_basics.columns:
        return []
    mask = df_basics["modalities"].apply(
        lambda x: x is not None and not isinstance(x, float) and any(modality_substr in m.lower() for m in x)
    )
    return df_basics[mask & (df_basics["data_level"] == "derived")]["name"].dropna().unique().tolist()


def _location_map(df_basics) -> dict:
    """Return a mapping of asset name -> S3 location from asset_basics."""
    if "name" in df_basics.columns and "location" in df_basics.columns:
        return dict(zip(df_basics["name"], df_basics["location"], strict=False))
    return {}


# --- Sync jobs ----------------------------------------------------------------


def _job_asset_basics() -> None:
    """Build asset_basics (and source_data). Runs first: registers the version.

    ``source_data`` (the ``d2r`` table) is built here rather than in the parallel
    ``fast`` job because the ``smartspim`` and ``exaspim`` jobs read it from cache.
    If it were built in ``fast`` those jobs could race ``fast`` and join against a
    stale ``d2r`` — dropping any derived asset newer than the previous run (e.g. a
    freshly stitched SmartSPIM asset would appear as raw-only). Building it in the
    single upstream prerequisite job guarantees it exists before any parallel job.

    The registry is deliberately *not* cleared here. Each job overwrites its own
    fragment in place on success, so a job that fails (or has not yet run) keeps
    its previous fragment and its table stays visible in the registry. Clearing
    up front would make every not-yet-rebuilt table vanish mid-run, and a failed
    nightly job would drop a table entirely even though its parquet data is intact.
    A full reset is achieved by bumping the cache version (fresh version folder).
    """
    BACKEND.register_version()
    TABLE_REGISTRY[NAMES["basics"]](force_update=True)
    publish_registry_fragment(NAMES["basics"])
    TABLE_REGISTRY[NAMES["d2r"]](force_update=True)
    publish_registry_fragment(NAMES["d2r"])


def _job_fast() -> None:
    """Build all fast non-partitioned cache tables from DocDB and external databases."""
    for key in ("upn", "usi", "ugt", "upgrade", "fib", "mouselight"):
        TABLE_REGISTRY[NAMES[key]](force_update=True)
        publish_registry_fragment(NAMES[key])
    for platform in PLATFORMS:
        TABLE_REGISTRY[NAMES["platform_qc"]](platform=platform, force_update=True)
    publish_registry_fragment(NAMES["platform_qc"])


def _job_storage_lens() -> None:
    """Build the storage_lens table (gated by access to an internal server)."""
    TABLE_REGISTRY[NAMES["storage_lens"]](force_update=True)
    publish_registry_fragment(NAMES["storage_lens"])


def _job_qc() -> None:
    """Build the per-subject quality_control table sequentially."""
    df_basics = _load_basics()
    subject_ids = df_basics["subject_id"].dropna().unique() if "subject_id" in df_basics.columns else []
    qc_fn = TABLE_REGISTRY[NAMES["qc"]]
    for subject_id in subject_ids:
        qc_fn(subject_id=subject_id, force_update=True)
    publish_registry_fragment(NAMES["qc"])


def _job_smartspim() -> None:
    """Build the SmartSPIM platform table."""
    TABLE_REGISTRY[NAMES["smartspim"]](force_update=True)
    publish_registry_fragment(NAMES["smartspim"])


def _job_exaspim() -> None:
    """Build the ExaSPIM platform table."""
    TABLE_REGISTRY[NAMES["exaspim"]](force_update=True)
    publish_registry_fragment(NAMES["exaspim"])


def _job_df() -> None:
    """Build the dynamic foraging tables (sessions, then per-subject trials/events)."""
    df_sessions = TABLE_REGISTRY[NAMES["df_sessions"]](force_update=True)
    publish_registry_fragment(NAMES["df_sessions"])
    subject_ids = df_sessions["subject_id"].dropna().unique() if "subject_id" in df_sessions.columns else []
    trials_fn = TABLE_REGISTRY[NAMES["df_trials"]]
    events_fn = TABLE_REGISTRY[NAMES["df_events"]]
    for subject_id in subject_ids:
        trials_fn(subject_id=subject_id, force_update=True)
        events_fn(subject_id=subject_id, force_update=True)
    publish_registry_fragment(NAMES["df_trials"])
    publish_registry_fragment(NAMES["df_events"])


def _job_fib_traces() -> None:
    """Build fiber photometry traces for each derived fib asset sequentially."""
    df_basics = _load_basics()
    location_map = _location_map(df_basics)
    fib_traces_fn = TABLE_REGISTRY[NAMES["fib_traces"]]
    for asset_name in _derived_asset_names(df_basics, "fib"):
        if BACKEND.partition_exists(f"{NAMES['fib_traces']}/{asset_name}"):
            continue
        try:
            fib_traces_fn(asset_name=asset_name, location=location_map.get(asset_name), force_update=True)
        except Exception as exc:
            # Isolate per-asset failures (e.g. missing Zarr or malformed metadata)
            # so one unsupported derived asset cannot abort the whole job.
            logging.exception(f"fib_traces failed for asset {asset_name}: {exc}")
    publish_registry_fragment(NAMES["fib_traces"])


def _job_operations() -> None:
    """Build every ``platform_*_operations`` table from a single CloudWatch pull.

    One Logs Insights query fetches lifecycle events across all registered
    operations pipelines and routes them to each table, so the CloudWatch logs are
    pulled once and reused. The pull is incremental: it reads the shared
    ``operations`` last-scan sidecar and queries only events ingested since the
    previous run, appending them to each acquisition's partition (the first run does
    a full lookback scan). Older events stay cached from prior runs, so each window
    stays small and no full re-scan is needed.
    """
    build_all_operations()
    publish_registry_fragment(NAMES["fib_operations"])
    publish_registry_fragment(NAMES["df_operations"])


def _job_ecephys_spikes() -> None:
    """Build sorted ecephys spike times for each derived ecephys asset sequentially."""
    df_basics = _load_basics()
    location_map = _location_map(df_basics)
    spikes_fn = TABLE_REGISTRY[NAMES["ecephys_spikes"]]
    for asset_name in _derived_asset_names(df_basics, "ecephys"):
        if BACKEND.partition_exists(f"{NAMES['ecephys_spikes']}/{asset_name}"):
            continue
        try:
            spikes_fn(asset_name=asset_name, location=location_map.get(asset_name), force_update=True)
        except Exception as exc:
            # Isolate per-asset failures (e.g. corrupt source NWB) so one bad asset
            # cannot abort the whole job. Log the asset name for later follow-up.
            logging.exception(f"ecephys_spikes failed for asset {asset_name}: {exc}")
    publish_registry_fragment(NAMES["ecephys_spikes"])


def _job_ecephys_units() -> None:
    """Build sorted ecephys units for each derived ecephys asset sequentially."""
    df_basics = _load_basics()
    location_map = _location_map(df_basics)
    units_fn = TABLE_REGISTRY[NAMES["ecephys_units"]]
    for asset_name in _derived_asset_names(df_basics, "ecephys"):
        if BACKEND.partition_exists(f"{NAMES['ecephys_units']}/{asset_name}"):
            continue
        try:
            units_fn(asset_name=asset_name, location=location_map.get(asset_name), force_update=True)
        except Exception as exc:
            # Isolate per-asset failures (e.g. corrupt source NWB) so one bad asset
            # cannot abort the whole job. Log the asset name for later follow-up.
            logging.exception(f"ecephys_units failed for asset {asset_name}: {exc}")
    publish_registry_fragment(NAMES["ecephys_units"])


def _raw_name_map(names: list) -> dict:
    """Return a mapping of derived asset name -> source raw asset name from source_data."""
    df = TABLE_REGISTRY[NAMES["d2r"]]()
    if df.empty or "name" not in df.columns or "source_data" not in df.columns:
        return {}
    wanted = set(names)
    subset = df[df["name"].isin(wanted)]
    return dict(zip(subset["name"], subset["source_data"], strict=False))


def _job_pophys() -> None:
    """Build pophys ROI contours for every derived pophys asset sequentially.

    Asset names are not a reliable indication of whether an asset carries a
    processable NWB. The table helper probes the asset contents and returns an
    empty result for unsupported assets. If a derived child has no processable
    NWB of its own, its single source asset is tried as a compatibility fallback
    and the resulting partition is still keyed by the child name used by the
    viewer.
    """
    df_basics = _load_basics()
    location_map = _location_map(df_basics)
    # General population-ophys assets use the ``pophys`` abbreviation, while
    # BCI single-plane assets are registered as ``single-plane-ophys``. Both
    # expose the same dense image_mask table once their behavior-NWB root is
    # discovered by platform_pophys.
    asset_names = _derived_asset_names(df_basics, "pophys")
    seen = set(asset_names)
    asset_names.extend(
        asset_name for asset_name in _derived_asset_names(df_basics, "ophys")
        if asset_name not in seen
    )
    visual_coding_names = set(_visual_coding_ophys_asset_names(df_basics))
    asset_names = [asset_name for asset_name in asset_names if asset_name not in visual_coding_names]
    raw_map = _raw_name_map(asset_names)
    pophys_fn = TABLE_REGISTRY[NAMES["pophys"]]
    for asset_name in asset_names:
        cache_key = f"{NAMES['pophys']}/{asset_name}"
        if BACKEND.partition_exists(cache_key):
            continue
        try:
            pophys_fn(
                asset_name=asset_name,
                location=location_map.get(asset_name),
                raw_name=raw_map.get(asset_name),
                force_update=True,
            )
            if BACKEND.partition_exists(cache_key):
                continue

            source_name = raw_map.get(asset_name)
            source_location = location_map.get(source_name)
            if not source_name or not source_location or source_name == asset_name:
                continue

            # Some legacy derived children are HDF5 assets while their source is
            # the processable NWB-Zarr asset. Keep the partition keyed by the
            # child name because raw-to-derived resolution returns that name.
            logging.info(
                "pophys asset %s has no cache partition; retrying from source %s",
                asset_name,
                source_name,
            )
            pophys_fn(
                asset_name=asset_name,
                location=source_location,
                raw_name=source_name,
                force_update=True,
            )
        except Exception as exc:
            # Isolate per-asset failures (e.g. corrupt source NWB) so one bad asset
            # cannot abort the whole job. Log the asset name for later follow-up.
            logging.exception(f"pophys failed for asset {asset_name}: {exc}")
    publish_registry_fragment(NAMES["pophys"])


def _visual_coding_ophys_asset_names(df_basics) -> list:
    """Return canonical Visual Coding Ophys assets owned by the isolated job."""
    required = {"project_name", "modalities", "data_level", "name"}
    if not required.issubset(df_basics.columns):
        return []
    project = df_basics["project_name"].fillna("").astype(str).str.contains("Visual Coding Ophys", case=False)

    def has_ophys_modality(values) -> bool:
        if values is None or (isinstance(values, float) and values != values):
            return False
        values = [values] if isinstance(values, str) else values
        return any("pophys" in str(value).lower() or "ophys" in str(value).lower() for value in values)

    modality = df_basics["modalities"].apply(
        has_ophys_modality
    )
    return df_basics[project & modality & (df_basics["data_level"] == "derived")]["name"].dropna().unique().tolist()


def _job_visual_coding_ophys() -> None:
    """Build sparse ROI contours for canonical Visual Coding Ophys assets."""
    df_basics = _load_basics()
    location_map = _location_map(df_basics)
    asset_names = _visual_coding_ophys_asset_names(df_basics)
    cache_fn = TABLE_REGISTRY[NAMES["visual_coding_ophys"]]
    for asset_name in asset_names:
        if BACKEND.partition_exists(f"{NAMES['visual_coding_ophys']}/{asset_name}"):
            continue
        try:
            cache_fn(
                asset_name=asset_name,
                location=location_map.get(asset_name),
                force_update=True,
            )
        except Exception as exc:
            logging.exception("visual_coding_ophys failed for asset %s: %s", asset_name, exc)
    publish_registry_fragment(NAMES["visual_coding_ophys"])


def _job_visual_learning() -> None:
    """Build the public Visual Learning cell-gene and co-registration tables."""
    for key in ("visual_learning_cell_gene", "visual_learning_coreg"):
        TABLE_REGISTRY[NAMES[key]](force_update=True)
        publish_registry_fragment(NAMES[key])


def _raw_asset_names_with_modality(df_basics, modality_substr: str) -> list:
    """Return raw asset names whose modalities contain ``modality_substr``."""
    if "modalities" not in df_basics.columns or "data_level" not in df_basics.columns:
        return []
    mask = df_basics["modalities"].apply(
        lambda x: x is not None and not isinstance(x, float) and any(modality_substr in m.lower() for m in x)
    )
    return df_basics[mask & (df_basics["data_level"] == "raw")]["name"].dropna().unique().tolist()


def _job_video_frame_times() -> None:
    """Build behavior-camera frame times for each raw asset that has behavior videos.

    Iterates raw acquisitions carrying the ``behavior-videos`` modality. Assets without
    a camstim NI-DAQ sync file (Harp rigs: VR foraging, harp Dynamic Foraging) write an
    empty partition and are effectively skipped; the viewer falls back to a scalar
    offset for those.
    """
    df_basics = _load_basics()
    location_map = _location_map(df_basics)
    asset_names = _raw_asset_names_with_modality(df_basics, "behavior-videos")
    frame_times_fn = TABLE_REGISTRY[NAMES["video_frame_times"]]
    for asset_name in asset_names:
        if BACKEND.partition_exists(f"{NAMES['video_frame_times']}/{asset_name}"):
            continue
        try:
            frame_times_fn(
                asset_name=asset_name,
                location=location_map.get(asset_name),
                force_update=True,
            )
        except Exception as exc:
            # Isolate per-asset failures (e.g. a corrupt sync file) so one bad asset
            # cannot abort the whole job. Log the asset name for later follow-up.
            logging.exception(f"video_frame_times failed for asset {asset_name}: {exc}")
    publish_registry_fragment(NAMES["video_frame_times"])


def _job_curriculum() -> None:
    """Build the behavior curriculum table."""
    TABLE_REGISTRY[NAMES["curriculum"]](force_update=True)
    publish_registry_fragment(NAMES["curriculum"])


def _job_time_to_qc() -> None:
    """Build the time-to-QC table."""
    TABLE_REGISTRY[NAMES["time_to_qc"]](force_update=True)
    publish_registry_fragment(NAMES["time_to_qc"])


# Registry of sync jobs. asset_basics must run before any other job (it resets the
# registry, registers the version, and produces the table every other job reads).
JOBS: dict[str, Callable[[], None]] = {
    "asset_basics": _job_asset_basics,
    "fast": _job_fast,
    "storage_lens": _job_storage_lens,
    "qc": _job_qc,
    "smartspim": _job_smartspim,
    "exaspim": _job_exaspim,
    "df": _job_df,
    "fib_traces": _job_fib_traces,
    "operations": _job_operations,
    "ecephys_spikes": _job_ecephys_spikes,
    "ecephys_units": _job_ecephys_units,
    "pophys": _job_pophys,
    "visual_coding_ophys": _job_visual_coding_ophys,
    "visual_learning": _job_visual_learning,
    "video_frame_times": _job_video_frame_times,
    "curriculum": _job_curriculum,
    "time_to_qc": _job_time_to_qc,
}

# Jobs that may run in parallel once asset_basics has completed.
PARALLEL_JOBS = tuple(name for name in JOBS if name != "asset_basics")


def run_sync_job(job: str | None = None) -> None:
    """Run a single named sync job.

    Args:
        job: The job name. If None, read from the ``BIODATA_CACHE_SYNC_JOB``
            environment variable. This is how a Code Ocean capsule selects which
            table it builds.
    """
    job = job or os.getenv(SYNC_JOB_ENV)
    if not job:
        raise ValueError(
            f"No sync job specified. Pass job= or set the {SYNC_JOB_ENV} environment variable. "
            f"Valid jobs: {sorted(JOBS)}"
        )
    if job not in JOBS:
        raise ValueError(f"Unknown sync job '{job}'. Valid jobs: {sorted(JOBS)}")
    JOBS[job]()


def update_all_tables(fast: bool = True, slow: bool = True) -> None:
    """Run every sync job in one process (local / non-pipeline convenience).

    asset_basics always runs first. Fast cache tables (DocDB-only queries) and slow
    cache tables (per-subject or S3 data) can be toggled independently via the
    fast/slow flags. Each job publishes its own registry fragment as it completes.

    Args:
        fast: If True, run the grouped fast DocDB-only cache tables.
        slow: If True, run the slow per-subject/S3 cache tables.
    """
    run_sync_job("asset_basics")

    if fast:
        run_sync_job("fast")

    if slow:
        for job in (
            "storage_lens",
            "qc",
            "smartspim",
            "exaspim",
            "df",
            "fib_traces",
            "operations",
            "ecephys_spikes",
            "ecephys_units",
            "pophys",
            "visual_coding_ophys",
            "visual_learning",
            "video_frame_times",
            "curriculum",
            "time_to_qc",
        ):
            run_sync_job(job)

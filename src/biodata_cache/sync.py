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
``asset_basics`` job additionally registers the version folder in
``cache_versions.json`` before any other job runs.

See ``PIPELINE.md`` for the capsule/pipeline layout and the version-bump procedure.
"""

import logging
import os
from collections.abc import Callable

from .cache_table_helpers.cell_by_everything import build_cell_by_everything
from .cache_table_helpers.platform_qc import PLATFORMS
from .cache_table_helpers.shared.cloudwatch_utils import build_all_operations
from .models import CacheTable
from .registry import BACKEND, NAMES, TABLE_REGISTRY
from .table_specs import TABLE_SPECS, table_specs_for_job

# Environment variable read by a capsule to decide which job it runs.
SYNC_JOB_ENV = "BIODATA_CACHE_SYNC_JOB"


# --- Registry fragment builders ----------------------------------------------
#
# One builder per cache table, keyed by the table's registry name (== CacheTable.name
# == the fragment filename). A job publishes only the fragments for the tables it
# builds, so the set of jobs collectively covers every entry below.


def _entry_builders() -> dict[str, Callable[[], CacheTable]]:
    """Return registry-entry factories derived from the table specifications."""
    return {spec.name: (lambda spec=spec: spec.cache_table(BACKEND)) for spec in TABLE_SPECS}


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
    """Build all fast cache tables from DocDB and external databases."""
    for spec in table_specs_for_job("fast"):
        if spec.key == "platform_qc":
            continue
        TABLE_REGISTRY[spec.name](force_update=True)
        publish_registry_fragment(spec.name)
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
    asset_names.extend(asset_name for asset_name in _derived_asset_names(df_basics, "ophys") if asset_name not in seen)
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

    modality = df_basics["modalities"].apply(has_ophys_modality)
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


def _job_cell_by_everything() -> None:
    """Build and publish the tables that join cell data across assets."""
    build_cell_by_everything()
    for spec in table_specs_for_job("cell-by-everything"):
        publish_registry_fragment(spec.name)


def _job_curriculum() -> None:
    """Build the behavior curriculum table."""
    TABLE_REGISTRY[NAMES["curriculum"]](force_update=True)
    publish_registry_fragment(NAMES["curriculum"])


def _job_time_to_qc() -> None:
    """Build the time-to-QC table."""
    TABLE_REGISTRY[NAMES["time_to_qc"]](force_update=True)
    publish_registry_fragment(NAMES["time_to_qc"])


# Registry of sync jobs. asset_basics must run before any other job (it registers
# the version and produces the tables every other job reads).
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
    "cell-by-everything": _job_cell_by_everything,
    "video_frame_times": _job_video_frame_times,
    "curriculum": _job_curriculum,
    "time_to_qc": _job_time_to_qc,
}

# Jobs whose outputs feed cell-by-everything must finish before that final job.
CELL_BY_EVERYTHING_SOURCE_JOBS = ("ecephys_units", "pophys", "visual_learning")

# Jobs that may run in parallel once asset_basics has completed.
PARALLEL_JOBS = tuple(name for name in JOBS if name not in ("asset_basics", "cell-by-everything"))


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
            "cell-by-everything",
        ):
            run_sync_job(job)

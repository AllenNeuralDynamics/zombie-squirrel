"""Declarative metadata for published biodata-cache tables."""

from dataclasses import dataclass
from importlib import import_module

from .models import CacheTable, CacheTableType, Column


@dataclass(frozen=True, slots=True)
class TableSpec:
    """Describe one published table and how its registry entry is built."""

    key: str
    name: str
    description: str
    table_type: CacheTableType
    columns_factory: str
    partition_key: str | None = None
    storage_name: str | None = None
    sync_job: str | None = None
    lifecycle: str = "nightly"

    @property
    def partitioned(self) -> bool:
        """Return whether the table is stored in partitions."""
        return self.partition_key is not None

    def columns(self) -> list[Column]:
        """Build this table's column definitions."""
        module_name, function_name = self.columns_factory.rsplit(".", 1)
        factory = getattr(import_module(module_name), function_name)
        return factory()

    def cache_table(self, backend) -> CacheTable:
        """Build this table's published registry entry for ``backend``."""
        storage_name = self.storage_name or self.name
        if self.partitioned:
            location = backend.get_location(storage_name, partitioned=True)
        else:
            location = backend.get_location(storage_name)
        return CacheTable(
            name=self.name,
            description=self.description,
            location=location,
            partitioned=self.partitioned,
            partition_key=self.partition_key,
            type=self.table_type,
            columns=self.columns(),
        )


TABLE_SPECS = (
    TableSpec(
        key="upn",
        name="unique_project_names",
        description="Unique project names across all assets",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.unique_project_names.unique_project_names_columns",
        sync_job="fast",
    ),
    TableSpec(
        key="usi",
        name="unique_subject_ids",
        description="Unique subject_ids across all assets",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.unique_subject_ids.unique_subject_ids_columns",
        sync_job="fast",
    ),
    TableSpec(
        key="ugt",
        name="unique_genotypes",
        description="Unique genotypes across all assets where subject.subject_details.genotype is present",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.unique_genotypes.unique_genotypes_columns",
        sync_job="fast",
    ),
    TableSpec(
        key="basics",
        name="asset_basics",
        description="Commonly used asset metadata, one row per data asset",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.asset_basics.asset_basics_columns",
        sync_job="asset_basics",
    ),
    TableSpec(
        key="d2r",
        name="source_data",
        description="Mapping from derived asset names to their source raw asset names",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.source_data.source_data_columns",
        sync_job="asset_basics",
    ),
    TableSpec(
        key="core",
        name="metadata_core",
        description="Presence of core AIND metadata files for each asset",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.metadata_core.metadata_core_columns",
        sync_job="fast",
    ),
    TableSpec(
        key="qc",
        name="quality_control",
        description="Quality control table with one row per QC metric, partitioned by subject_id",
        table_type=CacheTableType.asset,
        columns_factory="biodata_cache.cache_table_helpers.qc.qc_columns",
        partition_key="subject_id",
        storage_name="qc",
        sync_job="qc",
    ),
    TableSpec(
        key="smartspim",
        name="platform_smartspim",
        description="SmartSPIM assets including processing status and neuroglancer links",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.platform_smartspim.assets_smartspim_columns",
        sync_job="smartspim",
    ),
    TableSpec(
        key="exaspim",
        name="platform_exaspim",
        description="ExaSPIM assets including processing status and neuroglancer links",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.platform_exaspim.platform_exaspim_columns",
        sync_job="exaspim",
    ),
    TableSpec(
        key="upgrade",
        name="metadata_upgrade",
        description="Metadata upgrade status for each asset across versions",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.metadata_upgrade.metadata_upgrade_columns",
        sync_job="fast",
    ),
    TableSpec(
        key="fib",
        name="platform_fib",
        description="Fiber photometry assets with per-fiber targeted structure and intended channel measurement",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.platform_fib.platform_fib_columns",
        sync_job="fast",
    ),
    TableSpec(
        key="fib_traces",
        name="platform_fib_traces",
        description="Processed fiber photometry dF/F traces (one row per sample), partitioned by asset_name",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_fib_traces.platform_fib_traces_columns",
        partition_key="asset_name",
        sync_job="fib_traces",
    ),
    TableSpec(
        key="fib_operations",
        name="platform_fib_operations",
        description="Fiber photometry pipeline processing events (one row per lifecycle event), partitioned by asset_name",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_fib_operations.platform_fib_operations_columns",
        partition_key="asset_name",
        sync_job="operations",
    ),
    TableSpec(
        key="df_operations",
        name="platform_df_operations",
        description="Dynamic foraging pipeline processing events (one row per lifecycle event), partitioned by asset_name",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_df_operations.platform_df_operations_columns",
        partition_key="asset_name",
        sync_job="operations",
    ),
    TableSpec(
        key="ecephys_spikes",
        name="platform_ecephys_spikes",
        description="Sorted ecephys spike times (one row per spike), partitioned by asset_name",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_ecephys_spikes.platform_ecephys_spikes_columns",
        partition_key="asset_name",
        sync_job="ecephys_spikes",
    ),
    TableSpec(
        key="ecephys_units",
        name="platform_ecephys_units",
        description="Sorted ecephys units with quality/waveform metrics (one row per unit), partitioned by asset_name",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_ecephys_units.platform_ecephys_units_columns",
        partition_key="asset_name",
        sync_job="ecephys_units",
    ),
    TableSpec(
        key="pophys",
        name="platform_pophys",
        description="Population physiology (multiplane-ophys) ROI contours and metadata (one row per ROI), partitioned by asset_name",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_pophys.platform_pophys_columns",
        partition_key="asset_name",
        sync_job="pophys",
    ),
    TableSpec(
        key="visual_coding_ophys",
        name="platform_visual_coding_ophys",
        description="Visual Coding Ophys sparse ROI contours and projection metadata, partitioned by asset_name",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_visual_coding_ophys.platform_visual_coding_ophys_columns",
        partition_key="asset_name",
        sync_job="visual_coding_ophys",
    ),
    TableSpec(
        key="visual_learning_cell_gene",
        name="platform_visual_learning_cell_gene",
        description="Visual Learning HCR cell-by-gene counts and annotated cell types, partitioned by subject_id",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_visual_learning.platform_visual_learning_cell_gene_columns",
        partition_key="subject_id",
        sync_job="visual_learning",
    ),
    TableSpec(
        key="visual_learning_coreg",
        name="platform_visual_learning_coreg",
        description="Visual Learning imaging ROI to HCR cell co-registration, partitioned by subject_id",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_visual_learning.platform_visual_learning_coreg_columns",
        partition_key="subject_id",
        sync_job="visual_learning",
    ),
    TableSpec(
        key="cell_index",
        name="cell_index",
        description=(
            "One row per cell across every data asset: identity and provenance only "
            "(asset, subject, acquisition, modality, probe/plane, source unit/ROI id)."
        ),
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.cell_by_everything.tables.cell_index_columns",
        sync_job="cell-by-everything",
    ),
    TableSpec(
        key="cell_properties",
        name="cell_properties",
        description=(
            "Per-cell properties keyed by cell_key, one partition per asset_name. "
            "The table is wide and sparse because each modality supplies different properties."
        ),
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.cell_by_everything.tables.cell_properties_columns",
        partition_key="asset_name",
        sync_job="cell-by-everything",
    ),
    TableSpec(
        key="cell_genes",
        name="cell_genes",
        description=(
            "Transcriptomic genotyping keyed by cell_key, one partition per subject_id. "
            "Gene panels are kept separate from the wide cell_properties table."
        ),
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.cell_by_everything.tables.cell_genes_columns",
        partition_key="subject_id",
        sync_job="cell-by-everything",
    ),
    TableSpec(
        key="swdb_2025_bci",
        name="swdb_2025_bci",
        description="SWDB 2025 BCI single-neuron-stim session metadata (one row per curated derived asset)",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.public_collections.swdb_2025_bci_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="swdb_2025_v1dd",
        name="swdb_2025_v1dd",
        description="SWDB 2025 V1 Deep Dive metadata (one row per asset)",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.public_collections.swdb_2025_v1dd_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="swdb_2026_bci",
        name="swdb_2026_bci",
        description="SWDB 2026 public Code Ocean collection BCI data asset membership",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.public_collections.swdb_2026_bci_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="swdb_2026_v1dd",
        name="swdb_2026_v1dd",
        description="SWDB 2026 public Code Ocean collection V1DD data asset membership",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.public_collections.swdb_2026_v1dd_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="swdb_2026_visual_learning",
        name="swdb_2026_visual_learning",
        description="SWDB 2026 public Code Ocean collection Visual Learning data asset membership",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.public_collections.swdb_2026_visual_learning_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="swdb_2026_visual_coding_neuropixels",
        name="swdb_2026_visual_coding_neuropixels",
        description="SWDB 2026 public Code Ocean collection Visual Coding Neuropixels membership",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.public_collections.swdb_2026_visual_coding_neuropixels_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="swdb_2026_visual_coding_ophys",
        name="swdb_2026_visual_coding_ophys",
        description="SWDB 2026 public Code Ocean collection Visual Coding Ophys membership",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.public_collections.swdb_2026_visual_coding_ophys_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="swdb_2026_dynamic_routing",
        name="swdb_2026_dynamic_routing",
        description="SWDB 2026 public Code Ocean collection Dynamic Routing membership",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.public_collections.swdb_2026_dynamic_routing_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="swdb_2026_neuropixels_opto",
        name="swdb_2026_neuropixels_opto",
        description="SWDB 2026 public Code Ocean collection Neuropixels Opto membership",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.public_collections.swdb_2026_neuropixels_opto_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="visual_coding_neuropixels_units",
        name="platform_visual_coding_neuropixels_units",
        description="CCF unit locations for every Visual Coding Neuropixels session (one small unpartitioned table for the SWDB neuron-locations overview)",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.visual_coding_units.platform_visual_coding_neuropixels_units_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="swdb_dr_switch",
        name="platform_swdb_dr_switch",
        description="Dynamic Routing block-switch firing rate per QC-passing unit, averaged across every switch of each direction (one small unpartitioned table for the SWDB neuron-locations replay)",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.dr_switch.platform_swdb_dr_switch_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="swdb_dr_switch_markers",
        name="platform_swdb_dr_switch_markers",
        description="Representative trial-boundary times per Dynamic Routing block-switch direction (one small unpartitioned table for the SWDB neuron-locations replay's trial axis)",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.manual.swdb.dr_switch.platform_swdb_dr_switch_markers_columns",
        lifecycle="manual",
    ),
    TableSpec(
        key="video_frame_times",
        name="platform_behavior-videos_frame-times",
        description="Behavior-camera per-frame times from the camstim NI-DAQ sync file (one row per camera frame), partitioned by raw asset_name",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_video_frame_times.platform_video_frame_times_columns",
        partition_key="asset_name",
        sync_job="video_frame_times",
    ),
    TableSpec(
        key="df_sessions",
        name="platform_dynamic_foraging_sessions",
        description="Dynamic foraging session table (one row per session); mirrors upstream aind-dynamic-foraging-database",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_df.platform_dynamic_foraging_sessions_columns",
        sync_job="df",
    ),
    TableSpec(
        key="df_trials",
        name="platform_dynamic_foraging_trials",
        description="Dynamic foraging trial table (one row per trial), partitioned by subject_id; mirrors upstream aind-dynamic-foraging-database",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_df.platform_dynamic_foraging_trials_columns",
        partition_key="subject_id",
        sync_job="df",
    ),
    TableSpec(
        key="df_events",
        name="platform_dynamic_foraging_events",
        description="Dynamic foraging event table (one row per behavioral event), partitioned by subject_id; mirrors upstream aind-dynamic-foraging-database",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_df.platform_dynamic_foraging_events_columns",
        partition_key="subject_id",
        sync_job="df",
    ),
    TableSpec(
        key="curriculum",
        name="behavior_curriculum",
        description="Behavior assets with curriculum name and stage from trainer_state.json",
        table_type=CacheTableType.asset,
        columns_factory="biodata_cache.cache_table_helpers.behavior_curriculum.behavior_curriculum_columns",
        sync_job="curriculum",
    ),
    TableSpec(
        key="platform_qc",
        name="platform_qc",
        description="Tag-level QC statuses per platform, one row per asset/tag combination",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_qc.platform_qc_columns",
        partition_key="platform",
        sync_job="fast",
    ),
    TableSpec(
        key="time_to_qc",
        name="time_to_qc",
        description="Time from processing completion to QC completion for derived assets",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.time_to_qc.time_to_qc_columns",
        sync_job="time_to_qc",
    ),
    TableSpec(
        key="mouselight",
        name="platform_mouselight",
        description="Janelia MouseLight neuron list (one row per neuron) with label, soma region and tracing UUIDs",
        table_type=CacheTableType.platform,
        columns_factory="biodata_cache.cache_table_helpers.platform_mouselight.platform_mouselight_columns",
        sync_job="fast",
    ),
    TableSpec(
        key="storage_lens",
        name="storage_lens",
        description="Weekly S3 Storage Lens report (one row per prefix/storage class), sourced from RDS",
        table_type=CacheTableType.metadata,
        columns_factory="biodata_cache.cache_table_helpers.storage_lens.storage_lens_columns",
        sync_job="storage_lens",
    ),
)

TABLE_SPECS_BY_KEY = {spec.key: spec for spec in TABLE_SPECS}
TABLE_SPECS_BY_NAME = {spec.name: spec for spec in TABLE_SPECS}


def table_specs_for_job(job: str) -> tuple[TableSpec, ...]:
    """Return the tables owned by one sync job."""
    return tuple(spec for spec in TABLE_SPECS if spec.sync_job == job)


# ``r2d`` is a public helper, not a published table. Keep its old alias for
# callers while keeping it out of the table specification and registry metadata.
NAMES = {spec.key: spec.name for spec in TABLE_SPECS}
NAMES["r2d"] = "raw_to_derived"

# Backend partition paths use the physical storage name. ``qc`` is the only
# published table whose public name differs from that name.
PARTITION_KEYS = {spec.storage_name or spec.name: spec.partition_key for spec in TABLE_SPECS if spec.partitioned}
PARTITION_KEYS["qc_tag_status"] = "subject_id"

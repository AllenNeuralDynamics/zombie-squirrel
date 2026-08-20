---
name: zombie-cache-contract
description: Maintain biodata-cache tables and registry entries consumed by Zombie's browser DuckDB pages.
---

# Zombie cache contract

The published cache is versioned under `s3://allen-data-views/data-asset-cache/bdc-v<version>/`, with `cache_versions.json` selecting the latest version. Current registry files are distributed as `cache_registry/<table>.json`; `biodata_cache.utils.get_cache_registry()` also supports the legacy monolithic registry. A registry entry must accurately declare the table name, location, partitioning, type, and `Column` names/descriptions because Zombie builds SQL directly from these definitions. Do not change a column or partition key without checking Zombie consumers and the generated registry.

Zombie's core contract is `asset_basics` plus `source_data`. Other consumed tables include `unique_project_names`, `unique_subject_ids`, `metadata_upgrade`, `platform_smartspim`, `platform_exaspim`, `platform_fib`, `behavior_curriculum`, `time_to_qc`, `storage_lens`, `platform_mouselight`, `platform_dynamic_foraging_sessions`, subject-partitioned dynamic-foraging trials/events, fiber traces/operations, `platform_df_operations`, ecephys spikes/units, `platform_pophys`, and platform QC/QC tables. Partitioned locations must use the exact hive form `<root>/<version>/<table>/<partition_key>=<value>/data.pqt` (or the generated chunk files) that Zombie's explicit URL readers expect.

The SWDB pages expect `platform_swdb_*` tables and explicit asset partitions; confirm the current registry and builder for the deployed cache before adding or renaming them. Do not make Zombie glob a virtual-hosted S3 prefix. Preserve `asset_basics` and `source_data` availability even when an optional downstream job fails.

Cache tests use fake or memory S3 backends and inspect registry/table metadata without network access. Add contract coverage to the relevant backend, registry, sync, or cache-table-helper tests when a Zombie-facing schema changes.

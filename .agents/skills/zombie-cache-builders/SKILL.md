---
name: zombie-cache-builders
description: Extend biodata-cache builders, sync ordering, partition paths, and Zombie-facing derived tables.
---

# Zombie cache builders

Register a Zombie-facing table through the existing cache-table entry builders and sync job map; do not write a one-off parquet publisher. `asset_basics` runs first and produces `asset_basics` and `source_data`; fast metadata, storage lens, QC, SmartSPIM, exaSPIM, dynamic-foraging, fiber, ecephys, pophys, curriculum, and time-to-QC jobs run afterward. Distributed registry fragments are written per table, and a failed job must not erase unrelated successful fragments.

Use the established hive partition keys: `qc` by `subject_id`; `platform_qc` by `platform`; dynamic-foraging trials/events by `subject_id`; fiber traces/operations, DF operations, ecephys spikes/units, and pophys by `asset_name`. Build locations with the backend helpers so non-partitioned tables, partitioned `data.pqt`, and chunked `data_####.pqt` paths stay consistent. `duckdb_query()` already retries transient S3 503/SlowDown responses; reuse it.

When changing a builder, update its `CacheTable` columns and partition metadata together, then test the builder output, registry fragment, sync ordering, and S3 object layout using the existing fake/memory backends. Validate against the Zombie readers before releasing a new cache version.

# biodata-cache sync pipeline

This document describes how the cache-sync work is split into independent Code
Ocean capsules and wired into a Nextflow pipeline, and — most importantly — how
to **bump the `biodata-cache` version across every capsule and re-run them** so
the pipeline always has a reproducible run in place.

If you change how sync works (add/remove a table, change a job's dependencies,
etc.), update this file.

## Design in one paragraph

Instead of one capsule running the whole sync in a single process, each cache
table (or logical group of tables) is built by its own **sync job**. Every job is
the *same* capsule image, cloned once per job; the job it runs is selected at run
time by a single environment variable. `asset_basics` runs first because every
other job reads its output and it resets the registry; all other jobs then run in
parallel. Each job writes its own per-table registry fragment as it finishes, so
parallel jobs never contend on a shared JSON file.

## The one environment variable

Set **`BIODATA_CACHE_SYNC_JOB`** to select which table(s) a capsule builds.

Also required in every capsule (unchanged from before):

- `BIODATA_CACHE_BACKEND=S3`

### Valid values for `BIODATA_CACHE_SYNC_JOB`

Run **`asset_basics` first**, then all of the rest in parallel:

| `BIODATA_CACHE_SYNC_JOB` | Builds | Depends on | Notes |
|---|---|---|---|
| `asset_basics` | `asset_basics`, `source_data` | — | **Must run first.** Clears the registry fragments, registers the version in `cache_versions.json`, then builds `asset_basics` and `source_data`. `source_data` lives here (not in `fast`) because `smartspim`/`exaspim` read it from cache and would otherwise race a parallel `fast` job and join against a stale `source_data`. |
| `fast`            | `unique_project_names`, `unique_subject_ids`, `unique_genotypes`, `metadata_upgrade`, `platform_fib`, `platform_mouselight`, `platform_qc` | `asset_basics` | All the cheap DocDB-only tables, grouped into one capsule. |
| `qc`              | `quality_control` | `asset_basics` | Loops over every subject in `asset_basics` sequentially. |
| `smartspim`       | `platform_smartspim` | `asset_basics` | |
| `exaspim`         | `platform_exaspim` | `asset_basics` | |
| `df`              | `platform_dynamic_foraging_sessions`, `platform_dynamic_foraging_trials`, `platform_dynamic_foraging_events` | `asset_basics` | Builds sessions first, then loops per-subject for trials/events. |
| `fib_traces`      | `platform_fib_traces` | `asset_basics` | Loops over derived `fib` assets; skips assets whose partition already exists. |
| `operations`      | `platform_fib_operations`, `platform_df_operations` | — | Pulls all pipeline lifecycle events from CloudWatch in **one** Logs Insights query and routes them to each `platform_*_operations` table, so the log pull is done once and reused. Overwrites every acquisition's partition each run. |
| `ecephys_spikes`  | `platform_ecephys_spikes` | `asset_basics` | Loops over derived `ecephys` assets; skips existing partitions. |
| `ecephys_units`   | `platform_ecephys_units` | `asset_basics` | Loops over derived `ecephys` assets; skips existing partitions. |
| `pophys`          | `platform_pophys` | `asset_basics` | Loops over derived `pophys`/`ophys` assets (including BCI single-plane behavior-NWB assets); traces ROI contours and writes FOV projection PNGs under `pophys_fov/` when projections are available. Slow but small; skips existing partitions. |
| `visual_coding_ophys` | `platform_visual_coding_ophys` | `asset_basics` | Loops over canonical Visual Coding Ophys assets; caches sparse ROI contours and FOV projection PNGs under `visual_coding_ophys_fov/`. Kept separate from the generic `pophys` cache because these NWB-Zarr assets have a distinct layout. |
| `visual_learning` | `platform_visual_learning_cell_gene`, `platform_visual_learning_coreg` | public S3 collection | Downloads the six public HCR cell-by-gene CSV/H5AD products and six ROI-to-HCR co-registration tables, writing subject_id partitions for the Visual Learning dashboard. |
| `curriculum`      | `behavior_curriculum` | `asset_basics` | |
| `time_to_qc`      | `time_to_qc` | `asset_basics` | |

The canonical source of truth for these values is `JOBS` in
[`src/biodata_cache/sync.py`](src/biodata_cache/sync.py). `PARALLEL_JOBS` in that
module is exactly the set that runs after `asset_basics`.

An invalid or missing value raises `ValueError` listing the valid jobs, so a
mis-set capsule fails fast rather than silently doing nothing.

## What each capsule runs

Every capsule is the same image. Its `run_capsule.py` only needs to call the
dispatcher, which reads the environment variable:

```python
# code/run_capsule.py
from biodata_cache.sync import run_sync_job

if __name__ == "__main__":
    run_sync_job()  # reads BIODATA_CACHE_SYNC_JOB from the environment
```

(Equivalently, from a shell entrypoint: `python -c "from biodata_cache.sync import run_sync_job; run_sync_job()"`.)

To run a job explicitly without the env var (e.g. locally):
`run_sync_job("qc")`.

## Capsule compute settings

Per the design, **every** capsule uses the same modest settings and no internal
parallelism:

- **1 core / 8 GB RAM**
- No multiprocessing/threading — each job processes its subjects/assets
  sequentially. (The old `ThreadPoolExecutor` fan-out has been removed from
  `sync.py`.)

## Pipeline ordering

```
              ┌── fast
              ├── qc
              ├── smartspim
              ├── exaspim
asset_basics ─┼── df
              ├── fib_traces
              ├── operations
              ├── ecephys_spikes
              ├── ecephys_units
              ├── pophys
              ├── visual_coding_ophys
              ├── visual_learning
              ├── curriculum
              └── time_to_qc
```

`asset_basics` is the single upstream dependency; every other job depends only on
it and is independent of the others, so they all run concurrently.

The `swdb_2025_bci` and `swdb_2025_v1dd` tables are **not** part of this pipeline;
they are built on demand by [`scripts/build_swdb.py`](scripts/build_swdb.py).

## Registry: how the `cache_registry.json` is written

The registry is no longer written as one file at the end of a single run.
Instead it is stored as **one fragment per table** under the versioned folder:

```
data-asset-cache/bdc-v{MAJOR.MINOR}/cache_registry/<table_name>.json
```

- Each job writes only the fragment(s) for the tables it builds, as soon as they
  finish. A re-run overwrites the fragment in place.
- The `asset_basics` job (which runs first) calls `register_version()` to add the
  version folder to the top-level `data-asset-cache/cache_versions.json`. It does
  **not** clear the fragment directory: because each job overwrites its own
  fragment in place, a job that fails (or has not yet run) keeps its previous
  fragment and its table stays visible in the registry. Wiping up front would
  make a failed nightly job drop a table entirely even though its parquet data is
  intact. A full reset is achieved by bumping the cache version.
- `get_cache_registry()` merges all fragments back into a single `CacheRegistry`
  (sorted by table name). If no fragments exist it falls back to a legacy
  monolithic `cache_registry.json`, so older cache versions still read correctly.

**Consumer note:** anything that reads the raw `cache_registry.json` object
directly from S3 (rather than through `biodata_cache.get_cache_registry()`) must
be updated to read/merge the `cache_registry/` fragments instead. All Python
consumers using `get_cache_registry()` need no change.

## Bumping the biodata-cache version and re-running (the important part)

All capsules pin the **same** `biodata-cache` version so the whole pipeline is a
single reproducible run. Current pinned version: **`0.38.1`**.

To move the pipeline to a new version:

1. Publish the new `biodata-cache` release to PyPI (the existing
   `tag_and_publish` GitHub Action does this on a merge to `main`).
2. In the shared capsule image, bump the pinned version in the environment
   (e.g. `pip install biodata-cache[sync]==<new_version>`), commit, and let Code
   Ocean rebuild the environment. Because every job is a clone of this one image,
   this is the **only** place the version is specified.
3. Re-run the pipeline starting from `asset_basics`. `asset_basics` clears the
   registry fragments and registers the new `bdc-v{MAJOR.MINOR}` version folder,
   then the parallel jobs repopulate it. Older version folders remain intact.
4. Trigger the reproducible runs. This can be done from the Code Ocean UI, or via
   the API (`POST /api/v1/computations`) once the capsules exist — see the
   [Code Ocean API docs](https://docs.codeocean.com/user-guide/code-ocean-api).

> Note: the cache S3 layout is versioned by `MAJOR.MINOR` only
> (`bdc-v{major}.{minor}`), so patch releases (e.g. `0.38.0` → `0.38.1`) write to
> the same folder and overwrite it. A minor/major bump writes to a new folder.

## Capsule / repo locations

Fill this in as capsules are created (Code Ocean is not scriptable for capsule
creation, so these are set up manually):

| Job (`BIODATA_CACHE_SYNC_JOB`) | Code Ocean capsule ID | Notes |
|---|---|---|
| `asset_basics`   | _TBD_ | |
| `fast`           | _TBD_ | |
| `qc`             | _TBD_ | |
| `smartspim`      | _TBD_ | |
| `exaspim`        | _TBD_ | |
| `df`             | _TBD_ | |
| `fib_traces`     | _TBD_ | |
| `operations`     | _TBD_ | |
| `ecephys_spikes` | _TBD_ | |
| `ecephys_units`  | _TBD_ | |
| `pophys`         | _TBD_ | |
| `visual_coding_ophys` | _TBD_ | |
| `visual_learning` | _TBD_ | |
| `curriculum`     | _TBD_ | |
| `time_to_qc`     | _TBD_ | |

Pipeline repo (Nextflow): _TBD_

### Predecessor capsules (pre-split, for reference)

These ran the old single-process `update_all_tables()` and can be retired once
the split pipeline is live:

- **Data & Outreach Nightly Cache** — `a81370dd-2f0a-487d-914b-853ea2d3db3a`
- **Data & Outreach Fast Cache** — `ae73d916-c907-4e6f-b25b-d42dde4c5b34`

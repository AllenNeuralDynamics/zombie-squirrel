# biodata-cache

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-ruff-black)](https://docs.astral.sh/ruff/)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Python](https://img.shields.io/badge/python->=3.10,<3.14-blue?logo=python)

`biodata-cache` is a set of one-line functions that handle the entire process of caching and retrieving data (and metadata) from AIND data assets.

In the background, the cache repackages data/metadata into dataframes and stores them on S3 in versioned folders (`data-asset-cache/bdc-v{version}/`), or in memory for testing. Each release writes to its own versioned folder, so older versions of the website remain accessible while new versions are deployed. A top-level `data-asset-cache/cache_versions.json` index lists all available version folders.

Important: this package is not at 1.0. It is changing *fast* and breaking changes are still occurring, although rarely. To reduce the chance of impact on your code the cache tables are versioned. This does mean that if you want the latest version of the tables you need to keep biodata-cache up-to-date, but it also means your code won't immediately break when I change the way the tables work.

## Installation

Note that you **must set the backend to S3** or `biodata-cache` will automatically re-cache the tables locally in memory. This can take a LONG time.

```bash
pip install biodata-cache
export BIODATA_CACHE_BACKEND='S3'
```

## Usage

### Set backend

```bash
export BIODATA_CACHE_BACKEND='S3'
```

Options are 'S3', 'MEMORY'.

### Fetch data

```python
from biodata_cache import unique_project_names

project_names = unique_project_names()
```

#### Cache tables

Use `get_cache_registry()` to see all available cache tables and their metadata (descriptions, S3 paths, columns, etc.) for the installed version:

```python
from biodata_cache import get_cache_registry

registry = get_cache_registry()
```

Use `get_cache_versions()` to list all available version folders across all deployed releases:

```python
from biodata_cache import get_cache_versions

versions = get_cache_versions()
```

Each version stores one registry fragment per table at `s3://allen-data-views/data-asset-cache/bdc-v{version}/cache_registry/<table>.json`. `get_cache_registry()` merges these fragments and still reads older monolithic registries. The top-level index `s3://allen-data-views/data-asset-cache/cache_versions.json` lists all available version folders.

Hive-partitioned tables use `key=value` directory segments, enabling DuckDB queries like:

```python
import duckdb
duckdb.query("""
    SELECT * FROM read_parquet(
        's3://allen-data-views/data-asset-cache/bdc-v0.41/qc/subject_id=123/data.pqt',
        hive_partitioning=true,
        union_by_name=true
    )
""")
```

The `raw_to_derived` function is not a table stored in S3, instead it is used by passing an asset_name (or list of asset names) and a modality. The function returns the latest derived asset matching the requested pattern.


### Cells across every asset

The cell tables use `cell_key` to join identity, measurements, and transcriptomic data:

| Table | Rows | Partition |
|---|---|---|
| `cell_index` | One row per cell | None |
| `cell_properties` | Measurements for each cell | `asset_name` |
| `cell_genes` | Genotyping results | `subject_id` |

The `cell-by-everything` sync job runs after `ecephys_units`, `pophys`, and
`visual_learning`. It also reads Visual Coding Neuropixels NWB-Zarr data directly
so it does not depend on manual SWDB tables.


### Custom cache table

The `custom` function allows you to store and retrieve your own user-defined DataFrames in the cache by name. This requires write authentication to the active backend.

```python
from biodata_cache import custom
import pandas as pd

df = pd.DataFrame({"col": [1, 2, 3]})
custom("my_data", df)

retrieved_df = custom("my_data")
```

### Update all cache tables

The cache is rebuilt on Code Ocean by a set of per-table sync jobs wired into a
Nextflow pipeline (`asset_basics` first, then the rest in parallel, followed by
`cell-by-everything`). The `BIODATA_CACHE_SYNC_JOB` environment variable selects the job.
The job writes its own
registry fragment. See [PIPELINE.md](PIPELINE.md) for the job list, pipeline
layout, and version procedure.

To run a single job (as a capsule does):

```python
from biodata_cache.sync import run_sync_job
run_sync_job()  # reads BIODATA_CACHE_SYNC_JOB, or pass e.g. run_sync_job("qc")
```

From the repository, use `python scripts/run_sync.py <job>`.

To rebuild everything in one local process (not used by the pipeline):

```python
from biodata_cache.sync import update_all_tables
update_all_tables()
```

The `ecephys_virtual` job publishes VirtualiZarr/Kerchunk-compatible reference
manifests under `data-asset-cache/bdc-v{major}.{minor}/platform_ecephys_virtual/`
and a small Parquet URL index under `platform_ecephys_virtual_index/`. It can
be run independently after `asset_basics`:

```bash
BIODATA_CACHE_SYNC_JOB=ecephys_virtual python scripts/run_sync.py
```

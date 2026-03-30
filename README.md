# ZOMBIE Squirrel

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-90.0%25-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-99%25-brightgreen)
![Python](https://img.shields.io/badge/python->=3.10-blue?logo=python)

<img src="zombie-squirrel_logo.png" width="400" alt="Logo (image from ChatGPT)">

`zombie-squirrel` is a set of one-line functions that handle the entire process of caching and retrieving data (and metadata) from AIND data assets.

In the background, the ZOMBIE squirrel repackages data/metadata into dataframes and stores them on S3 in a flat bucket, or in memory for testing.

## Installation

```bash
pip install zombie-squirrel
```

## Usage

### Set backend

```bash
export FOREST_TYPE='S3'
```

Options are 'S3', 'MEMORY'.

### Scurry (fetch) data

```python
from zombie_squirrel import unique_project_names

project_names = unique_project_names()
```

#### Acorns

`get_squirrel_info` returns the following information about all available acorns:

| Acorn | Description | Location | Type | Partitioned | Columns |
| ----- | ----------- | -------- | ---- | ----------- | ------- |
| `unique_project_names` | Unique project names across all assets | `s3://allen-data-views/data-asset-cache/zs_unique_project_names.pqt` | metadata | False | `project_name` |
| `unique_subject_ids` | Unique subject_ids across all assets | `s3://allen-data-views/data-asset-cache/zs_unique_subject_ids.pqt` | metadata | False | `subject_id` |
| `asset_basics` | Commonly used asset metadata, one row per data asset | `s3://allen-data-views/data-asset-cache/zs_asset_basics.pqt` | metadata | False | `_id`, `_last_modified`, `modalities`, `project_name`, `data_level`, `subject_id`, `acquisition_start_time`, `acquisition_end_time`, `code_ocean`, `process_date`, `genotype`, `location`, `name` |
| `source_data` | Mapping from derived asset names to their source raw asset names | `s3://allen-data-views/data-asset-cache/zs_source_data.pqt` | metadata | False | `name`, `source_data`, `pipeline_name`, `processing_time` |
| `quality_control` | Quality control table with one row per QC metric | `s3://allen-data-views/data-asset-cache/zs_qc/` | asset | True (by `subject_id`) | `name`, `stage`, `modality`, `value`, `status`, `asset_name` |
| `assets_smartspim` | SmartSPIM assets with processing status and neuroglancer links | `s3://allen-data-views/data-asset-cache/zs_assets_smartspim.pqt` | metadata | False | `subject_id`, `genotype`, `institution`, `acquisition_start_time`, `processing_end_time`, `stitched_link`, `processed`, `name`, `channel_1`, `segmentation_link_1`, `quantification_link_1`, `channel_2`, `segmentation_link_2`, `quantification_link_2`, `channel_3`, `segmentation_link_3`, `quantification_link_3` |

The `raw_to_derived` function is not a table stored in S3, instead it is used by passing an asset_name (or list of asset names) and a modality. The function returns the latest derived asset matching the requested pattern.

### Custom acorn

The `custom` function allows you to store and retrieve your own user-defined DataFrames in the cache by name. This requires write authentication to the active backend.

```python
from zombie_squirrel import custom
import pandas as pd

df = pd.DataFrame({"col": [1, 2, 3]})
custom("my_data", df)

retrieved_df = custom("my_data")
```

### Hide all the acorns

We run a nightly capsule on Code Ocean with this code to hide all acorns (not the custom ones).

```python
from zombie_squirrel.sync import hide_acorns
hide_acorns()
```

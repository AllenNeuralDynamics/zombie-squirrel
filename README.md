# zombie-squirrel

[![License](https://img.shields.io/badge/license-MIT-brightgreen)](LICENSE)
![Code Style](https://img.shields.io/badge/code%20style-black-black)
[![semantic-release: angular](https://img.shields.io/badge/semantic--release-angular-e10079?logo=semantic-release)](https://github.com/semantic-release/semantic-release)
![Interrogate](https://img.shields.io/badge/interrogate-100.0%25-brightgreen)
![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen)
![Python](https://img.shields.io/badge/python->=3.10-blue?logo=python)

<img src="zombie-squirrel_logo.png" width="400" alt="Logo (image from ChatGPT)">

## Installation

```bash
pip install zombie-squirrel

```bash
uv sync
```

## Usage

### Set backend

```bash
export REDSHIFT_SECRETS='/aind/prod/redshift/credentials/readwrite'
export TREE_SPECIES='REDSHIFT'
```

Options are 'REDSHIFT', 'MEMORY'.

### Scurry (fetch) data

```python
from zombie_squirrel import unique_project_names

project_names = unique_project_names()
```

| Function | Description |
| -------- | ----------- |
| `unique_project_names` | Fetch unique project names from docdb |
| `unique_subject_ids` | Fetch unique subject IDs from docdb |
| `asset_basics` | Fetch basic asset metadata including modalities, projects, and subject info |
| `source_data` | Fetch source data references for derived records |
| `raw_to_derived` | Fetch mapping of raw records to their derived records | 

### Hide the acorns

```python
from zombie_squirrel.sync import hide_acorns
hide_acorns()
```

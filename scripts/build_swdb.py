"""Build the standalone SWDB 2025 metadata cache tables and upload to the backend.

Runs the SWDB 2025 metadata aggregations (ported from the SWDB 2025 DataIntro
notebooks), builds each table and writes it via whatever backend
``BIODATA_CACHE_BACKEND`` selects (memory unless set). Use
``BIODATA_CACHE_BACKEND=s3`` to write to the real cache.

These tables are intentionally not part of the nightly sync pipeline; run this
script to (re)build them.

Usage:
    python scripts/build_swdb.py
"""

import logging

from biodata_cache.registry import NAMES, TABLE_REGISTRY
from biodata_cache.sync import publish_registry_fragment

TABLES = ["swdb_2025_bci", "swdb_2025_v1dd"]


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    for key in TABLES:
        name = NAMES[key]
        df = TABLE_REGISTRY[name](force_update=True)
        publish_registry_fragment(name)
        logging.info(f"Built {name}: {len(df)} rows, {df['name'].nunique()} assets")


if __name__ == "__main__":
    main()

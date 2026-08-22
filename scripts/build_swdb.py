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
import os

from biodata_cache.registry import NAMES, TABLE_REGISTRY
from biodata_cache.sync import publish_registry_fragment

TABLES = [
    "swdb_2026_bci",
    "swdb_2026_v1dd",
    "swdb_2026_visual_learning",
    "swdb_2026_visual_coding_neuropixels",
    "swdb_2026_visual_coding_ophys",
    "swdb_2026_dynamic_routing",
    "swdb_2026_neuropixels_opto",
]


def main():
    if os.getenv("BIODATA_CACHE_BACKEND", "").lower() != "s3":
        raise RuntimeError("The SWDB public-collection builder must run with BIODATA_CACHE_BACKEND=s3")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    for key in TABLES:
        name = NAMES[key]
        df = TABLE_REGISTRY[name](force_update=True)
        publish_registry_fragment(name)
        logging.info(f"Built {name}: {len(df)} rows, {df['name'].nunique()} assets")


if __name__ == "__main__":
    main()

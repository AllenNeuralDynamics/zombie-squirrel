"""Build the Visual Coding Neuropixels unit-location cache table and upload to the backend.

Fetches CCF unit locations for every session in the public Visual Coding
Neuropixels collection and writes one small unpartitioned parquet table. This
table is intentionally not part of the nightly sync pipeline (there is no
per-asset dependency to schedule against); run this script to (re)build it.

Usage:
    BIODATA_CACHE_BACKEND=s3 python scripts/build_platform_visual_coding_neuropixels_units.py
"""

import logging
import os

from biodata_cache.registry import NAMES, TABLE_REGISTRY
from biodata_cache.sync import publish_registry_fragment

TABLE_KEY = "visual_coding_neuropixels_units"


def main():
    if os.getenv("BIODATA_CACHE_BACKEND", "").lower() != "s3":
        raise RuntimeError("This builder must run with BIODATA_CACHE_BACKEND=s3")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    name = NAMES[TABLE_KEY]
    df = TABLE_REGISTRY[name](force_update=True)
    publish_registry_fragment(name)
    logging.info(f"Built {name}: {len(df)} rows, {df['asset_name'].nunique()} assets")


if __name__ == "__main__":
    main()

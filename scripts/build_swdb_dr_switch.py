"""Build the Dynamic Routing block-switch activity cache table and upload to the backend.

Computes, for every QC-passing unit in the SWDB 2026 Dynamic Routing dataset, its
average firing-rate time course around its session's block switches (aud_to_vis and
vis_to_aud separately) and writes one small unpartitioned parquet table. This table
is intentionally not part of the nightly sync pipeline (it is a one-off aggregate
over a fixed 12-asset dataset); run this script to (re)build it.

Usage:
    BIODATA_CACHE_BACKEND=s3 python scripts/build_swdb_dr_switch.py
"""

import logging
import os

from biodata_cache.registry import NAMES, TABLE_REGISTRY
from biodata_cache.sync import publish_registry_fragment

TABLE_KEY = "swdb_dr_switch"


def main():
    if os.getenv("BIODATA_CACHE_BACKEND", "").lower() != "s3":
        raise RuntimeError("This builder must run with BIODATA_CACHE_BACKEND=s3")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    name = NAMES[TABLE_KEY]
    df = TABLE_REGISTRY[name](force_update=True)
    publish_registry_fragment(name)
    logging.info(
        f"Built {name}: {len(df)} rows, {df['asset_name'].nunique()} assets, "
        f"{df['unit_id'].nunique()} unique unit_ids"
    )

    # Written as a byproduct of the same builder call above; just publish its
    # registry fragment now that the data is in the cache.
    markers_name = NAMES["swdb_dr_switch_markers"]
    markers_df = TABLE_REGISTRY[markers_name]()
    publish_registry_fragment(markers_name)
    logging.info(f"Built {markers_name}: {len(markers_df)} rows")


if __name__ == "__main__":
    main()

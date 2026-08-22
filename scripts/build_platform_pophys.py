"""Run the pophys ROI cache sync on a few (random) test assets.

Picks derived pophys assets from `asset_basics`, force-builds
the `platform_pophys` cache table for each (reading segmentation masks from the S3
NWB Zarr, tracing ROI contours, and writing FOV projection PNGs), then reads the
partition back and prints a short summary so you can sanity-check the output.

By default it uses whatever backend `BIODATA_CACHE_BACKEND` selects (memory unless
set). Use `BIODATA_CACHE_BACKEND=s3` to write to the real cache.

Usage:
    python scripts/build_platform_pophys.py                       # 3 random assets
    python scripts/build_platform_pophys.py --num 5 --seed 42
    python scripts/build_platform_pophys.py --assets multiplane-ophys_..._processed_...
    python scripts/build_platform_pophys.py --refresh-basics
"""

import argparse
import json
import logging
import random

from biodata_cache.registry import NAMES, TABLE_REGISTRY


def _derived_pophys_assets(refresh_basics: bool) -> tuple[list[str], dict[str, str]]:
    """Return derived pophys asset names and a name -> S3 location map.

    The table helper probes each derived pophys asset and selects the compatible NWB
    layout, so asset names are not used to decide whether an asset is processable.
    """
    df_basics = TABLE_REGISTRY[NAMES["basics"]](force_update=refresh_basics)

    pophys_mask = df_basics["modalities"].apply(
        lambda x: x is not None and not isinstance(x, float) and any("pophys" in m.lower() for m in x)
    )
    derived = df_basics[pophys_mask & (df_basics["data_level"] == "derived")]
    asset_names = derived["name"].dropna().unique().tolist()
    location_map = dict(zip(df_basics["name"], df_basics["location"], strict=False))
    return asset_names, location_map


def _raw_name_map(asset_names: list[str]) -> dict[str, str]:
    """Return a derived-asset -> source raw-asset name map from source_data."""
    df = TABLE_REGISTRY[NAMES["d2r"]]()
    if df.empty or "name" not in df.columns or "source_data" not in df.columns:
        return {}
    subset = df[df["name"].isin(set(asset_names))]
    return dict(zip(subset["name"], subset["source_data"], strict=False))


def _summarize(asset_name: str) -> bool:
    """Read the freshly built partition back and log a short summary.

    Returns True if the partition has ROIs, False if it is empty (the asset has no
    segmentation NWB to extract — a legitimate skip, not a failure).
    """
    try:
        df = TABLE_REGISTRY[NAMES["pophys"]](asset_name=asset_name)
    except ValueError:
        df = None
    if df is None or df.empty:
        logging.info(f"  {asset_name}: no ROIs extracted (no segmentation NWB) — skipped")
        return False
    planes = df["plane"].nunique()
    contour_vertices = df["contour"].apply(lambda c: len(json.loads(c)))
    logging.info(
        f"  {asset_name}: {len(df)} ROIs across {planes} planes "
        f"(soma={int(df['is_soma'].sum())}, "
        f"median contour verts={int(contour_vertices.median())})"
    )
    for plane, group in df.groupby("plane"):
        structure = group["structure"].iloc[0]
        depth = group["depth_um"].iloc[0]
        logging.info(f"    {plane}: {len(group)} ROIs — structure={structure} depth_um={depth}")
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--assets",
        nargs="+",
        default=None,
        help="Explicit derived pophys asset names to build (overrides random sampling)",
    )
    parser.add_argument("--num", type=int, default=3, help="Number of random assets to build (default: 3)")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducible asset selection")
    parser.add_argument(
        "--refresh-basics",
        action="store_true",
        help="Force-update asset_basics before selecting assets (default: use cached)",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    all_asset_names, location_map = _derived_pophys_assets(args.refresh_basics)
    logging.info(f"Found {len(all_asset_names)} processed multiplane-ophys assets in asset_basics.")

    if args.assets:
        requested = set(args.assets)
        asset_names = [a for a in all_asset_names if a in requested]
        for name in sorted(requested - set(all_asset_names)):
            logging.warning(f"Requested asset not a processed multiplane-ophys asset in asset_basics: {name}")
    else:
        rng = random.Random(args.seed)
        asset_names = rng.sample(all_asset_names, min(args.num, len(all_asset_names)))

    if not asset_names:
        logging.warning("No assets to build. Exiting.")
        return

    raw_map = _raw_name_map(asset_names)
    pophys_fn = TABLE_REGISTRY[NAMES["pophys"]]

    logging.info(f"Building platform_pophys for {len(asset_names)} test assets:")
    for name in asset_names:
        logging.info(f"  - {name}")

    successes = 0
    skipped = 0
    failures: list[tuple[str, str]] = []
    for asset_name in asset_names:
        logging.info(f"Building platform_pophys/{asset_name}...")
        try:
            pophys_fn(
                asset_name=asset_name,
                location=location_map.get(asset_name),
                raw_name=raw_map.get(asset_name),
                force_update=True,
            )
            if _summarize(asset_name):
                successes += 1
            else:
                skipped += 1
        except Exception as e:
            failures.append((asset_name, str(e)))
            logging.warning(f"  {asset_name}: FAILED ({e})")

    logging.info(f"Done: {successes} succeeded, {skipped} skipped (no segmentation NWB), {len(failures)} failed.")
    for asset_name, err in failures:
        logging.warning(f"  failed asset {asset_name}: {err}")


if __name__ == "__main__":
    main()

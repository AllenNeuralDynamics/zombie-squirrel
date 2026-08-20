"""Run the behavior-video frame-times cache sync on a few test raw assets.

Picks raw acquisitions that carry the ``behavior-videos`` modality from
``asset_basics``, force-builds the ``platform_behavior-videos_frame-times`` cache
table for each (reading the camstim NI-DAQ sync file from S3, computing per-camera
rising-edge frame times aligned to the session clock, and dropping lost frames), then
reads the partition back and prints a short summary so you can sanity-check output.

Assets without a camstim sync file (Harp rigs: VR foraging, harp Dynamic Foraging)
produce an empty partition and are reported as skipped — that is expected, not a
failure.

By default it uses whatever backend ``BIODATA_CACHE_BACKEND`` selects (memory unless
set). Use ``BIODATA_CACHE_BACKEND=s3`` to write to the real cache.

Usage:
    python scripts/build_platform_video_frame_times.py                       # 3 random assets
    python scripts/build_platform_video_frame_times.py --num 5 --seed 42
    python scripts/build_platform_video_frame_times.py --assets multiplane-ophys_717824_2024-04-12_09-34-43
    python scripts/build_platform_video_frame_times.py --refresh-basics
"""

import argparse
import logging
import random

from biodata_cache.registry import NAMES, TABLE_REGISTRY


def _raw_video_assets(refresh_basics: bool) -> tuple[list[str], dict[str, str]]:
    """Return raw asset names with behavior-videos and a name -> S3 location map."""
    df_basics = TABLE_REGISTRY[NAMES["basics"]](force_update=refresh_basics)
    mask = df_basics["modalities"].apply(
        lambda x: x is not None and not isinstance(x, float) and any("behavior-videos" in m.lower() for m in x)
    )
    raw = df_basics[mask & (df_basics["data_level"] == "raw")]
    asset_names = raw["name"].dropna().unique().tolist()
    location_map = dict(zip(df_basics["name"], df_basics["location"], strict=False))
    return asset_names, location_map


def _summarize(asset_name: str) -> bool:
    """Read the freshly built partition back and log a short summary.

    Returns True if the partition has frame times, False if empty (no sync file — a
    legitimate Harp-rig skip, not a failure).
    """
    try:
        df = TABLE_REGISTRY[NAMES["video_frame_times"]](asset_name=asset_name)
    except ValueError:
        df = None
    if df is None or df.empty:
        logging.info(f"  {asset_name}: no frame times (no camstim sync file) — skipped")
        return False
    logging.info(f"  {asset_name}: {len(df)} frame times across {df['camera'].nunique()} cameras")
    for camera, group in df.groupby("camera"):
        t = group.sort_values("frame_index")["t"].to_numpy()
        fps = (len(t) - 1) / (t[-1] - t[0]) if len(t) > 1 and t[-1] > t[0] else float("nan")
        logging.info(
            f"    {camera}: {len(t)} frames  t=[{t[0]:.3f}, {t[-1]:.3f}]s  fps~{fps:.4f}"
        )
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--assets",
        nargs="+",
        default=None,
        help="Explicit raw asset names to build (overrides random sampling)",
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

    all_asset_names, location_map = _raw_video_assets(args.refresh_basics)
    logging.info(f"Found {len(all_asset_names)} raw assets with behavior-videos in asset_basics.")

    if args.assets:
        requested = set(args.assets)
        asset_names = [a for a in all_asset_names if a in requested]
        for name in sorted(requested - set(all_asset_names)):
            logging.warning(f"Requested asset not a raw behavior-videos asset in asset_basics: {name}")
    else:
        rng = random.Random(args.seed)
        asset_names = rng.sample(all_asset_names, min(args.num, len(all_asset_names)))

    if not asset_names:
        logging.warning("No assets to build. Exiting.")
        return

    frame_times_fn = TABLE_REGISTRY[NAMES["video_frame_times"]]

    logging.info(f"Building platform_behavior-videos_frame-times for {len(asset_names)} test assets:")
    for name in asset_names:
        logging.info(f"  - {name}")

    successes = 0
    skipped = 0
    failures: list[tuple[str, str]] = []
    for asset_name in asset_names:
        logging.info(f"Building frame times for {asset_name}...")
        try:
            frame_times_fn(asset_name=asset_name, location=location_map.get(asset_name), force_update=True)
            if _summarize(asset_name):
                successes += 1
            else:
                skipped += 1
        except Exception as e:
            failures.append((asset_name, str(e)))
            logging.warning(f"  {asset_name}: FAILED ({e})")

    logging.info(f"Done: {successes} succeeded, {skipped} skipped (no sync file), {len(failures)} failed.")
    for asset_name, err in failures:
        logging.warning(f"  failed asset {asset_name}: {err}")


if __name__ == "__main__":
    main()

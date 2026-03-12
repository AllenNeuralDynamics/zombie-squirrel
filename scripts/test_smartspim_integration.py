"""Integration test for assets_smartspim acorn.

Runs the acorn for a single known stitched SmartSPIM asset and prints the
resulting DataFrame for manual inspection.
"""

import os

import pandas as pd

os.environ["FOREST_TYPE"] = "memory"

from zombie_squirrel.acorn_helpers.assets_smartspim import (  # noqa: E402
    _build_rows,
    _fetch_stitched_metadata,
)

EXAMPLE_STITCHED = "SmartSPIM_828521_2026-03-03_04-49-46_stitched_2026-03-05_10-26-24"


def test_single_asset():
    metadata = _fetch_stitched_metadata([EXAMPLE_STITCHED])
    rows = _build_rows([EXAMPLE_STITCHED], metadata)
    df = pd.DataFrame(rows)

    for col in df.columns:
        print(f"{col}: {df[col].iloc[0]}")
    return df


if __name__ == "__main__":
    test_single_asset()

"""Simple test to fetch and view QC data for a subject."""

import os

# Ensure we use memory backend for testing
os.environ["FOREST_TYPE"] = "memory"

from zombie_squirrel import qc

subject_id = "818323"

print(f"Fetching QC data for subject: {subject_id}")
print("=" * 80)

df = qc(subject_id=subject_id, force_update=True)

print(f"\nShape: {df.shape}")
print(f"Columns: {list(df.columns)}")
print(f"\nUnique assets: {len(df['asset_name'].unique())}")
print("\nAsset names:")
for asset in df["asset_name"].unique():
    count = len(df[df["asset_name"] == asset])
    print(f"  - {asset}: {count} metrics")

print(f"\n{df.head(20)}")
print("\n" + "=" * 80)

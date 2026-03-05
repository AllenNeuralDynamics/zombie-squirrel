"""Proof test: write one subject's QC data to S3 and verify timestamp is TIMESTAMPTZ."""

import os
import unittest

os.environ["FOREST_TYPE"] = "s3"

import duckdb

from zombie_squirrel import qc
from zombie_squirrel.acorns import TREE

SUBJECT_ID = "818323"
BUCKET = "aind-scratch-data"


class TestQCTimestampProof(unittest.TestCase):
    """Proof test for QC timestamp storage type."""

    def test_timestamp_is_timestamptz_after_write(self):
        """Test timestamp column stored as TIMESTAMPTZ after write."""
        print(f"\nWriting QC data for subject {SUBJECT_ID} to S3...", flush=True)
        qc(subject_id=SUBJECT_ID, force_update=True)

        s3_path = TREE.get_location(f"qc/{SUBJECT_ID}")
        print(f"Reading schema from {s3_path}...", flush=True)

        rows = duckdb.query(f"DESCRIBE SELECT * FROM read_parquet('{s3_path}')").fetchall()
        col_types = {row[0]: row[1] for row in rows}

        print("Schema:", col_types, flush=True)

        self.assertIn("timestamp", col_types, "timestamp column missing from parquet schema")
        self.assertEqual(
            col_types["timestamp"].upper(),
            "TIMESTAMP WITH TIME ZONE",
            f"Expected TIMESTAMP WITH TIME ZONE, got {col_types['timestamp']!r}",
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)

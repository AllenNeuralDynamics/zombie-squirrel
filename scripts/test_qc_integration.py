"""Integration tests confirming QC parquet files on S3 store timestamps as TIMESTAMPTZ."""

import unittest

import boto3
import duckdb

BUCKET = "allen-data-views"
QC_PREFIX = "data-asset-cache/zs_qc/"
TIMESTAMP_COLUMN = "timestamp"
EXPECTED_TYPE = "TIMESTAMP WITH TIME ZONE"


class TestQCTimestampType(unittest.TestCase):
    """Test QC timestamp types in S3 parquet files."""

    def setUp(self):
        """Initialize S3 client and fetch QC file list."""
        self.s3_client = boto3.client("s3")
        paginator = self.s3_client.get_paginator("list_objects_v2")
        self.qc_keys = []
        for page in paginator.paginate(Bucket=BUCKET, Prefix=QC_PREFIX):
            for obj in page.get("Contents", []):
                self.qc_keys.append(obj["Key"])

    def test_qc_files_exist_on_s3(self):
        """Test QC files exist on S3."""
        self.assertGreater(len(self.qc_keys), 0, f"No QC files found at s3://{BUCKET}/{QC_PREFIX}")

    def test_all_qc_files_have_timestamptz(self):
        """Test all QC files have TIMESTAMPTZ timestamp column."""
        self.assertGreater(len(self.qc_keys), 0, f"No QC files found at s3://{BUCKET}/{QC_PREFIX}")

        total = len(self.qc_keys)
        print(f"\nChecking {total} QC parquet files for TIMESTAMPTZ...")
        failures = []
        for i, key in enumerate(self.qc_keys, 1):
            s3_path = f"s3://{BUCKET}/{key}"
            print(f"  [{i}/{total}] {key}", flush=True)
            rows = duckdb.query(f"DESCRIBE SELECT * FROM read_parquet('{s3_path}')").fetchall()
            col_types = {row[0]: row[1] for row in rows}

            if TIMESTAMP_COLUMN not in col_types:
                msg = f"  ERROR: missing '{TIMESTAMP_COLUMN}' column"
                print(msg, flush=True)
                failures.append(f"{key}: missing '{TIMESTAMP_COLUMN}' column")
            elif col_types[TIMESTAMP_COLUMN].upper() != EXPECTED_TYPE:
                msg = f"  ERROR: '{TIMESTAMP_COLUMN}' is {col_types[TIMESTAMP_COLUMN]!r}, expected {EXPECTED_TYPE!r}"
                print(msg, flush=True)
                failures.append(
                    f"{key}: '{TIMESTAMP_COLUMN}' is {col_types[TIMESTAMP_COLUMN]!r}, expected {EXPECTED_TYPE!r}"
                )

        self.assertEqual(
            failures,
            [],
            "The following QC files have incorrect timestamp types:\n" + "\n".join(f"  {f}" for f in failures),
        )


if __name__ == "__main__":
    unittest.main()

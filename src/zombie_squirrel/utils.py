"""Utility functions for zombie-squirrel package."""

import json
import logging

import boto3
from pydantic import BaseModel


class SquirrelMessage(BaseModel):
    """Structured logging message for zombie-squirrel operations."""

    tree: str
    acorn: str
    message: str

    def to_json(self) -> str:
        """Convert message to JSON string."""
        return self.model_dump_json()


def setup_logging():
    """Configure logging for zombie-squirrel package.

    Sets up INFO level logging with timestamp format.
    Safe to call multiple times - uses force=True to reconfigure."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)


def prefix_table_name(table_name: str) -> str:
    """Add zombie-squirrel prefix and parquet extension to filenames.

    Args:
        table_name: The base table name.

    Returns:
        Filename with 'zs_' prefix and '.pqt' extension."""
    return "zs_" + table_name + ".pqt"


def get_s3_cache_path(filename: str) -> str:
    """Get the full S3 path for a cache file.

    Args:
        filename: The cache filename (e.g., "zs_unique_project_names.pqt").

    Returns:
        Full S3 path: application-caches/filename"""
    return f"application-caches/{filename}"


def load_columns_from_metadata(table_name: str) -> list:
    """Load column metadata from S3 for a given table.

    For partitioned tables like qc/{subject_id}, reads from the base table's
    metadata file (e.g., zs_qc.json).

    Args:
        table_name: The table name, may include partitions (e.g., "qc/subject123").

    Returns:
        List of column names from the table's metadata JSON."""
    base_name = table_name.split("/")[0]
    key = get_s3_cache_path(f"zs_{base_name}.json")
    s3 = boto3.client("s3")
    response = s3.get_object(Bucket="aind-scratch-data", Key=key)
    data = json.loads(response["Body"].read())
    return data["columns"]

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


def load_columns_from_metadata(table_name: str) -> list[str]:
    """Load column names from metadata JSON file.

    Args:
        table_name: The table name (e.g., "unique_project_names").

    Returns:
        List of column names from the metadata JSON file.

    Raises:
        FileNotFoundError: If metadata file not found in S3."""
    base_name = prefix_table_name(table_name)
    json_filename = base_name.replace(".pqt", ".json")

    json_key = get_s3_cache_path(json_filename)

    s3_client = boto3.client("s3")
    bucket = "aind-scratch-data"

    response = s3_client.get_object(Bucket=bucket, Key=json_key)
    metadata = json.loads(response["Body"].read())
    return metadata["columns"]

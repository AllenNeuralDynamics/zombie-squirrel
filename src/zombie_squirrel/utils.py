"""Utility functions for zombie-squirrel package."""

import logging
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
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        force=True
    )


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

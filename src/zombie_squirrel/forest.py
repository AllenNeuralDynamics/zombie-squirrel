"""Storage backend interfaces for caching data."""

import io
import logging
from abc import ABC, abstractmethod

import boto3
import duckdb
import pandas as pd

from zombie_squirrel.utils import get_s3_cache_path, prefix_table_name, SquirrelMessage


class Tree(ABC):
    """Base class for a storage backend (the cache)."""

    def __init__(self) -> None:
        """Initialize the Tree."""
        super().__init__()

    @abstractmethod
    def hide(self, table_name: str, data: pd.DataFrame) -> None:
        """Store records in the cache."""
        pass  # pragma: no cover

    @abstractmethod
    def scurry(self, table_name: str) -> pd.DataFrame:
        """Fetch records from the cache."""
        pass  # pragma: no cover


class S3Tree(Tree):
    """Stores and retrieves caches using AWS S3 with parquet files."""

    def __init__(self) -> None:
        """Initialize S3Acorn with S3 client."""
        self.bucket = "aind-scratch-data"
        self.s3_client = boto3.client("s3")

    def hide(self, table_name: str, data: pd.DataFrame) -> None:
        """Store DataFrame as parquet file in S3."""
        filename = prefix_table_name(table_name)
        s3_key = get_s3_cache_path(filename)

        # Convert DataFrame to parquet bytes
        parquet_buffer = io.BytesIO()
        data.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)

        # Upload to S3
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
        )
        logging.info(SquirrelMessage(
            tree="S3Tree",
            acorn=table_name,
            message=f"Stored cache to s3://{self.bucket}/{s3_key}"
        ).to_json())

    def scurry(self, table_name: str) -> pd.DataFrame:
        """Fetch DataFrame from S3 parquet file."""
        filename = prefix_table_name(table_name)
        s3_key = get_s3_cache_path(filename)

        try:
            # Read directly from S3 using DuckDB
            query = f"""
                SELECT * FROM read_parquet(
                    's3://{self.bucket}/{s3_key}'
                )
            """
            result = duckdb.query(query).to_df()
            logging.info(SquirrelMessage(
                tree="S3Tree",
                acorn=table_name,
                message=f"Retrieved cache from s3://{self.bucket}/{s3_key}"
            ).to_json())
            return result
        except Exception as e:
            logging.warning(SquirrelMessage(
                tree="S3Tree",
                acorn=table_name,
                message=f"Error fetching from cache {s3_key}: {e}"
            ).to_json())
            return pd.DataFrame()


class MemoryTree(Tree):
    """A simple in-memory backend for testing or local development."""

    def __init__(self) -> None:
        """Initialize MemoryAcorn with empty store."""
        super().__init__()
        self._store: dict[str, pd.DataFrame] = {}

    def hide(self, table_name: str, data: pd.DataFrame) -> None:
        """Store DataFrame in memory."""
        self._store[table_name] = data

    def scurry(self, table_name: str) -> pd.DataFrame:
        """Fetch DataFrame from memory."""
        return self._store.get(table_name, pd.DataFrame())

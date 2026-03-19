"""Storage backend interfaces for caching data."""

import io
import json
import logging
from abc import ABC, abstractmethod

import boto3
import duckdb
import pandas as pd

from zombie_squirrel.utils import SquirrelMessage, get_s3_cache_path, prefix_table_name


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
    def scurry(self, table_name: str | list[str]) -> pd.DataFrame:
        """Fetch records from the cache.

        Args:
            table_name: Single table name or list of table names.
                When a list is provided, merges all tables and adds
                an 'asset_name' column to differentiate sources.

        """
        pass  # pragma: no cover

    @abstractmethod
    def get_location(self, table_name: str, partitioned: bool = False) -> str:
        """Return the storage location string for a given table."""
        pass  # pragma: no cover

    @abstractmethod
    def plant(self, key: str, data: str) -> None:
        """Write a JSON string to the storage root under the given key."""
        pass  # pragma: no cover


class S3Tree(Tree):
    """Stores and retrieves caches using AWS S3 with parquet files."""

    def __init__(self) -> None:
        """Initialize S3Acorn with S3 client."""
        self.bucket = "allen-data-views"
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
        logging.info(
            SquirrelMessage(
                tree="S3Tree", acorn=table_name, message=f"Stored cache to s3://{self.bucket}/{s3_key}"
            ).to_json()
        )

        metadata = {"columns": data.columns.tolist()}
        if table_name.startswith("qc/"):
            json_key = "data-asset-cache/zs_qc.json"
        else:
            json_filename = filename.replace(".pqt", ".json")
            json_key = get_s3_cache_path(json_filename)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=json_key,
            Body=json.dumps(metadata),
        )

    def scurry(self, table_name: str | list[str]) -> pd.DataFrame:
        """Fetch DataFrame from S3 parquet file(s).

        When given a list of table names, merges them using DuckDB
        and adds an 'asset_name' column.
        """
        if isinstance(table_name, list):
            return self._scurry_multiple(table_name)
        return self._scurry_single(table_name)

    def _s3_key_exists(self, s3_key: str) -> bool:
        """Return True if the S3 object exists."""
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=s3_key)
            return True
        except self.s3_client.exceptions.ClientError:
            return False

    def _scurry_single(self, table_name: str) -> pd.DataFrame:
        """Fetch a single table from S3."""
        filename = prefix_table_name(table_name)
        s3_key = get_s3_cache_path(filename)

        if not self._s3_key_exists(s3_key):
            logging.warning(
                SquirrelMessage(
                    tree="S3Tree", acorn=table_name, message=f"Cache not found: {s3_key}"
                ).to_json()
            )
            return pd.DataFrame()

        try:
            query = f"""
                SELECT * FROM read_parquet(
                    's3://{self.bucket}/{s3_key}'
                )
            """
            result = duckdb.query(query).to_df()
            logging.info(
                SquirrelMessage(
                    tree="S3Tree", acorn=table_name, message=f"Retrieved cache from s3://{self.bucket}/{s3_key}"
                ).to_json()
            )
            return result
        except Exception as e:
            logging.warning(
                SquirrelMessage(
                    tree="S3Tree", acorn=table_name, message=f"Error fetching from cache {s3_key}: {e}"
                ).to_json()
            )
            return pd.DataFrame()

    def get_location(self, table_name: str, partitioned: bool = False) -> str:
        """Return the S3 URI for a given table."""
        if partitioned:
            return f"s3://{self.bucket}/data-asset-cache/zs_{table_name}/"
        filename = prefix_table_name(table_name)
        s3_key = get_s3_cache_path(filename)
        return f"s3://{self.bucket}/{s3_key}"

    def plant(self, key: str, data: str) -> None:  # pragma: no cover
        """Write a JSON string to the zombie-squirrel root in S3."""
        s3_key = f"data-asset-cache/{key}"
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=data.encode(),
            ContentType="application/json",
        )
        logging.info(
            SquirrelMessage(
                tree="S3Tree", acorn=key, message=f"Published metadata to s3://{self.bucket}/{s3_key}"
            ).to_json()
        )

    def _scurry_multiple(self, table_names: list[str]) -> pd.DataFrame:
        """Fetch and merge multiple tables from S3."""
        parquet_paths = []
        asset_names = []

        for tbl_name in table_names:
            filename = prefix_table_name(tbl_name)
            s3_key = get_s3_cache_path(filename)
            s3_path = f"s3://{self.bucket}/{s3_key}"
            parquet_paths.append(f"'{s3_path}'")
            asset_names.append(tbl_name)

        try:
            union_query = " UNION ALL ".join(
                [
                    f"SELECT *, '{asset}' as asset_name FROM read_parquet({path})"
                    for path, asset in zip(parquet_paths, asset_names, strict=False)
                ]
            )
            result = duckdb.query(union_query).to_df()
            logging.info(
                SquirrelMessage(
                    tree="S3Tree", acorn="merged", message=f"Merged {len(table_names)} tables from S3"
                ).to_json()
            )
            return result
        except Exception as e:
            logging.warning(
                SquirrelMessage(tree="S3Tree", acorn="merged", message=f"Error merging tables: {e}").to_json()
            )
            return pd.DataFrame()


class MemoryTree(Tree):
    """A simple in-memory backend for testing or local development."""

    def __init__(self) -> None:
        """Initialize MemoryAcorn with empty store."""
        super().__init__()
        self._store: dict[str, pd.DataFrame] = {}
        self._json_store: dict[str, str] = {}

    def hide(self, table_name: str, data: pd.DataFrame) -> None:
        """Store DataFrame in memory."""
        logging.info(
            SquirrelMessage(
                tree="MemoryTree", acorn=table_name, message=f"Storing cache in memory for {table_name}"
            ).to_json()
        )
        self._store[table_name] = data

    def scurry(self, table_name: str | list[str]) -> pd.DataFrame:
        """Fetch DataFrame from memory.

        When given a list of table names, merges them and adds
        an 'asset_name' column.
        """
        if isinstance(table_name, list):
            return self._scurry_multiple(table_name)
        return self._scurry_single(table_name)

    def _scurry_single(self, table_name: str) -> pd.DataFrame:
        """Fetch a single table from memory."""
        logging.info(
            SquirrelMessage(
                tree="MemoryTree", acorn=table_name, message=f"Fetching cache from memory for {table_name}"
            ).to_json()
        )
        return self._store.get(table_name, pd.DataFrame())

    def get_location(self, table_name: str, partitioned: bool = False) -> str:
        """Return the in-memory identifier for a given table."""
        if partitioned:
            return f"{table_name}/"
        return prefix_table_name(table_name)

    def plant(self, key: str, data: str) -> None:
        """Store a JSON string in the in-memory JSON store."""
        logging.info(
            SquirrelMessage(tree="MemoryTree", acorn=key, message=f"Storing metadata in memory for {key}").to_json()
        )
        self._json_store[key] = data

    def _scurry_multiple(self, table_names: list[str]) -> pd.DataFrame:
        """Fetch and merge multiple tables from memory."""
        dfs = []
        for tbl_name in table_names:
            df = self._store.get(tbl_name, pd.DataFrame())
            if not df.empty:
                df = df.copy()
                df["asset_name"] = tbl_name
                dfs.append(df)

        if not dfs:
            logging.warning(
                SquirrelMessage(
                    tree="MemoryTree", acorn="merged", message=f"No valid tables found among {table_names}"
                ).to_json()
            )
            return pd.DataFrame()

        result = pd.concat(dfs, ignore_index=True)
        logging.info(
            SquirrelMessage(
                tree="MemoryTree", acorn="merged", message=f"Merged {len(dfs)} tables from memory"
            ).to_json()
        )
        return result

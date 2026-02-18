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
    def hide(self, table_name: str, data: pd.DataFrame, write_metadata: bool = True) -> None:
        """Store records in the cache.

        Args:
            table_name: Name of the table to cache.
            data: DataFrame to cache.
            write_metadata: If True, write metadata JSON with column names. Default True.
        """
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


class S3Tree(Tree):
    """Stores and retrieves caches using AWS S3 with parquet files."""

    def __init__(self) -> None:
        """Initialize S3Acorn with S3 client."""
        self.bucket = "aind-scratch-data"
        self.s3_client = boto3.client("s3")

    def hide(self, table_name: str, data: pd.DataFrame, write_metadata: bool = True) -> None:
        """Store DataFrame as parquet file in S3.

        Args:
            table_name: Name of the table to cache.
            data: DataFrame to cache.
            write_metadata: If True, write metadata JSON with column names. Default True.
        """
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

        if write_metadata:
            self._write_metadata_json(table_name, data)

    def scurry(self, table_name: str | list[str]) -> pd.DataFrame:
        """Fetch DataFrame from S3 parquet file(s).

        When given a list of table names, merges them using DuckDB
        and adds an 'asset_name' column.
        """
        if isinstance(table_name, list):
            return self._scurry_multiple(table_name)
        return self._scurry_single(table_name)

    def _scurry_single(self, table_name: str) -> pd.DataFrame:
        """Fetch a single table from S3."""
        filename = prefix_table_name(table_name)
        s3_key = get_s3_cache_path(filename)

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

    def _write_metadata_json(self, table_name: str, data: pd.DataFrame) -> None:
        """Write metadata JSON file with DataFrame column names."""
        # For QC tables (qc/* pattern), write to top-level zs_qc.json
        if table_name.startswith("qc/"):
            json_filename = "zs_qc.json"
        else:
            base_name = prefix_table_name(table_name)
            json_filename = base_name.replace(".pqt", ".json")

        json_key = get_s3_cache_path(json_filename)
        metadata = {"columns": data.columns.tolist()}

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=json_key,
            Body=json.dumps(metadata, indent=2),
        )
        logging.info(
            SquirrelMessage(
                tree="S3Tree",
                acorn=table_name,
                message=f"Stored metadata to s3://{self.bucket}/{json_key}",
            ).to_json()
        )


class MemoryTree(Tree):
    """A simple in-memory backend for testing or local development."""

    def __init__(self) -> None:
        """Initialize MemoryAcorn with empty store."""
        super().__init__()
        self._store: dict[str, pd.DataFrame] = {}

    def hide(self, table_name: str, data: pd.DataFrame, write_metadata: bool = True) -> None:
        """Store DataFrame in memory.

        Args:
            table_name: Name of the table to cache.
            data: DataFrame to cache.
            write_metadata: If True, write metadata JSON with column names. Default True.
        """
        logging.info(
            SquirrelMessage(
                tree="MemoryTree", acorn=table_name, message=f"Storing cache in memory for {table_name}"
            ).to_json()
        )
        self._store[table_name] = data

        if write_metadata:
            self._write_metadata_json(table_name, data)

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

    def _write_metadata_json(self, table_name: str, data: pd.DataFrame) -> None:
        """Write metadata JSON (no-op for MemoryTree)."""
        # For MemoryTree, we don't write to disk
        if table_name.startswith("qc/"):
            json_filename = "zs_qc.json"
        else:
            base_name = prefix_table_name(table_name)
            json_filename = base_name.replace(".pqt", ".json")

        logging.info(
            SquirrelMessage(
                tree="MemoryTree",
                acorn=table_name,
                message=f"Metadata would be stored as {json_filename}",
            ).to_json()
        )

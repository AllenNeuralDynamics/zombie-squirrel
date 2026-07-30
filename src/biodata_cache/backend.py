"""Storage backend interfaces for caching data."""

import io
import json
import logging
from abc import ABC, abstractmethod

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from biodata_cache.utils import BDC_VERSION, CacheLogMessage, duckdb_query

_CACHE_ROOT = "data-asset-cache"
_VERSION_FOLDER = f"bdc-v{BDC_VERSION}"

HIVE_PARTITION_KEYS = {
    "qc": "subject_id",
    "qc_tag_status": "subject_id",
    "platform_qc": "platform",
    "platform_dynamic_foraging_trials": "subject_id",
    "platform_dynamic_foraging_events": "subject_id",
    "platform_fib_traces": "asset_name",
    "platform_fib_operations": "asset_name",
    "platform_df_operations": "asset_name",
    "platform_ecephys_spikes": "asset_name",
    "platform_ecephys_units": "asset_name",
    "platform_pophys": "asset_name",
}
# S3 error codes that mean the object genuinely does not exist (a legitimate empty
# cache) as opposed to a read failure.
_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}



class Backend(ABC):
    """Base class for a cache storage backend."""

    def __init__(self) -> None:
        """Initialize the Backend."""
        super().__init__()

    @abstractmethod
    def write(self, table_name: str, data: pd.DataFrame) -> None:
        """Write records to the cache."""
        pass  # pragma: no cover

    @abstractmethod
    def read(self, table_name: str | list[str]) -> pd.DataFrame:
        """Read records from the cache.

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
    def put_json(self, key: str, data: str) -> None:
        """Write a JSON string to the storage root under the given key."""
        pass  # pragma: no cover

    @abstractmethod
    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        """Write raw bytes (e.g. a PNG) to the storage root under the given key."""
        pass  # pragma: no cover

    @abstractmethod
    def get_json(self, key: str) -> str:
        """Read a JSON string from the storage root under the given key."""
        pass  # pragma: no cover

    @abstractmethod
    def get_versions_index(self) -> list[str]:
        """Return the list of all available version folders from cache_versions.json."""
        pass  # pragma: no cover

    @abstractmethod
    def register_version(self) -> None:
        """Add the active version folder to the top-level cache_versions.json index."""
        pass  # pragma: no cover

    @abstractmethod
    def put_registry_fragment(self, name: str, data: str) -> None:
        """Write a single cache-table registry fragment (cache_registry/<name>.json).

        Each sync job writes only its own fragment(s), so parallel jobs never
        contend on a single cache_registry.json object. A re-run overwrites the
        fragment in place.
        """
        pass  # pragma: no cover

    @abstractmethod
    def list_registry_fragments(self) -> list[str]:
        """Return the JSON strings of every registry fragment for the active version."""
        pass  # pragma: no cover

    @abstractmethod
    def clear_registry(self) -> None:
        """Remove all registry fragments for the active version (fresh-run reset)."""
        pass  # pragma: no cover

    def partition_exists(self, table_name: str) -> bool:
        """Return True if data already exists for the given partition."""
        return False


class S3Backend(Backend):
    """Stores and retrieves caches using AWS S3 with parquet files."""

    def __init__(self) -> None:
        """Initialize S3Backend with S3 client."""
        self.bucket = "allen-data-views"
        self.s3_client = boto3.client("s3")

    def write(self, table_name: str, data: pd.DataFrame) -> None:
        """Store DataFrame as parquet file in S3."""
        if "/" in table_name:
            base, value = table_name.split("/", 1)
            partition_key = HIVE_PARTITION_KEYS[base]
            s3_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}/{partition_key}={value}/data.pqt"
            json_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}.json"
        else:
            s3_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{table_name}.pqt"
            json_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{table_name}.json"

        parquet_buffer = io.BytesIO()
        table = pa.Table.from_pandas(data, preserve_index=False)
        float_cols = [f.name for f in table.schema if pa.types.is_floating(f.type)]
        dict_cols = [f.name for f in table.schema if f.name not in float_cols]
        pq.write_table(
            table,
            parquet_buffer,
            compression="zstd",
            use_dictionary=dict_cols if dict_cols else False,
            column_encoding={col: "BYTE_STREAM_SPLIT" for col in float_cols} or None,
        )
        parquet_buffer.seek(0)

        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=parquet_buffer.getvalue(),
        )
        logging.info(
            CacheLogMessage(
                backend="S3Backend", table=table_name, message=f"Stored cache to s3://{self.bucket}/{s3_key}"
            ).to_json()
        )

        metadata = {"columns": data.columns.tolist()}
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=json_key,
            Body=json.dumps(metadata),
        )

    def read(self, table_name: str | list[str]) -> pd.DataFrame:
        """Fetch DataFrame from S3 parquet file(s).

        When given a list of table names, merges them using DuckDB
        and adds an 'asset_name' column.
        """
        if isinstance(table_name, list):
            return self._read_multiple(table_name)
        return self._read_single(table_name)

    def clear_partition(self, table_name: str) -> None:
        """Delete all parquet chunk files in a hive partition."""
        if "/" not in table_name:
            return
        base, value = table_name.split("/", 1)
        partition_key = HIVE_PARTITION_KEYS[base]
        prefix = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}/{partition_key}={value}/"
        paginator = self.s3_client.get_paginator("list_objects_v2")
        to_delete = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                to_delete.append({"Key": obj["Key"]})
        for i in range(0, len(to_delete), 1000):
            self.s3_client.delete_objects(
                Bucket=self.bucket,
                Delete={"Objects": to_delete[i : i + 1000]},
            )

    def partition_exists(self, table_name: str) -> bool:
        """Return True if any parquet chunk exists for a hive partition."""
        if "/" not in table_name:
            return False
        base, value = table_name.split("/", 1)
        partition_key = HIVE_PARTITION_KEYS[base]
        prefix = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}/{partition_key}={value}/"
        resp = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
        return resp.get("KeyCount", 0) > 0

    def write_chunk(self, table_name: str, data: pd.DataFrame, chunk_idx: int) -> None:
        """Append one numbered parquet chunk to a hive partition."""
        base, value = table_name.split("/", 1)
        partition_key = HIVE_PARTITION_KEYS[base]
        s3_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}/{partition_key}={value}/data_{chunk_idx:04d}.pqt"
        json_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}.json"

        parquet_buffer = io.BytesIO()
        table = pa.Table.from_pandas(data, preserve_index=False)
        float_cols = [f.name for f in table.schema if pa.types.is_floating(f.type)]
        dict_cols = [f.name for f in table.schema if f.name not in float_cols]
        pq.write_table(
            table,
            parquet_buffer,
            compression="zstd",
            use_dictionary=dict_cols if dict_cols else False,
            column_encoding={col: "BYTE_STREAM_SPLIT" for col in float_cols} or None,
        )
        parquet_buffer.seek(0)
        self.s3_client.put_object(Bucket=self.bucket, Key=s3_key, Body=parquet_buffer.getvalue())
        logging.info(
            CacheLogMessage(
                backend="S3Backend", table=table_name, message=f"Stored chunk {chunk_idx} to s3://{self.bucket}/{s3_key}"
            ).to_json()
        )
        metadata = {"columns": data.columns.tolist()}
        self.s3_client.put_object(
            Bucket=self.bucket, Key=json_key, Body=json.dumps(metadata)
        )

    def _cache_object_exists(self, table_name: str) -> bool:
        """Return True if the cache object(s) for a table exist in S3.

        Used to distinguish a genuinely absent cache (empty, safe to (re)populate)
        from a read failure (which must be raised, never silently treated as empty).
        Raises on ambiguous S3 errors so failures are never mistaken for absence.
        """
        if "/" in table_name:
            base, value = table_name.split("/", 1)
            partition_key = HIVE_PARTITION_KEYS[base]
            prefix = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}/{partition_key}={value}/"
            resp = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=prefix, MaxKeys=1)
            return resp.get("KeyCount", 0) > 0

        key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{table_name}.pqt"
        try:
            self.s3_client.head_object(Bucket=self.bucket, Key=key)
            return True
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code in _NOT_FOUND_CODES:
                return False
            raise

    def _read_single(self, table_name: str) -> pd.DataFrame:
        """Fetch a single table from S3.

        A genuinely absent cache object returns an empty DataFrame; any other read
        failure is raised rather than silently returned as empty, so a transient
        error can never masquerade as an empty cache and trigger a re-fetch.
        """
        if "/" in table_name:
            base, value = table_name.split("/", 1)
            partition_key = HIVE_PARTITION_KEYS[base]
            s3_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}/{partition_key}={value}/data*.pqt"
        else:
            s3_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{table_name}.pqt"

        query = f"""
            SELECT * FROM read_parquet(
                's3://{self.bucket}/{s3_key}'
            )
        """
        try:
            result = duckdb_query(query)
            logging.info(
                CacheLogMessage(
                    backend="S3Backend", table=table_name, message=f"Retrieved cache from s3://{self.bucket}/{s3_key}"
                ).to_json()
            )
            return result
        except Exception as e:
            if not self._cache_object_exists(table_name):
                logging.info(
                    CacheLogMessage(
                        backend="S3Backend", table=table_name, message=f"No cache found at s3://{self.bucket}/{s3_key}"
                    ).to_json()
                )
                return pd.DataFrame()
            logging.error(
                CacheLogMessage(
                    backend="S3Backend", table=table_name, message=f"Error fetching from cache {s3_key}: {e}"
                ).to_json()
            )
            raise


    def get_location(self, table_name: str, partitioned: bool = False) -> str:
        """Return the S3 URI for a given table."""
        if partitioned:
            return f"s3://{self.bucket}/{_CACHE_ROOT}/{_VERSION_FOLDER}/{table_name}/"
        if "/" in table_name:
            base, value = table_name.split("/", 1)
            partition_key = HIVE_PARTITION_KEYS[base]
            return f"s3://{self.bucket}/{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}/{partition_key}={value}/data.pqt"
        return f"s3://{self.bucket}/{_CACHE_ROOT}/{_VERSION_FOLDER}/{table_name}.pqt"

    def put_json(self, key: str, data: str) -> None:  # pragma: no cover
        """Write a JSON string to the versioned folder in S3 and update the index."""
        s3_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{key}"
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=data.encode(),
            ContentType="application/json",
        )
        logging.info(
            CacheLogMessage(
                backend="S3Backend", table=key, message=f"Published metadata to s3://{self.bucket}/{s3_key}"
            ).to_json()
        )
        self.register_version()

    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:  # pragma: no cover
        """Write raw bytes to the versioned folder in S3."""
        s3_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{key}"
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=data,
            ContentType=content_type,
        )
        logging.info(
            CacheLogMessage(
                backend="S3Backend", table=key, message=f"Published bytes to s3://{self.bucket}/{s3_key}"
            ).to_json()
        )

    def register_version(self) -> None:  # pragma: no cover
        """Add the active version folder to the top-level cache_versions.json index."""
        index_key = f"{_CACHE_ROOT}/cache_versions.json"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=index_key)
            existing = json.loads(response["Body"].read().decode())
        except Exception:
            existing = []
        if _VERSION_FOLDER not in existing:
            existing.append(_VERSION_FOLDER)
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=index_key,
            Body=json.dumps(existing).encode(),
            ContentType="application/json",
        )

    def _registry_prefix(self) -> str:
        """Return the S3 prefix that holds the per-table registry fragments."""
        return f"{_CACHE_ROOT}/{_VERSION_FOLDER}/cache_registry/"

    def put_registry_fragment(self, name: str, data: str) -> None:  # pragma: no cover
        """Write a single cache-table registry fragment to S3."""
        s3_key = f"{self._registry_prefix()}{name}.json"
        self.s3_client.put_object(
            Bucket=self.bucket,
            Key=s3_key,
            Body=data.encode(),
            ContentType="application/json",
        )
        logging.info(
            CacheLogMessage(
                backend="S3Backend", table=name, message=f"Published registry fragment to s3://{self.bucket}/{s3_key}"
            ).to_json()
        )

    def list_registry_fragments(self) -> list[str]:  # pragma: no cover
        """Return the JSON strings of every registry fragment for the active version."""
        paginator = self.s3_client.get_paginator("list_objects_v2")
        fragments = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._registry_prefix()):
            for obj in page.get("Contents", []):
                if not obj["Key"].endswith(".json"):
                    continue
                response = self.s3_client.get_object(Bucket=self.bucket, Key=obj["Key"])
                fragments.append(response["Body"].read().decode())
        return fragments

    def clear_registry(self) -> None:  # pragma: no cover
        """Delete every registry fragment for the active version."""
        paginator = self.s3_client.get_paginator("list_objects_v2")
        to_delete = []
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._registry_prefix()):
            for obj in page.get("Contents", []):
                to_delete.append({"Key": obj["Key"]})
        for i in range(0, len(to_delete), 1000):
            self.s3_client.delete_objects(Bucket=self.bucket, Delete={"Objects": to_delete[i : i + 1000]})

    def get_json(self, key: str) -> str:  # pragma: no cover
        """Read a JSON string from the versioned folder in S3."""
        s3_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{key}"
        response = self.s3_client.get_object(Bucket=self.bucket, Key=s3_key)
        return response["Body"].read().decode()

    def get_versions_index(self) -> list[str]:  # pragma: no cover
        """Return the list of all available version folders from the top-level cache_versions.json."""
        index_key = f"{_CACHE_ROOT}/cache_versions.json"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=index_key)
            return json.loads(response["Body"].read().decode())
        except Exception:
            return []

    def _read_multiple(self, table_names: list[str]) -> pd.DataFrame:
        """Fetch and merge multiple tables from S3."""
        parquet_paths = []
        asset_names = []

        for tbl_name in table_names:
            s3_key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{tbl_name}.pqt"
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
            result = duckdb_query(union_query)
            logging.info(
                CacheLogMessage(
                    backend="S3Backend", table="merged", message=f"Merged {len(table_names)} tables from S3"
                ).to_json()
            )
            return result
        except Exception as e:
            if any(not self._cache_object_exists(tbl_name) for tbl_name in table_names):
                logging.info(
                    CacheLogMessage(
                        backend="S3Backend", table="merged", message="No cache found for one or more merged tables"
                    ).to_json()
                )
                return pd.DataFrame()
            logging.error(
                CacheLogMessage(backend="S3Backend", table="merged", message=f"Error merging tables: {e}").to_json()
            )
            raise


class MemoryBackend(Backend):
    """A simple in-memory backend for testing or local development."""

    def __init__(self) -> None:
        """Initialize MemoryBackend with empty store."""
        super().__init__()
        self._store: dict[str, pd.DataFrame] = {}
        self._json_store: dict[str, str] = {}
        self._bytes_store: dict[str, bytes] = {}

    def write(self, table_name: str, data: pd.DataFrame) -> None:
        """Store DataFrame in memory."""
        logging.info(
            CacheLogMessage(
                backend="MemoryBackend", table=table_name, message=f"Storing cache in memory for {table_name}"
            ).to_json()
        )
        self._store[table_name] = data

    def read(self, table_name: str | list[str]) -> pd.DataFrame:
        """Fetch DataFrame from memory.

        When given a list of table names, merges them and adds
        an 'asset_name' column.
        """
        if isinstance(table_name, list):
            return self._read_multiple(table_name)
        return self._read_single(table_name)

    def _read_single(self, table_name: str) -> pd.DataFrame:
        """Fetch a single table from memory."""
        logging.info(
            CacheLogMessage(
                backend="MemoryBackend", table=table_name, message=f"Fetching cache from memory for {table_name}"
            ).to_json()
        )
        return self._store.get(table_name, pd.DataFrame())

    def get_location(self, table_name: str, partitioned: bool = False) -> str:
        """Return the in-memory identifier for a given table."""
        if partitioned:
            return f"{_VERSION_FOLDER}/{table_name}/"
        if "/" in table_name:
            base, value = table_name.split("/", 1)
            partition_key = HIVE_PARTITION_KEYS[base]
            return f"{_VERSION_FOLDER}/{base}/{partition_key}={value}/data.pqt"
        return f"{_VERSION_FOLDER}/{table_name}.pqt"

    def put_json(self, key: str, data: str) -> None:
        """Store a JSON string in the versioned in-memory JSON store and update index."""
        logging.info(
            CacheLogMessage(
                backend="MemoryBackend", table=key, message=f"Storing metadata in memory for {key}"
            ).to_json()
        )
        self._json_store[f"{_VERSION_FOLDER}/{key}"] = data
        self.register_version()

    def put_bytes(self, key: str, data: bytes, content_type: str) -> None:
        """Store raw bytes in the versioned in-memory bytes store."""
        self._bytes_store[f"{_VERSION_FOLDER}/{key}"] = data

    def register_version(self) -> None:
        """Add the active version folder to the in-memory cache_versions.json index."""
        existing = json.loads(self._json_store.get("cache_versions.json", "[]"))
        if _VERSION_FOLDER not in existing:
            existing.append(_VERSION_FOLDER)
        self._json_store["cache_versions.json"] = json.dumps(existing)

    def _registry_prefix(self) -> str:
        """Return the in-memory key prefix that holds the per-table registry fragments."""
        return f"{_VERSION_FOLDER}/cache_registry/"

    def put_registry_fragment(self, name: str, data: str) -> None:
        """Store a single cache-table registry fragment in memory."""
        self._json_store[f"{self._registry_prefix()}{name}.json"] = data

    def list_registry_fragments(self) -> list[str]:
        """Return the JSON strings of every registry fragment for the active version."""
        prefix = self._registry_prefix()
        return [value for key, value in self._json_store.items() if key.startswith(prefix)]

    def clear_registry(self) -> None:
        """Remove all in-memory registry fragments for the active version."""
        prefix = self._registry_prefix()
        for key in [k for k in self._json_store if k.startswith(prefix)]:
            del self._json_store[key]

    def get_json(self, key: str) -> str:
        """Read a JSON string from the versioned in-memory JSON store."""
        return self._json_store.get(f"{_VERSION_FOLDER}/{key}", "{}")

    def get_versions_index(self) -> list[str]:
        """Return the list of all available version folders from the in-memory index."""
        return json.loads(self._json_store.get("cache_versions.json", "[]"))

    def partition_exists(self, table_name: str) -> bool:
        """Return True if a partition has stored data in memory."""
        df = self._store.get(table_name)
        return df is not None and not df.empty

    def clear_partition(self, table_name: str) -> None:
        """Remove all chunks stored for a partitioned table."""
        self._store.pop(table_name, None)

    def write_chunk(self, table_name: str, data: pd.DataFrame, chunk_idx: int) -> None:
        """Append one chunk to the in-memory store for a partitioned table."""
        existing = self._store.get(table_name, pd.DataFrame())
        self._store[table_name] = (
            pd.concat([existing, data], ignore_index=True) if not existing.empty else data.copy()
        )

    def _read_multiple(self, table_names: list[str]) -> pd.DataFrame:
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
                CacheLogMessage(
                    backend="MemoryBackend", table="merged", message=f"No valid tables found among {table_names}"
                ).to_json()
            )
            return pd.DataFrame()

        result = pd.concat(dfs, ignore_index=True)
        logging.info(
            CacheLogMessage(
                backend="MemoryBackend", table="merged", message=f"Merged {len(dfs)} tables from memory"
            ).to_json()
        )
        return result

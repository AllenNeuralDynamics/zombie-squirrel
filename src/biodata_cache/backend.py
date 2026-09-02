"""Storage backend interfaces for caching data."""

import io
import json
import logging
import threading
from abc import ABC, abstractmethod
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Literal

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from biodata_cache.table_specs import PARTITION_KEYS
from biodata_cache.utils import BDC_VERSION, CacheLogMessage, duckdb_query

_CACHE_ROOT = "data-asset-cache"
_VERSION_FOLDER = f"bdc-v{BDC_VERSION}"

# Compatibility alias for callers that imported the old backend constant.
HIVE_PARTITION_KEYS = PARTITION_KEYS
# S3 error codes that mean the object genuinely does not exist (a legitimate empty
# cache) as opposed to a read failure.
_NOT_FOUND_CODES = {"404", "NoSuchKey", "NotFound"}

PredicateOperator = Literal["eq", "contains", "lt", "gte"]


@dataclass(frozen=True, slots=True)
class Predicate:
    """A typed, parameterized filter for :meth:`Backend.read_filtered`."""

    column: str
    operator: PredicateOperator
    value: object

    def __post_init__(self) -> None:
        """Validate the predicate operator at construction time."""
        if self.operator not in {"eq", "contains", "lt", "gte"}:
            raise ValueError(f"Unsupported predicate operator: {self.operator!r}")


def _normalize_filters(
    filters: Sequence[Predicate] | Mapping[str, object] | None,
) -> tuple[Predicate, ...]:
    """Normalize the public filter forms into typed predicates.

    A mapping is accepted as a convenience for exact-match filters. Values may
    also be ``(operator, value)`` pairs when a non-equality predicate is needed.
    No form accepts SQL fragments.
    """
    if filters is None:
        return ()
    if isinstance(filters, Mapping):
        normalized = []
        for column, value in filters.items():
            if isinstance(value, tuple) and len(value) == 2 and value[0] in {"eq", "contains", "lt", "gte"}:
                normalized.append(Predicate(column, value[0], value[1]))
            else:
                normalized.append(Predicate(column, "eq", value))
        return tuple(normalized)
    if isinstance(filters, (str, bytes)):
        raise TypeError("filters must be a sequence of Predicate objects or a mapping")
    normalized = tuple(filters)
    if not all(isinstance(predicate, Predicate) for predicate in normalized):
        raise TypeError("filters must contain only Predicate objects")
    return normalized


def _validate_pagination(limit: int | None, offset: int) -> None:
    """Validate pagination values before interpolating safe integer clauses."""
    if limit is not None and (not isinstance(limit, int) or isinstance(limit, bool) or limit < 0):
        raise ValueError("limit must be a non-negative integer or None")
    if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
        raise ValueError("offset must be a non-negative integer")


def _validated_identifier(name: str, available_columns: set[str] | None = None) -> str:
    """Return a safely quoted column identifier after schema validation."""
    if not isinstance(name, str) or not name or "\x00" in name:
        raise ValueError(f"Invalid column name: {name!r}")
    if available_columns is not None and name not in available_columns:
        raise ValueError(f"Unknown column: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def _empty_filtered_result(
    columns: Sequence[str] | None,
    include_total: bool,
) -> pd.DataFrame | tuple[pd.DataFrame, int]:
    """Build the empty result shape used for absent caches and no matches."""
    result = pd.DataFrame(columns=list(columns)) if columns is not None else pd.DataFrame()
    return (result, 0) if include_total else result


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

    def cache_exists(self, table_name: str) -> bool:
        """Return whether a non-empty cache is available for ``table_name``.

        Backends with a cheap storage-level existence check should override this
        method. The fallback preserves compatibility for custom backends.
        """
        return not self.read(table_name).empty

    def read_filtered(
        self,
        table_name: str,
        *,
        filters: Sequence[Predicate] | Mapping[str, object] | None = None,
        columns: Sequence[str] | None = None,
        order_by: str | None = None,
        limit: int | None = 100,
        offset: int = 0,
        include_total: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, int]:
        """Read a filtered, projected page from one cache table.

        ``filters`` contains typed :class:`Predicate` values (or a mapping for
        equality filters); callers never provide SQL. Concrete backends provide
        the execution strategy.
        """
        raise NotImplementedError(f"{self.__class__.__name__} does not support filtered reads")

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
        # Column sidecars already written by this process, keyed by object key.
        # See _put_columns_sidecar.
        self._sidecar_columns: dict[str, tuple[str, ...]] = {}
        self._sidecar_lock = threading.Lock()

    def _put_columns_sidecar(self, json_key: str, data: pd.DataFrame) -> None:
        """Write the ``<table>.json`` column sidecar, skipping an unchanged rewrite.

        For a partitioned table this sidecar describes the *table*, not the
        partition, so writing it inside every partition write meant one redundant
        PUT of identical bytes per partition -- 5000+ of them for a table like
        ``cell_properties`` or ``platform_pophys``, each an extra round trip in a
        job that is entirely round-trip bound. Remembering what this process has
        already written collapses that to one PUT per distinct column list.

        A table whose partitions genuinely differ in columns (e.g.
        ``platform_ecephys_units``, whose set varies by sorting-pipeline version)
        still gets a write whenever the list changes, so the sidecar ends up
        describing one real partition exactly as before -- previously it was
        whichever partition happened to be written last, which was already
        arbitrary.
        """
        columns = tuple(data.columns)
        with self._sidecar_lock:
            if self._sidecar_columns.get(json_key) == columns:
                return
            # Claim it before the PUT so concurrent writers do not duplicate it.
            self._sidecar_columns[json_key] = columns
        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=json_key,
                Body=json.dumps({"columns": list(columns)}),
            )
        except Exception:
            # Do not let a failed PUT leave the key marked as written.
            with self._sidecar_lock:
                self._sidecar_columns.pop(json_key, None)
            raise

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

        self._put_columns_sidecar(json_key, data)

    def read(self, table_name: str | list[str]) -> pd.DataFrame:
        """Fetch DataFrame from S3 parquet file(s).

        When given a list of table names, merges them using DuckDB
        and adds an 'asset_name' column.
        """
        if isinstance(table_name, list):
            return self._read_multiple(table_name)
        return self._read_single(table_name)

    def cache_exists(self, table_name: str) -> bool:
        """Return whether a non-empty cache object is available in S3."""
        return self._cache_object_exists(table_name)

    @staticmethod
    def _sql_string_literal(value: str) -> str:
        """Escape a string used as a fixed DuckDB string literal."""
        return value.replace("'", "''")

    def _parquet_key(self, table_name: str) -> str:
        """Return the versioned S3 key or glob for a table."""
        if "/" in table_name:
            base, value = table_name.split("/", 1)
            partition_key = HIVE_PARTITION_KEYS[base]
            return f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}/{partition_key}={value}/data*.pqt"
        return f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{table_name}.pqt"

    def _parquet_uri(self, table_name: str) -> str:
        """Return the S3 URI used by DuckDB for a table."""
        return f"s3://{self.bucket}/{self._parquet_key(table_name)}"

    def _read_sidecar_columns(self, table_name: str) -> list[str] | None:
        """Read a table's published column sidecar when it is available."""
        base = table_name.split("/", 1)[0]
        key = f"{_CACHE_ROOT}/{_VERSION_FOLDER}/{base}.json"
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            body = response["Body"].read()
            if isinstance(body, bytes):
                body = body.decode()
            columns = json.loads(body).get("columns")
            if isinstance(columns, list) and all(isinstance(column, str) for column in columns):
                return columns
        except ClientError as exc:
            code = str(exc.response.get("Error", {}).get("Code", ""))
            if code not in _NOT_FOUND_CODES:
                raise
        except (AttributeError, KeyError, TypeError, ValueError):
            # Older or malformed sidecars are handled by the DuckDB schema
            # fallback below. The parquet object remains the source of truth.
            pass
        return None

    def _table_columns(self, table_name: str) -> list[str]:
        """Return the columns in a cache table, preferring its sidecar."""
        columns = self._read_sidecar_columns(table_name)
        if columns is not None:
            return columns

        schema = duckdb_query(
            f"DESCRIBE SELECT * FROM read_parquet('{self._sql_string_literal(self._parquet_uri(table_name))}')"
        )
        if "column_name" not in schema:
            raise RuntimeError(f"Could not determine schema for cache table {table_name!r}")
        return schema["column_name"].tolist()

    def read_filtered(
        self,
        table_name: str,
        *,
        filters: Sequence[Predicate] | Mapping[str, object] | None = None,
        columns: Sequence[str] | None = None,
        order_by: str | None = None,
        limit: int | None = 100,
        offset: int = 0,
        include_total: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, int]:
        """Read a filtered page directly from Parquet using DuckDB pushdown."""
        _validate_pagination(limit, offset)
        predicates = _normalize_filters(filters)
        requested_columns = None if columns is None else list(columns)
        if requested_columns is not None and not requested_columns:
            raise ValueError("columns must contain at least one column")

        if not self._cache_object_exists(table_name):
            return _empty_filtered_result(requested_columns, include_total)

        needs_schema = requested_columns is not None or bool(predicates) or order_by is not None
        available_columns = set(self._table_columns(table_name)) if needs_schema else None
        if requested_columns is not None:
            for column in requested_columns:
                _validated_identifier(column, available_columns)
        for predicate in predicates:
            _validated_identifier(predicate.column, available_columns)
        if order_by is not None:
            _validated_identifier(order_by, available_columns)

        select_sql = "*" if requested_columns is None else ", ".join(
            _validated_identifier(column, available_columns) for column in requested_columns
        )
        where_clauses = []
        parameters: list[object] = []
        for predicate in predicates:
            column = _validated_identifier(predicate.column, available_columns)
            if predicate.operator == "eq":
                if predicate.value is None:
                    where_clauses.append(f"{column} IS NULL")
                else:
                    where_clauses.append(f"{column} = ?")
                    parameters.append(predicate.value)
            elif predicate.operator == "contains":
                where_clauses.append(
                    f"contains(lower(CAST({column} AS VARCHAR)), lower(CAST(? AS VARCHAR)))"
                )
                parameters.append(str(predicate.value))
            else:
                comparison = "<" if predicate.operator == "lt" else ">="
                where_clauses.append(
                    f"TRY_CAST({column} AS TIMESTAMPTZ) {comparison} TRY_CAST(? AS TIMESTAMPTZ)"
                )
                parameters.append(predicate.value)

        source_sql = f"read_parquet('{self._sql_string_literal(self._parquet_uri(table_name))}')"
        where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        order_sql = f" ORDER BY {_validated_identifier(order_by, available_columns)} ASC NULLS LAST" if order_by else ""
        pagination_sql = f" LIMIT {limit}" if limit is not None else ""
        if offset:
            pagination_sql += f" OFFSET {offset}"
        query = f"SELECT {select_sql} FROM {source_sql}{where_sql}{order_sql}{pagination_sql}"

        try:
            total = None
            if include_total:
                total_query = f"SELECT COUNT(*) AS __total_matches FROM {source_sql}{where_sql}"
                total_result = duckdb_query(total_query, parameters)
                total = int(total_result.iloc[0]["__total_matches"]) if not total_result.empty else 0
            result = duckdb_query(query, parameters)
            if include_total:
                return result, total
            return result
        except Exception as exc:
            if not self._cache_object_exists(table_name):
                return _empty_filtered_result(requested_columns, include_total)
            logging.error(
                CacheLogMessage(
                    backend="S3Backend", table=table_name, message=f"Error fetching filtered cache: {exc}"
                ).to_json()
            )
            raise

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
                backend="S3Backend",
                table=table_name,
                message=f"Stored chunk {chunk_idx} to s3://{self.bucket}/{s3_key}",
            ).to_json()
        )
        self._put_columns_sidecar(json_key, data)

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

    def cache_exists(self, table_name: str) -> bool:
        """Return whether a non-empty table is available in memory."""
        data = self._store.get(table_name)
        return data is not None and not data.empty

    def read_filtered(
        self,
        table_name: str,
        *,
        filters: Sequence[Predicate] | Mapping[str, object] | None = None,
        columns: Sequence[str] | None = None,
        order_by: str | None = None,
        limit: int | None = 100,
        offset: int = 0,
        include_total: bool = False,
    ) -> pd.DataFrame | tuple[pd.DataFrame, int]:
        """Read a filtered page with the same semantics as :class:`S3Backend`."""
        _validate_pagination(limit, offset)
        predicates = _normalize_filters(filters)
        requested_columns = None if columns is None else list(columns)
        if requested_columns is not None and not requested_columns:
            raise ValueError("columns must contain at least one column")

        data = self._store.get(table_name)
        if data is None or data.empty:
            return _empty_filtered_result(requested_columns, include_total)

        available_columns = set(data.columns)
        if requested_columns is not None:
            for column in requested_columns:
                _validated_identifier(column, available_columns)
        for predicate in predicates:
            _validated_identifier(predicate.column, available_columns)
        if order_by is not None:
            _validated_identifier(order_by, available_columns)

        filtered = data.copy()
        for predicate in predicates:
            series = filtered[predicate.column]
            if predicate.operator == "eq":
                if predicate.value is None:
                    mask = series.isna()
                elif isinstance(predicate.value, str):
                    mask = series.astype("string") == predicate.value
                else:
                    mask = series == predicate.value
            elif predicate.operator == "contains":
                needle = str(predicate.value).lower()

                def contains_value(value, needle=needle) -> bool:
                    """Match a literal substring in scalar or list-valued cells."""
                    if isinstance(value, (list, tuple, set)):
                        return any(needle in str(item).lower() for item in value)
                    try:
                        if pd.isna(value):
                            return False
                    except (TypeError, ValueError):
                        pass
                    return needle in str(value).lower()

                mask = series.map(contains_value)
            else:
                values = pd.to_datetime(series, utc=True, errors="coerce")
                comparison_value = pd.to_datetime(predicate.value, utc=True, errors="raise")
                mask = values < comparison_value if predicate.operator == "lt" else values >= comparison_value
            filtered = filtered.loc[mask]

        if order_by is not None:
            filtered = filtered.sort_values(order_by, kind="stable", na_position="last")
        total = len(filtered)
        page = filtered.iloc[offset:] if limit is None else filtered.iloc[offset : offset + limit]
        if requested_columns is not None:
            page = page.loc[:, requested_columns]
        else:
            page = page.copy()
        return (page, total) if include_total else page

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
        self._store[table_name] = pd.concat([existing, data], ignore_index=True) if not existing.empty else data.copy()

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

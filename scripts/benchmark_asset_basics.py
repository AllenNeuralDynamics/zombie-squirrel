"""Compare DocDB and cache reads for the asset_basics data."""

from __future__ import annotations

import argparse
import json
import statistics
import time
from collections.abc import Callable
from dataclasses import dataclass

import duckdb
import requests
from aind_data_access_api.document_db import MetadataDbClient

from biodata_cache.backend import S3Backend
from biodata_cache.registry import API_GATEWAY_HOST

DOCDB_FIELDS = [
    "_created",
    "data_description.modalities",
    "data_description.project_name",
    "data_description.data_level",
    "subject.subject_id",
    "acquisition.acquisition_start_time",
    "acquisition.acquisition_end_time",
    "acquisition.acquisition_type",
    "acquisition.subject_details.date_of_birth",
    "acquisition.subject_details.year_of_birth",
    "processing.data_processes.start_date_time",
    "subject.subject_details.genotype",
    "other_identifiers",
    "location",
    "name",
    "acquisition.experimenters",
    "acquisition.instrument_id",
    "data_description.investigators",
]

CACHE_COLUMNS = [
    "_id",
    "_last_modified",
    "created",
    "modalities",
    "project_name",
    "data_level",
    "subject_id",
    "acquisition_start_time",
    "acquisition_end_time",
    "code_ocean",
    "process_date",
    "genotype",
    "age",
    "acquisition_type",
    "location",
    "name",
    "experimenters",
    "experimenters_normalized",
    "instrument_id",
    "instrument_id_normalized",
    "investigators",
    "investigators_normalized",
]


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    docdb_projection: dict[str, int]
    cache_columns: tuple[str, ...]
    docdb_via_api: bool = False


CASES = (
    BenchmarkCase("_id", {"_id": 1}, ("_id",)),
    BenchmarkCase(
        "nested acquisition type",
        {"acquisition.acquisition_type": 1},
        ("acquisition_type",),
    ),
    BenchmarkCase(
        "several nested fields",
        {
            "acquisition.acquisition_type": 1,
            "acquisition.acquisition_start_time": 1,
            "acquisition.instrument_id": 1,
            "data_description.project_name": 1,
            "subject.subject_id": 1,
        },
        ("acquisition_type", "acquisition_start_time", "instrument_id", "project_name", "subject_id"),
        True,
    ),
    BenchmarkCase(
        "asset_basics projection",
        {field: 1 for field in ["_id", "_last_modified", *DOCDB_FIELDS]},
        tuple(CACHE_COLUMNS),
        True,
    ),
)


def pull_docdb_http(host: str, projection: dict[str, int]) -> list[dict]:
    url = f"https://{host}/v2/metadata_index/data_assets"
    response = requests.get(
        url,
        params={
            "filter": json.dumps({}),
            "projection": json.dumps(projection),
            "limit": "0",
        },
        timeout=300,
    )
    response.raise_for_status()
    return response.json()


def pull_docdb_api(client: MetadataDbClient, projection: dict[str, int]) -> list[dict]:
    return client.retrieve_docdb_records(filter_query={}, projection=projection, limit=0)


def pull_cache(location: str, columns: tuple[str, ...]):
    selected_columns = ", ".join(f'"{column}"' for column in columns)
    query = f"SELECT {selected_columns} FROM read_parquet('{location}')"
    with duckdb.connect() as connection:
        return connection.sql(query).df()


def time_pull(pull: Callable[[], object], warmups: int, repeats: int) -> tuple[list[float], int]:
    for _ in range(warmups):
        pull()

    durations = []
    row_count = 0
    for _ in range(repeats):
        started = time.perf_counter()
        result = pull()
        durations.append(time.perf_counter() - started)
        row_count = len(result)  # type: ignore[arg-type]
    return durations, row_count


def print_result(case: BenchmarkCase, source: str, durations: list[float], row_count: int) -> None:
    median = statistics.median(durations)
    samples = ", ".join(f"{duration:.3f}s" for duration in durations)
    print(f"{case.name:<26} {source:<6} rows={row_count:>7,} median={median:.3f}s samples=[{samples}]")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--warmups", type=int, default=0)
    parser.add_argument("--host", default=API_GATEWAY_HOST)
    parser.add_argument("--cache-location")
    args = parser.parse_args()
    if args.repeats < 1:
        parser.error("--repeats must be at least 1")
    if args.warmups < 0:
        parser.error("--warmups must not be negative")
    return args


def main() -> None:
    args = parse_args()
    cache_location = args.cache_location or S3Backend().get_location("asset_basics")
    client = MetadataDbClient(host=args.host, version="v2")

    print(f"DocDB host: {args.host}")
    print(f"Cache: {cache_location}")
    print(f"Warmups: {args.warmups}; repeats: {args.repeats}")
    print()
    print(f"{'case':<26} {'source':<6} {'result':>33}")
    print("-" * 74)

    for case in CASES:
        docdb_pull = (
            lambda: pull_docdb_api(client, case.docdb_projection)
            if case.docdb_via_api
            else lambda: pull_docdb_http(args.host, case.docdb_projection)
        )
        docdb_durations, docdb_rows = time_pull(
            docdb_pull, args.warmups, args.repeats
        )
        cache_durations, cache_rows = time_pull(
            lambda: pull_cache(cache_location, case.cache_columns), args.warmups, args.repeats
        )
        print_result(case, "DocDB API" if case.docdb_via_api else "HTTP", docdb_durations, docdb_rows)
        print_result(case, "cache", cache_durations, cache_rows)
        if docdb_rows != cache_rows:
            print(f"  row-count drift: DocDB has {docdb_rows - cache_rows:+,} rows relative to cache")


if __name__ == "__main__":
    main()
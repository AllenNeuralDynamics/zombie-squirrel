"""Compare direct Zarr, cache Parquet, and virtual-Zarr spike reads.

The timed operation materializes every selected ``units/spike_times`` array.
Direct Zarr opens each source NWB store independently. The cache case reads
the exact published ``platform_ecephys_spikes`` partitions. The virtual case
opens one fsspec reference store whose manifest points at the same source
Zarr objects.

Usage:
    source ~/.zshrc
    switch prod
    .venv/bin/python scripts/virtual-zarr-test/benchmark_spike_reads.py
"""

from __future__ import annotations

import argparse
import math
import statistics
import time
from collections.abc import Callable, MutableMapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

import duckdb
import numpy as np
import zarr
from botocore.exceptions import ClientError
from common import fetch_json, make_s3_client, split_s3_uri
from fsspec.implementations.reference import ReferenceFileSystem
from fsspec.mapping import FSMap
from fsspec.spec import AbstractFileSystem

_SPIKE_CHUNK_BATCHES = 8
_SOURCE_READ_WORKERS = 4


@dataclass(frozen=True)
class ReadResult:
    """Counts and a lightweight value checksum for one materialized read."""

    values: int
    checksum: float
    arrays: int


class Boto3RangeFileSystem(AbstractFileSystem):
    """Minimal fsspec filesystem for S3 byte-range references."""

    protocol = "s3"

    def __init__(self, client, **kwargs):
        super().__init__(**kwargs)
        self.client = client

    @classmethod
    def _strip_protocol(cls, path: str) -> str:
        return path[5:] if path.startswith("s3://") else path

    def cat_file(self, path, start=None, end=None, **kwargs):
        """Read an S3 object, using a half-open byte range when provided."""
        bucket, key = split_s3_uri(path if path.startswith("s3://") else f"s3://{path}")
        request = {"Bucket": bucket, "Key": key}
        if start is not None or end is not None:
            first = 0 if start is None else int(start)
            last = "" if end is None else str(int(end) - 1)
            request["Range"] = f"bytes={first}-{last}"
        response = self.client.get_object(**request)
        return response["Body"].read()

    def cat_ranges(self, paths, starts, ends, max_gap=None, on_error="return", **kwargs):
        """Fetch independent reference ranges concurrently."""
        if not isinstance(starts, list):
            starts = [starts] * len(paths)
        if not isinstance(ends, list):
            ends = [ends] * len(paths)

        def fetch(item):
            path, start, end = item
            try:
                return self.cat_file(path, start, end, **kwargs)
            except Exception as exc:
                if on_error == "return":
                    return exc
                raise

        items = list(zip(paths, starts, ends, strict=True))
        with ThreadPoolExecutor(max_workers=min(32, max(1, len(items)))) as executor:
            return list(executor.map(fetch, items))


class Boto3ZarrStore(MutableMapping[str, bytes]):
    """Read-only mapping used to open a source Zarr store through boto3."""

    def __init__(self, client, bucket: str, prefix: str):
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.rstrip("/")

    def __getitem__(self, key: str) -> bytes:
        object_key = f"{self.prefix}/{key.lstrip('/')}"
        response = self.client.get_object(Bucket=self.bucket, Key=object_key)
        return response["Body"].read()

    def getitems(self, keys, **kwargs):
        """Fetch Zarr chunks concurrently, as a normal Zarr store may do."""

        def fetch(key):
            try:
                return key, self[key]
            except ClientError as exc:
                code = str(exc.response.get("Error", {}).get("Code", ""))
                if code in {"404", "NoSuchKey", "NotFound"}:
                    return key, None
                raise

        with ThreadPoolExecutor(max_workers=min(32, max(1, len(keys)))) as executor:
            return {key: value for key, value in executor.map(fetch, keys) if value is not None}

    def __setitem__(self, key: str, value: bytes) -> None:
        raise TypeError("Boto3ZarrStore is read-only")

    def __delitem__(self, key: str) -> None:
        raise TypeError("Boto3ZarrStore is read-only")

    def __iter__(self):
        return iter(())

    def __len__(self) -> int:
        return 0


def _source_parts(source: dict) -> tuple[str, str]:
    """Return the bucket and Zarr prefix for one catalog source."""
    return split_s3_uri(source["source_uri"])


def _sum_results(results: list[ReadResult]) -> ReadResult:
    """Combine per-source read results."""
    return ReadResult(
        values=sum(result.values for result in results),
        checksum=sum(result.checksum for result in results),
        arrays=sum(result.arrays for result in results),
    )


def _read_direct_source(client, source: dict) -> ReadResult:
    """Read one source store directly."""
    if not source["has_spike_times"]:
        return ReadResult(0, 0.0, 0)
    bucket, prefix = _source_parts(source)
    try:
        root = zarr.open_consolidated(Boto3ZarrStore(client, bucket, prefix), mode="r")
        spike_times = root["units/spike_times"]
    except KeyError as exc:
        raise RuntimeError(f"Could not open spike_times for {source['group_path']}") from exc
    count, total = _read_spike_array(spike_times)
    return ReadResult(count, total, 1)


def _read_spike_array(spike_times) -> tuple[int, float]:
    """Read one one-dimensional spike array in bounded, chunk-aligned batches."""
    size = int(spike_times.shape[0])
    chunk_size = int(spike_times.chunks[0])
    batch_size = chunk_size * _SPIKE_CHUNK_BATCHES
    checksum = 0.0
    for start in range(0, size, batch_size):
        data = np.asarray(spike_times[start : min(start + batch_size, size)])
        checksum += float(np.sum(data, dtype=np.float64))
        del data
    return size, checksum


def read_direct_zarr(client, sources: list[dict]) -> ReadResult:
    """Open every source store directly and materialize its spike times."""
    with ThreadPoolExecutor(max_workers=min(_SOURCE_READ_WORKERS, max(1, len(sources)))) as executor:
        return _sum_results(list(executor.map(lambda source: _read_direct_source(client, source), sources)))


def read_cache_parquet(cache_files: list[str]) -> ReadResult:
    """Read all published spike partitions into Python rows."""
    if not cache_files:
        return ReadResult(0, 0.0, 0)
    paths = ", ".join(repr(path) for path in cache_files)
    query = f"SELECT count(*) AS n, sum(spike_time) AS total FROM read_parquet([{paths}])"
    with duckdb.connect() as connection:
        count, total = connection.execute(query).fetchone()
    return ReadResult(
        values=int(count),
        checksum=float(total or 0.0),
        arrays=len(cache_files),
    )


def read_virtual_zarr(client, manifest: dict, sources: list[dict]) -> ReadResult:
    """Open all source groups through one fsspec reference filesystem."""
    target_fs = Boto3RangeFileSystem(client)
    reference_fs = ReferenceFileSystem(manifest, fs={"s3": target_fs}, max_gap=0)

    def read_source(source: dict) -> ReadResult:
        """Read one source group through the shared reference filesystem."""
        if not source["has_spike_times"]:
            return ReadResult(0, 0.0, 0)
        try:
            mapper: FSMap = reference_fs.get_mapper(root=source["group_path"])
            root = zarr.open_consolidated(mapper, mode="r")
            spike_times = root["units/spike_times"]
        except KeyError as exc:
            raise RuntimeError(f"Could not open virtual spike_times for {source['group_path']}") from exc
        count, total = _read_spike_array(spike_times)
        return ReadResult(count, total, 1)

    with ThreadPoolExecutor(max_workers=min(_SOURCE_READ_WORKERS, max(1, len(sources)))) as executor:
        return _sum_results(list(executor.map(read_source, sources)))


def _time_reads(
    name: str,
    read: Callable[[], ReadResult],
    warmups: int,
    repeats: int,
) -> tuple[str, list[float], ReadResult]:
    """Run warmups and timed repetitions for one read method."""
    for _ in range(warmups):
        read()
    durations: list[float] = []
    result = ReadResult(0, 0.0, 0)
    for _ in range(repeats):
        started = time.perf_counter()
        result = read()
        durations.append(time.perf_counter() - started)
    return name, durations, result


def _print_result(name: str, durations: list[float], result: ReadResult) -> None:
    """Print one benchmark row."""
    samples = ", ".join(f"{duration:.3f}" for duration in durations)
    print(
        f"{name:<18} median={statistics.median(durations):.3f}s "
        f"min={min(durations):.3f}s values={result.values:>10,} "
        f"arrays={result.arrays:>3} samples=[{samples}]"
    )


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subject-id", default="795133")
    parser.add_argument("--manifest-uri")
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if args.warmups < 0:
        parser.error("--warmups must not be negative")
    if args.repeats < 1:
        parser.error("--repeats must be at least 1")
    return args


def main() -> None:
    """Run and print the three-way comparison."""
    args = parse_args()
    client = make_s3_client()
    manifest_uri = args.manifest_uri or (
        f"s3://aind-scratch-data/virtual-zarr-test/subject={args.subject_id}/virtual-zarr.json"
    )
    manifest = fetch_json(client, manifest_uri)
    metadata = manifest["metadata"]
    if str(metadata["subject_id"]) != str(args.subject_id):
        raise ValueError(f"Manifest subject {metadata['subject_id']} does not match --subject-id {args.subject_id}")
    sources = metadata["sources"]
    cache_files = [path for asset in metadata["assets"] for path in asset["cache_files"]]
    expected = metadata["asset_count"]
    distinct_sessions = metadata["session_count"]

    print(
        f"subject={args.subject_id} assets={expected} sessions={distinct_sessions} "
        f"source_stores={len(sources)} cache_files={len(cache_files)} "
        f"warmups={args.warmups} repeats={args.repeats}"
    )
    print("read method        median/min include open + materialize; values should agree")
    print("-" * 92)

    reads = [
        ("direct zarr", lambda: read_direct_zarr(client, sources)),
        ("cache parquet", lambda: read_cache_parquet(cache_files)),
        ("virtual zarr", lambda: read_virtual_zarr(client, fetch_json(client, manifest_uri), sources)),
    ]
    results = []
    for name, read in reads:
        result = _time_reads(name, read, args.warmups, args.repeats)
        results.append(result)
        _print_result(*result)

    values = {result.values for _, _, result in results}
    checksums = [result.checksum for _, _, result in results]
    checksums_agree = all(
        math.isclose(checksums[0], checksum, rel_tol=1e-10, abs_tol=1e-2)
        for checksum in checksums[1:]
    )
    if len(values) != 1 or not checksums_agree:
        print("WARNING: result counts/checksums differ:")
        for name, _, result in results:
            print(f"  {name}: values={result.values:,}, checksum={result.checksum:.6f}")


if __name__ == "__main__":
    main()

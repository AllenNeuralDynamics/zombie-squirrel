# Virtual Zarr spike-read test

This experiment compares materializing all `units/spike_times` arrays for one
multi-session subject using:

1. each source NWB Zarr store directly;
2. the published `platform_ecephys_spikes` Parquet partitions; and
3. one fsspec reference store (a kerchunk-style Virtual Zarr).

The default subject is `795133`: 14 derived ecephys assets covering 12 distinct
acquisition sessions in the current `bdc-v0.40` cache. The virtual store does
not copy spike data. It contains JSON references to source Zarr metadata and
spike chunk objects in `aind-open-data`, stored under:

`s3://aind-scratch-data/virtual-zarr-test/subject=795133/`

The layout follows the reference-manifest idea described in the
[Virtual Zarr overview](https://www.earthmover.io/blog/virtual-zarr), but is
kept intentionally small: only `units/spike_times` is exposed because that is
the workload being measured.

## Run

Install the optional dependency if needed:

```bash
uv sync --extra sync --extra virtual-zarr-test
```

Use the production AWS profile before either command that reads or writes S3:

```bash
source ~/.zshrc
switch prod
.venv/bin/python scripts/virtual-zarr-test/build_virtual_zarr.py --subject-id 795133
.venv/bin/python scripts/virtual-zarr-test/benchmark_spike_reads.py --subject-id 795133
```

The builder uploads `virtual-zarr.json` and `catalog.json`. The benchmark
reports median and minimum wall time across three repetitions, along with the
materialized value count and a checksum. Change `--warmups` and `--repeats` for
more stable measurements, or pass `--cache-version 0.40` to pin the cache.

The benchmark intentionally uses a fresh DuckDB connection for each Parquet
repetition and opens each Zarr group for each direct/virtual repetition. It
does not include the one-time manifest-building scan; it does include fetching
the virtual manifest and opening the manifest-referenced metadata and chunks.

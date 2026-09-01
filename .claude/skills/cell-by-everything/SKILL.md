---
name: cell-by-everything
description: Add cells, properties, or genotyping to the cell_index / cell_properties / cell_genes cache tables in biodata-cache. Use when asked to put a new modality, project, pipeline output, or per-cell measurement into the "cell by everything" tables, to extend what is tracked per cell, or when working in src/biodata_cache/cache_table_helpers/cell_by_everything/.
---

# Adding to the cell-by-everything tables

## What these tables are

Three tables answer "what do we know about every single cell in every single
data asset?". They live in
`src/biodata_cache/cache_table_helpers/cell_by_everything/`.

| Table | Grain | Partition | Purpose |
|---|---|---|---|
| `cell_index` | one row per cell | none | Provenance and identity only. Narrow, so a consumer wanting every cell everywhere makes one small fetch. |
| `cell_properties` | one row per cell | `asset_name` | Every measurement. Wide and deliberately sparse. |
| `cell_genes` | one row per genotyped cell | `subject_id` | Transcriptomic labels and gene counts. |

All three join on **`cell_key`**.

**They are mostly a projection, not an extraction.** The build reads parquet that
the sync pipeline has already cached (`platform_ecephys_units`,
`platform_pophys`, `platform_visual_learning_coreg`, …).

**An asset whose `cell_properties` partition already exists is never re-read and
never rewritten.** Writing is the expensive half — a full pophys run is ~5,200
sequential S3 PUTs, about half an hour — so this is what makes a re-run cheap.
`cell_index` and `cell_genes` are single global objects and *are* rewritten every
run; their rows for a skipped asset are recovered from the existing partition's
`container` / `cell_ref` columns, which exist for exactly that purpose. A
partition missing those columns (written by an older build) is rebuilt.

Use `build_cell_by_everything(force_rewrite=True)` when partitions are *stale*
rather than merely present — after changing the property schema, a source
mapping, or anything feeding `cell_key`.

The one exception is `nwb_units.py`, which reads public NWB-Zarr directly. See the
next section for why, and when you must do the same.

## Never depend on a one-off table

Only project a cache table that a **sync job** rebuilds. Check before you wire a
source up:

```bash
grep -n "publish_registry_fragment(NAMES\[" src/biodata_cache/sync.py
```

A table with a registry entry but no job is published by a `scripts/build_*.py`
run, out of band, and is **not** re-run by the pipeline. As of writing that is
every `swdb_*` table, `platform_visual_coding_neuropixels_units`, and
`platform_swdb_dr_switch*`. Projecting one silently pins the cell tables to
whenever someone last ran that script, and nothing will fail loudly to tell you.

When cells are only reachable through such a source, **copy the reading logic into
`cell_by_everything/nwb_units.py`** and give the `CellSource` a `reader` instead of
a `table` (see `VISUAL_CODING_NEUROPIXELS_UNITS`). Copy, do not import and do not
refactor the original into a shared utility: this package has to keep working when
the one-off table and its module go stale, and a shared helper would let unrelated
changes move under it. Duplication is the correct call here.

A `reader` is `asset_name -> DataFrame`, returning source-shaped rows; the
`containers` / `cell_refs` / `properties` mapping then applies exactly as for a
cache-table source, and a reader that raises costs that one asset, not the run.

## Four invariants — breaking any of these is a real bug

1. **`cell_key` must stay stable across rebuilds.** It is
   `blake2b(asset_name \x1f container \x1f cell_ref)`. Never derive it from a row
   number, a sort order, or anything a re-run can change; never change the hash
   input for an existing source, or every persisted link breaks.
2. **A source that lacks a property contributes NULL, never a placeholder and
   never a special-cased code path.** Sparsity is the intended shape.
3. **Never widen `cell_properties` with a gene panel or anything else with
   hundreds of columns.** See "Where to put a new column" below.
4. **Never depend on a one-off, script-built table.** See the section above.

## Recipe: add a new source of cells

Almost always this is *one new `CellSource` entry* in `sources.py` and nothing
else. The builder is generic.

1. **Find the cached table** that already holds one row per cell for the new
   modality/project. If none exists, you are writing a new extraction table
   first — that is a separate job, not this one.
2. **Read a real partition** to see the actual column names and dtypes. Do not
   trust the `*_columns()` definitions alone; pipelines drift, and columns can be
   present but entirely NULL.
   ```bash
   aws s3 cp "s3://allen-data-views/data-asset-cache/bdc-v<VERSION>/<table>/<key>=<value>/" ./p/ --recursive
   ```
3. **Add the `CellSource`** to `sources.py` and to the `SOURCES` tuple:
   ```python
   MY_SOURCE = CellSource(
       name="my_source",                       # appears in cell_index.source
       modality="ecephys",                     # or "ophys", etc.
       table=registry.NAMES["my_table"],
       partition_key="asset_name",             # None if unpartitioned
       containers=("electrode_group_name", "device_name"),  # first present wins
       cell_refs=("unit_name", "unit_id", "id"),
       properties={"structure": ("structure",), "mean_rate": ("firing_rate",)},
       enumerate_assets=lambda df: _derived_names(df, "ecephys"),
   )
   ```
4. **Choose `containers` and `cell_refs` carefully** — they are hash inputs, so
   changing them later renumbers every cell of that source:
   - `container` is the physical channel: the **human-readable** probe name or
     imaging-plane name. Prefer `electrode_group_name` ("probeB") over
     `device_name`, which is a probe serial number in some exports.
   - `cell_ref` is the source's own identifier, so a caller can get back to the
     source NWB. Give the most specific candidate first.
5. **Add the job dependency.** If the new source is built by a sync job, add that
   job to `CELL_BY_EVERYTHING_SOURCE_JOBS` in `sync.py` and to the
   `cell-by-everything` row in `PIPELINE.md`.
6. **Test with real column shapes.** Copy the fixture style in
   `tests/cache_table_helpers/test_cell_by_everything.py`: seed a `MemoryBackend`
   with frames shaped like the real partition, then assert on the built tables.
7. **Keep the unit tests offline.** A reader-backed source will hit S3 from the
   test suite unless you patch `tables.SOURCES` to exclude it (the existing
   `seeded_backend` fixture does this). If the suite's runtime jumps, that is why.

Candidate sources not yet wired up: `platform_visual_coding_ophys` (ROIs, own
layout — and built by a real job, so it can be projected), V1DD, BCI. Each should
be one `CellSource`.

## Recipe: add a new property

1. Add one entry to `PROPERTY_COLUMNS` in `sources.py`:
   ```python
   "waveform_duration": ("float32", "Peak-to-trough duration of the mean waveform (ms)"),
   ```
   Supported dtypes are `float32`, `boolean` and `string`. The builder fills the
   column with the dtype's own null for every source that does not map it, and
   `cell_properties_columns()` picks up the description automatically — no
   registry edit needed.
2. Add the mapping to whichever sources supply it, as a candidate tuple.
3. **Re-run with `force_rewrite=True`.** Existing partitions are skipped by
   default, so a plain re-run will *not* backfill your new column into assets that
   are already written — they will silently keep the old schema.
4. **Prefer a canonical name over a pipeline name.** `mean_rate` rather than
   `firing_rate`, so the ophys and ecephys answers land in one column a caller
   can group by. Map pipeline-specific names to it via the candidate tuple.
5. Add an assertion to `test_properties_are_sparse_per_modality` or a new test
   that the column is filled for the sources that have it and NULL elsewhere.

Adding sparse columns here is cheap — see the measurements below — so do not
agonize over whether a property is "worth" a column. Do think about whether the
name is the one every future source will want to fill.

## Where to put a new column

Measured on synthetic data matching these tables' shape:

| Layout | Size | Typical metric query |
|---|---|---|
| 3M rows × 60 mixed columns, one wide sparse table | 360 MB | 16 ms |
| Same columns split into per-modality side tables + join | 364 MB | 61 ms |
| 500 gene columns at 1% density merged into a 3M-row core | 76 MB | — |
| Same genes kept in a separate 30k-row table | 33 MB | — |

So:

- **Dozens of sparse columns → `cell_properties`.** NULL bitmaps compress to
  nothing and DuckDB prunes unread columns, so a wide sparse table is the same
  size as split tables and ~4x faster than joining them.
- **Hundreds of columns at low density → a new sibling table** partitioned by
  whatever key makes its rows dense, joined on `cell_key`. This is why
  `cell_genes` exists. A second gene panel, a per-cell embedding, or a
  tuning-curve matrix belongs in its own table, following `genes.py`.
- **Identity/provenance only → `cell_index`**, and be reluctant: every column
  here is paid by every consumer of the "all cells" fetch.

Re-run the numbers rather than guessing if a new case is genuinely ambiguous.

## Joining across reprocessings

The same acquisition is often published as several derived assets (reprocessings,
per-collection exports). Each gets its own `cell_key`, which is correct —
`cell_key` identifies a cell-*in-an-asset*, which is what provenance needs.

When a side product was computed against a *different* reprocessing than the one
you are annotating, **join on `session_key`** (`<subject_id>_<YYYY-MM-DD>`, from
`keys.session_key`), never on `asset_name`. This is exactly the Visual Learning
case: the co-registration tables name the September-2025 processing while the
published collection uses August-2026. `genes.py` is the worked example.

If you ever need "all rows that refer to one physical cell", that is a
`cell_group_key` column on `cell_index` — a new column, not a change to
`cell_key`.

## Verify your change

```bash
uv run python -m pytest tests/cache_table_helpers/test_cell_by_everything.py tests/test_sync.py tests/test_sync_coverage.py -q
```

If you added a table (not just a column), the fragment counts in
`tests/test_sync_coverage.py` and `tests/test_sync.py` need bumping.

Then build against real cached data before trusting it — seed a `MemoryBackend`
from downloaded partitions, run `build_cell_by_everything()`, and check the
non-null count per column per source. A column that is silently all-NULL for a
source you expected to fill it is the usual failure, and no unit test with
hand-written fixtures will catch it.

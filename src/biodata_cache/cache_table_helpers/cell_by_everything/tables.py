"""The three cell-by-everything cache tables.

``cell_index``, ``cell_properties`` and ``cell_genes`` together answer "what do
we know about every single cell in every single data asset?". They are built mostly as a
*projection* over per-cell tables the sync pipeline already caches, which is why
the job is cheap enough to rebuild wholesale on every sync. The exception is a
source with no pipeline-built table of its own, which reads NWB-Zarr directly via
``nwb_units.py`` rather than depending on a one-off script-built table.

Why three tables and not one, or one per source:

* ``cell_index`` (unpartitioned, narrow) is the provenance table: one row per
  cell, identity and asset only. Consumers that want "every cell everywhere"
  fetch this single small file.
* ``cell_properties`` (partitioned by ``asset_name``, wide and sparse) holds the
  measurements. Benchmarking a 3M-row, 60-column layout showed sparsity is
  effectively free in parquet -- NULL bitmaps compress to nothing and DuckDB
  prunes unread columns -- while splitting the same columns into per-modality
  side tables made a typical metric query ~4x slower for no space saving. So one
  wide sparse table, and new properties can be added freely.
* ``cell_genes`` (partitioned by ``subject_id``) is separate because extreme
  width is where sparsity stops being free; see ``genes.py``.

All three share ``cell_key`` (see ``keys.py``), which is a hash of the cell's
natural key and is therefore stable across rebuilds.
"""

import logging
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import NamedTuple

import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.asset_basics import asset_basics
from biodata_cache.cache_table_helpers.cell_by_everything.genes import (
    GENE_TABLE_COLUMN_ORDER,
    build_cell_genes,
    cell_type_lookup,
    gene_annotations,
)
from biodata_cache.cache_table_helpers.cell_by_everything.keys import session_key
from biodata_cache.cache_table_helpers.cell_by_everything.sources import (
    PROPERTY_COLUMNS,
    SOURCES,
    CellSource,
    project_source,
)
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

# Identity columns duplicated into each cell_properties partition. They make a
# partition self-describing (you can tell which cell a row is without joining
# cell_index) and, crucially, let an already-written partition be reused to
# rebuild cell_index without re-reading the source.
#
# ``source`` records which CellSource owns the partition, and it is load-bearing:
# asset enumerations overlap (all 57 Visual Coding assets are also "derived
# ecephys" assets in asset_basics), so "a partition exists for this asset" does
# NOT mean "this source wrote it". Without the ownership check, one source reuses
# another's partition and the same cells land in cell_index twice under two
# different source labels. asset_name and modality are implied once source
# matches, and are supplied by the build loop.
_IDENTITY_COLUMNS = ["source", "container", "cell_ref"]

INDEX_COLUMN_ORDER = [
    "cell_key",
    "asset_name",
    "subject_id",
    "session_key",
    "project_name",
    "modality",
    "source",
    "container",
    "cell_ref",
]
PROPERTY_COLUMN_ORDER = ["cell_key", *_IDENTITY_COLUMNS, *PROPERTY_COLUMNS]

# Source partitions are small parquet objects; the cost of the build is almost
# entirely S3 round trips, so read them concurrently.
_MAX_WORKERS = 32

# Assets read per batch. Bounds peak memory to one batch of source partitions
# (see _iter_source_frames) while keeping every worker busy.
_READ_BATCH = 256

# Property partitions buffered before a concurrent flush. Kept well below
# _READ_BATCH so peak memory stays bounded by the source batch, not by pending
# writes; the partitions themselves are ~20-30 KiB each.
_WRITE_BATCH = 64


def _log(table_key: str, message: str) -> None:
    """Emit a structured cache log message for one of the cell tables."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES[table_key],
            message=message,
        ).to_json()
    )


def _read_asset_rows(source: CellSource, asset_name: str) -> pd.DataFrame:
    """Read one asset's rows from a source, or empty if absent or unreadable.

    Per-asset failures are swallowed with a log line rather than raised: a single
    corrupt partition or unreadable NWB store must not abort the whole
    projection.
    """
    try:
        if source.reader is not None:
            return source.reader(asset_name)
        cache_key = f"{source.table}/{asset_name}"
        if not registry.BACKEND.partition_exists(cache_key):
            return pd.DataFrame()
        return registry.BACKEND.read(cache_key)
    except Exception as exc:
        _log("cell_index", f"Could not read {source.name} for {asset_name}: {type(exc).__name__}: {exc}")
        return pd.DataFrame()


class AssetWork(NamedTuple):
    """One asset's input to the projection loop.

    Exactly one of ``rows`` / ``existing`` is set. ``existing`` means the asset's
    partition is already written and was read back for reuse; ``rows`` means the
    source was read and the partition must be built.
    """

    asset_name: str
    rows: pd.DataFrame | None
    existing: pd.DataFrame | None
    partition_existed: bool


def _read_existing_partition(asset_name: str) -> pd.DataFrame:
    """Read one asset's already-written properties partition, or empty on failure."""
    try:
        return registry.BACKEND.read(f"{registry.NAMES['cell_properties']}/{asset_name}")
    except Exception as exc:
        _log("cell_index", f"Could not read existing partition for {asset_name}: {type(exc).__name__}: {exc}")
        return pd.DataFrame()


def _properties_partition_exists(asset_name: str) -> bool:
    """Return True if this asset's cell_properties partition is already written."""
    return registry.BACKEND.partition_exists(f"{registry.NAMES['cell_properties']}/{asset_name}")


def _iter_source_frames(
    source: CellSource, df_basics: pd.DataFrame, force_rewrite: bool
) -> Iterator[AssetWork]:
    """Yield one :class:`AssetWork` per asset this source supplies.

    Both kinds of read are issued concurrently per batch: the source rows for
    assets that must be built, and the existing partitions for assets that can be
    reused. Reading reused partitions one at a time in the projection loop was
    measured as the dominant cost of a re-run -- with every partition reused it
    would be thousands of serial round trips, which defeats the point of skipping
    the writes in the first place.

    ``partition_existed`` lets the caller skip ``clear_partition`` for an asset it
    has just been told has no partition -- one saved S3 LIST per new asset, which
    matters at thousands of assets.

    An asset whose ``cell_properties`` partition is already written has its source
    left unread and its partition left unwritten, matching every other per-asset
    sync job in this package.

    Deliberately a generator read in bounded batches rather than a dict of every
    partition. A source like ``platform_pophys`` has thousands of partitions whose
    combined size is gigabytes (the ROI ``contour`` strings dominate); collecting
    them all before projecting peaked at nearly 2 GiB resident in testing and
    would OOM a modest sync capsule. Batching caps the reads in flight, and the
    caller drops each frame once its partition is written, so peak memory is one
    batch rather than the whole source.

    ``ThreadPoolExecutor.map`` cannot be used across the whole asset list for the
    same reason: it queues every task at once and buffers completed results until
    they are consumed, which reproduces the unbounded growth even though the
    iterator looks lazy.
    """
    if source.reader is None and source.partition_key is None:
        # An unpartitioned cache table is one object holding every asset's cells;
        # read it once and split it rather than issuing a request per asset.
        table = registry.BACKEND.read(source.table)
        if table.empty or "asset_name" not in table.columns:
            return
        for name, rows in table.groupby("asset_name", sort=False):
            existed = _properties_partition_exists(name)
            if not force_rewrite and existed:
                yield AssetWork(name, None, _read_existing_partition(name), True)
            else:
                yield AssetWork(name, rows, None, existed)
        return

    asset_names = source.enumerate_assets(df_basics)
    for start in range(0, len(asset_names), _READ_BATCH):
        batch = asset_names[start : start + _READ_BATCH]

        existing: set[str] = set()
        if not force_rewrite:
            # One existence check per asset, run concurrently: a serial pass over
            # thousands of assets would cost more than the reads it saves.
            with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
                flags = list(executor.map(_properties_partition_exists, batch))
            existing = {name for name, flag in zip(batch, flags, strict=True) if flag}
        reusable = [name for name in batch if name in existing]
        if reusable:
            with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
                partitions = list(executor.map(_read_existing_partition, reusable))
            for name, partition in zip(reusable, partitions, strict=True):
                yield AssetWork(name, None, partition, True)
            del partitions

        to_read = [name for name in batch if name not in existing]
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
            frames = list(executor.map(lambda name: _read_asset_rows(source, name), to_read))
        for name, rows in zip(to_read, frames, strict=True):
            if not rows.empty:
                # force_rewrite skipped the existence check, so assume a partition
                # may be there and clear it; otherwise we know there is none.
                yield AssetWork(name, rows, None, force_rewrite)
        del frames


class AssetContext(NamedTuple):
    """Asset-level lookups needed to annotate index rows, built once per build.

    Deriving these inside the per-asset annotation was the single biggest cost in
    the whole job: it rebuilt two dicts from the entire ~105k-row asset_basics on
    every call, measured at 206 ms per asset -- roughly 18 minutes of pure
    single-threaded CPU across a full run, dwarfing the S3 time it was mistaken
    for. Build it once and pass it down.
    """

    subjects: dict[str, str]
    projects: dict[str, str]


def _asset_context(df_basics: pd.DataFrame) -> AssetContext:
    """Build the asset_name -> subject / project lookups once for a whole build."""
    subjects: dict[str, str] = {}
    projects: dict[str, str] = {}
    if "name" in df_basics.columns:
        names = df_basics["name"].to_numpy()
        if "subject_id" in df_basics.columns:
            subjects = dict(zip(names, df_basics["subject_id"].to_numpy(), strict=False))
        if "project_name" in df_basics.columns:
            projects = dict(zip(names, df_basics["project_name"].to_numpy(), strict=False))
    return AssetContext(subjects=subjects, projects=projects)


def _cast_index(index: pd.DataFrame) -> pd.DataFrame:
    """Order and cast an index frame to the written schema.

    Every index column is a string. Casting in one place keeps the written dtypes
    identical whether a row was freshly projected from a source or recovered from
    an existing cell_properties partition.
    """
    index = index.reindex(columns=INDEX_COLUMN_ORDER)
    return index.astype({column: "string" for column in INDEX_COLUMN_ORDER})


def _annotate_asset_index(index: pd.DataFrame, asset_name: str, context: AssetContext) -> pd.DataFrame:
    """Add the asset-level context columns for a single asset's index rows.

    All three values are constant across one asset's rows, so they are looked up
    once and broadcast rather than mapped row by row.
    """
    index["session_key"] = session_key(asset_name)
    index["subject_id"] = context.subjects.get(asset_name)
    index["project_name"] = context.projects.get(asset_name)
    return _cast_index(index)


def _annotate_index(index: pd.DataFrame, context: AssetContext) -> pd.DataFrame:
    """Add the asset-level context columns to a multi-asset index frame.

    Mapped per *unique asset*, not per row: session_key parses a name with a regex
    and the frame can hold millions of rows across a few thousand assets.
    """
    unique_names = index["asset_name"].unique()
    session_keys = {name: session_key(name) for name in unique_names}
    index["session_key"] = index["asset_name"].map(session_keys).astype("string")
    index["subject_id"] = index["asset_name"].map(context.subjects).astype("string")
    index["project_name"] = index["asset_name"].map(context.projects).astype("string")
    return _cast_index(index)


def _coerce_properties(properties: pd.DataFrame) -> pd.DataFrame:
    """Cast every property column to its declared dtype, filling absent ones with NULL."""
    for name, (dtype, _description) in PROPERTY_COLUMNS.items():
        if name not in properties.columns:
            # No `value` argument: pandas fills with the dtype's own null (NaN for
            # float32, pd.NA for the nullable boolean/string extension dtypes).
            properties[name] = pd.Series(index=properties.index, dtype=dtype)
            continue
        if dtype == "float32":
            properties[name] = pd.to_numeric(properties[name], errors="coerce").astype("float32")
        elif dtype == "boolean":
            # is_soma and friends arrive as 0/1 integers; astype("boolean") on a
            # numeric column maps them correctly and preserves NULLs.
            properties[name] = properties[name].astype("boolean")
        else:
            properties[name] = properties[name].astype("string")
    for name in _IDENTITY_COLUMNS:
        properties[name] = properties[name].astype("string")
    return properties.reindex(columns=PROPERTY_COLUMN_ORDER)


def _apply_gene_labels(index: pd.DataFrame, properties: pd.DataFrame, lookup: pd.DataFrame) -> pd.DataFrame:
    """Fill ``cell_type_label`` from the transcriptomic lookup where the source had none.

    A source's own label always wins: an ecephys decoder class describes the same
    cell from the same recording, whereas the transcriptomic label is only
    available for co-registered ophys ROIs, which have no label of their own.
    """
    if lookup.empty:
        return properties
    keys = index[["cell_key", "session_key", "container", "cell_ref"]]
    resolved = keys.merge(lookup, on=["session_key", "container", "cell_ref"], how="inner")
    if resolved.empty:
        return properties
    labels = dict(zip(resolved["cell_key"], resolved["cell_type_label"], strict=False))
    from_genes = properties["cell_key"].map(labels).astype("string")
    properties["cell_type_label"] = properties["cell_type_label"].fillna(from_genes)
    return properties


def _index_from_existing(source: CellSource, asset_name: str, properties: pd.DataFrame) -> pd.DataFrame:
    """Rebuild one asset's index rows from its already-written properties partition.

    cell_index is a single unpartitioned object, so it must be rewritten in full
    on every run even when no property partition changes. Recovering its rows from
    the cheap identity columns of the existing partition avoids re-reading (and
    re-writing) the source for an asset that is already done.
    """
    return pd.DataFrame(
        {
            "cell_key": properties["cell_key"].astype("string").to_numpy(),
            "asset_name": asset_name,
            "modality": source.modality,
            "source": source.name,
            "container": properties["container"].astype("string").to_numpy(),
            "cell_ref": properties["cell_ref"].astype("string").to_numpy(),
        }
    )


def _partition_owner(properties: pd.DataFrame) -> str | None:
    """Return the source name that wrote a properties partition, if recorded."""
    if "source" not in properties.columns or properties.empty:
        return None
    owner = properties["source"].iloc[0]
    return None if pd.isna(owner) else str(owner)


def build_cell_by_everything(force_rewrite: bool = False) -> None:
    """Build the three cell-by-everything tables from the cached source tables.

    An asset whose ``cell_properties`` partition already exists is left alone: its
    source is not read and its partition is not rewritten, matching every other
    per-asset sync job here. Its ``cell_index`` rows are recovered from the
    existing partition instead, because ``cell_index`` and ``cell_genes`` are
    global objects and must be rewritten in full on every run.

    Sources with nothing cached simply contribute no rows.

    Args:
        force_rewrite: If True, re-read every source and overwrite every
            ``cell_properties`` partition. Use after changing the property schema
            or a source mapping, when existing partitions are stale rather than
            merely present.
    """
    setup_logging()
    df_basics = asset_basics()
    context = _asset_context(df_basics)
    annotations = gene_annotations()
    labels = cell_type_lookup(annotations)

    index_frames: list[pd.DataFrame] = []
    # asset_name -> source that wrote its partition in this run. cell_properties is
    # partitioned by asset_name alone, so two sources producing rows for one asset
    # would silently clobber each other; refuse the second write loudly instead.
    claimed: dict[str, str] = {}
    for source in SOURCES:
        source_cells = 0
        reused = 0
        pending: list[tuple[str, pd.DataFrame, bool]] = []

        def _flush(batch: list[tuple[str, pd.DataFrame, bool]]) -> None:
            """Write a batch of property partitions concurrently.

            Each write is 2-3 sequential S3 round trips for a ~20 KiB object, so
            doing them one asset at a time is entirely round-trip bound: measured
            at 1.2 s per partition, or 25 KiB/s. Writing a batch concurrently is
            the difference between minutes and hours over thousands of assets.
            """
            if not batch:
                return

            def _write(item: tuple[str, pd.DataFrame, bool]) -> None:
                """Write one asset's partition, clearing it only if it may exist."""
                name, frame, existed = item
                cache_key = f"{registry.NAMES['cell_properties']}/{name}"
                if existed:
                    registry.BACKEND.clear_partition(cache_key)
                registry.BACKEND.write(cache_key, frame)

            with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
                list(executor.map(_write, batch))
            batch.clear()

        for work in _iter_source_frames(source, df_basics, force_rewrite):
            asset_name, rows, existed = work.asset_name, work.rows, work.partition_existed
            if work.existing is not None:
                owner = _partition_owner(work.existing)
                if owner is not None and owner != source.name:
                    # Another source owns this asset. Asset enumerations overlap,
                    # so this is normal, not an error: leave it to its owner
                    # rather than claiming its cells under the wrong source.
                    continue
                if not work.existing.empty and set(_IDENTITY_COLUMNS).issubset(work.existing.columns):
                    asset_index = _index_from_existing(source, asset_name, work.existing)
                    index_frames.append(asset_index)
                    source_cells += len(asset_index)
                    reused += 1
                    del asset_index
                    continue
                # Written by an older build with no identity columns, so its index
                # rows cannot be recovered; fall through and rebuild it.
                rows = _read_asset_rows(source, asset_name)
                if rows is None or rows.empty:
                    continue

            projected = project_source(source, asset_name, rows)
            if projected is None:
                continue
            asset_index, asset_properties = projected
            # Only the narrow index rows are retained across the whole build; the
            # wide property frames are written and dropped one batch at a time.
            index_frames.append(asset_index)
            source_cells += len(asset_index)

            annotated = _annotate_asset_index(asset_index.copy(), asset_name, context)
            for column in ("container", "cell_ref"):
                asset_properties[column] = asset_index[column].to_numpy()
            asset_properties["source"] = source.name
            properties = _apply_gene_labels(annotated, _coerce_properties(asset_properties), labels)

            previous = claimed.get(asset_name)
            if previous is not None and previous != source.name:
                _log(
                    "cell_index",
                    f"Asset {asset_name} already written by source {previous}; "
                    f"refusing to overwrite it with source {source.name}",
                )
                index_frames.pop()
                source_cells -= len(asset_index)
                continue
            claimed[asset_name] = source.name
            pending.append((asset_name, properties, existed))
            if len(pending) >= _WRITE_BATCH:
                _flush(pending)
            del rows, projected, asset_properties, annotated, properties
        _flush(pending)
        if source_cells == 0:
            _log("cell_index", f"No rows available for source {source.name}")
            continue
        _log(
            "cell_index",
            f"Projected {source_cells} cells from source {source.name} "
            f"({reused} assets reused from existing partitions)",
        )

    if not index_frames:
        raise RuntimeError("No cells were projected; every source cache table is empty")

    index = _annotate_index(pd.concat(index_frames, ignore_index=True), context)
    index = index.sort_values(["asset_name", "container", "cell_ref"]).reset_index(drop=True)
    registry.BACKEND.write(registry.NAMES["cell_index"], index)
    _log("cell_index", f"Wrote {len(index)} cells across {index['asset_name'].nunique()} assets")

    genes = build_cell_genes(index, annotations)
    for subject_id, subject_genes in genes.groupby("subject_id", sort=False):
        cache_key = f"{registry.NAMES['cell_genes']}/{subject_id}"
        registry.BACKEND.clear_partition(cache_key)
        registry.BACKEND.write(cache_key, subject_genes.reset_index(drop=True))


@registry.register_table(registry.NAMES["cell_index"])
def cell_index(force_update: bool = False, force_rewrite: bool = False) -> pd.DataFrame:
    """Return one row per cell across every data asset, identity and provenance only.

    This is the entry point to the cell-by-everything tables: join ``cell_key`` to
    ``cell_properties`` for measurements and to ``cell_genes`` for transcriptomics.
    Kept narrow and unpartitioned so a consumer wanting every cell at once makes a
    single fetch.

    Args:
        force_update: If True, build all three cell-by-everything tables from the
            cached per-cell source tables and write them to the cache. Existing
            cell_properties partitions are skipped, not rewritten.
        force_rewrite: If True, also overwrite every existing cell_properties
            partition instead of reusing it.

    Returns:
        DataFrame with one row per cell (see cell_index_columns).
    """
    df = registry.BACKEND.read(registry.NAMES["cell_index"])
    if df.empty or force_update or force_rewrite:
        build_cell_by_everything(force_rewrite=force_rewrite)
        df = registry.BACKEND.read(registry.NAMES["cell_index"])
    return df


@registry.register_table(registry.NAMES["cell_properties"])
def cell_properties(
    asset_name: str | None = None,
    force_update: bool = False,
    lazy: bool = False,
    force_rewrite: bool = False,
) -> pd.DataFrame | str:
    """Return per-cell properties for one asset, keyed by ``cell_key``.

    A wide, deliberately sparse table: every cell carries only the properties its
    modality and pipeline actually produce, and the rest are NULL. Partitioned by
    ``asset_name`` so a consumer can fetch exactly the assets it needs.

    Args:
        asset_name: Asset whose cell properties to read. Required unless
            force_update is set.
        force_update: If True, build all three cell-by-everything tables and write
            them to the cache, skipping cell_properties partitions that already
            exist. An empty DataFrame is returned; read again without force_update
            to retrieve the data.
        lazy: If True, return the partition's storage location string (for DuckDB)
            instead of loading the DataFrame.
        force_rewrite: If True, also overwrite every existing cell_properties
            partition instead of reusing it.

    Returns:
        DataFrame with one row per cell (see cell_properties_columns); the
        partition location string if lazy=True; or an empty DataFrame if
        force_update=True.

    Raises:
        ValueError: If asset_name is None and force_update is False.
    """
    if force_update or force_rewrite:
        build_cell_by_everything(force_rewrite=force_rewrite)
        if not lazy:
            return pd.DataFrame()
    if asset_name is None:
        raise ValueError("asset_name is required to read cell_properties")

    cache_key = f"{registry.NAMES['cell_properties']}/{asset_name}"
    if lazy:
        return registry.BACKEND.get_location(cache_key)
    return registry.BACKEND.read(cache_key)


@registry.register_table(registry.NAMES["cell_genes"])
def cell_genes(
    subject_id: str | None = None,
    force_update: bool = False,
    lazy: bool = False,
    force_rewrite: bool = False,
) -> pd.DataFrame | str:
    """Return transcriptomic genotyping for the cells that have any, by subject.

    Very few cells are genotyped and gene panels are wide, so this is a separate
    table joined to the others on ``cell_key`` rather than more columns on
    ``cell_properties``.

    Args:
        subject_id: Subject whose genotyped cells to read. Required unless
            force_update is set.
        force_update: If True, build all three cell-by-everything tables and write
            them to the cache, skipping cell_properties partitions that already
            exist. An empty DataFrame is returned; read again without force_update
            to retrieve the data.
        lazy: If True, return the partition's storage location string (for DuckDB)
            instead of loading the DataFrame.
        force_rewrite: If True, also overwrite every existing cell_properties
            partition instead of reusing it.

    Returns:
        DataFrame with one row per genotyped cell (see cell_genes_columns); the
        partition location string if lazy=True; or an empty DataFrame if
        force_update=True.

    Raises:
        ValueError: If subject_id is None and force_update is False.
    """
    if force_update or force_rewrite:
        build_cell_by_everything(force_rewrite=force_rewrite)
        if not lazy:
            return pd.DataFrame()
    if subject_id is None:
        raise ValueError("subject_id is required to read cell_genes")

    cache_key = f"{registry.NAMES['cell_genes']}/{subject_id}"
    if lazy:
        return registry.BACKEND.get_location(cache_key)
    return registry.BACKEND.read(cache_key)


def cell_index_columns() -> list[Column]:
    """Return cell_index cache table column definitions."""
    return [
        Column(name="cell_key", description="Stable primary key for the cell; joins cell_properties and cell_genes"),
        Column(name="asset_name", description="Data asset the cell was read from; joins asset_basics"),
        Column(name="subject_id", description="Subject the cell was recorded in"),
        Column(name="session_key", description="Acquisition identifier '<subject_id>_<YYYY-MM-DD>'; shared by every reprocessing of one session"),
        Column(name="project_name", description="Project the source asset belongs to"),
        Column(name="modality", description="Recording modality of the cell ('ecephys' or 'ophys')"),
        Column(name="source", description="Cache table the cell was projected from (e.g. 'ecephys_units', 'pophys')"),
        Column(name="container", description="Physical channel the cell was recorded through: probe name for ecephys, imaging plane name for ophys"),
        Column(name="cell_ref", description="The source table's own identifier for the cell (unit id or ROI id), for going back to the source NWB"),
    ]


def cell_properties_columns() -> list[Column]:
    """Return cell_properties cache table column definitions."""
    return [
        Column(name="cell_key", description="Stable cell key; joins cell_index and cell_genes"),
        Column(name="source", description="CellSource that wrote this partition; asset enumerations overlap, so this records which source owns the asset"),
        Column(name="container", description="Probe name (ecephys) or imaging plane name (ophys); duplicated from cell_index so a partition is self-describing"),
        Column(name="cell_ref", description="The source table's own unit/ROI identifier; duplicated from cell_index so a partition is self-describing"),
        *(Column(name=name, description=description) for name, (_dtype, description) in PROPERTY_COLUMNS.items()),
    ]


def cell_genes_columns() -> list[Column]:
    """Return cell_genes cache table column definitions."""
    descriptions = {
        "cell_key": "Stable cell key; joins cell_index and cell_properties",
        "subject_id": "Subject the genotyped cell was recorded in",
        "hcr_id": "HCR cell identifier the imaging ROI was co-registered to",
        "cell_class": "Broad annotated cell class",
        "cell_subclass": "Annotated cell subclass",
        "cell_type": "Annotated transcriptomic cluster/cell type",
        "cluster_id": "Numeric annotated transcriptomic cluster identifier",
        "total_counts": "Total counts across gene channels",
        "n_genes": "Number of detected gene channels",
    }
    return [
        Column(name=name, description=descriptions.get(name, f"HCR expression count for {name}"))
        for name in GENE_TABLE_COLUMN_ORDER
    ]

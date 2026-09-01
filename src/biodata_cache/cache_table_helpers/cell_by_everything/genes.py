"""Transcriptomic genotyping for cell-by-everything cells.

Genotyping is kept in its own table rather than folded into ``cell_properties``
for a measured reason. Merging 500 gene columns at 1% density into a 3M-row core
grew the parquet from 33 MB to 76 MB in testing -- per-column, per-row-group page
metadata is paid whether or not a row group has data -- while moderate sparsity
(dozens of columns, modality-density) was essentially free. Gene panels only get
wider, so they live separately and join on ``cell_key``.

Today the only genotyped cells are the Visual Learning HCR panel: 22 gene
channels plus a transcriptomic cluster label, reaching imaging ROIs through two
hops. ``platform_visual_learning_coreg`` maps an imaging ROI to an ``hcr_id``,
and ``platform_visual_learning_cell_gene`` maps that ``hcr_id`` to counts and
labels.

The co-registration tables were computed against the September-2025 reprocessing
of each session while the published collection uses the August-2026 one, so the
asset names do not match and cannot be the join key. The link is made on
``session_key`` (``<subject_id>_<YYYY-MM-DD>``) instead, which identifies the
acquisition and therefore survives reprocessing. A welcome consequence: every
reprocessing of a genotyped session picks up the same genotyping.
"""

import logging

import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.platform_visual_learning import (
    CELL_GENE_COLUMN_ORDER,
    COREG_ASSETS,
    GENE_COLUMNS,
)
from biodata_cache.utils import CacheLogMessage

# Columns carried over from the cell-gene table, in order, after cell_key. The
# per-cell QC fields (total_counts, n_genes) come along because they are how a
# caller decides whether to trust a label.
_LABEL_COLUMNS = ["cell_class", "cell_subclass", "cell_type", "cluster_id", "total_counts", "n_genes"]
GENE_TABLE_COLUMN_ORDER = ["cell_key", "subject_id", "hcr_id", *_LABEL_COLUMNS, *GENE_COLUMNS]

# hcr_id <= 0 is the co-registration table's "no match" sentinel, not a cell id.
_UNMATCHED_HCR_ID = 0


def _log(message: str) -> None:
    """Emit a structured cache log message for the cell_genes table."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES["cell_genes"],
            message=message,
        ).to_json()
    )


def _read_subject_partitions(table_key: str) -> pd.DataFrame:
    """Read every subject partition of a Visual Learning table into one frame.

    Both source tables are partitioned by ``subject_id`` and have no
    unpartitioned object, so they must be read partition by partition.
    """
    frames = []
    for subject_id in COREG_ASSETS:
        frame = registry.BACKEND.read(f"{registry.NAMES[table_key]}/{subject_id}")
        if not frame.empty:
            frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def gene_annotations() -> pd.DataFrame:
    """Return every genotyped cell keyed by its acquisition-level natural key.

    Returns:
        DataFrame with ``session_key``, ``container``, ``cell_ref`` (matching the
        ``cell_index`` columns of the same name), plus ``subject_id``, ``hcr_id``,
        the cell-type labels and the gene counts. Empty if the Visual Learning
        source tables are not cached.
    """
    coreg = _read_subject_partitions("visual_learning_coreg")
    cell_gene = _read_subject_partitions("visual_learning_cell_gene")
    if coreg.empty or cell_gene.empty:
        _log("Visual Learning coreg or cell-gene table is empty; no genotyping available")
        return pd.DataFrame()

    matched = coreg[coreg["hcr_id"].notna() & (coreg["hcr_id"] > _UNMATCHED_HCR_ID)].copy()
    matched = matched[matched["roi_id"].notna() & matched["session_key"].notna()]
    if matched.empty:
        return pd.DataFrame()

    # cell_id is stored as a string in the cell-gene table and hcr_id as a
    # nullable integer in the co-registration table; align them before joining.
    matched["_hcr_str"] = matched["hcr_id"].astype("Int64").astype(str)
    labels = cell_gene.copy()
    labels["_hcr_str"] = labels["cell_id"].astype(str)
    labels["subject_id"] = labels["subject_id"].astype(str)
    matched["subject_id"] = matched["subject_id"].astype(str)

    # Intersect with what the source actually has: a gene channel that was not
    # imaged for a subject is simply absent from its partition, and
    # build_cell_genes fills any missing panel column with NULL afterwards.
    keep = [
        column
        for column in CELL_GENE_COLUMN_ORDER
        if column not in ("subject_id", "cell_id") and column in labels.columns
    ]
    joined = matched.merge(
        labels[["subject_id", "_hcr_str", *keep]],
        on=["subject_id", "_hcr_str"],
        how="inner",
    )
    if joined.empty:
        return pd.DataFrame()

    joined["container"] = joined["plane_id"].astype("string")
    # cell_ref in cell_index is the source table's own identifier rendered as a
    # string; roi_id here is the same ROI index, so render it the same way.
    joined["cell_ref"] = joined["roi_id"].astype("Int64").astype(str).astype("string")
    joined["session_key"] = joined["session_key"].astype("string")
    joined["hcr_id"] = joined["hcr_id"].astype("Int64")

    out = joined[["session_key", "container", "cell_ref", "subject_id", "hcr_id", *keep]]
    _log(f"Resolved {len(out)} genotyped cells across {out['session_key'].nunique()} acquisitions")
    return out.reset_index(drop=True)


def cell_type_lookup(annotations: pd.DataFrame) -> pd.DataFrame:
    """Return just the cell-type label per natural key, for ``cell_properties``.

    The transcriptomic cluster is the single most useful property these tables
    can carry, so it is promoted into ``cell_properties.cell_type_label``
    alongside the ecephys decoder label, while the counts stay in ``cell_genes``.

    Args:
        annotations: Output of :func:`gene_annotations`.

    Returns:
        DataFrame with ``session_key``, ``container``, ``cell_ref`` and
        ``cell_type_label``; empty if there are no annotations.
    """
    if annotations.empty or "cell_type" not in annotations.columns:
        return pd.DataFrame(columns=["session_key", "container", "cell_ref", "cell_type_label"])
    lookup = annotations[["session_key", "container", "cell_ref", "cell_type"]].copy()
    lookup = lookup.rename(columns={"cell_type": "cell_type_label"})
    lookup["cell_type_label"] = lookup["cell_type_label"].astype("string")
    return lookup.drop_duplicates(["session_key", "container", "cell_ref"])


def build_cell_genes(index: pd.DataFrame, annotations: pd.DataFrame) -> pd.DataFrame:
    """Attach ``cell_key`` to every genotyped cell.

    Args:
        index: The freshly built ``cell_index``, which must carry ``session_key``.
        annotations: Output of :func:`gene_annotations`.

    Returns:
        DataFrame with one row per genotyped ``cell_key``, ordered by
        :data:`GENE_TABLE_COLUMN_ORDER`. Empty if nothing joins.
    """
    if index.empty or annotations.empty:
        return pd.DataFrame(columns=GENE_TABLE_COLUMN_ORDER)

    keys = index[["cell_key", "session_key", "container", "cell_ref"]].dropna(subset=["session_key"])
    joined = keys.merge(annotations, on=["session_key", "container", "cell_ref"], how="inner")
    if joined.empty:
        _log("No cell_index rows joined to the genotyping tables")
        return pd.DataFrame(columns=GENE_TABLE_COLUMN_ORDER)

    for column in GENE_COLUMNS:
        if column not in joined.columns:
            joined[column] = pd.NA
        joined[column] = pd.to_numeric(joined[column], errors="coerce").astype("float32")

    out = joined.reindex(columns=GENE_TABLE_COLUMN_ORDER)
    _log(f"Built {len(out)} genotyped cell rows for {out['subject_id'].nunique()} subjects")
    return out.sort_values(["subject_id", "cell_key"]).reset_index(drop=True)

"""Stable cell identity for the cell-by-everything tables.

``cell_key`` is the primary key that ties ``cell_index``, ``cell_properties`` and
``cell_genes`` together. It must be **stable across re-cache runs**: every
downstream link (a saved selection, a notebook, another cache table) breaks if a
rebuild renumbers cells. It is therefore a hash of the cell's natural key rather
than a row number.

The natural key is ``(asset_name, container, cell_ref)``:

* ``asset_name`` is the provenance -- which data asset this row came from;
* ``container`` is the physical channel the cell was recorded through (a probe
  name for ecephys, an imaging-plane name for ophys);
* ``cell_ref`` is the source's own identifier for the cell within that container
  (a unit id or an ROI id).

That triple is unique by construction and is exactly what a caller needs to go
back to the source NWB, so nothing else has to be stored to make the key
reproducible.

Note that the *same physical cell* recorded in one acquisition but published in
several derived assets (a reprocessing, a per-collection export) gets a
*different* ``cell_key`` in each. That is intentional: ``cell_key`` identifies a
cell-in-an-asset, which is what provenance tracking needs. Grouping rows that
refer to one physical cell is a separate concern -- see ``session_key`` below and
the ``cell_group_key`` note in the skill.
"""

import hashlib
import re

import pandas as pd

# Length of the hex digest kept for cell_key. 16 hex characters = 64 bits, which
# is ~1e-8 collision probability at 100M cells and keeps the column narrow enough
# that cell_index stays a few MB.
_KEY_CHARS = 16

# Matches the AIND asset-name convention <...>_<subject_id>_<YYYY-MM-DD>_<HH-MM-SS>_<...>,
# used to recover the acquisition a derived asset came from.
_SESSION_RE = re.compile(r"_(?P<subject>\d{6,})_(?P<date>\d{4}-\d{2}-\d{2})_\d{2}-\d{2}-\d{2}")


def cell_key(asset_name: str, container: str, cell_ref: str) -> str:
    """Return the stable ``cell_key`` for one cell.

    Args:
        asset_name: Data asset the cell was read from.
        container: Probe name (ecephys) or imaging plane name (ophys).
        cell_ref: The source's own identifier for the cell within the container.

    Returns:
        A 16-character hex digest, stable across runs and processes.
    """
    # Unit separator as the delimiter: it cannot occur in an asset name, probe
    # name or unit/ROI id, so distinct triples can never hash to one string.
    natural_key = f"{asset_name}\x1f{container}\x1f{cell_ref}"
    return hashlib.blake2b(natural_key.encode("utf-8"), digest_size=_KEY_CHARS // 2).hexdigest()


def cell_keys(asset_name: str, containers: pd.Series, cell_refs: pd.Series) -> pd.Series:
    """Return ``cell_key`` for every cell of one asset.

    Args:
        asset_name: Data asset the cells were read from (constant for the series).
        containers: Per-cell container names.
        cell_refs: Per-cell source identifiers.

    Returns:
        Series of hex digests aligned to the inputs.
    """
    pairs = zip(containers.astype(str), cell_refs.astype(str), strict=True)
    return pd.Series(
        [cell_key(asset_name, container, cell_ref) for container, cell_ref in pairs],
        index=containers.index,
        dtype="string",
    )


def session_key(asset_name: str) -> str | None:
    """Return ``<subject_id>_<YYYY-MM-DD>`` for an asset, or None if unparseable.

    This identifies the *acquisition* rather than the asset, so it survives
    reprocessing: ``..._782149_2025-03-28_10-55-25_processed_2025-09-11_...`` and
    ``..._782149_2025-03-28_10-55-25_processed_2026-08-19_...`` share one
    session_key. It is the join key for side products that were computed against
    an older reprocessing of the same session -- notably the Visual Learning
    co-registration tables, whose ``session_name`` refers to the 2025-09
    processing while the published collection uses the 2026-08 one.

    Args:
        asset_name: Data asset name.

    Returns:
        The session key, or None if the name does not follow the AIND convention.
    """
    match = _SESSION_RE.search(asset_name)
    if match is None:
        return None
    return f"{match.group('subject')}_{match.group('date')}"

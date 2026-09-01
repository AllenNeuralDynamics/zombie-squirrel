"""Declarative source adapters for the cell-by-everything tables.

Every cell in ``cell_index`` / ``cell_properties`` comes from one **source**: an
already-cached per-cell table elsewhere in this package. A source is described
once, declaratively, by a :class:`CellSource`; the builder in ``tables.py`` is
generic and needs no per-source code.

That is the whole point of this module: adding a modality or a project to the
cell-by-everything tables should be a new ``CellSource`` entry (and, if the
property is genuinely new, one line in :data:`PROPERTY_COLUMNS`), not a new code
path. See the ``cell-by-everything`` skill for the step-by-step recipe.

Two conventions make the declarations tolerant of source drift:

* every column mapping is a *tuple of candidates*, first-present-wins, because
  the same quantity is named differently by different pipelines (a probe is
  ``electrode_group_name`` in the Dynamic Routing NWB export but ``probe_name``
  in the AllenSDK Visual Coding export); and
* a source that lacks a property contributes NULL for it rather than being
  special-cased. Sparsity is the expected shape of these tables and costs
  almost nothing in parquet.
"""

from collections.abc import Callable
from dataclasses import dataclass, field

import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.cell_by_everything.nwb_units import read_allensdk_unit_locations
from biodata_cache.cache_table_helpers.swdb_public_assets import SWDB_2026_DERIVED_ASSETS

# --- Canonical property schema ------------------------------------------------
#
# The v1 property set, deliberately short. Each entry is
#   target column -> (dtype, description)
# and every source maps whichever of these it can supply. Adding a property here
# plus a mapping in one source's `properties` is all that is needed; parquet
# stores the NULLs for every other source essentially for free.
PROPERTY_COLUMNS: dict[str, tuple[str, str]] = {
    "structure": ("string", "CCF structure acronym for the cell (peak channel for ecephys, imaging-plane target for ophys)"),
    "ccf_ap": ("float32", "Anterior-posterior CCF coordinate (microns); NULL for cells with no registration"),
    "ccf_dv": ("float32", "Dorsal-ventral CCF coordinate (microns); NULL for cells with no registration"),
    "ccf_ml": ("float32", "Medial-lateral CCF coordinate (microns, Dynamic Routing convention: small = right)"),
    "depth_um": ("float32", "Depth below the brain surface (microns): probe depth for ecephys, imaging depth for ophys"),
    "mean_rate": ("float32", "Mean event rate over the recording (Hz); spike rate for ecephys"),
    "num_spikes": ("float32", "Total number of detected spikes (ecephys only)"),
    "presence_ratio": ("float32", "Fraction of the recording in which the cell is active"),
    "snr": ("float32", "Signal-to-noise ratio of the cell's waveform (ecephys only)"),
    "qc_pass": ("boolean", "Whether the cell passes its pipeline's default quality criteria (ecephys) or is classified as a soma (ophys)"),
    "cell_type_label": ("string", "Cell-type label: sorter decoder class (sua/mua/noise) for ecephys, transcriptomic cluster for co-registered ophys"),
    "cell_type_probability": ("float32", "Confidence of cell_type_label, where the source provides one"),
    "soma_probability": ("float32", "Segmentation classifier probability that the ROI is a soma (ophys only)"),
    "area_px": ("float32", "ROI area in FOV pixels (ophys only)"),
}


@dataclass(frozen=True)
class CellSource:
    """One already-cached per-cell table feeding the cell-by-everything tables.

    Attributes:
        name: Short identifier used in log messages and in ``cell_index.source``.
        modality: Value written to ``cell_index.modality``.
        table: Registry key in ``registry.NAMES`` for the source cache table.
        partition_key: Hive partition key of the source table, or None if the
            source is a single unpartitioned table.
        containers: Candidate source columns for ``cell_index.container``, in
            priority order. The first one present and non-empty wins.
        cell_refs: Candidate source columns for ``cell_index.cell_ref``.
        properties: Mapping of canonical property name (a key of
            :data:`PROPERTY_COLUMNS`) to candidate source columns.
        enumerate_assets: Given the ``asset_basics`` DataFrame, return the asset
            names this source may contribute. Assets with no cached partition are
            skipped by the builder, so over-reporting here is harmless.
        reader: Optional ``asset_name -> rows`` function used *instead of* reading
            a cache table. Set this only when no regularly-rebuilt cache table
            holds the cells -- see :data:`VISUAL_CODING_NEUROPIXELS_UNITS`. When
            set, ``table`` and ``partition_key`` are ignored.
    """

    name: str
    modality: str
    table: str | None
    partition_key: str | None
    containers: tuple[str, ...]
    cell_refs: tuple[str, ...]
    properties: dict[str, tuple[str, ...]] = field(default_factory=dict)
    enumerate_assets: Callable[[pd.DataFrame], list[str]] = lambda df: []
    reader: Callable[[str], pd.DataFrame] | None = None


def _first_present(df: pd.DataFrame, candidates: tuple[str, ...]) -> pd.Series | None:
    """Return the first candidate column that exists and has at least one value.

    A column that exists but is entirely null is treated as absent so that a
    lower-priority candidate can still supply the value. ``platform_pophys``, for
    instance, always writes a ``structure`` column but leaves it null for assets
    whose imaging-plane location string is not parseable.
    """
    for candidate in candidates:
        if candidate in df.columns and df[candidate].notna().any():
            return df[candidate]
    return None


def _derived_names(df_basics: pd.DataFrame, modality_substr: str) -> list[str]:
    """Return derived asset names in ``asset_basics`` carrying a modality."""
    if "modalities" not in df_basics.columns or "data_level" not in df_basics.columns:
        return []
    mask = df_basics["modalities"].apply(
        lambda values: values is not None
        and not isinstance(values, float)
        and any(modality_substr in str(value).lower() for value in values)
    )
    return df_basics[mask & (df_basics["data_level"] == "derived")]["name"].dropna().unique().tolist()


def _derived_ecephys_names(df_basics: pd.DataFrame) -> list[str]:
    """Return derived ecephys asset names owned by the ``platform_ecephys_units`` source.

    Excludes the public Visual Coding Neuropixels collection. Those assets carry
    the ecephys modality in asset_basics, so a plain modality query claims all 57
    of them, but their cells come from :data:`VISUAL_CODING_NEUROPIXELS_UNITS`
    (they have no ``platform_ecephys_units`` partition at all). Two sources
    enumerating one asset means one can reuse the other's cell_properties
    partition and emit its cells a second time under the wrong source label.
    """
    excluded = set(SWDB_2026_DERIVED_ASSETS["vcn"])
    return [name for name in _derived_names(df_basics, "ecephys") if name not in excluded]


def _derived_ophys_names(df_basics: pd.DataFrame) -> list[str]:
    """Return derived asset names for every ophys flavour ``platform_pophys`` covers.

    General population-ophys assets register the ``pophys`` modality while BCI
    single-plane assets register ``ophys``; both land in ``platform_pophys``.
    """
    names = _derived_names(df_basics, "pophys")
    seen = set(names)
    names.extend(name for name in _derived_names(df_basics, "ophys") if name not in seen)
    return names


# --- The sources ---------------------------------------------------------------

ECEPHYS_UNITS = CellSource(
    name="ecephys_units",
    modality="ecephys",
    table=registry.NAMES["ecephys_units"],
    partition_key="asset_name",
    # electrode_group_name is the readable probe name ("probeB"); device_name is
    # the probe serial number in the Dynamic Routing export, so it is only a
    # fallback for pipelines that do not write an electrode group name.
    containers=("electrode_group_name", "device_name"),
    cell_refs=("unit_name", "unit_id", "id"),
    properties={
        "structure": ("structure",),
        "ccf_ap": ("ccf_ap",),
        "ccf_dv": ("ccf_dv",),
        "ccf_ml": ("ccf_ml",),
        "depth_um": ("depth",),
        "mean_rate": ("firing_rate",),
        "num_spikes": ("num_spikes",),
        "presence_ratio": ("presence_ratio",),
        "snr": ("snr",),
        "qc_pass": ("default_qc", "is_qc_pass"),
        "cell_type_label": ("decoder_label",),
        "cell_type_probability": ("decoder_probability",),
    },
    enumerate_assets=_derived_ecephys_names,
)

# Read straight from the public NWB-Zarr rather than from
# platform_visual_coding_neuropixels_units. That table is published by a one-off
# script and is NOT rebuilt by the sync pipeline, so projecting it would pin these
# tables to whenever the script last ran. See nwb_units.py, which deliberately
# carries its own copy of the reading logic for the same reason.
VISUAL_CODING_NEUROPIXELS_UNITS = CellSource(
    name="visual_coding_neuropixels_units",
    modality="ecephys",
    table=None,
    partition_key=None,
    containers=("probe_name",),
    cell_refs=("unit_id",),
    properties={
        "structure": ("structure",),
        "ccf_ap": ("ccf_ap",),
        "ccf_dv": ("ccf_dv",),
        "ccf_ml": ("ccf_ml",),
    },
    # The collection is a fixed published set of assets, not a DocDB query.
    enumerate_assets=lambda df: list(SWDB_2026_DERIVED_ASSETS["vcn"]),
    reader=read_allensdk_unit_locations,
)

POPHYS_ROIS = CellSource(
    name="pophys",
    modality="ophys",
    table=registry.NAMES["pophys"],
    partition_key="asset_name",
    containers=("plane",),
    cell_refs=("roi_id",),
    properties={
        "structure": ("structure",),
        "depth_um": ("depth_um",),
        # is_soma is the ophys analogue of a unit's default_qc: the segmentation
        # classifier's accept/reject call on the ROI.
        "qc_pass": ("is_soma",),
        "soma_probability": ("soma_probability",),
        "area_px": ("area_px",),
    },
    enumerate_assets=_derived_ophys_names,
)

SOURCES: tuple[CellSource, ...] = (
    ECEPHYS_UNITS,
    VISUAL_CODING_NEUROPIXELS_UNITS,
    POPHYS_ROIS,
)


def project_source(source: CellSource, asset_name: str, rows: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame] | None:
    """Project one asset's source rows onto the index and property schemas.

    Args:
        source: The source description.
        asset_name: Asset the rows belong to.
        rows: The source table's rows for that asset.

    Returns:
        ``(index_frame, property_frame)`` sharing a ``cell_key`` column, or None
        if the rows carry neither a usable container nor a cell reference.
    """
    from biodata_cache.cache_table_helpers.cell_by_everything.keys import cell_keys

    if rows.empty:
        return None
    container = _first_present(rows, source.containers)
    cell_ref = _first_present(rows, source.cell_refs)
    if container is None or cell_ref is None:
        return None

    keys = cell_keys(asset_name, container, cell_ref)
    index = pd.DataFrame(
        {
            "cell_key": keys,
            "asset_name": asset_name,
            "modality": source.modality,
            "source": source.name,
            "container": container.astype("string").to_numpy(),
            "cell_ref": cell_ref.astype("string").to_numpy(),
        }
    )

    properties = pd.DataFrame({"cell_key": keys})
    for target, candidates in source.properties.items():
        values = _first_present(rows, candidates)
        if values is None:
            continue
        properties[target] = values.to_numpy()
    return index, properties

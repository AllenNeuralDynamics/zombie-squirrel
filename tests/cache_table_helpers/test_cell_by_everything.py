"""Tests for the cell-by-everything cache tables."""

import dataclasses
import itertools
import json
import threading
from unittest.mock import patch

import pandas as pd
import pytest

from biodata_cache.backend import MemoryBackend
from biodata_cache.cache_table_helpers.cell_by_everything import (
    build_cell_by_everything,
    cell_genes,
    cell_genes_columns,
    cell_index,
    cell_index_columns,
    cell_properties,
    cell_properties_columns,
)
from biodata_cache.cache_table_helpers.cell_by_everything.genes import (
    build_cell_genes,
    cell_type_lookup,
    gene_annotations,
)
from biodata_cache.cache_table_helpers.cell_by_everything.keys import cell_key, cell_keys, session_key
from biodata_cache.cache_table_helpers.cell_by_everything.nwb_units import (
    _find_nwb_prefixes,
    _load_units_metadata,
    _probe_names_by_probe_id,
    read_allensdk_unit_locations,
)
from biodata_cache.cache_table_helpers.cell_by_everything.sources import (
    ECEPHYS_UNITS,
    POPHYS_ROIS,
    PROPERTY_COLUMNS,
    SOURCES,
    VISUAL_CODING_NEUROPIXELS_UNITS,
    _first_present,
    project_source,
)

EPHYS_ASSET = "ecephys_662892_2023-08-24_14-28-28_nwb_2026-08-04_15-13-29"
OPHYS_ASSET = "multiplane-ophys_782149_2025-03-28_10-55-25_processed_2026-08-19_00-34-09"


def _basics():
    """Return a minimal asset_basics covering one ecephys and one ophys asset."""
    return pd.DataFrame(
        {
            "name": [EPHYS_ASSET, OPHYS_ASSET],
            "subject_id": ["662892", "782149"],
            "project_name": ["Dynamic Routing", "Learning mFISH-V1omFISH"],
            "modalities": [["ecephys"], ["pophys"]],
            "data_level": ["derived", "derived"],
        }
    )


def _units():
    """Return two rows shaped like a platform_ecephys_units partition."""
    return pd.DataFrame(
        {
            "device_name": ["18005123131", "18005123131"],
            "electrode_group_name": ["probeA", "probeB"],
            "unit_id": ["A-0", "B-1"],
            "structure": ["VISp", "LGd"],
            "ccf_ap": [6100, 6200],
            "ccf_dv": [7400, 7500],
            "ccf_ml": [3800, 3900],
            "firing_rate": [13.32, 5.37],
            "num_spikes": [95456.0, 38480.0],
            "presence_ratio": [1.0, 0.9],
            "snr": [6.7, 4.0],
            "default_qc": [True, False],
            "decoder_label": ["sua", "mua"],
            "decoder_probability": [0.7, 0.71],
        }
    )


def _rois():
    """Return three rows shaped like a platform_pophys partition."""
    return pd.DataFrame(
        {
            "asset_name": [OPHYS_ASSET] * 3,
            "plane": ["VISp_0", "VISp_0", "VISp_1"],
            "structure": [None, None, None],
            "depth_um": [None, None, None],
            "roi_id": [8, 9, 3],
            "is_soma": [1, 0, 1],
            "soma_probability": [0.91, 0.02, 0.77],
            "area_px": [210, 60, 150],
        }
    )


def _coreg():
    """Return co-registration rows, including the unmatched hcr_id sentinel.

    ``session_name`` deliberately names an *older* reprocessing than
    ``OPHYS_ASSET`` -- as the real tables do -- so the session_key join is what
    has to make the link.
    """
    return pd.DataFrame(
        {
            "subject_id": ["782149"] * 3,
            "session_name": ["multiplane-ophys_782149_2025-03-28_10-55-25_processed_2025-09-11_20-46-54"] * 3,
            "session_key": ["782149_2025-03-28"] * 3,
            "plane_id": ["VISp_0", "VISp_0", "VISp_1"],
            "roi_id": pd.array([8, 9, 3], dtype="Int64"),
            "hcr_id": pd.array([13835, -1, 19376], dtype="Int64"),
        }
    )


def _cell_gene():
    """Return cell-by-gene rows for the two matched HCR cells."""
    return pd.DataFrame(
        {
            "subject_id": ["782149", "782149"],
            "cell_id": ["13835", "19376"],
            "cell_class": ["inhibitory", "inhibitory"],
            "cell_subclass": ["Lamp5", "Vip"],
            "cell_type": ["Lamp5-1 (Npy/Cck)", "Vip-8 (Chat/Calb2/Pthlh)"],
            "cluster_id": [1, 8],
            "total_counts": [120, 340],
            "n_genes": [5, 9],
            "R5-594-Sst": [5.0, 0.0],
        }
    )


@pytest.fixture
def seeded_backend():
    """Return a MemoryBackend preloaded with every source table, patched in."""
    backend = MemoryBackend()
    backend.write(f"platform_ecephys_units/{EPHYS_ASSET}", _units())
    backend.write(f"platform_pophys/{OPHYS_ASSET}", _rois())
    backend.write("platform_visual_learning_coreg/782149", _coreg())
    backend.write("platform_visual_learning_cell_gene/782149", _cell_gene())
    # The Visual Coding source reads public NWB-Zarr directly, so it is stubbed
    # out here: these tests must never touch S3.
    offline_sources = (ECEPHYS_UNITS, POPHYS_ROIS)
    with patch("biodata_cache.registry.BACKEND", backend), patch(
        "biodata_cache.cache_table_helpers.cell_by_everything.tables.asset_basics", _basics
    ), patch(
        "biodata_cache.cache_table_helpers.cell_by_everything.tables.SOURCES", offline_sources
    ):
        yield backend


# --- keys ---------------------------------------------------------------------


def test_cell_key_is_deterministic_and_distinguishes_every_field():
    assert cell_key("asset", "probeA", "0") == cell_key("asset", "probeA", "0")
    distinct = {
        cell_key("asset", "probeA", "0"),
        cell_key("asset", "probeA", "1"),
        cell_key("asset", "probeB", "0"),
        cell_key("other", "probeA", "0"),
    }
    assert len(distinct) == 4


def test_cell_key_delimiter_prevents_field_boundary_collisions():
    """Concatenation without a delimiter would collide on these two triples."""
    assert cell_key("a", "bc", "d") != cell_key("a", "b", "cd")


def test_cell_keys_matches_the_scalar_function():
    containers = pd.Series(["probeA", "probeB"])
    refs = pd.Series([0, 1])
    result = cell_keys("asset", containers, refs)
    assert result.tolist() == [cell_key("asset", "probeA", "0"), cell_key("asset", "probeB", "1")]


def test_session_key_survives_reprocessing():
    older = "multiplane-ophys_782149_2025-03-28_10-55-25_processed_2025-09-11_20-46-54"
    newer = "multiplane-ophys_782149_2025-03-28_10-55-25_processed_2026-08-19_00-34-09"
    assert session_key(older) == session_key(newer) == "782149_2025-03-28"


def test_session_key_is_none_for_a_nonconforming_name():
    assert session_key("some-collection-export") is None


# --- sources ------------------------------------------------------------------


def test_first_present_skips_an_all_null_column():
    """platform_pophys writes structure but leaves it null for unparseable planes."""
    frame = pd.DataFrame({"structure": [None, None], "location": ["VISp", "VISp"]})
    assert _first_present(frame, ("structure", "location")).tolist() == ["VISp", "VISp"]


def test_first_present_returns_none_when_no_candidate_exists():
    assert _first_present(pd.DataFrame({"other": [1]}), ("a", "b")) is None


def test_project_source_returns_none_without_a_cell_reference():
    rows = pd.DataFrame({"plane": ["VISp_0"]})
    assert project_source(POPHYS_ROIS, OPHYS_ASSET, rows) is None


def test_project_source_maps_index_and_properties():
    index, properties = project_source(POPHYS_ROIS, OPHYS_ASSET, _rois())
    assert index["container"].tolist() == ["VISp_0", "VISp_0", "VISp_1"]
    assert index["cell_ref"].tolist() == ["8", "9", "3"]
    assert index["modality"].tolist() == ["ophys"] * 3
    assert properties["cell_key"].tolist() == index["cell_key"].tolist()
    assert properties["soma_probability"].tolist() == [0.91, 0.02, 0.77]
    # pophys supplies no rate, so the column is simply absent before coercion.
    assert "mean_rate" not in properties.columns


# --- genes --------------------------------------------------------------------


def test_gene_annotations_drops_the_unmatched_sentinel(seeded_backend):
    annotations = gene_annotations()
    assert annotations["hcr_id"].tolist() == [13835, 19376]
    assert annotations["cell_ref"].tolist() == ["8", "3"]


def test_gene_annotations_is_empty_without_sources():
    with patch("biodata_cache.registry.BACKEND", MemoryBackend()):
        assert gene_annotations().empty


def test_build_cell_genes_is_empty_when_nothing_joins():
    index = pd.DataFrame(
        {
            "cell_key": ["k"],
            "session_key": ["999999_2020-01-01"],
            "container": ["VISp_0"],
            "cell_ref": ["8"],
        }
    )
    annotations = pd.DataFrame(
        {
            "session_key": ["782149_2025-03-28"],
            "container": ["VISp_0"],
            "cell_ref": ["8"],
            "subject_id": ["782149"],
            "hcr_id": pd.array([1], dtype="Int64"),
        }
    )
    assert build_cell_genes(index, annotations).empty


def test_cell_type_lookup_is_empty_without_annotations():
    assert cell_type_lookup(pd.DataFrame()).empty


# --- the built tables ---------------------------------------------------------


def test_build_populates_all_three_tables(seeded_backend):
    build_cell_by_everything()

    index = seeded_backend.read("cell_index")
    assert len(index) == 5
    assert index["cell_key"].is_unique
    assert set(index["source"]) == {"ecephys_units", "pophys"}
    assert set(index["modality"]) == {"ecephys", "ophys"}
    # The readable probe name wins over the probe serial in device_name.
    assert set(index[index["modality"] == "ecephys"]["container"]) == {"probeA", "probeB"}
    assert set(index["project_name"]) == {"Dynamic Routing", "Learning mFISH-V1omFISH"}

    genes = seeded_backend.read("cell_genes/782149")
    assert len(genes) == 2
    assert set(genes["cell_key"]) <= set(index["cell_key"])
    assert genes["R5-594-Sst"].dtype == "float32"


def test_properties_are_sparse_per_modality(seeded_backend):
    build_cell_by_everything()

    ephys = seeded_backend.read(f"cell_properties/{EPHYS_ASSET}")
    ophys = seeded_backend.read(f"cell_properties/{OPHYS_ASSET}")
    # Both partitions carry the full canonical schema...
    assert list(ephys.columns) == list(ophys.columns)
    # ...but each fills only what its pipeline produces.
    assert ephys["snr"].notna().all()
    assert ephys["soma_probability"].isna().all()
    assert ophys["soma_probability"].notna().all()
    assert ophys["snr"].isna().all()


def test_property_dtypes_match_the_declared_schema(seeded_backend):
    build_cell_by_everything()
    ephys = seeded_backend.read(f"cell_properties/{EPHYS_ASSET}")
    for name, (dtype, _description) in PROPERTY_COLUMNS.items():
        assert str(ephys[name].dtype) == dtype, name


def test_transcriptomic_label_reaches_ophys_cells_across_a_reprocessing(seeded_backend):
    build_cell_by_everything()
    ophys = seeded_backend.read(f"cell_properties/{OPHYS_ASSET}")
    labels = ophys["cell_type_label"].dropna().tolist()
    assert sorted(labels) == ["Lamp5-1 (Npy/Cck)", "Vip-8 (Chat/Calb2/Pthlh)"]


def test_source_label_wins_over_the_transcriptomic_one(seeded_backend):
    build_cell_by_everything()
    ephys = seeded_backend.read(f"cell_properties/{EPHYS_ASSET}")
    assert sorted(ephys["cell_type_label"].tolist()) == ["mua", "sua"]


def test_cell_keys_are_stable_across_a_rebuild(seeded_backend):
    build_cell_by_everything()
    first = seeded_backend.read("cell_index")["cell_key"].tolist()
    build_cell_by_everything()
    assert seeded_backend.read("cell_index")["cell_key"].tolist() == first


def test_build_raises_when_every_source_is_empty():
    with patch("biodata_cache.registry.BACKEND", MemoryBackend()), patch(
        "biodata_cache.cache_table_helpers.cell_by_everything.tables.asset_basics", _basics
    ), patch(
        "biodata_cache.cache_table_helpers.cell_by_everything.tables.SOURCES",
        (ECEPHYS_UNITS, POPHYS_ROIS),
    ):
        with pytest.raises(RuntimeError, match="No cells were projected"):
            build_cell_by_everything()


# --- the public table functions ------------------------------------------------


def test_cell_index_builds_on_an_empty_cache(seeded_backend):
    assert len(cell_index()) == 5


def test_cell_properties_reads_one_partition(seeded_backend):
    cell_index(force_update=True)
    assert len(cell_properties(asset_name=EPHYS_ASSET)) == 2


def test_cell_properties_lazy_returns_a_location(seeded_backend):
    location = cell_properties(asset_name=EPHYS_ASSET, lazy=True)
    assert EPHYS_ASSET in location


def test_cell_properties_requires_an_asset_name(seeded_backend):
    with pytest.raises(ValueError, match="asset_name is required"):
        cell_properties()


def test_cell_genes_requires_a_subject_id(seeded_backend):
    with pytest.raises(ValueError, match="subject_id is required"):
        cell_genes()


def test_cell_genes_reads_one_partition(seeded_backend):
    cell_index(force_update=True)
    assert len(cell_genes(subject_id="782149")) == 2


def test_force_update_returns_empty_and_writes_the_cache(seeded_backend):
    assert cell_properties(force_update=True).empty
    assert not seeded_backend.read("cell_index").empty


# --- registry column definitions ------------------------------------------------


def test_column_definitions_cover_the_written_columns(seeded_backend):
    build_cell_by_everything()
    index = seeded_backend.read("cell_index")
    ephys = seeded_backend.read(f"cell_properties/{EPHYS_ASSET}")
    genes = seeded_backend.read("cell_genes/782149")

    assert [column.name for column in cell_index_columns()] == list(index.columns)
    assert [column.name for column in cell_properties_columns()] == list(ephys.columns)
    assert [column.name for column in cell_genes_columns()] == list(genes.columns)


def test_every_column_definition_has_a_description():
    for columns in (cell_index_columns(), cell_properties_columns(), cell_genes_columns()):
        assert all(column.description for column in columns)


# --- the self-contained NWB reader ---------------------------------------------
#
# nwb_units.py deliberately keeps its own copy of the NWB-Zarr reading logic so
# these tables never depend on the one-off Visual Coding units table. These tests
# cover the copied code against a stub S3 client -- never the network.


class _StubS3:
    """Minimal S3 client stub: prefix listings and object bodies from dicts."""

    def __init__(self, listings=None, objects=None):
        self.listings = listings or {}
        self.objects = objects or {}

    def list_objects_v2(self, Bucket, Prefix, Delimiter=None, **kwargs):  # noqa: N803
        """Return the canned common-prefix listing for a prefix."""
        prefixes = self.listings.get(Prefix, [])
        return {"CommonPrefixes": [{"Prefix": prefix} for prefix in prefixes]}

    def get_object(self, Bucket, Key):  # noqa: N803
        """Return the canned body for a key, or raise as S3 would for a miss."""
        if Key not in self.objects:
            raise KeyError(Key)

        class _Body:
            def __init__(self, data):
                self.data = data

            def read(self):
                """Return the object bytes."""
                return self.data

        return {"Body": _Body(self.objects[Key])}


def test_find_nwb_prefixes_prefers_the_nwb_subfolder():
    client = _StubS3(
        listings={
            "asset/nwb/": ["asset/nwb/session.nwb.zarr/"],
            "asset/": ["asset/other.nwb.zarr/"],
        }
    )
    assert _find_nwb_prefixes(client, "bucket", "asset") == ["asset/nwb/session.nwb.zarr"]


def test_find_nwb_prefixes_falls_back_to_the_asset_root():
    client = _StubS3(listings={"asset/nwb/": [], "asset/": ["asset/session.nwb.zarr/", "asset/derived/"]})
    assert _find_nwb_prefixes(client, "bucket", "asset") == ["asset/session.nwb.zarr"]


def test_load_units_metadata_skips_an_empty_zmetadata():
    client = _StubS3(objects={"store/.zmetadata": b""})
    assert _load_units_metadata(client, "bucket", "store") is None


def test_load_units_metadata_skips_malformed_json():
    client = _StubS3(objects={"store/.zmetadata": b"{not json"})
    assert _load_units_metadata(client, "bucket", "store") is None


def test_load_units_metadata_skips_a_store_without_units():
    body = json.dumps({"metadata": {"acquisition/.zgroup": {}}}).encode()
    client = _StubS3(objects={"store/.zmetadata": body})
    assert _load_units_metadata(client, "bucket", "store") is None


def test_load_units_metadata_returns_bytes_and_parsed_metadata():
    metadata = {"units/id/.zarray": {"shape": [3]}}
    body = json.dumps({"metadata": metadata}).encode()
    client = _StubS3(objects={"store/.zmetadata": body})
    raw, parsed = _load_units_metadata(client, "bucket", "store")
    assert raw == body
    assert parsed == metadata


def test_probe_names_resolve_from_electrode_group_attrs():
    prefix = "store/general/extracellular_ephys/"
    client = _StubS3(
        listings={prefix: [f"{prefix}probeA/", f"{prefix}probeB/", f"{prefix}electrodes/"]},
        objects={
            f"{prefix}probeA/.zattrs": json.dumps({"probe_id": 11}).encode(),
            f"{prefix}probeB/.zattrs": json.dumps({"probe_id": 22}).encode(),
        },
    )
    assert _probe_names_by_probe_id(client, "bucket", "store") == {11: "probeA", 22: "probeB"}


def test_probe_names_skip_an_unreadable_group():
    prefix = "store/general/extracellular_ephys/"
    client = _StubS3(
        listings={prefix: [f"{prefix}probeA/", f"{prefix}probeB/"]},
        objects={f"{prefix}probeA/.zattrs": json.dumps({"probe_id": 11}).encode()},
    )
    assert _probe_names_by_probe_id(client, "bucket", "store") == {11: "probeA"}


def test_reader_returns_empty_without_an_nwb_store():
    with patch("boto3.client", return_value=_StubS3(listings={"asset/nwb/": [], "asset/": []})):
        assert read_allensdk_unit_locations("asset").empty


def test_visual_coding_source_reads_nwb_rather_than_a_cache_table():
    """The one-off units table must not be a dependency of these tables."""
    assert VISUAL_CODING_NEUROPIXELS_UNITS.table is None
    assert VISUAL_CODING_NEUROPIXELS_UNITS.reader is read_allensdk_unit_locations
    assert len(VISUAL_CODING_NEUROPIXELS_UNITS.enumerate_assets(pd.DataFrame())) > 0


def test_reader_failure_is_isolated_to_one_asset(seeded_backend):
    """A source whose reader raises contributes no cells instead of aborting."""

    def _boom(asset_name):
        raise RuntimeError("unreadable store")

    exploding = dataclasses.replace(VISUAL_CODING_NEUROPIXELS_UNITS, reader=_boom)
    with patch(
        "biodata_cache.cache_table_helpers.cell_by_everything.tables.SOURCES",
        (ECEPHYS_UNITS, POPHYS_ROIS, exploding),
    ):
        build_cell_by_everything()
    # The two healthy sources still produced their cells.
    assert len(seeded_backend.read("cell_index")) == 5


def test_reader_source_contributes_cells_through_the_generic_pipeline(seeded_backend):
    """A reader-backed source maps onto index and properties like any other."""
    rows = pd.DataFrame(
        {
            "probe_name": ["probeA", "probeC"],
            "unit_id": [951, 952],
            "structure": ["VISp", "LGd"],
            "ccf_ap": [7000, 7100],
            "ccf_dv": [3000, 3100],
            "ccf_ml": [8000, 8100],
        }
    )
    stub = dataclasses.replace(
        VISUAL_CODING_NEUROPIXELS_UNITS,
        enumerate_assets=lambda df: ["vcn_asset"],
        reader=lambda asset_name: rows,
    )
    with patch(
        "biodata_cache.cache_table_helpers.cell_by_everything.tables.SOURCES", (stub,)
    ), patch(
        "biodata_cache.cache_table_helpers.cell_by_everything.tables.asset_basics",
        lambda: pd.DataFrame(
            {
                "name": ["vcn_asset"],
                "subject_id": ["123456"],
                "project_name": ["Visual Coding"],
                "modalities": [["ecephys"]],
                "data_level": ["derived"],
            }
        ),
    ):
        build_cell_by_everything()

    index = seeded_backend.read("cell_index")
    assert index["source"].tolist() == ["visual_coding_neuropixels_units"] * 2
    assert index["container"].tolist() == ["probeA", "probeC"]
    assert index["cell_ref"].tolist() == ["951", "952"]

    properties = seeded_backend.read("cell_properties/vcn_asset")
    assert properties["ccf_ml"].tolist() == [8000.0, 8100.0]
    # The collection provides no rate or QC, so those stay NULL.
    assert properties["mean_rate"].isna().all()
    assert properties["qc_pass"].isna().all()


def test_source_frames_are_streamed_not_collected():
    """_iter_source_frames must be a generator: collecting every partition OOMs."""
    import inspect

    from biodata_cache.cache_table_helpers.cell_by_everything.tables import _iter_source_frames

    assert inspect.isgeneratorfunction(_iter_source_frames)


def test_source_frames_reads_in_bounded_batches():
    """Reads are issued per batch, so peak memory is one batch, not the whole source."""
    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    assets = [f"asset_{i:04d}" for i in range(9)]
    in_flight = []
    source = dataclasses.replace(
        VISUAL_CODING_NEUROPIXELS_UNITS,
        enumerate_assets=lambda df: assets,
        reader=lambda name: pd.DataFrame({"probe_name": ["probeA"], "unit_id": [1]}),
    )
    with patch.object(tables, "_READ_BATCH", 4):
        original = tables._read_asset_rows

        def _tracking(src, name):
            in_flight.append(name)
            return original(src, name)

        with patch.object(tables, "_read_asset_rows", _tracking):
            produced = []
            for work in tables._iter_source_frames(source, pd.DataFrame(), True):
                produced.append(work.asset_name)
                # Only the current batch may have been read so far.
                assert len(in_flight) <= ((len(produced) - 1) // 4 + 1) * 4

    assert produced == assets


# --- skip already-written partitions -------------------------------------------


def test_existing_partitions_are_not_rewritten(seeded_backend):
    """A second build must not re-read the source or rewrite a written partition."""
    build_cell_by_everything()
    first = seeded_backend.read("cell_index")

    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    reads, writes = [], []
    original_read, original_write = tables._read_asset_rows, seeded_backend.write

    def _tracking_read(source, asset_name):
        reads.append(asset_name)
        return original_read(source, asset_name)

    def _tracking_write(table_name, data):
        writes.append(table_name)
        return original_write(table_name, data)

    with patch.object(tables, "_read_asset_rows", _tracking_read), patch.object(
        seeded_backend, "write", _tracking_write
    ):
        build_cell_by_everything()

    assert reads == [], f"sources were re-read: {reads}"
    assert not [name for name in writes if name.startswith("cell_properties/")]
    # cell_index and cell_genes are global objects and are still rewritten.
    assert "cell_index" in writes
    # ...and the reused rows reproduce the original index exactly.
    rebuilt = seeded_backend.read("cell_index")
    assert rebuilt.equals(first)


def test_force_rewrite_rebuilds_every_partition(seeded_backend):
    build_cell_by_everything()

    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    reads = []
    original_read = tables._read_asset_rows

    def _tracking_read(source, asset_name):
        reads.append(asset_name)
        return original_read(source, asset_name)

    with patch.object(tables, "_read_asset_rows", _tracking_read):
        build_cell_by_everything(force_rewrite=True)

    assert sorted(reads) == sorted([EPHYS_ASSET, OPHYS_ASSET])


def test_properties_partition_carries_the_identity_columns(seeded_backend):
    """Without these, an existing partition could not rebuild its cell_index rows."""
    build_cell_by_everything()
    properties = seeded_backend.read(f"cell_properties/{EPHYS_ASSET}")
    index = seeded_backend.read("cell_index")
    merged = index.merge(properties, on="cell_key", suffixes=("_idx", "_prop"))
    assert merged["container_idx"].tolist() == merged["container_prop"].tolist()
    assert merged["cell_ref_idx"].tolist() == merged["cell_ref_prop"].tolist()


def test_a_partition_without_identity_columns_is_rebuilt(seeded_backend):
    """Partitions from an older build lack the identity columns and must be redone."""
    build_cell_by_everything()
    legacy = seeded_backend.read(f"cell_properties/{EPHYS_ASSET}").drop(columns=["container", "cell_ref"])
    seeded_backend.write(f"cell_properties/{EPHYS_ASSET}", legacy)

    build_cell_by_everything()

    repaired = seeded_backend.read(f"cell_properties/{EPHYS_ASSET}")
    assert {"container", "cell_ref"}.issubset(repaired.columns)
    assert len(seeded_backend.read("cell_index")) == 5


# --- write efficiency ----------------------------------------------------------


def test_writes_are_issued_concurrently(seeded_backend):
    """Serial writes are round-trip bound; a batch must go out concurrently."""
    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    threads = set()
    original = seeded_backend.write

    def _tracking(table_name, data):
        if table_name.startswith("cell_properties/"):
            threads.add(threading.current_thread().name)
        return original(table_name, data)

    with patch.object(seeded_backend, "write", _tracking), patch.object(tables, "_WRITE_BATCH", 1):
        build_cell_by_everything()

    # Every property write happened on a pool worker, not the main thread.
    assert threads
    assert "MainThread" not in threads


def test_absent_partitions_are_not_cleared(seeded_backend):
    """clear_partition is an extra S3 LIST; skip it when we know there is nothing."""
    cleared = []
    original = seeded_backend.clear_partition

    def _tracking(table_name):
        if table_name.startswith("cell_properties/"):
            cleared.append(table_name)
        return original(table_name)

    with patch.object(seeded_backend, "clear_partition", _tracking):
        build_cell_by_everything()
    assert cleared == [], f"cleared partitions that did not exist: {cleared}"


def test_existing_partitions_are_cleared_on_force_rewrite(seeded_backend):
    build_cell_by_everything()
    cleared = []
    original = seeded_backend.clear_partition

    def _tracking(table_name):
        if table_name.startswith("cell_properties/"):
            cleared.append(table_name)
        return original(table_name)

    with patch.object(seeded_backend, "clear_partition", _tracking):
        build_cell_by_everything(force_rewrite=True)
    assert sorted(cleared) == sorted(
        [f"cell_properties/{EPHYS_ASSET}", f"cell_properties/{OPHYS_ASSET}"]
    )


def test_pending_writes_are_flushed_at_the_end_of_a_source(seeded_backend):
    """A partial final batch must still be written."""
    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    with patch.object(tables, "_WRITE_BATCH", 1000):
        build_cell_by_everything()
    assert len(seeded_backend.read(f"cell_properties/{EPHYS_ASSET}")) == 2
    assert len(seeded_backend.read(f"cell_properties/{OPHYS_ASSET}")) == 3


# --- source ownership of a partition -------------------------------------------
#
# Regression tests for a real bug: the skip check was keyed on asset_name alone,
# so a source would reuse a partition another source had written. All 57 Visual
# Coding assets also carry the ecephys modality in asset_basics, so ecephys_units
# claimed them and cell_index ended up with 133,777 duplicate cell_keys.


def test_source_enumerations_do_not_overlap():
    """Two sources enumerating one asset lets one reuse the other's partition."""
    from biodata_cache.cache_table_helpers.swdb_public_assets import SWDB_2026_DERIVED_ASSETS

    basics = pd.DataFrame(
        {
            "name": list(SWDB_2026_DERIVED_ASSETS["vcn"]),
            "modalities": [["ecephys"]] * len(SWDB_2026_DERIVED_ASSETS["vcn"]),
            "data_level": ["derived"] * len(SWDB_2026_DERIVED_ASSETS["vcn"]),
        }
    )
    enumerated = {source.name: set(source.enumerate_assets(basics)) for source in SOURCES}
    for first, second in itertools.combinations(enumerated, 2):
        overlap = enumerated[first] & enumerated[second]
        assert not overlap, f"{first} and {second} both enumerate {sorted(overlap)[:3]}"


def test_ecephys_units_does_not_enumerate_visual_coding_assets():
    from biodata_cache.cache_table_helpers.swdb_public_assets import SWDB_2026_DERIVED_ASSETS

    vcn = list(SWDB_2026_DERIVED_ASSETS["vcn"])
    basics = pd.DataFrame(
        {"name": vcn, "modalities": [["ecephys"]] * len(vcn), "data_level": ["derived"] * len(vcn)}
    )
    assert ECEPHYS_UNITS.enumerate_assets(basics) == []


def test_partition_records_its_owning_source(seeded_backend):
    build_cell_by_everything()
    ephys = seeded_backend.read(f"cell_properties/{EPHYS_ASSET}")
    ophys = seeded_backend.read(f"cell_properties/{OPHYS_ASSET}")
    assert set(ephys["source"]) == {"ecephys_units"}
    assert set(ophys["source"]) == {"pophys"}


def test_a_source_does_not_reuse_another_sources_partition(seeded_backend):
    """The ownership check must hold even if enumerations do overlap."""
    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    build_cell_by_everything()
    baseline = seeded_backend.read("cell_index")

    # Force the overlap the enumeration fix removes: make the ophys source also
    # enumerate the ecephys asset, whose partition pophys does not own.
    greedy = dataclasses.replace(POPHYS_ROIS, enumerate_assets=lambda df: [OPHYS_ASSET, EPHYS_ASSET])
    with patch.object(tables, "SOURCES", (ECEPHYS_UNITS, greedy)):
        build_cell_by_everything()

    rebuilt = seeded_backend.read("cell_index")
    assert rebuilt["cell_key"].is_unique, "a cell was emitted twice under two sources"
    assert len(rebuilt) == len(baseline)
    assert set(rebuilt[rebuilt["source"] == "ecephys_units"]["asset_name"]) == {EPHYS_ASSET}


def test_two_sources_never_clobber_one_partition(seeded_backend):
    """A second source writing rows for a claimed asset is refused, not silent."""
    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    # Both sources produce rows for the SAME asset name; cell_properties is keyed
    # on asset_name only, so one must win and the other must be refused.
    seeded_backend.write(f"platform_pophys/{EPHYS_ASSET}", _rois())
    greedy = dataclasses.replace(POPHYS_ROIS, enumerate_assets=lambda df: [EPHYS_ASSET])
    with patch.object(tables, "SOURCES", (ECEPHYS_UNITS, greedy)):
        build_cell_by_everything()

    index = seeded_backend.read("cell_index")
    assert index["cell_key"].is_unique
    # The first source keeps the asset; the partition is not a mix of both.
    partition = seeded_backend.read(f"cell_properties/{EPHYS_ASSET}")
    assert set(partition["source"]) == {"ecephys_units"}
    assert set(index[index["asset_name"] == EPHYS_ASSET]["source"]) == {"ecephys_units"}


def test_cell_index_keys_are_unique_after_a_reuse_build(seeded_backend):
    """The invariant that the bug violated, asserted directly."""
    build_cell_by_everything()
    build_cell_by_everything()
    index = seeded_backend.read("cell_index")
    assert index["cell_key"].is_unique
    assert len(index) == 5


def test_reuse_reads_are_batched_concurrently(seeded_backend):
    """Reading reused partitions one at a time is what made a re-run slow."""
    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    build_cell_by_everything()

    threads = set()
    original = tables._read_existing_partition

    def _tracking(asset_name):
        threads.add(threading.current_thread().name)
        return original(asset_name)

    with patch.object(tables, "_read_existing_partition", _tracking):
        build_cell_by_everything()

    assert threads, "no existing partitions were read back"
    assert "MainThread" not in threads, "reuse reads ran serially on the main thread"


# --- per-asset annotation cost --------------------------------------------------
#
# Regression tests for the dominant cost of the whole job: the per-asset
# annotation used to rebuild two dicts from the entire ~105k-row asset_basics on
# every call (206 ms/asset, ~18 min of CPU across a run). The lookups are now
# built once per build and passed down.


def test_asset_context_is_built_once_per_build(seeded_backend):
    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    calls = []
    original = tables._asset_context

    def _tracking(df_basics):
        calls.append(len(df_basics))
        return original(df_basics)

    with patch.object(tables, "_asset_context", _tracking):
        build_cell_by_everything()

    assert len(calls) == 1, f"asset context rebuilt {len(calls)} times"


def test_per_asset_annotation_does_not_touch_asset_basics(seeded_backend):
    """The per-asset path must take the prebuilt lookups, not the whole table."""
    import inspect

    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    params = inspect.signature(tables._annotate_asset_index).parameters
    assert "df_basics" not in params
    assert "context" in params


def test_annotation_yields_the_same_values_by_either_path():
    """Per-asset broadcast and multi-asset map must agree exactly."""
    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    basics = _basics()
    context = tables._asset_context(basics)
    index = pd.DataFrame(
        {
            "cell_key": ["k1", "k2"],
            "asset_name": [OPHYS_ASSET, OPHYS_ASSET],
            "modality": ["ophys", "ophys"],
            "source": ["pophys", "pophys"],
            "container": ["VISp_0", "VISp_0"],
            "cell_ref": ["8", "9"],
        }
    )
    per_asset = tables._annotate_asset_index(index.copy(), OPHYS_ASSET, context)
    multi = tables._annotate_index(index.copy(), context)
    assert per_asset.equals(multi)
    assert per_asset["subject_id"].tolist() == ["782149", "782149"]
    assert per_asset["session_key"].tolist() == ["782149_2025-03-28", "782149_2025-03-28"]


def test_annotation_handles_an_asset_missing_from_asset_basics():
    from biodata_cache.cache_table_helpers.cell_by_everything import tables

    context = tables._asset_context(_basics())
    index = pd.DataFrame(
        {
            "cell_key": ["k1"],
            "asset_name": ["not_in_basics"],
            "modality": ["ophys"],
            "source": ["pophys"],
            "container": ["VISp_0"],
            "cell_ref": ["8"],
        }
    )
    annotated = tables._annotate_asset_index(index, "not_in_basics", context)
    assert pd.isna(annotated["subject_id"].iloc[0])
    assert pd.isna(annotated["session_key"].iloc[0])

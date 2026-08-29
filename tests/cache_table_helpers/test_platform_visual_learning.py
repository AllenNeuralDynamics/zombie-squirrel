"""Tests for the public Visual Learning lookup-table cache builders."""

import io
from unittest.mock import patch

import h5py
import numpy as np
import pandas as pd

from biodata_cache.backend import MemoryBackend
from biodata_cache.cache_table_helpers.platform_visual_learning import (
    CELL_GENE_COLUMN_ORDER,
    COREG_COLUMN_ORDER,
    _build_partitions,
    _merge_cell_gene_data,
    _read_cell_gene,
    _read_coreg,
    _read_h5ad_observations,
    platform_visual_learning_cell_gene,
    platform_visual_learning_cell_gene_columns,
    platform_visual_learning_coreg_columns,
)


def _h5ad_bytes():
    buffer = io.BytesIO()
    with h5py.File(buffer, "w") as handle:
        obs = handle.create_group("obs")
        obs.create_dataset("cell_id", data=np.array([b"3", b"8"], dtype="S2"))
        obs.create_dataset("cluster_id", data=np.array([2, -1], dtype="int64"))
        obs.create_dataset("total_counts", data=np.array([10, 4], dtype="int64"))
        obs.create_dataset("n_genes", data=np.array([3, 1], dtype="int64"))
        for name, categories, codes in (
            ("class", [b"excitatory", b"unassigned"], [0, 1]),
            ("subclass", [b"Vip", b"none"], [0, 1]),
            ("cluster", [b"Exc-1", b"unassigned"], [0, 1]),
        ):
            group = obs.create_group(name)
            group.attrs["encoding-type"] = "categorical"
            group.create_dataset("categories", data=np.array(categories, dtype="S16"))
            group.create_dataset("codes", data=np.array(codes, dtype="int8"))
    return buffer.getvalue()


def test_h5ad_observations_decode_categoricals():
    observations = _read_h5ad_observations(_h5ad_bytes())
    assert observations.to_dict("records") == [
        {
            "cell_id": "3",
            "cell_class": "excitatory",
            "cell_subclass": "Vip",
            "cell_type": "Exc-1",
            "cluster_id": 2,
            "total_counts": 10,
            "n_genes": 3,
        },
        {
            "cell_id": "8",
            "cell_class": "unassigned",
            "cell_subclass": "none",
            "cell_type": "unassigned",
            "cluster_id": -1,
            "total_counts": 4,
            "n_genes": 1,
        },
    ]


def test_merge_cell_gene_data_preserves_count_columns_and_labels():
    counts = pd.DataFrame({"cell_id": [3], "R1-488-GFP": [7]})
    labels = pd.DataFrame({
        "cell_id": ["3"],
        "cell_class": ["excitatory"],
        "cell_subclass": ["none"],
        "cell_type": ["Exc-1"],
        "cluster_id": [2],
        "total_counts": [7],
        "n_genes": [1],
    })
    merged = _merge_cell_gene_data(counts, labels, "782149")
    assert list(merged.columns) == CELL_GENE_COLUMN_ORDER
    assert merged.iloc[0]["subject_id"] == "782149"
    assert merged.iloc[0]["cell_type"] == "Exc-1"
    assert merged.iloc[0]["R1-488-GFP"] == 7


def test_read_cell_gene_downloads_csv_and_h5ad():
    csv = b"cell_id,R1-488-GFP\n3,7\n"
    with patch(
        "biodata_cache.cache_table_helpers.platform_visual_learning._download_object",
        side_effect=[csv, _h5ad_bytes()],
    ):
        result = _read_cell_gene("782149", "hcr")
    assert result.iloc[0]["cell_id"] == "3"
    assert result.iloc[0]["cell_type"] == "Exc-1"


def test_read_coreg_adds_subject_and_numeric_roi_id():
    csv = (
        b"Unnamed: 0,session_name,session_key,unique_roicat_id,matched,unique_roi_id,"
        b"cz_stack_id,max_iou,plane_id,resolved_cz_stack_id,undecided,changed,hcr_id\n"
        b"0,sess,key,VISp_0_0012,True,782149_2025-03-28_VISp_0_0012,1,0.9,0,1,False,False,3\n"
    )
    with patch(
        "biodata_cache.cache_table_helpers.platform_visual_learning._download_object",
        return_value=csv,
    ):
        result = _read_coreg("782149", "coreg")
    assert list(result.columns) == COREG_COLUMN_ORDER
    assert result.iloc[0]["subject_id"] == "782149"
    assert result.iloc[0]["roi_id"] == 12
    assert result.iloc[0]["hcr_id"] == 3


def test_build_partitions_reads_existing_partition_without_rebuilding():
    backend = MemoryBackend()
    existing = pd.DataFrame({"subject_id": ["1"], "value": [4]})
    backend.write("platform_visual_learning_cell_gene/1", existing)

    def reader(subject, asset):
        return pd.DataFrame({"subject_id": [subject], "value": [9]})

    with patch("biodata_cache.cache_table_helpers.platform_visual_learning.registry.BACKEND", backend):
        result = _build_partitions("visual_learning_cell_gene", {"1": "asset"}, reader, False, None)
    assert result.iloc[0]["value"] == 4


def test_cell_gene_builder_writes_selected_partition():
    backend = MemoryBackend()
    with patch(
        "biodata_cache.cache_table_helpers.platform_visual_learning.registry.BACKEND",
        backend,
    ), patch(
        "biodata_cache.cache_table_helpers.platform_visual_learning.CELL_GENE_ASSETS",
        {"1": "asset"},
    ), patch(
        "biodata_cache.cache_table_helpers.platform_visual_learning._read_cell_gene",
        return_value=pd.DataFrame({"subject_id": ["1"], "cell_id": ["3"]}),
    ):
        result = platform_visual_learning_cell_gene(force_update=True, subject_id="1")
    assert result.iloc[0]["cell_id"] == "3"
    assert backend.read("platform_visual_learning_cell_gene/1").shape[0] == 1


def test_cell_gene_columns_include_all_public_genes():
    names = [column.name for column in platform_visual_learning_cell_gene_columns()]
    assert names == CELL_GENE_COLUMN_ORDER
    assert len(names) == 30
    assert platform_visual_learning_coreg_columns()[-1].name == "roi_id"

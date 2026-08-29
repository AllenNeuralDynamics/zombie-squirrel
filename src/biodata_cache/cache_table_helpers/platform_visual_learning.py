"""Visual Learning cell-gene and calcium-imaging co-registration tables.

The public Visual Learning collection publishes two small lookup products next
to the six HCR acquisitions:

* an annotated cell-by-gene CSV plus H5AD per subject; and
* a co-registration CSV mapping imaging-session ROI columns to HCR cell IDs.

The cache keeps one partition per subject so the browser can fetch only the
annotation and registration rows needed for one selected session. Calcium
traces remain in the public processed NWB-Zarr and are read on demand by the
frontend.
"""

import io
import logging

import boto3
import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

PUBLIC_BUCKET = "aind-open-data"
SWDB_2026_VISUAL_LEARNING_CELL_GENE_COLLECTION_ID = "17b62829-8f3f-4bc8-8b49-186bec834db5"
SWDB_2026_VISUAL_LEARNING_COREG_COLLECTION_ID = "9a9d03ce-560f-4ec9-bdb5-8b9f58e79093"

CELL_GENE_ASSETS = {
    "782149": "HCR_782149_unmixed-calibrated_2026-08-20_21-46-28",
    "788406": "HCR_788406_unmixed-calibrated_2026-08-20_19-43-57",
    "790322": "HCR_790322_unmixed-calibrated_2026-08-20_21-02-05",
    "800792": "HCR_800792_unmixed-calibrated_2026-08-20_23-32-08",
    "800995": "HCR_800995_unmixed-calibrated_2026-08-20_17-16-54",
    "804363": "HCR_804363_unmixed-calibrated_2026-08-20_18-18-28",
}

COREG_ASSETS = {
    "782149": "multiplane-ophys_782149_2025-03-28_10-55-25_processed_2025-09-11_20-46-54_CTL-coreg-id-mapping_2026-07-06_07-45-32",
    "788406": "multiplane-ophys_788406_2025-05-29_11-29-10_processed_2025-10-06_07-53-22_CTL-coreg-id-mapping_2026-07-06_07-56-06",
    "790322": "multiplane-ophys_790322_2025-06-11_15-09-29_processed_2025-10-06_19-49-40_CTL-coreg-id-mapping_2026-07-06_07-58-20",
    "800792": "multiplane-ophys_800792_2025-07-29_12-31-47_processed_2025-07-30_17-12-17_CTL-coreg-id-mapping_2026-07-06_07-52-42",
    "800995": "multiplane-ophys_800995_2025-08-05_13-25-58_processed_2025-08-06_13-03-12_CTL-coreg-id-mapping_2026-07-06_14-56-09",
    "804363": "multiplane-ophys_804363_2025-08-12_13-01-00_processed_2025-08-13_14-19-58_CTL-coreg-id-mapping_2026-07-06_07-57-01",
}

GENE_COLUMNS = [
    "R1-488-GFP", "R1-561-Slc17a7", "R2-488-Ndnf", "R2-514-Hpse", "R2-561-Pthlh",
    "R2-594-Chat", "R2-638-Tac1", "R3-488-Calb1", "R3-514-Mme", "R3-561-Crh",
    "R3-594-Reln", "R3-638-Tac2", "R4-488-Lamp5", "R4-514-Calb2", "R4-561-Pdyn",
    "R4-594-Penk", "R4-638-Gad2", "R5-488-Npy", "R5-514-Pvalb", "R5-561-Cck",
    "R5-594-Sst", "R5-638-Vip",
]

CELL_GENE_COLUMN_ORDER = [
    "subject_id", "cell_id", "cell_class", "cell_subclass", "cell_type",
    "cluster_id", "total_counts", "n_genes", *GENE_COLUMNS,
]

COREG_SOURCE_COLUMNS = [
    "session_name", "session_key", "unique_roicat_id", "matched", "unique_roi_id",
    "cz_stack_id", "max_iou", "plane_id", "resolved_cz_stack_id", "undecided", "changed", "hcr_id",
]
COREG_COLUMN_ORDER = ["subject_id", *COREG_SOURCE_COLUMNS, "roi_id"]


def _download_object(key: str) -> bytes:
    """Download one public collection object."""
    client = boto3.client("s3")
    return client.get_object(Bucket=PUBLIC_BUCKET, Key=key)["Body"].read()


def _decode_value(value):
    """Convert HDF5 byte/string scalars to plain Python values."""
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return value.item() if hasattr(value, "item") else value


def _read_h5ad_field(node):
    """Read a regular or categorical H5AD observation field."""
    import h5py

    if isinstance(node, h5py.Group) and node.attrs.get("encoding-type") == "categorical":
        categories = [_decode_value(value) for value in node["categories"][:]]
        codes = node["codes"][:]
        return [categories[int(code)] if int(code) >= 0 else None for code in codes]
    return [_decode_value(value) for value in node[:]]


def _read_h5ad_observations(data: bytes) -> pd.DataFrame:
    """Read the cell labels and QC fields from an annotated H5AD object."""
    import h5py

    with h5py.File(io.BytesIO(data), "r") as handle:
        obs = handle["obs"]
        fields = {
            "cell_id": "cell_id",
            "class": "cell_class",
            "subclass": "cell_subclass",
            "cluster": "cell_type",
            "cluster_id": "cluster_id",
            "total_counts": "total_counts",
            "n_genes": "n_genes",
        }
        return pd.DataFrame({target: _read_h5ad_field(obs[source]) for source, target in fields.items()})


def _merge_cell_gene_data(csv_data: pd.DataFrame, observations: pd.DataFrame, subject_id: str) -> pd.DataFrame:
    """Join expression counts to the H5AD cell-type annotations."""
    counts = csv_data.copy()
    counts["cell_id"] = counts["cell_id"].map(_decode_value).astype(str)
    labels = observations.copy()
    labels["cell_id"] = labels["cell_id"].map(_decode_value).astype(str)
    merged = counts.merge(labels, on="cell_id", how="left", validate="one_to_one")
    merged.insert(0, "subject_id", str(subject_id))
    merged["cell_type"] = merged["cell_type"].where(
        merged["cell_type"].notna() & merged["cell_type"].astype(str).ne(""), "unassigned"
    )
    for column in GENE_COLUMNS:
        if column not in merged:
            merged[column] = pd.NA
    return merged.reindex(columns=CELL_GENE_COLUMN_ORDER)


def _read_cell_gene(subject_id: str, asset_name: str) -> pd.DataFrame:
    """Read and combine one subject's CSV counts and H5AD labels."""
    csv_key = f"{asset_name}/{subject_id}_cellxgene.csv"
    h5ad_key = f"{asset_name}/{subject_id}_cellxgene_annotated.h5ad"
    counts = pd.read_csv(io.BytesIO(_download_object(csv_key)))
    observations = _read_h5ad_observations(_download_object(h5ad_key))
    return _merge_cell_gene_data(counts, observations, subject_id)


def _read_coreg(subject_id: str, asset_name: str) -> pd.DataFrame:
    """Read one subject's ROI-to-HCR co-registration table."""
    key = f"{asset_name}/{subject_id}_coreg_id_mapping_table.csv"
    data = pd.read_csv(io.BytesIO(_download_object(key)))
    data = data.drop(columns=["Unnamed: 0"], errors="ignore")
    data.insert(0, "subject_id", str(subject_id))
    data["roi_id"] = pd.to_numeric(
        data["unique_roi_id"].astype(str).str.extract(r"_(\d+)$")[0], errors="coerce"
    ).astype("Int64")
    data["hcr_id"] = pd.to_numeric(data["hcr_id"], errors="coerce").astype("Int64")
    return data.reindex(columns=COREG_COLUMN_ORDER)


def _log(table_key: str, message: str) -> None:
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES[table_key],
            message=message,
        ).to_json()
    )


def _build_partitions(table_key: str, assets: dict[str, str], reader, force_update: bool, subject_id: str | None):
    """Build or read selected subject partitions and return their concatenation."""
    table = registry.NAMES[table_key]
    selected = {str(subject_id): assets[str(subject_id)]} if subject_id is not None else assets
    frames = []
    for current_subject, asset_name in selected.items():
        cache_key = f"{table}/{current_subject}"
        if not force_update and registry.BACKEND.partition_exists(cache_key):
            frames.append(registry.BACKEND.read(cache_key))
            continue
        _log(table_key, f"Updating subject {current_subject} from {asset_name}")
        frame = reader(current_subject, asset_name)
        if force_update:
            registry.BACKEND.clear_partition(cache_key)
        registry.BACKEND.write(cache_key, frame)
        frames.append(frame)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


@registry.register_table(registry.NAMES["visual_learning_cell_gene"])
def platform_visual_learning_cell_gene(force_update: bool = False, subject_id: str | None = None) -> pd.DataFrame:
    """Build/read cell-by-gene partitions keyed by subject_id."""
    if not force_update and subject_id is None:
        return registry.BACKEND.read(registry.NAMES["visual_learning_cell_gene"])
    setup_logging()
    return _build_partitions("visual_learning_cell_gene", CELL_GENE_ASSETS, _read_cell_gene, force_update, subject_id)


@registry.register_table(registry.NAMES["visual_learning_coreg"])
def platform_visual_learning_coreg(force_update: bool = False, subject_id: str | None = None) -> pd.DataFrame:
    """Build/read ROI-to-HCR co-registration partitions keyed by subject_id."""
    if not force_update and subject_id is None:
        return registry.BACKEND.read(registry.NAMES["visual_learning_coreg"])
    setup_logging()
    return _build_partitions("visual_learning_coreg", COREG_ASSETS, _read_coreg, force_update, subject_id)


def platform_visual_learning_cell_gene_columns() -> list[Column]:
    """Return registry columns for the cell-by-gene table."""
    descriptions = {
        "subject_id": "Subject identifier",
        "cell_id": "HCR cell identifier",
        "cell_class": "Broad annotated cell class",
        "cell_subclass": "Annotated cell subclass",
        "cell_type": "Annotated transcriptomic cluster/cell type",
        "cluster_id": "Numeric annotated transcriptomic cluster identifier",
        "total_counts": "Total counts across gene channels",
        "n_genes": "Number of detected gene channels",
    }
    return [Column(name=name, description=descriptions.get(name, f"HCR expression count for {name}"))
            for name in CELL_GENE_COLUMN_ORDER]


def platform_visual_learning_coreg_columns() -> list[Column]:
    """Return registry columns for the co-registration table."""
    descriptions = {
        "subject_id": "Subject identifier",
        "session_name": "Processed imaging session name",
        "session_key": "Unique imaging session key",
        "unique_roicat_id": "Unique ROI identifier from RoI-CaT",
        "matched": "Whether the ROI was matched by co-registration",
        "unique_roi_id": "Subject/session/ROI identifier",
        "cz_stack_id": "Cell-z stack identifier",
        "max_iou": "Maximum intersection-over-union score",
        "plane_id": "Imaging plane index",
        "resolved_cz_stack_id": "Resolved cell-z stack identifier",
        "undecided": "Whether the match remains undecided",
        "changed": "Whether the match was manually changed",
        "hcr_id": "HCR cell identifier",
        "roi_id": "ROI column index in the processed NWB trace array",
    }
    return [Column(name=name, description=descriptions[name]) for name in COREG_COLUMN_ORDER]

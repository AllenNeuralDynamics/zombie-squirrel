"""Population physiology (pophys) ROI cache table (partitioned by asset_name).

Pulls per-ROI segmentation from the derived multiplane-ophys NWB (Zarr) on S3 and
stores one row per (imaging plane, ROI) with a simplified boundary contour in FOV
pixel coordinates, plus per-ROI quality fields and per-plane imaging metadata. The
raw segmentation masks are far too large to serve to a browser, so contours are
precomputed here. Normalized 8-bit FOV projection PNGs (max and average) are written
as side artifacts under ``pophys_fov/<asset_name>/`` for use as a viewer backdrop.

This job is slow (each plane's ``image_mask`` is (N_roi, 512, 512) float32) but the
resulting table is small, so it runs as its own parallel sync group.
"""

import io
import json
import logging
import re
from concurrent.futures import ThreadPoolExecutor

import boto3
import numpy as np
import pandas as pd
from botocore.config import Config

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.asset_basics import asset_basics
from biodata_cache.models import Column
from biodata_cache.utils import CacheLogMessage, setup_logging

_PROCESSING_GROUP = "processing"
_OPTOPHYS_GROUP = "general/optophysiology"
_ROI_TABLE = "image_segmentation/roi_table"
_S3_URI_RE = re.compile(r"^s3://([^/]+)/(.+)$")
_LOCATION_RE = re.compile(r"Structure:\s*(?P<structure>\S+).*?Depth:\s*(?P<depth>[\d.]+)", re.IGNORECASE)
_SIMPLIFY_TOLERANCE = 0.75
_MAX_WORKERS = 32


def _log(message: str) -> None:
    """Emit a structured cache log message for the pophys table."""
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES["pophys"],
            message=message,
        ).to_json()
    )


def _parse_s3(location: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into ``(bucket, key)`` with no trailing slash."""
    match = _S3_URI_RE.match(location)
    if match is None:
        raise ValueError(f"Not an S3 URI: {location}")
    return match.group(1), match.group(2).rstrip("/")


def _find_nwb_prefix(client, bucket: str, key: str) -> str | None:
    """Return the S3 key prefix of the pophys NWB Zarr directory, or None.

    Looks for a directory ending in ``.nwb.zarr`` (falling back to ``.nwb``) directly
    under the asset root and under an ``nwb/`` subfolder.
    """
    for base in (key, f"{key}/nwb"):
        resp = client.list_objects_v2(Bucket=bucket, Prefix=f"{base}/", Delimiter="/")
        for entry in resp.get("CommonPrefixes", []):
            prefix = entry["Prefix"].rstrip("/")
            if prefix.endswith(".nwb.zarr") or prefix.endswith(".nwb"):
                return prefix
    return None


def _plane_names(metadata: dict) -> list[str]:
    """Return the imaging-plane group names that carry an ROI table, from consolidated metadata."""
    planes = []
    suffix = f"/{_ROI_TABLE}/id/.zarray"
    prefix = f"{_PROCESSING_GROUP}/"
    for meta_key in metadata:
        if meta_key.startswith(prefix) and meta_key.endswith(suffix):
            planes.append(meta_key[len(prefix) : -len(suffix)])
    return sorted(planes)


def _plane_array_prefixes(plane: str) -> list[str]:
    """Return the NWB-relative array group prefixes needed for one plane."""
    roi = f"{_PROCESSING_GROUP}/{plane}/{_ROI_TABLE}"
    images = f"{_PROCESSING_GROUP}/{plane}/images"
    return [
        f"{roi}/image_mask",
        f"{roi}/id",
        f"{roi}/is_soma",
        f"{roi}/soma_probability",
        f"{images}/max_projection",
        f"{images}/average_projection",
    ]


def _download_zarr_store(client, bucket: str, nwb_prefix: str, planes: list[str], zmetadata: bytes) -> dict:
    """Concurrently download the consolidated metadata and every requested plane array.

    Only the ``.zmetadata`` and the chunks under the ROI-table / projection arrays of
    the requested planes are fetched. Returns an in-memory zarr store dict keyed by
    NWB-relative paths.
    """
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for plane in planes:
        for array_prefix in _plane_array_prefixes(plane):
            for page in paginator.paginate(Bucket=bucket, Prefix=f"{nwb_prefix}/{array_prefix}/"):
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])

    def _fetch(s3_key: str) -> tuple[str, bytes]:
        """Download one object and return its NWB-relative key with bytes."""
        body = client.get_object(Bucket=bucket, Key=s3_key)["Body"].read()
        return s3_key[len(nwb_prefix) + 1 :], body

    store = {".zmetadata": zmetadata}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        for rel_key, body in executor.map(_fetch, keys):
            store[rel_key] = body
    return store


def _open_nwb_zarr(location: str):
    """Open the pophys NWB Zarr group for an asset, or None if no NWB file is found.

    Reads only the consolidated metadata and the ROI/projection chunks via boto3
    (concurrently), avoiding any full-file download. Returns the consolidated zarr
    root group.
    """
    import zarr

    bucket, key = _parse_s3(location)
    client = boto3.client("s3", config=Config(max_pool_connections=_MAX_WORKERS))
    nwb_prefix = _find_nwb_prefix(client, bucket, key)
    if nwb_prefix is None:
        return None
    zmetadata = client.get_object(Bucket=bucket, Key=f"{nwb_prefix}/.zmetadata")["Body"].read()
    if len(zmetadata) == 0:
        return None
    try:
        metadata = json.loads(zmetadata).get("metadata", {})
    except json.JSONDecodeError:
        return None
    planes = _plane_names(metadata)
    if not planes:
        return None
    store = _download_zarr_store(client, bucket, nwb_prefix, planes, zmetadata)
    return zarr.open_consolidated(store, mode="r")


def _parse_location_attr(location_attr: str) -> tuple[str | None, float | None]:
    """Parse ``structure`` and ``depth_um`` from an imaging-plane ``location`` string."""
    if not location_attr:
        return None, None
    match = _LOCATION_RE.search(location_attr)
    if match is None:
        return None, None
    return match.group("structure"), float(match.group("depth"))


def _mask_contour(mask: np.ndarray) -> list[list[float]] | None:
    """Return a simplified boundary polygon (list of [x, y]) for a boolean ROI mask.

    Traces the mask boundary, keeps the largest contour when an ROI is split into
    disjoint pieces, and simplifies with Douglas-Peucker to keep the vertex count low.
    Coordinates are FOV pixels (x=col, y=row), matching the projection images.
    """
    from shapely.geometry import Polygon
    from skimage import measure

    contours = measure.find_contours(mask.astype(float), 0.5)
    if not contours:
        return None
    largest = max(contours, key=len)
    if len(largest) < 4:
        return None
    polygon = Polygon(largest[:, ::-1]).simplify(_SIMPLIFY_TOLERANCE)
    if polygon.is_empty or polygon.exterior is None:
        return None
    return [[float(x), float(y)] for x, y in polygon.exterior.coords]


def _projection_png(projection: np.ndarray) -> bytes:
    """Return a normalized 8-bit grayscale PNG for a float projection image."""
    from PIL import Image

    finite = projection[np.isfinite(projection)]
    if finite.size == 0:
        lo, hi = 0.0, 1.0
    else:
        lo, hi = np.percentile(finite, [1.0, 99.5])
    if hi <= lo:
        hi = lo + 1.0
    scaled = np.clip((np.nan_to_num(projection, nan=lo) - lo) / (hi - lo), 0.0, 1.0)
    image = Image.fromarray((scaled * 255).astype(np.uint8), mode="L")
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()


def _write_fov_png(asset_name: str, plane: str, name: str, projection: np.ndarray) -> None:
    """Write a normalized FOV projection PNG side artifact for a plane."""
    key = f"pophys_fov/{asset_name}/{plane}_{name}.png"
    registry.BACKEND.put_bytes(key, _projection_png(projection), "image/png")


def _extract_plane_rois(
    root, plane: str, asset_name: str, raw_name: str | None
) -> list[dict]:
    """Build one ROI record per segmented ROI in a single imaging plane.

    Reads the plane's segmentation masks, per-ROI quality fields, and imaging-plane
    metadata; traces each mask into a simplified contour; and writes the max/average
    projection PNGs as FOV backdrops.
    """
    group = root[_PROCESSING_GROUP][plane]
    roi_table = group["image_segmentation"]["roi_table"]

    image_mask = roi_table["image_mask"][:]
    roi_ids = roi_table["id"][:]
    is_soma = roi_table["is_soma"][:] if "is_soma" in roi_table else np.zeros(len(roi_ids), dtype="int64")
    soma_probability = (
        roi_table["soma_probability"][:]
        if "soma_probability" in roi_table
        else np.full(len(roi_ids), np.nan, dtype="float32")
    )

    optophys = root.get(f"{_OPTOPHYS_GROUP}/{plane}")
    attrs = dict(optophys.attrs) if optophys is not None else {}
    structure, depth_um = _parse_location_attr(attrs.get("location", ""))
    imaging_rate = attrs.get("imaging_rate")
    grid_spacing = attrs.get("grid_spacing")

    images = group["images"] if "images" in group else None
    if images is not None and "max_projection" in images:
        _write_fov_png(asset_name, plane, "max", images["max_projection"][:])
    if images is not None and "average_projection" in images:
        _write_fov_png(asset_name, plane, "avg", images["average_projection"][:])

    rows = []
    for i in range(len(roi_ids)):
        mask = image_mask[i] > 0
        if not mask.any():
            continue
        contour = _mask_contour(mask)
        if contour is None:
            continue
        ys, xs = np.nonzero(mask)
        rows.append(
            {
                "asset_name": asset_name,
                "raw_name": raw_name,
                "plane": plane,
                "structure": structure,
                "depth_um": depth_um,
                "imaging_rate": float(imaging_rate) if imaging_rate is not None else None,
                "grid_spacing_um": json.dumps(list(grid_spacing)) if grid_spacing is not None else None,
                "roi_id": int(roi_ids[i]),
                "is_soma": int(is_soma[i]),
                "soma_probability": float(soma_probability[i]),
                "centroid_x": float(xs.mean()),
                "centroid_y": float(ys.mean()),
                "area_px": int(mask.sum()),
                "contour": json.dumps(contour),
            }
        )
    return rows


def _fetch_asset_pophys(asset_name: str, location: str | None = None, raw_name: str | None = None) -> pd.DataFrame:
    """Fetch and cache pophys ROI records for one asset from its S3 NWB file.

    Returns an empty DataFrame; callers should read back from the backend.

    Args:
        asset_name: Derived pophys asset name whose ROIs to fetch.
        location: The asset's S3 location. When provided (bulk sync path), the full
            asset_basics table is not read; when None (single-asset path), the
            location is looked up from asset_basics.
        raw_name: The source raw asset name, stored on each row for the viewer.
    """
    setup_logging()
    cache_key = f"{registry.NAMES['pophys']}/{asset_name}"
    _log(f"Updating cache for asset {asset_name}")

    registry.BACKEND.clear_partition(cache_key)

    if location is None:
        basics = asset_basics()
        asset = basics[basics["name"] == asset_name]
        if asset.empty:
            _log(f"Asset {asset_name} not found in asset_basics")
            return pd.DataFrame()
        location = asset.iloc[0]["location"]

    if not location:
        _log(f"No location for asset {asset_name}")
        return pd.DataFrame()

    root = _open_nwb_zarr(location)
    if root is None:
        _log(f"No pophys NWB file found for asset {asset_name}")
        return pd.DataFrame()

    rows = []
    for plane in sorted(root[_PROCESSING_GROUP].group_keys()):
        plane_group = root[_PROCESSING_GROUP][plane]
        if "image_segmentation" not in plane_group:
            continue
        rows.extend(_extract_plane_rois(root, plane, asset_name, raw_name))
    del root

    if not rows:
        _log(f"No ROIs extracted for asset {asset_name}")
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values(["plane", "roi_id"]).reset_index(drop=True)
    registry.BACKEND.write(cache_key, df)
    _log(f"Cached {len(df)} pophys ROIs for asset {asset_name}")
    return pd.DataFrame()


@registry.register_table(registry.NAMES["pophys"])
def platform_pophys(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
    raw_name: str | None = None,
) -> pd.DataFrame | str:
    """Return pophys ROI records for a single derived multiplane-ophys asset.

    One row per (imaging plane, ROI) with a simplified boundary contour in FOV pixel
    coordinates, per-ROI quality fields, and per-plane imaging metadata. Data is
    cached per asset_name partition; FOV projection PNGs are written as side
    artifacts under ``pophys_fov/<asset_name>/``.

    Args:
        asset_name: Derived pophys asset name whose ROIs to fetch.
        force_update: If True, bypass cache and pull fresh data from the S3 NWB file,
            writing the result to the cache. An empty DataFrame is returned; read
            again without force_update (or use lazy=True) to retrieve the data.
        lazy: If True, return the partition's storage location string (for DuckDB)
            instead of loading the DataFrame.
        location: Optional S3 location of the asset. When provided during a
            force_update, the full asset_basics table is not read.
        raw_name: Optional source raw asset name, stored on each row.

    Returns:
        DataFrame of ROI records; the partition location string if lazy=True; or an
        empty DataFrame if force_update=True (data is written to the cache).

    Raises:
        ValueError: If the cache is empty for the asset and force_update is False.
    """
    cache_key = f"{registry.NAMES['pophys']}/{asset_name}"

    if lazy:
        if force_update:
            _fetch_asset_pophys(asset_name, location=location, raw_name=raw_name)
        return registry.BACKEND.get_location(cache_key)

    if force_update:
        return _fetch_asset_pophys(asset_name, location=location, raw_name=raw_name)

    df = registry.BACKEND.read(cache_key)
    if df.empty:
        raise ValueError(
            f"Cache is empty for asset {asset_name}. Use force_update=True to fetch data from S3."
        )

    return df


def platform_pophys_columns() -> list[Column]:
    """Return platform_pophys cache table column definitions."""
    return [
        Column(name="asset_name", description="Derived pophys (multiplane-ophys) asset name"),
        Column(name="raw_name", description="Source raw asset name for the derived pophys asset"),
        Column(name="plane", description="Imaging plane name (e.g. VISp_0)"),
        Column(name="structure", description="Targeted brain structure parsed from the imaging-plane location"),
        Column(name="depth_um", description="Imaging depth in microns parsed from the imaging-plane location"),
        Column(name="imaging_rate", description="Imaging frame rate in Hz"),
        Column(name="grid_spacing_um", description="Pixel size in microns as a JSON [x, y] pair"),
        Column(name="roi_id", description="ROI id; matches the trace 'rois' ids in the NWB"),
        Column(name="is_soma", description="1 if the ROI is classified as a soma, else 0"),
        Column(name="soma_probability", description="Classifier probability that the ROI is a soma"),
        Column(name="centroid_x", description="ROI centroid x (column) in FOV pixels"),
        Column(name="centroid_y", description="ROI centroid y (row) in FOV pixels"),
        Column(name="area_px", description="ROI area in pixels"),
        Column(name="contour", description="Simplified boundary polygon as a JSON list of [x, y] FOV pixel points"),
    ]

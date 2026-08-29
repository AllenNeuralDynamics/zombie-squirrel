"""Visual Coding Ophys ROI cache.

Visual Coding Ophys assets are canonical derived assets whose ``source_data`` is
null.  Their NWB-Zarr stores also use the older single-plane NWB layout rather
than the multiplane ``processing/<plane>/image_segmentation`` layout handled by
``platform_pophys``.  Keep this extractor separate so changes to either legacy
format do not change the ordinary population-ophys cache contract.
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

_S3_URI_RE = re.compile(r"^s3://([^/]+)/(.+)$")
_NWB_ROOT_SUFFIXES = (".nwb.zarr", ".nwb")
_ROI_TABLE = "processing/ophys/ImageSegmentation/PlaneSegmentation"
_PROJECTION_GROUP = "processing/ophys/SummaryImages"
_OPTOPHYS_GROUP = "general/optophysiology/ImagingPlane"
_DFF_DATA = "processing/ophys/DfOverF/DfOverF/data"
_DFF_TIMESTAMPS = "processing/ophys/Fluorescence/Corrected/timestamps"
_DFF_EVENTS = "processing/ophys/DfOverF/DfOverFEvents/data"
_SIMPLIFY_TOLERANCE = 0.75
_MAX_WORKERS = 32


def _log(message: str) -> None:
    logging.info(
        CacheLogMessage(
            backend=registry.BACKEND.__class__.__name__,
            table=registry.NAMES["visual_coding_ophys"],
            message=message,
        ).to_json()
    )


def _parse_s3(location: str) -> tuple[str, str]:
    match = _S3_URI_RE.match(location)
    if match is None:
        raise ValueError(f"Not an S3 URI: {location}")
    return match.group(1), match.group(2).rstrip("/")


def _find_nwb_prefix(client, bucket: str, key: str) -> str | None:
    """Find the public NWB-Zarr root directly under an asset location."""
    candidates = []
    for base in (key, f"{key}/nwb"):
        response = client.list_objects_v2(Bucket=bucket, Prefix=f"{base}/", Delimiter="/")
        for entry in response.get("CommonPrefixes", []):
            prefix = entry["Prefix"].rstrip("/")
            if prefix.lower().endswith(_NWB_ROOT_SUFFIXES):
                candidates.append(prefix)
        if candidates:
            return sorted(candidates, key=lambda prefix: (not prefix.lower().endswith(".nwb.zarr"), prefix))[0]
    return None


def _array_paths() -> list[str]:
    return [
        f"{_ROI_TABLE}/id",
        f"{_ROI_TABLE}/global_roi_id",
        f"{_ROI_TABLE}/pixel_mask",
        f"{_ROI_TABLE}/pixel_mask_index",
        f"{_PROJECTION_GROUP}/maximum_intensity_projection",
        f"{_OPTOPHYS_GROUP}/location",
        f"{_OPTOPHYS_GROUP}/imaging_rate",
        f"{_OPTOPHYS_GROUP}/grid_spacing",
    ]


def _download_store(client, bucket: str, nwb_prefix: str, metadata: bytes) -> dict:
    """Download only the sparse mask and projection arrays used by the cache."""
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for array_path in _array_paths():
        for page in paginator.paginate(Bucket=bucket, Prefix=f"{nwb_prefix}/{array_path}/"):
            keys.extend(obj["Key"] for obj in page.get("Contents", []))

    def fetch(s3_key: str) -> tuple[str, bytes]:
        body = client.get_object(Bucket=bucket, Key=s3_key)["Body"].read()
        return s3_key[len(nwb_prefix) + 1 :], body

    store = {".zmetadata": metadata}
    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as executor:
        for relative_key, body in executor.map(fetch, keys):
            store[relative_key] = body
    return store


def _open_nwb(location: str):
    import zarr

    bucket, key = _parse_s3(location)
    client = boto3.client("s3", config=Config(max_pool_connections=_MAX_WORKERS))
    nwb_prefix = _find_nwb_prefix(client, bucket, key)
    if nwb_prefix is None:
        return None
    metadata = client.get_object(Bucket=bucket, Key=f"{nwb_prefix}/.zmetadata")["Body"].read()
    if not metadata:
        return None
    try:
        consolidated = json.loads(metadata).get("metadata", {})
    except (TypeError, json.JSONDecodeError):
        return None
    if f"{_ROI_TABLE}/pixel_mask/.zarray" not in consolidated:
        return None
    return zarr.open_consolidated(_download_store(client, bucket, nwb_prefix, metadata), mode="r")


def _array_value(group, name):
    if group is None:
        return None
    try:
        if name not in group:
            return None
        value = np.asarray(group[name][:])
    except (KeyError, TypeError):
        return None
    if value.size == 0:
        return None
    return value.item() if value.size == 1 else value.tolist()


def _parse_location(location) -> tuple[str | None, float | None]:
    text = str(location or "")
    structure = re.search(r"(?:Structure|Targeted structure)\s*:\s*([^,;]+)", text, re.IGNORECASE)
    depth = re.search(r"(?:Depth\s*:\s*)?(\d+(?:\.\d+)?)\s*(?:um|microns?)", text, re.IGNORECASE)
    return (structure.group(1).strip() if structure else None, float(depth.group(1)) if depth else None)


def _sparse_mask(pixel_mask: np.ndarray) -> tuple[np.ndarray, int, int] | None:
    if len(pixel_mask) == 0:
        return None
    if pixel_mask.dtype.names:
        xs, ys, weights = pixel_mask["x"], pixel_mask["y"], pixel_mask["weight"]
    else:
        xs, ys, weights = pixel_mask[:, 0], pixel_mask[:, 1], pixel_mask[:, 2]
    valid = np.isfinite(weights) & (weights > 0)
    if not valid.any():
        return None
    xs = xs[valid].astype(int)
    ys = ys[valid].astype(int)
    x_origin = int(xs.min()) - 1
    y_origin = int(ys.min()) - 1
    mask = np.zeros((int(ys.max()) - y_origin + 2, int(xs.max()) - x_origin + 2), dtype=bool)
    mask[ys - y_origin, xs - x_origin] = True
    return mask, x_origin, y_origin


def _mask_contour(mask: np.ndarray) -> list[list[float]] | None:
    from shapely.geometry import Polygon
    from skimage import measure

    contours = measure.find_contours(mask.astype(float), 0.5)
    if not contours:
        return None
    contour = max(contours, key=len)
    if len(contour) < 4:
        return None
    polygon = Polygon(contour[:, ::-1]).simplify(_SIMPLIFY_TOLERANCE)
    if polygon.is_empty or polygon.exterior is None:
        return None
    return [[float(x), float(y)] for x, y in polygon.exterior.coords]


def _projection_png(projection: np.ndarray) -> bytes:
    from PIL import Image

    finite = projection[np.isfinite(projection)]
    lo, hi = np.percentile(finite, [1.0, 99.5]) if finite.size else (0.0, 1.0)
    if hi <= lo:
        hi = lo + 1.0
    scaled = np.clip((np.nan_to_num(projection, nan=lo) - lo) / (hi - lo), 0.0, 1.0)
    output = io.BytesIO()
    Image.fromarray((scaled * 255).astype(np.uint8), mode="L").save(output, format="PNG")
    return output.getvalue()


def _extract_rois(root, asset_name: str) -> list[dict]:
    roi_table = root[_ROI_TABLE]
    roi_ids = roi_table["id"][:]
    global_ids = roi_table["global_roi_id"][:] if "global_roi_id" in roi_table else roi_ids
    pixel_mask = roi_table["pixel_mask"][:]
    pixel_mask_index = roi_table["pixel_mask_index"][:]

    optophys = root.get(_OPTOPHYS_GROUP)
    structure, depth_um = _parse_location(_array_value(optophys, "location"))
    imaging_rate = _array_value(optophys, "imaging_rate")
    grid_spacing = _array_value(optophys, "grid_spacing")

    projection = root.get(_PROJECTION_GROUP)
    if projection is not None and "maximum_intensity_projection" in projection:
        registry.BACKEND.put_bytes(
            f"visual_coding_ophys_fov/{asset_name}/max.png",
            _projection_png(projection["maximum_intensity_projection"][:]),
            "image/png",
        )

    rows = []
    for index, roi_id in enumerate(roi_ids):
        end = int(pixel_mask_index[index])
        start = int(pixel_mask_index[index - 1]) if index else 0
        if end <= start or end > len(pixel_mask):
            continue
        sparse = _sparse_mask(pixel_mask[start:end])
        if sparse is None:
            continue
        mask, x_origin, y_origin = sparse
        contour = _mask_contour(mask)
        if contour is None:
            continue
        contour = [[x + x_origin, y + y_origin] for x, y in contour]
        ys, xs = np.nonzero(mask)
        rows.append({
            "asset_name": asset_name,
            "plane": "ophys",
            "roi_id": int(roi_id),
            "global_roi_id": int(global_ids[index]),
            "structure": structure,
            "depth_um": depth_um,
            "imaging_rate": float(imaging_rate) if imaging_rate is not None else None,
            "grid_spacing_um": json.dumps(list(grid_spacing)) if grid_spacing is not None else None,
            "centroid_x": float((xs + x_origin).mean()),
            "centroid_y": float((ys + y_origin).mean()),
            "area_px": int(mask.sum()),
            "contour": json.dumps(contour),
        })
    return rows


def _fetch_asset(asset_name: str, location: str | None = None) -> pd.DataFrame:
    setup_logging()
    cache_key = f"{registry.NAMES['visual_coding_ophys']}/{asset_name}"
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

    root = _open_nwb(location)
    if root is None:
        _log(f"No Visual Coding Ophys segmentation NWB found for asset {asset_name}")
        return pd.DataFrame()
    rows = _extract_rois(root, asset_name)
    if not rows:
        _log(f"No Visual Coding Ophys ROIs extracted for asset {asset_name}")
        return pd.DataFrame()
    frame = pd.DataFrame(rows).sort_values("roi_id").reset_index(drop=True)
    registry.BACKEND.write(cache_key, frame)
    _log(f"Cached {len(frame)} Visual Coding Ophys ROIs for asset {asset_name}")
    return pd.DataFrame()


@registry.register_table(registry.NAMES["visual_coding_ophys"])
def platform_visual_coding_ophys(
    asset_name: str,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    cache_key = f"{registry.NAMES['visual_coding_ophys']}/{asset_name}"
    if lazy:
        if force_update:
            _fetch_asset(asset_name, location=location)
        return registry.BACKEND.get_location(cache_key)
    if force_update:
        return _fetch_asset(asset_name, location=location)
    frame = registry.BACKEND.read(cache_key)
    if frame.empty:
        raise ValueError(f"Cache is empty for Visual Coding Ophys asset {asset_name}. Use force_update=True to fetch data.")
    return frame


def platform_visual_coding_ophys_columns() -> list[Column]:
    return [
        Column(name="asset_name", description="Visual Coding Ophys asset name"),
        Column(name="plane", description="Single imaging plane name"),
        Column(name="roi_id", description="ROI id; matches the dF/F trace column"),
        Column(name="global_roi_id", description="Global ROI id from the NWB PlaneSegmentation table"),
        Column(name="structure", description="Targeted brain structure"),
        Column(name="depth_um", description="Imaging depth in microns"),
        Column(name="imaging_rate", description="Imaging frame rate in Hz"),
        Column(name="grid_spacing_um", description="Pixel size in microns as a JSON pair"),
        Column(name="centroid_x", description="ROI centroid x in FOV pixels"),
        Column(name="centroid_y", description="ROI centroid y in FOV pixels"),
        Column(name="area_px", description="ROI area in pixels"),
        Column(name="contour", description="Simplified ROI boundary polygon as JSON points"),
    ]

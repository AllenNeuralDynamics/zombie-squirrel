"""Population physiology (pophys) ROI cache table (partitioned by asset_name).

Pulls per-ROI segmentation from derived ophys NWB (Zarr) assets on S3 and stores one
row per (imaging plane, ROI) with a simplified boundary contour in FOV pixel
coordinates, plus per-ROI quality fields and per-plane imaging metadata. This
includes BCI single-plane assets, whose Zarr root is nested under ``*_behavior_nwb``.
The raw segmentation masks are far too large to serve to a browser, so contours are
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
_LEGACY_SINGLE_PLANE = "ophys"
_LEGACY_SINGLE_ROI = "processing/ophys/ImageSegmentation/PlaneSegmentation"
_LEGACY_SINGLE_IMAGES = "processing/ophys/SummaryImages"
_LEGACY_SINGLE_OPTOPHYS = "general/optophysiology/ImagingPlane"
_S3_URI_RE = re.compile(r"^s3://([^/]+)/(.+)$")
_LOCATION_RE = re.compile(r"Structure:\s*(?P<structure>\S+).*?Depth:\s*(?P<depth>[\d.]+)", re.IGNORECASE)
_DEPTH_ONLY_RE = re.compile(r"(?:Depth\s*:\s*)?(?P<depth>[\d.]+)\s*(?:um|microns?)", re.IGNORECASE)
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

    Most derived pophys assets expose a directory ending in ``.nwb.zarr`` (falling
    back to ``.nwb``) directly under the asset root or under an ``nwb/`` subfolder.
    BCI single-plane assets keep the same Zarr root in a sibling directory named
    ``*_behavior_nwb``. Prefer the conventional NWB suffixes when both layouts are
    present, but accept the BCI name so its dense ROI masks enter the same cache.
    """
    candidates = []
    for base in (key, f"{key}/nwb"):
        resp = client.list_objects_v2(Bucket=bucket, Prefix=f"{base}/", Delimiter="/")
        for entry in resp.get("CommonPrefixes", []):
            prefix = entry["Prefix"].rstrip("/")
            lower = prefix.lower()
            if lower.endswith(".nwb.zarr"):
                candidates.append((0, prefix))
            elif lower.endswith(".nwb"):
                candidates.append((1, prefix))
            elif lower.endswith("_behavior_nwb"):
                candidates.append((2, prefix))
        conventional = [candidate for candidate in candidates if candidate[0] < 2]
        if conventional:
            return min(conventional)[1]
    return min(candidates)[1] if candidates else None


def _plane_names(metadata: dict) -> list[str]:
    """Return the imaging-plane group names that carry an ROI table, from consolidated metadata."""
    planes = []
    suffix = f"/{_ROI_TABLE}/id/.zarray"
    prefix = f"{_PROCESSING_GROUP}/"
    for meta_key in metadata:
        if meta_key.startswith(prefix) and meta_key.endswith(suffix):
            planes.append(meta_key[len(prefix) : -len(suffix)])
    return sorted(planes)


def _legacy_plane_names(metadata: dict) -> list[str]:
    """Return legacy imaging-plane names with sparse ROI tables."""
    planes = []
    suffix = f"/{_ROI_TABLE}/pixel_mask/.zarray"
    prefix = f"{_PROCESSING_GROUP}/"
    for meta_key in metadata:
        if meta_key.startswith(prefix) and meta_key.endswith(suffix):
            planes.append(meta_key[len(prefix) : -len(suffix)])
    if f"{_LEGACY_SINGLE_ROI}/pixel_mask/.zarray" in metadata:
        planes.append(_LEGACY_SINGLE_PLANE)
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


def _download_array_store(client, bucket: str, nwb_prefix: str, array_prefixes: list[str], zmetadata: bytes) -> dict:
    """Concurrently download the consolidated metadata and every requested plane array.

    Only the ``.zmetadata`` and the chunks under the ROI-table / projection arrays of
    the requested planes are fetched. Returns an in-memory zarr store dict keyed by
    NWB-relative paths.
    """
    keys = []
    paginator = client.get_paginator("list_objects_v2")
    for array_prefix in dict.fromkeys(array_prefixes):
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


def _download_zarr_store(client, bucket: str, nwb_prefix: str, planes: list[str], zmetadata: bytes) -> dict:
    """Download the arrays used by the modern dense-mask pophys NWB layout."""
    prefixes = [prefix for plane in planes for prefix in _plane_array_prefixes(plane)]
    return _download_array_store(client, bucket, nwb_prefix, prefixes, zmetadata)


def _download_legacy_zarr_store(
    client, bucket: str, nwb_prefix: str, planes: list[str], metadata: dict, zmetadata: bytes
) -> dict:
    """Download arrays used by the legacy sparse pixel-mask pophys NWB layout."""
    prefixes = []
    for plane in planes:
        if plane == _LEGACY_SINGLE_PLANE:
            array_prefixes = [
                f"{_LEGACY_SINGLE_ROI}/{name}"
                for name in ("id", "is_soma", "pixel_mask", "pixel_mask_index")
            ]
            array_prefixes.extend(
                [f"{_LEGACY_SINGLE_IMAGES}/maximum_intensity_projection"]
                + [
                    f"{_LEGACY_SINGLE_OPTOPHYS}/{name}"
                    for name in ("location", "imaging_rate", "grid_spacing")
                ]
            )
            prefixes.extend(array_prefixes)
            continue
        roi_prefix = f"{_PROCESSING_GROUP}/{plane}/{_ROI_TABLE}/"
        image_prefix = f"{_PROCESSING_GROUP}/{plane}/images/"
        optophys_prefix = f"{_OPTOPHYS_GROUP}/{plane}/"
        for metadata_key in metadata:
            if not metadata_key.endswith("/.zarray"):
                continue
            array_path = metadata_key[: -len("/.zarray")]
            name = array_path.rsplit("/", 1)[-1]
            if (
                array_path.startswith(roi_prefix)
                and name in {"id", "is_soma", "pixel_mask", "pixel_mask_index"}
                or array_path.startswith(image_prefix)
                and "projection" in name
                or array_path.startswith(optophys_prefix)
                and name in {"location", "imaging_rate", "grid_spacing"}
            ):
                prefixes.append(array_path)
    return _download_array_store(client, bucket, nwb_prefix, prefixes, zmetadata)


def _nwb_context(location: str):
    """Return the S3 client, NWB prefix, metadata bytes, and consolidated metadata."""
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
    except (TypeError, json.JSONDecodeError):
        return None
    return client, bucket, nwb_prefix, zmetadata, metadata


def _open_nwb_zarr(location: str):
    """Open a modern dense-mask pophys NWB Zarr group, or None if not present.

    Reads only the consolidated metadata and the ROI/projection chunks via boto3
    (concurrently), avoiding any full-file download. Returns the consolidated zarr
    root group.
    """
    import zarr

    context = _nwb_context(location)
    if context is None:
        return None
    client, bucket, nwb_prefix, zmetadata, metadata = context
    planes = [
        plane
        for plane in _plane_names(metadata)
        if f"{_PROCESSING_GROUP}/{plane}/{_ROI_TABLE}/image_mask/.zarray" in metadata
    ]
    if not planes:
        return None
    store = _download_zarr_store(client, bucket, nwb_prefix, planes, zmetadata)
    return zarr.open_consolidated(store, mode="r")


def _open_legacy_nwb_zarr(location: str):
    """Open a legacy sparse pixel-mask pophys NWB Zarr group, or None if absent."""
    import zarr

    context = _nwb_context(location)
    if context is None:
        return None
    client, bucket, nwb_prefix, zmetadata, metadata = context
    planes = _legacy_plane_names(metadata)
    if not planes:
        return None
    store = _download_legacy_zarr_store(client, bucket, nwb_prefix, planes, metadata, zmetadata)
    return zarr.open_consolidated(store, mode="r")


def _parse_location_attr(location_attr: str) -> tuple[str | None, float | None]:
    """Parse ``structure`` and ``depth_um`` from an imaging-plane ``location`` string."""
    if not location_attr:
        return None, None
    location_attr = str(location_attr)
    match = _LOCATION_RE.search(location_attr)
    if match is not None:
        return match.group("structure"), float(match.group("depth"))
    match = _DEPTH_ONLY_RE.search(location_attr)
    if match is None:
        return None, None
    return None, float(match.group("depth"))


def _array_value(group, name):
    """Return a scalar or list from a Zarr array in a group."""
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


def _sparse_mask(pixel_mask: np.ndarray) -> tuple[np.ndarray, int, int] | None:
    """Convert an NWB sparse pixel mask to a padded local boolean mask."""
    if len(pixel_mask) == 0:
        return None
    if pixel_mask.dtype.names:
        xs = pixel_mask["x"]
        ys = pixel_mask["y"]
        weights = pixel_mask["weight"]
    else:
        xs = pixel_mask[:, 0]
        ys = pixel_mask[:, 1]
        weights = pixel_mask[:, 2]
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

    image_masks = roi_table["image_mask"][:]
    roi_ids = roi_table["id"][:]
    is_soma = roi_table["is_soma"][:] if "is_soma" in roi_table else np.zeros(len(roi_ids), dtype="int64")
    soma_probability = (
        roi_table["soma_probability"][:]
        if "soma_probability" in roi_table
        else np.full(len(roi_ids), np.nan, dtype="float32")
    )

    optophys = root.get(f"{_OPTOPHYS_GROUP}/{plane}")
    attrs = dict(optophys.attrs) if optophys is not None else {}
    location_attr = attrs.get("location")
    structure, depth_um = _parse_location_attr(location_attr)
    imaging_rate = attrs.get("imaging_rate")
    grid_spacing = attrs.get("grid_spacing")

    images = group["images"] if "images" in group else None
    if images is not None and "max_projection" in images:
        _write_fov_png(asset_name, plane, "max", images["max_projection"][:])
    if images is not None and "average_projection" in images:
        _write_fov_png(asset_name, plane, "avg", images["average_projection"][:])

    rows = []
    for i in range(len(roi_ids)):
        mask = image_masks[i] > 0
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


def _extract_legacy_plane_rois(
    root, plane: str, asset_name: str, raw_name: str | None
) -> list[dict]:
    """Build ROI records from a legacy NWB sparse pixel-mask table."""
    group = root[_PROCESSING_GROUP][plane]
    roi_table = group["image_segmentation"]["roi_table"]
    roi_ids = roi_table["id"][:]
    pixel_mask = roi_table["pixel_mask"][:]
    pixel_mask_index = roi_table["pixel_mask_index"][:]
    is_soma = roi_table["is_soma"][:] if "is_soma" in roi_table else np.zeros(len(roi_ids), dtype="int64")

    optophys = root.get(f"{_OPTOPHYS_GROUP}/{plane}")
    attrs = dict(optophys.attrs) if optophys is not None else {}
    location_attr = attrs.get("location") or _array_value(optophys, "location")
    structure, depth_um = _parse_location_attr(location_attr)
    imaging_rate = attrs.get("imaging_rate")
    if imaging_rate is None:
        imaging_rate = _array_value(optophys, "imaging_rate")
    grid_spacing = attrs.get("grid_spacing")
    if grid_spacing is None:
        grid_spacing = _array_value(optophys, "grid_spacing")

    images = group["images"] if "images" in group else None
    if images is not None:
        for name in (f"max_projection_denoised_{plane}", f"max_projection_raw_{plane}"):
            if name in images:
                _write_fov_png(asset_name, plane, "max", images[name][:])
                break
        for name in (f"mean_projection_denoised_{plane}", f"mean_projection_raw_{plane}"):
            if name in images:
                _write_fov_png(asset_name, plane, "avg", images[name][:])
                break

    rows = []
    for i in range(len(roi_ids)):
        end = int(pixel_mask_index[i])
        start = int(pixel_mask_index[i - 1]) if i else 0
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
        xs = xs + x_origin
        ys = ys + y_origin
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
                "soma_probability": None,
                "centroid_x": float(xs.mean()),
                "centroid_y": float(ys.mean()),
                "area_px": int(mask.sum()),
                "contour": json.dumps(contour),
            }
        )
    return rows


def _extract_legacy_single_plane_rois(
    root, asset_name: str, raw_name: str | None
) -> list[dict]:
    """Build ROI records from the older shared legacy PlaneSegmentation table."""
    roi_table = root[_LEGACY_SINGLE_ROI]
    roi_ids = roi_table["id"][:]
    pixel_mask = roi_table["pixel_mask"][:]
    pixel_mask_index = roi_table["pixel_mask_index"][:]
    is_soma = roi_table["is_soma"][:] if "is_soma" in roi_table else np.zeros(len(roi_ids), dtype="int64")

    optophys = root.get(_LEGACY_SINGLE_OPTOPHYS)
    attrs = dict(optophys.attrs) if optophys is not None else {}
    location_attr = attrs.get("location") or _array_value(optophys, "location")
    structure, depth_um = _parse_location_attr(location_attr)
    imaging_rate = attrs.get("imaging_rate") or _array_value(optophys, "imaging_rate")
    grid_spacing = attrs.get("grid_spacing") or _array_value(optophys, "grid_spacing")

    images = root.get(_LEGACY_SINGLE_IMAGES)
    if images is not None and "maximum_intensity_projection" in images:
        _write_fov_png(asset_name, _LEGACY_SINGLE_PLANE, "max", images["maximum_intensity_projection"][:])

    rows = []
    for i in range(len(roi_ids)):
        end = int(pixel_mask_index[i])
        start = int(pixel_mask_index[i - 1]) if i else 0
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
        xs = xs + x_origin
        ys = ys + y_origin
        rows.append(
            {
                "asset_name": asset_name,
                "raw_name": raw_name,
                "plane": _LEGACY_SINGLE_PLANE,
                "structure": structure,
                "depth_um": depth_um,
                "imaging_rate": float(imaging_rate) if imaging_rate is not None else None,
                "grid_spacing_um": json.dumps(list(grid_spacing)) if grid_spacing is not None else None,
                "roi_id": int(roi_ids[i]),
                "is_soma": int(is_soma[i]),
                "soma_probability": None,
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
    extractor = _extract_plane_rois
    if root is None:
        root = _open_legacy_nwb_zarr(location)
        extractor = _extract_legacy_plane_rois
    if root is None:
        _log(f"No pophys NWB file found for asset {asset_name}")
        return pd.DataFrame()

    rows = []
    for plane in sorted(root[_PROCESSING_GROUP].group_keys()):
        if plane == _LEGACY_SINGLE_PLANE:
            rows.extend(_extract_legacy_single_plane_rois(root, asset_name, raw_name))
            continue
        plane_group = root[_PROCESSING_GROUP][plane]
        if "image_segmentation" not in plane_group:
            continue
        rows.extend(extractor(root, plane, asset_name, raw_name))
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
    """Return pophys ROI records for a single derived ophys asset.

    One row per (imaging plane, ROI) with a simplified boundary contour in FOV pixel
    coordinates, per-ROI quality fields, and per-plane imaging metadata. Data is
    cached per asset_name partition; FOV projection PNGs are written as side
    artifacts under ``pophys_fov/<asset_name>/``.

    Args:
        asset_name: Derived ophys asset name whose ROIs to fetch.
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
        Column(name="asset_name", description="Derived ophys asset name"),
        Column(name="raw_name", description="Source raw asset name for the derived ophys asset"),
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

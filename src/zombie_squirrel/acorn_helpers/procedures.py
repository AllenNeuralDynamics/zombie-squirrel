"""Procedures acorn - subject procedures and brain injections tables."""

import logging

import pandas as pd
from aind_data_access_api.document_db import MetadataDbClient

import zombie_squirrel.acorns as acorns
from zombie_squirrel.squirrel import Column
from zombie_squirrel.utils import (
    SquirrelMessage,
    setup_logging,
)


def _to_float(value) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _serialize_materials(materials: list) -> str:
    """Serialize a list of injection materials to a semicolon-separated string of names."""
    if not materials:
        return ""
    return "; ".join(m.get("name") or "" for m in materials)


def _axis_names_from_coord_sys(coord_sys: dict) -> list[str]:
    """Return ordered axis names from a coordinate system dict."""
    axes = coord_sys.get("axes") or []
    return [a.get("name", f"axis_{i}") for i, a in enumerate(axes)]


def _coord_systems_from_procedures(proc_block: dict, surgery: dict) -> dict[str, list[str]]:
    """Build a name->axis_names mapping from top-level and surgery coordinate systems."""
    result = {}
    for cs in (proc_block.get("coordinate_system"), surgery.get("coordinate_system")):
        if cs and isinstance(cs, dict):
            name = cs.get("name", "")
            if name:
                result[name] = _axis_names_from_coord_sys(cs)
    return result


def _extract_translation_by_axes(coordinates: list, axis_names: list[str]) -> dict:
    """Map the first Translation and Rotation transform values to axis-named columns."""
    result = {name: None for name in axis_names}
    result.update({f"{name}_rotation": None for name in axis_names})
    for site in coordinates:
        if not isinstance(site, list):
            continue
        for transform in site:
            obj_type = transform.get("object_type")
            if obj_type == "Translation":
                vals = transform.get("translation") or []
                result.update({axis_names[i]: vals[i] if i < len(vals) else None for i in range(len(axis_names))})
            elif obj_type == "Rotation":
                vals = transform.get("rotation") or []
                result.update({f"{axis_names[i]}_rotation": vals[i] if i < len(vals) else None for i in range(len(axis_names))})
    return result


def _extract_surgery_fields(surgery: dict) -> dict:
    """Extract flat surgery-level metadata fields (excluding coordinate_system and measured_coordinates)."""
    anaesthesia = surgery.get("anaesthesia") or {}
    experimenters = surgery.get("experimenters") or []
    return {
        "surgery_protocol_id": surgery.get("protocol_id"),
        "experimenters": "; ".join(str(e) for e in experimenters),
        "ethics_review_id": surgery.get("ethics_review_id"),
        "animal_weight_prior": _to_float(surgery.get("animal_weight_prior")),
        "animal_weight_post": _to_float(surgery.get("animal_weight_post")),
        "weight_unit": surgery.get("weight_unit"),
        "anaesthetic_type": anaesthesia.get("anaesthetic_type"),
        "anaesthesia_duration": anaesthesia.get("duration"),
        "anaesthesia_duration_unit": anaesthesia.get("duration_unit"),
        "anaesthesia_level": anaesthesia.get("level"),
        "workstation_id": surgery.get("workstation_id"),
        "surgery_notes": surgery.get("notes"),
    }


def _extract_first_dynamics(dynamics: list) -> dict:
    """Extract profile, volume, and volume_unit from the first dynamics entry."""
    if not dynamics:
        return {"injection_profile": None, "injection_volume": None, "injection_volume_unit": None}
    d = dynamics[0]
    return {
        "injection_profile": d.get("profile"),
        "injection_volume": d.get("volume"),
        "injection_volume_unit": d.get("volume_unit"),
    }


@acorns.register_acorn(acorns.NAMES["procedures"])
def procedures(force_update: bool = False) -> pd.DataFrame:
    """Fetch subject procedures summary with one row per procedure per surgery across all subjects.

    Returns a DataFrame with columns: procedure_key, subject_id, surgery_start_date,
    and procedure_type.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with one row per procedure per surgery.

    """
    df = acorns.TREE.scurry(acorns.NAMES["procedures"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from database.")

    if df.empty or force_update:
        proc_df, _ = _fetch_all_procedures()
        df = proc_df

    return df


@acorns.register_acorn(acorns.NAMES["injections"])
def brain_injections(force_update: bool = False) -> pd.DataFrame:
    """Fetch detailed Injection and BrainInjection data across all subjects.

    Returns a DataFrame with one row per injection procedure, including
    coordinates, materials, dynamics, and targeted structure.

    Args:
        force_update: If True, bypass cache and fetch fresh data from database.

    Returns:
        DataFrame with detailed injection procedure data.

    """
    df = acorns.TREE.scurry(acorns.NAMES["injections"])

    if df.empty and not force_update:
        raise ValueError("Cache is empty. Use force_update=True to fetch data from database.")

    if df.empty or force_update:
        _, inj_df = _fetch_all_procedures()
        df = inj_df

    return df


def _fetch_all_procedures() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch all procedures records from the database and cache both tables."""
    setup_logging()

    logging.info(
        SquirrelMessage(
            tree=acorns.TREE.__class__.__name__,
            acorn=acorns.NAMES["procedures"],
            message="Updating cache",
        ).to_json()
    )

    client = MetadataDbClient(
        host=acorns.API_GATEWAY_HOST,
        version="v2",
    )

    all_records = client.retrieve_docdb_records(
        filter_query={},
        projection={"_id": 1},
        limit=0,
    )
    all_ids = {r["_id"] for r in all_records}

    # Batch retrieve 50 at a time
    records = []
    batch_size = 50
    for i, batch_start in enumerate(range(0, len(all_ids), batch_size)):
        batch_ids = list(all_ids)[batch_start : batch_start + batch_size]
        batch_records = client.retrieve_docdb_records(
            filter_query={"_id": {"$in": batch_ids}},
            projection={"procedures": 1, "subject": 1},
            limit=0,
        )
        records.extend(batch_records)
        logging.info(
            SquirrelMessage(
                tree=acorns.TREE.__class__.__name__,
                acorn=acorns.NAMES["procedures"],
                message=f"Fetched batch {i + 1}/{(len(all_ids) + batch_size - 1) // batch_size} ({len(records)}/{len(all_ids)} records)",
            ).to_json()
        )


    proc_rows = []
    inj_rows = []
    seen_subject_ids = set()
    total = len(records)

    for i, record in enumerate(records):
        proc_block = record.get("procedures", {}) or {}
        subject_block = record.get("subject", {}) or {}
        sid = subject_block.get("subject_id", "")

        if not sid or sid in seen_subject_ids:
            continue
        seen_subject_ids.add(sid)
        logging.info(f"[{i + 1}/{total}] Processing subject {sid}")

        subject_procedures = proc_block.get("subject_procedures", []) or []
        for surgery_idx, surgery in enumerate(subject_procedures):
            if surgery.get("object_type") != "Surgery":
                continue
            surgery_start_date = surgery.get("start_date", "")
            coord_sys_map = _coord_systems_from_procedures(proc_block, surgery)
            surgery_fields = _extract_surgery_fields(surgery)
            inner_procedures = surgery.get("procedures", []) or []
            for proc_idx, proc in enumerate(inner_procedures):
                proc_type = proc.get("object_type", "")
                procedure_key = f"{sid}_{surgery_idx}_{proc_idx}"
                proc_rows.append(
                    {
                        "procedure_key": procedure_key,
                        "subject_id": sid,
                        "surgery_start_date": surgery_start_date,
                        "procedure_type": proc_type,
                    }
                )
                if proc_type in ("Brain injection", "Injection"):
                    inj_rows.append(_extract_injection_row(procedure_key, sid, surgery_start_date, proc, coord_sys_map, surgery_fields))

    proc_df = pd.DataFrame(proc_rows) if proc_rows else pd.DataFrame()
    inj_df = pd.DataFrame(inj_rows) if inj_rows else pd.DataFrame()

    acorns.TREE.hide(acorns.NAMES["procedures"], proc_df)
    acorns.TREE.hide(acorns.NAMES["injections"], inj_df)

    return proc_df, inj_df


def _extract_injection_row(
    procedure_key: str,
    subject_id: str,
    surgery_start_date: str,
    proc: dict,
    coord_sys_map: dict[str, list[str]],
    surgery_fields: dict,
) -> dict:
    """Extract a flat row dict from an Injection or BrainInjection procedure dict."""
    targeted = proc.get("targeted_structure") or {}
    targeted_name = targeted.get("name", "") if isinstance(targeted, dict) else ""
    targeted_acronym = targeted.get("acronym", "") if isinstance(targeted, dict) else ""

    relative_position = proc.get("relative_position") or []
    if isinstance(relative_position, list):
        relative_position = "; ".join(str(p) for p in relative_position)

    cs_name = proc.get("coordinate_system_name", "")
    axis_names = coord_sys_map.get(cs_name, [])
    coord_cols = _extract_translation_by_axes(proc.get("coordinates") or [], axis_names)

    row = {
        "procedure_key": procedure_key,
        "subject_id": subject_id,
        "surgery_start_date": surgery_start_date,
        "procedure_type": proc.get("object_type", ""),
        "targeted_structure_name": targeted_name,
        "targeted_structure_acronym": targeted_acronym,
        "relative_position": relative_position,
        "coordinate_system_name": cs_name,
        "injection_materials": _serialize_materials(proc.get("injection_materials") or []),
        "protocol_id": proc.get("protocol_id", ""),
    }
    row.update(surgery_fields)
    row.update(coord_cols)
    row.update(_extract_first_dynamics(proc.get("dynamics") or []))
    return row


def procedures_columns() -> list[Column]:
    """Return procedures acorn column definitions."""
    return [
        Column(name="procedure_key", description="Unique key for this procedure, joins to brain_injections table"),
        Column(name="subject_id", description="Subject ID"),
        Column(name="surgery_start_date", description="Start date of the surgery"),
        Column(name="procedure_type", description="Type of procedure (e.g. Brain injection, Headframe)"),
    ]


def brain_injections_columns() -> list[Column]:
    """Return brain injections acorn column definitions."""
    return [
        Column(name="procedure_key", description="Unique key for this procedure, joins to procedures table"),
        Column(name="subject_id", description="Subject ID"),
        Column(name="surgery_start_date", description="Start date of the surgery"),
        Column(name="procedure_type", description="Injection type (Brain injection or Injection)"),
        Column(name="targeted_structure_name", description="Full name of targeted brain structure"),
        Column(name="targeted_structure_acronym", description="Acronym of targeted brain structure"),
        Column(name="relative_position", description="Relative anatomical position (e.g. Left; Right)"),
        Column(name="coordinate_system_name", description="Name of the coordinate system used"),
        Column(name="<axis_name>", description="One column per axis in the coordinate system (e.g. AP, ML, SI, Depth)"),
        Column(name="injection_materials", description="Semicolon-separated injection material names"),
        Column(name="injection_profile", description="Injection profile (e.g. Bolus, Continuous)"),
        Column(name="injection_volume", description="Injection volume"),
        Column(name="injection_volume_unit", description="Injection volume unit (e.g. nanoliter)"),
        Column(name="protocol_id", description="Protocol ID (DOI)"),
        Column(name="surgery_protocol_id", description="Surgery protocol ID"),
        Column(name="experimenters", description="Semicolon-separated list of experimenters"),
        Column(name="ethics_review_id", description="Ethics review ID"),
        Column(name="animal_weight_prior", description="Animal weight before surgery"),
        Column(name="animal_weight_post", description="Animal weight after surgery"),
        Column(name="weight_unit", description="Unit for animal weight measurements"),
        Column(name="anaesthetic_type", description="Type of anaesthetic used"),
        Column(name="anaesthesia_duration", description="Duration of anaesthesia"),
        Column(name="anaesthesia_duration_unit", description="Unit for anaesthesia duration"),
        Column(name="anaesthesia_level", description="Level of anaesthesia"),
        Column(name="workstation_id", description="Workstation ID used for surgery"),
        Column(name="surgery_notes", description="Free-text notes about the surgery"),
    ]

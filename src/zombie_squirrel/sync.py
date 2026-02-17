"""Synchronization utilities for updating all cached data."""

from .acorns import ACORN_REGISTRY, NAMES


def hide_acorns():
    """Trigger force update of all registered acorn functions.

    Updates each acorn individually. For the QC acorn, fetches
    unique subject IDs from asset_basics and updates each individually."""
    # Update simple acorns
    ACORN_REGISTRY[NAMES["upn"]](force_update=True)
    ACORN_REGISTRY[NAMES["usi"]](force_update=True)

    # Update asset_basics and get subject IDs for QC
    df_basics = ACORN_REGISTRY[NAMES["basics"]](force_update=True)

    ACORN_REGISTRY[NAMES["d2r"]](force_update=True)
    ACORN_REGISTRY[NAMES["r2d"]](force_update=True)

    # Update QC for each subject
    subject_ids = df_basics["subject_id"].dropna().unique()
    qc_acorn = ACORN_REGISTRY[NAMES["qc"]]
    for subject_id in subject_ids:
        qc_acorn(subject_id=subject_id, force_update=True)

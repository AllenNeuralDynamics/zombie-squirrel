"""Synchronization utilities for updating all cached data."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from .acorns import ACORN_REGISTRY, NAMES


def hide_acorns():
    """Trigger force update of all registered acorn functions.

    Updates each acorn individually. For the QC acorn, fetches
    unique subject IDs from asset_basics and updates each individually,
    using parallelization when multiple subjects are available."""
    # Update simple acorns
    ACORN_REGISTRY[NAMES["upn"]](force_update=True)
    ACORN_REGISTRY[NAMES["usi"]](force_update=True)

    # Update asset_basics and get subject IDs for QC
    df_basics = ACORN_REGISTRY[NAMES["basics"]](force_update=True)

    ACORN_REGISTRY[NAMES["d2r"]](force_update=True)
    ACORN_REGISTRY[NAMES["r2d"]](force_update=True)

    # Update QC for each subject
    subject_ids = df_basics["subject_id"].dropna().unique()
    if len(subject_ids) == 0:
        return

    qc_acorn = ACORN_REGISTRY[NAMES["qc"]]

    # Process first subject with write_metadata=True
    qc_acorn(subject_id=subject_ids[0], force_update=True, write_metadata=True)

    # Process remaining subjects in parallel if available
    if len(subject_ids) > 1:
        try:
            with ThreadPoolExecutor() as executor:
                futures = [
                    executor.submit(qc_acorn, subject_id=subject_id, force_update=True, write_metadata=False)
                    for subject_id in subject_ids[1:]
                ]
                # Wait for all tasks to complete
                for future in as_completed(futures):
                    future.result()  # Raise any exceptions that occurred
        except Exception:
            # Fall back to sequential processing if parallelization fails
            for subject_id in subject_ids[1:]:
                qc_acorn(subject_id=subject_id, force_update=True, write_metadata=False)

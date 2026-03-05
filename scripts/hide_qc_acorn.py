"""Run the QC hide_acorn for all subjects without updating other acorns."""

from concurrent.futures import ThreadPoolExecutor, as_completed

from zombie_squirrel.acorns import ACORN_REGISTRY, NAMES


def main():
    """Hide QC acorn for all subjects."""
    df_basics = ACORN_REGISTRY[NAMES["basics"]](force_update=False)
    subject_ids = df_basics["subject_id"].dropna().unique()
    print(f"Found {len(subject_ids)} subjects. Fetching QC data...")

    qc_acorn = ACORN_REGISTRY[NAMES["qc"]]
    try:
        with ThreadPoolExecutor() as executor:
            futures = {
                executor.submit(qc_acorn, subject_id=subject_id, force_update=True): subject_id
                for subject_id in subject_ids
            }
            for i, future in enumerate(as_completed(futures), 1):
                subject_id = futures[future]
                future.result()
                print(f"[{i}/{len(subject_ids)}] Done: {subject_id}")
    # no test coverage needed on exception
    except Exception:  # noqa: PERF203
        for subject_id in subject_ids:
            qc_acorn(subject_id=subject_id, force_update=True)

    print("QC cache update complete.")


if __name__ == "__main__":
    main()

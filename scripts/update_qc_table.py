"""Run the QC sync job."""

from biodata_cache.sync import run_sync_job


def main() -> None:
    """Run the canonical QC sync job."""
    run_sync_job("qc")


if __name__ == "__main__":
    main()

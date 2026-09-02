"""Run the fast sync job, including platform QC."""

from biodata_cache.sync import run_sync_job


def main() -> None:
    """Run the canonical fast sync job."""
    run_sync_job("fast")


if __name__ == "__main__":
    main()

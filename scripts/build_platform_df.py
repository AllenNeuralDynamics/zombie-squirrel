"""Run the dynamic foraging sync job."""

from biodata_cache.sync import run_sync_job


def main() -> None:
    """Run the canonical dynamic foraging sync job."""
    run_sync_job("df")


if __name__ == "__main__":
    main()

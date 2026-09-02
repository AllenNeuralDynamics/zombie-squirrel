"""Run the population physiology sync job."""

from biodata_cache.sync import run_sync_job


def main() -> None:
    """Run the canonical population physiology sync job."""
    run_sync_job("pophys")


if __name__ == "__main__":
    main()

"""Run the canonical ecephys sync jobs."""

from biodata_cache.sync import run_sync_job


def main() -> None:
    """Run the spike and unit sync jobs in sequence."""
    run_sync_job("ecephys_spikes")
    run_sync_job("ecephys_units")


if __name__ == "__main__":
    main()

"""Run one biodata-cache sync job."""

import argparse

from biodata_cache.sync import JOBS, run_sync_job


def main() -> None:
    """Run the requested job or the job selected by the environment."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job", nargs="?", choices=sorted(JOBS), help="Sync job; defaults to BIODATA_CACHE_SYNC_JOB")
    args = parser.parse_args()
    run_sync_job(args.job)


if __name__ == "__main__":
    main()

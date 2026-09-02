"""Run the fiber-trace sync job."""

from biodata_cache.sync import run_sync_job


def main() -> None:
    """Run the canonical fiber-trace sync job."""
    run_sync_job("fib_traces")


if __name__ == "__main__":
    main()

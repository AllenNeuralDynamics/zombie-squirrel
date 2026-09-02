"""Run the behavior-video frame-times sync job."""

from biodata_cache.sync import run_sync_job


def main() -> None:
    """Run the canonical behavior-video frame-times sync job."""
    run_sync_job("video_frame_times")


if __name__ == "__main__":
    main()

"""Integration tests for zombie-squirrel package using Redshift backend.

Tests that verify squirrel functions work correctly with the Redshift
cache backend."""

import logging
import os
import time

from zombie_squirrel import unique_project_names, unique_subject_ids

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _test_redshift_unique_project_names():
    """Test fetching unique project names from Redshift backend.

    Measures performance and validates result is a list of project names."""
    # Set environment variable to use Redshift backend
    os.environ["TREE_SPECIES"] = "redshift"
    # Run the function
    start = time.time()
    project_names = unique_project_names()
    end = time.time()
    logging.info(f"Fetched {len(project_names)} project names in {end - start:.2} seconds.")
    assert isinstance(project_names, list), "Result should be a list"


def _test_redshift_unique_subject_ids():
    """Test fetching unique subject IDs from Redshift backend.

    Measures performance and validates result is a list of subject IDs."""
    # Set environment variable to use Redshift backend
    os.environ["TREE_SPECIES"] = "redshift"
    # Run the function
    start = time.time()
    subject_ids = unique_subject_ids()
    end = time.time()
    logging.info(f"Fetched {len(subject_ids)} subject IDs in {end - start:.2} seconds.")
    assert isinstance(subject_ids, list), "Result should be a list"


def main():
    """Execute all integration tests."""
    _test_redshift_unique_project_names()
    _test_redshift_unique_subject_ids()


if __name__ == "__main__":
    main()

"""Integration tests for zombie-squirrel package using Redshift backend.

Tests that verify squirrel functions work correctly with the Redshift
cache backend."""

import time

import pandas as pd

from zombie_squirrel import (
    asset_basics,
    raw_to_derived,
    source_data,
    unique_project_names,
    unique_subject_ids,
)


def test_s3_unique_project_names():
    """Test fetching unique project names from S3 backend.
    Measures performance and validates result is a list of project names."""
    start = time.time()
    project_names = unique_project_names()
    end = time.time()
    elapsed = end - start
    print(f"Fetched {len(project_names)} project names in {elapsed:.2} seconds.")
    assert isinstance(project_names, list), "Result should be a list"


def test_s3_unique_subject_ids():
    """Test fetching unique subject IDs from S3 backend.
    Measures performance and validates result is a list of subject IDs."""
    start = time.time()
    subject_ids = unique_subject_ids()
    end = time.time()
    elapsed = end - start
    print(f"Fetched {len(subject_ids)} subject IDs in {elapsed:.2} seconds.")
    assert isinstance(subject_ids, list), "Result should be a list"


def test_s3_asset_basics():
    """Test fetching asset basics from S3 backend.
    Validates result is a DataFrame with expected columns."""
    start = time.time()
    df = asset_basics()
    end = time.time()
    elapsed = end - start
    print(f"Fetched {len(df)} asset records in {elapsed:.2} seconds.")
    assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
    expected_columns = {
        "_id",
        "_last_modified",
        "modalities",
        "project_name",
        "data_level",
        "subject_id",
        "acquisition_start_time",
        "acquisition_end_time",
        "process_date",
        "genotype",
        "location",
    }
    assert set(df.columns) == expected_columns


def test_s3_source_data():
    """Test fetching source data references from S3 backend.
    Validates result is a DataFrame with _id and source_data columns."""
    start = time.time()
    df = source_data()
    end = time.time()
    elapsed = end - start
    print(f"Fetched {len(df)} source data records in {elapsed:.2} seconds.")
    assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
    assert set(df.columns) == {"_id", "source_data"}


def test_s3_raw_to_derived():
    """Test fetching raw to derived mapping from S3 backend.
    Validates result is a DataFrame with _id and derived_records columns."""
    start = time.time()
    df = raw_to_derived()
    end = time.time()
    elapsed = end - start
    print(f"Fetched {len(df)} raw to derived mappings in {elapsed:.2} seconds.")
    assert isinstance(df, pd.DataFrame), "Result should be a DataFrame"
    assert set(df.columns) == {"_id", "derived_records"}


def main():
    """Execute all integration tests."""
    test_s3_unique_project_names()
    test_s3_unique_subject_ids()
    test_s3_asset_basics()
    test_s3_source_data()
    test_s3_raw_to_derived()


if __name__ == "__main__":
    main()

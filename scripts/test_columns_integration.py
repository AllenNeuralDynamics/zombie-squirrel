"""Integration test for column metadata functions.

Tests that column metadata functions can retrieve column information from S3."""

from zombie_squirrel import unique_project_names_columns, qc_columns


def test_unique_project_names_columns():
    """Test retrieving column names for unique project names.

    This integration test fetches the actual column metadata from S3
    and prints the column names."""
    columns = unique_project_names_columns()
    print("Unique project names columns:")
    print(columns)
    assert isinstance(columns, list), "Result should be a list"
    assert len(columns) > 0, "Should have at least one column"


def test_qc_columns():
    """Test retrieving column names for QC.

    This integration test fetches the actual column metadata from S3
    and prints the column names."""
    columns = qc_columns()
    print("QC columns:")
    print(columns)
    assert isinstance(columns, list), "Result should be a list"
    assert len(columns) > 0, "Should have at least one column"


if __name__ == "__main__":
    test_unique_project_names_columns()
    test_qc_columns()

"""Utility functions for zombie-squirrel package."""


def prefix_table_name(table_name: str) -> str:
    """Add zombie-squirrel prefix to table names.

    Args:
        table_name: The base table name.

    Returns:
        Table name with 'zs_' prefix."""
    return "zs_" + table_name

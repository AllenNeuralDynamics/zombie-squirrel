"""Utility functions"""
import pandas as pd
import logging

from zombie_squirrel.acorns import Acorn


def prefix_table_name(table_name: str) -> str:
    return "zs_" + table_name


def rds_get_handle_empty(acorn: Acorn, table_name: str) -> pd.DataFrame:
    """Utility function for testing purposes."""
    try:
        df = acorn.scurry(table_name)
    except Exception as e:
        logging.warning(f"Error fetching from cache: {e}")
        df = pd.DataFrame()

    return df

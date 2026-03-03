"""Custom acorn: user-defined named cache entries."""

import pandas as pd

import zombie_squirrel.acorns as acorns


def custom(name: str, force_update: bool = False, df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Store or retrieve a user-defined DataFrame by name.

    Args:
        name: Key to store or retrieve the DataFrame under.
        force_update: If True, store the provided DataFrame in the cache.
        df: DataFrame to store. Required when force_update is True.

    Returns:
        The stored DataFrame.

    Raises:
        ValueError: If force_update is True but df is not provided.
        ValueError: If force_update is False and the cache is empty for the given name.
    """
    if force_update:
        if df is None:
            raise ValueError("df must be provided when force_update=True.")
        acorns.TREE.hide(name, df)
        return df

    result = acorns.TREE.scurry(name)
    if result.empty:
        raise ValueError(f"Cache is empty for '{name}'. Use force_update=True to store data.")
    return result

"""Synchronization utilities for updating all cached data."""

from .squirrels import SQUIRREL_REGISTRY


def hide_acorns():
    """Trigger force update of all registered squirrel functions.

    Calls each squirrel function with force_update=True to refresh
    all cached data in the acorn backend."""
    for squirrel in SQUIRREL_REGISTRY.values():
        squirrel(force_update=True)

"""Synchronization utilities for updating all cached data."""

from .acorns import ACORN_REGISTRY


def hide_acorns():
    """Trigger force update of all registered acorn functions.

    Calls each acorn function with force_update=True to refresh
    all cached data in the tree backend."""
    for acorn in ACORN_REGISTRY.values():
        acorn(force_update=True)

"""Synchronization utilities for updating all cached data."""

import logging

from .acorns import ACORN_REGISTRY


def hide_acorns():
    """Trigger force update of all registered acorn functions.

    Calls each acorn function with force_update=True to refresh
    all cached data in the tree backend."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    for acorn in ACORN_REGISTRY.values():
        acorn(force_update=True)

"""Synchronization utilities for updating all cached data."""

import logging

from .squirrels import SQUIRREL_REGISTRY


def hide_acorns():
    """Trigger force update of all registered squirrel functions.

    Calls each squirrel function with force_update=True to refresh
    all cached data in the acorn backend."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )
    for squirrel in SQUIRREL_REGISTRY.values():
        squirrel(force_update=True)

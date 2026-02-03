"""Acorns: functions to fetch and cache data from MongoDB."""

import logging
import os
from collections.abc import Callable
from typing import Any

from zombie_squirrel.forest import (
    MemoryTree,
    S3Tree,
)

# --- Backend setup ---------------------------------------------------

API_GATEWAY_HOST = "api.allenneuraldynamics.org"

forest_type = os.getenv("FOREST_TYPE", "memory").lower()

if forest_type == "S3":  # pragma: no cover
    logging.info("Using S3 forest for caching")
    TREE = S3Tree()
else:
    logging.info("Using in-memory forest for caching")
    TREE = MemoryTree()

# --- Acorn registry and names -----------------------------------------------------

NAMES = {
    "upn": "unique_project_names",
    "usi": "unique_subject_ids",
    "basics": "asset_basics",
    "d2r": "source_data",
    "r2d": "raw_to_derived",
}

ACORN_REGISTRY: dict[str, Callable[[], Any]] = {}


def register_acorn(name: str):
    """Decorator for registering new acorns."""

    def decorator(func):
        """Register function in acorn registry."""
        ACORN_REGISTRY[name] = func
        return func

    return decorator

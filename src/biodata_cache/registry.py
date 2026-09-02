"""Cache table registry and backend setup."""

import logging
import os
from collections.abc import Callable
from typing import Any

from biodata_cache.backend import (
    MemoryBackend,
    S3Backend,
)
from biodata_cache.table_specs import NAMES, TABLE_SPECS_BY_NAME  # noqa: F401
from biodata_cache.utils import CacheLogMessage

# --- Backend setup ---------------------------------------------------

API_GATEWAY_HOST = "api.allenneuraldynamics.org"

backend_type = os.getenv("BIODATA_CACHE_BACKEND", "memory").lower()

if backend_type == "s3":  # pragma: no cover
    logging.info(
        CacheLogMessage(backend="S3Backend", table="system", message="Initializing S3 backend for caching").to_json()
    )
    BACKEND = S3Backend()
elif backend_type == "memory":  # pragma: no cover
    logging.info(
        CacheLogMessage(
            backend="MemoryBackend", table="system", message="Initializing in-memory backend for caching"
        ).to_json()
    )
    BACKEND = MemoryBackend()
else:  # pragma: no cover
    raise ValueError(f"Unknown BIODATA_CACHE_BACKEND: {backend_type}")

# --- Cache table registry ----------------------------------------------------

TABLE_REGISTRY: dict[str, Callable[[], Any]] = {}


def register_table(name: str):
    """Register cache table function with registry."""
    if name not in TABLE_SPECS_BY_NAME:
        raise KeyError(f"No TableSpec exists for registered table {name!r}")

    def decorator(func):
        """Register function in cache table registry."""
        TABLE_REGISTRY[name] = func
        return func

    return decorator

"""Cache table registry: functions to fetch and cache data from MongoDB."""

import logging
import os
from collections.abc import Callable
from typing import Any

from biodata_cache.backend import (
    MemoryBackend,
    S3Backend,
)
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

# --- Cache table registry and names -------------------------------------------

NAMES = {
    "upn": "unique_project_names",
    "usi": "unique_subject_ids",
    "ugt": "unique_genotypes",
    "basics": "asset_basics",
    "d2r": "source_data",
    "r2d": "raw_to_derived",
    "qc": "quality_control",
    "smartspim": "platform_smartspim",
    "exaspim": "platform_exaspim",
    "upgrade": "metadata_upgrade",
    "fib": "platform_fib",
    "fib_traces": "platform_fib_traces",
    "fib_operations": "platform_fib_operations",
    "df_operations": "platform_df_operations",
    "ecephys_spikes": "platform_ecephys_spikes",
    "ecephys_units": "platform_ecephys_units",
    "pophys": "platform_pophys",
    "core": "metadata_core",
    "df_sessions": "platform_dynamic_foraging_sessions",
    "df_trials": "platform_dynamic_foraging_trials",
    "df_events": "platform_dynamic_foraging_events",
    "curriculum": "behavior_curriculum",
    "platform_qc": "platform_qc",
    "time_to_qc": "time_to_qc",
    "mouselight": "platform_mouselight",
    "storage_lens": "storage_lens",
    "swdb_2025_bci": "swdb_2025_bci",
    "swdb_2025_v1dd": "swdb_2025_v1dd",
}

TABLE_REGISTRY: dict[str, Callable[[], Any]] = {}


def register_table(name: str):
    """Register cache table function with registry."""

    def decorator(func):
        """Register function in cache table registry."""
        TABLE_REGISTRY[name] = func
        return func

    return decorator

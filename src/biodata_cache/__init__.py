"""biodata-cache: caching and synchronization for AIND metadata.

Provides functions to fetch and cache project names, subject IDs, and asset
metadata from the AIND metadata database with support for multiple backends.
Also exposes get_cache_registry to retrieve the cache_registry.json registry of all
available cache tables and their metadata.
"""

__version__ = "0.40.1"

from biodata_cache.cache_table_helpers.asset_basics import asset_basics  # noqa: F401
from biodata_cache.cache_table_helpers.behavior_curriculum import behavior_curriculum  # noqa: F401
from biodata_cache.cache_table_helpers.custom import custom  # noqa: F401
from biodata_cache.cache_table_helpers.metadata_upgrade import metadata_upgrade  # noqa: F401
from biodata_cache.cache_table_helpers.platform_df import (  # noqa: F401
    platform_dynamic_foraging_events,
    platform_dynamic_foraging_sessions,
    platform_dynamic_foraging_trials,
)
from biodata_cache.cache_table_helpers.platform_ecephys_spikes import platform_ecephys_spikes  # noqa: F401
from biodata_cache.cache_table_helpers.platform_ecephys_units import platform_ecephys_units  # noqa: F401
from biodata_cache.cache_table_helpers.platform_exaspim import platform_exaspim  # noqa: F401
from biodata_cache.cache_table_helpers.platform_fib import platform_fib  # noqa: F401
from biodata_cache.cache_table_helpers.platform_fib_operations import platform_fib_operations  # noqa: F401
from biodata_cache.cache_table_helpers.platform_fib_traces import platform_fib_traces  # noqa: F401
from biodata_cache.cache_table_helpers.platform_mouselight import platform_mouselight  # noqa: F401
from biodata_cache.cache_table_helpers.platform_pophys import platform_pophys  # noqa: F401
from biodata_cache.cache_table_helpers.platform_qc import platform_qc  # noqa: F401
from biodata_cache.cache_table_helpers.platform_smartspim import assets_smartspim  # noqa: F401
from biodata_cache.cache_table_helpers.platform_swdb import (  # noqa: F401
    platform_swdb_events,
    platform_swdb_eye,
    platform_swdb_performance,
    platform_swdb_running,
    platform_swdb_sessions,
    platform_swdb_trials,
)
from biodata_cache.cache_table_helpers.qc import qc, qc_columns  # noqa: F401
from biodata_cache.cache_table_helpers.raw_to_derived import raw_to_derived  # noqa: F401
from biodata_cache.cache_table_helpers.source_data import source_data  # noqa: F401
from biodata_cache.cache_table_helpers.storage_lens import storage_lens  # noqa: F401
from biodata_cache.cache_table_helpers.time_to_qc import time_to_qc  # noqa: F401
from biodata_cache.cache_table_helpers.unique_genotypes import (  # noqa: F401
    unique_genotypes,
)
from biodata_cache.cache_table_helpers.unique_project_names import (  # noqa: F401
    unique_project_names,
)
from biodata_cache.cache_table_helpers.unique_subject_ids import (  # noqa: F401
    unique_subject_ids,
)
from biodata_cache.utils import get_cache_registry, get_cache_versions  # noqa: F401

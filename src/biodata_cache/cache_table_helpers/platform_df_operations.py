"""Dynamic foraging processing-status cache table (partitioned by asset_name).

Reconstructs the processing lifecycle of each dynamic foraging acquisition from
the structured pipeline logs emitted to CloudWatch by the
``dynamic-foraging-processing-pipeline``. One row is stored per lifecycle event
(``stage_start`` / ``stage_complete`` / ``stage_error``); ``asset_name`` (the
acquisition name) is the partition key and joins to ``asset_basics``.

The CloudWatch log processing is shared with the other ``*_operations`` tables in
``shared.cloudwatch_utils``; this module only pins the pipeline name.
"""

import pandas as pd

import biodata_cache.registry as registry
from biodata_cache.cache_table_helpers.shared import cloudwatch_utils as cw
from biodata_cache.models import Column

_PIPELINE_NAME = "dynamic-foraging-processing-pipeline"
_TABLE_KEY = "df_operations"

cw.register_operations_pipeline(_PIPELINE_NAME, _TABLE_KEY)


def fetch_all_df_operations(lookback_days: int = cw.DEFAULT_LOOKBACK_DAYS) -> list[str]:
    """Fetch every acquisition's events in one bulk query and write all partitions."""
    return cw.fetch_all_operations(_TABLE_KEY, _PIPELINE_NAME, lookback_days)


@registry.register_table(registry.NAMES["df_operations"])
def platform_df_operations(
    asset_name: str | None = None,
    force_update: bool = False,
    lazy: bool = False,
    location: str | None = None,
) -> pd.DataFrame | str:
    """Return dynamic foraging processing events, or rebuild the whole table.

    One row per pipeline lifecycle event (``stage_start`` / ``stage_complete`` /
    ``stage_error``) found in the CloudWatch pipeline logs, across every pipeline
    stage (``process_name``). Data is cached per asset_name partition. Group by
    asset_name for a per-acquisition processing-status view.

    A ``force_update`` with no ``asset_name`` rebuilds every partition from a single
    bulk CloudWatch query (the normal sync path); this table is not built one asset
    at a time. Passing ``asset_name`` targets a single acquisition (ad-hoc refresh
    or read).

    Args:
        asset_name: Acquisition name to read/refresh (joins to asset_basics
            ``name``). If None, a ``force_update`` rebuilds the whole table and a
            read is not supported.
        force_update: If True, re-query CloudWatch and write the cache (the whole
            table when ``asset_name`` is None, otherwise just that acquisition). An
            empty DataFrame is returned; read again without force_update.
        lazy: If True, return the partition's storage location string (for DuckDB)
            instead of loading the DataFrame. Requires ``asset_name``.
        location: Unused; accepted for a uniform sync interface.

    Returns:
        DataFrame of processing events; the partition location string if lazy=True;
        or an empty DataFrame if force_update=True (data is written to the cache).

    Raises:
        ValueError: If ``asset_name`` is None on a read, or the cache is empty for
            the asset and force_update is False.
    """
    return cw.platform_operations(_TABLE_KEY, _PIPELINE_NAME, asset_name, force_update, lazy)


def platform_df_operations_columns() -> list[Column]:
    """Return platform_df_operations cache table column definitions."""
    return cw.operations_columns()

"""Zombie-squirrel: caching and synchronization for AIND metadata.

Provides functions to fetch and cache project names, subject IDs, and asset
metadata from the AIND metadata database with support for multiple backends.
Also exposes get_squirrel_info to retrieve the squirrel.json registry of all
available acorns and their metadata.
"""

__version__ = "0.17.1"

from zombie_squirrel.acorn_helpers.asset_basics import asset_basics  # noqa: F401
from zombie_squirrel.acorn_helpers.assets_smartspim import assets_smartspim  # noqa: F401
from zombie_squirrel.acorn_helpers.custom import custom  # noqa: F401
from zombie_squirrel.acorn_helpers.procedures import brain_injections, procedures  # noqa: F401
from zombie_squirrel.acorn_helpers.qc import qc, qc_columns  # noqa: F401
from zombie_squirrel.acorn_helpers.raw_to_derived import raw_to_derived  # noqa: F401
from zombie_squirrel.acorn_helpers.source_data import source_data  # noqa: F401
from zombie_squirrel.acorn_helpers.unique_project_names import (  # noqa: F401
    unique_project_names,
)
from zombie_squirrel.acorn_helpers.unique_subject_ids import (  # noqa: F401
    unique_subject_ids,
)
from zombie_squirrel.utils import get_squirrel_info  # noqa: F401

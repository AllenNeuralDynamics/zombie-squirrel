"""Compatibility module for the manually managed SWDB builders."""

import sys as _sys

from biodata_cache.cache_table_helpers.manual.swdb import public_collections as _implementation

_sys.modules[__name__] = _implementation

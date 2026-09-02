"""Utility functions for the biodata-cache package."""

import logging
import re
import time
from collections.abc import Sequence
from typing import Any

import pandas as pd
import semver
from pydantic import BaseModel

from biodata_cache import __version__ as _BDC_FULL_VERSION

_parsed = semver.Version.parse(_BDC_FULL_VERSION)
BDC_VERSION = f"{_parsed.major}.{_parsed.minor}"


class CacheLogMessage(BaseModel):
    """Structured logging message for biodata-cache operations."""

    backend: str
    table: str
    message: str

    def to_json(self) -> str:
        """Convert message to JSON string."""
        return self.model_dump_json()


def setup_logging():
    """Configure logging for the biodata-cache package.

    Sets up INFO level logging with timestamp format.
    Safe to call multiple times - uses force=True to reconfigure.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", force=True)


def get_cache_registry():
    """Fetch and return the cache registry from the active backend.

    The registry is stored as one fragment per cache table under
    ``cache_registry/<name>.json`` (each sync job writes its own fragment). This
    merges every fragment into a single CacheRegistry, sorted by table name for a
    stable ordering. Falls back to a legacy monolithic ``cache_registry.json`` if
    no fragments are present (older cache versions written before the split).
    """
    import biodata_cache.registry as registry
    from biodata_cache.models import CacheRegistry, CacheTable

    fragments = registry.BACKEND.list_registry_fragments()
    if fragments:
        tables = [CacheTable.model_validate_json(fragment) for fragment in fragments]
        tables.sort(key=lambda table: table.name)
        return CacheRegistry(tables=tables)

    data = registry.BACKEND.get_json("cache_registry.json")
    return CacheRegistry.model_validate_json(data)


def get_cache_versions() -> list[str]:
    """Return the list of all available cache version folders."""
    import biodata_cache.registry as registry

    return registry.BACKEND.get_versions_index()


_INSTRUMENT_ID_RE = re.compile(r"^[^_-]+[_-](.+)_(\d{8}|\d{4}-\d{2}-\d{2}|2[3-6]\d{4})$")


def normalize_instrument_id(instrument_id: str | None) -> str:
    if not instrument_id:
        return ""
    s = str(instrument_id)
    m = _INSTRUMENT_ID_RE.match(s)
    name = m.group(1) if m else s
    return re.sub(r"[_-]", "", name)


def normalize_name(name: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9 ]", " ", str(name))
    s = re.sub(r"\s+", " ", s).strip()
    return s.title()


_S3_RETRY_ATTEMPTS = 5
_S3_RETRY_BACKOFF = 2.0


def duckdb_query(query: str, parameters: Sequence[Any] | None = None) -> "pd.DataFrame":
    """Execute a DuckDB query, optionally with bound parameters.

    Bound parameters are used by filtered cache reads so user-supplied values
    never become SQL text. Retries retain the existing S3 rate-limit behavior.
    """
    import duckdb

    for attempt in range(_S3_RETRY_ATTEMPTS):
        try:
            with duckdb.connect() as con:
                if parameters is None:
                    return con.sql(query).df()
                return con.execute(query, parameters).df()
        except Exception as exc:
            msg = str(exc)
            if "503" in msg or "SlowDown" in msg or "Service Unavailable" in msg:
                if attempt < _S3_RETRY_ATTEMPTS - 1:
                    delay = _S3_RETRY_BACKOFF * (2 ** attempt)
                    logging.warning(
                        f"S3 rate limit, retrying in {delay:.1f}s "
                        f"(attempt {attempt + 1}/{_S3_RETRY_ATTEMPTS})"
                    )
                    time.sleep(delay)
                    continue
            raise
    raise RuntimeError(f"DuckDB query failed after {_S3_RETRY_ATTEMPTS} attempts")


def _merge_key(display_name: str) -> str:
    return display_name.lower().replace(" ", "")


def _resolve_first_names(names: list) -> list:
    to_remove = set()
    for i, name in enumerate(names):
        parts = name.split()
        if len(parts) == 1:
            first = parts[0].lower()
            matches = [n for n in names if len(n.split()) > 1 and n.split()[0].lower() == first]
            if len(matches) == 1:
                to_remove.add(i)
    return [name for i, name in enumerate(names) if i not in to_remove]


def parse_experimenters(val: str | None) -> list:
    if not val:
        return []
    seen: set = set()
    result = []
    for part in str(val).split(","):
        normalized = normalize_name(part)
        if not normalized:
            continue
        key = _merge_key(normalized)
        if key not in seen:
            seen.add(key)
            result.append(normalized)
    return result


def build_first_name_map(all_names: list) -> dict:
    unique = list(dict.fromkeys(all_names))
    result = {}
    for name in unique:
        parts = name.split()
        if len(parts) == 1:
            first = parts[0].lower()
            matches = [n for n in unique if len(n.split()) > 1 and n.split()[0].lower() == first]
            if len(matches) == 1:
                result[name] = matches[0]
    return result


def apply_first_name_map(names: list, first_name_map: dict) -> list:
    seen: set = set()
    result = []
    for name in names:
        mapped = first_name_map.get(name, name)
        if mapped not in seen:
            seen.add(mapped)
            result.append(mapped)
    return result


def normalize_experimenters(names: list) -> list:
    seen: set = set()
    result = []
    for val in names:
        for normalized in parse_experimenters(val):
            key = _merge_key(normalized)
            if key not in seen:
                seen.add(key)
                result.append(normalized)
    return _resolve_first_names(result)

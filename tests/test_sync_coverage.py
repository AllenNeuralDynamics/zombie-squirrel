"""Additional tests for the fragment-based registry round trip."""

from unittest.mock import patch

from biodata_cache.backend import MemoryBackend
from biodata_cache.models import CacheRegistry
from biodata_cache.sync import publish_cache_registry
from biodata_cache.utils import get_cache_registry


def test_fragments_round_trip_through_memory_backend():
    """Fragments written by publish_cache_registry are merged back by get_cache_registry."""
    backend = MemoryBackend()
    with patch("biodata_cache.sync.BACKEND", backend), patch("biodata_cache.registry.BACKEND", backend):
        publish_cache_registry()
        registry = get_cache_registry()

    assert isinstance(registry, CacheRegistry)
    assert len(registry.tables) == 22
    names = [table.name for table in registry.tables]
    # merged registry is sorted by name for stable ordering
    assert names == sorted(names)
    assert "asset_basics" in names
    assert "quality_control" in names


def test_get_cache_registry_falls_back_to_legacy_monolith():
    """With no fragments present, a legacy cache_registry.json is used."""
    backend = MemoryBackend()
    legacy = CacheRegistry(tables=[]).model_dump_json()
    backend.put_json("cache_registry.json", legacy)
    with patch("biodata_cache.registry.BACKEND", backend):
        registry = get_cache_registry()
    assert isinstance(registry, CacheRegistry)
    assert registry.tables == []


def test_clear_registry_removes_fragments():
    backend = MemoryBackend()
    with patch("biodata_cache.sync.BACKEND", backend):
        publish_cache_registry()
        assert len(backend.list_registry_fragments()) == 22
        backend.clear_registry()
        assert backend.list_registry_fragments() == []

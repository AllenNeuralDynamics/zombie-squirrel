"""Unit tests for cache table registry mechanism."""

from biodata_cache.registry import NAMES, TABLE_REGISTRY


def test_table_registry_contains_core_functions():
    assert NAMES["upn"] in TABLE_REGISTRY
    assert NAMES["usi"] in TABLE_REGISTRY
    assert NAMES["ugt"] in TABLE_REGISTRY
    assert NAMES["basics"] in TABLE_REGISTRY
    assert NAMES["d2r"] in TABLE_REGISTRY
    assert NAMES["qc"] in TABLE_REGISTRY


def test_registry_values_are_callable():
    for name, func in TABLE_REGISTRY.items():
        assert callable(func), f"{name} is not callable"


def test_names_dict_completeness():
    for key in ["upn", "usi", "ugt", "basics", "d2r", "r2d", "qc"]:
        assert key in NAMES

"""Unit tests for acorn registry mechanism.

Tests for acorn registration and NAMES dictionary."""

import unittest

from zombie_squirrel.acorns import (
    ACORN_REGISTRY,
    NAMES,
)


class TestAcornRegistration(unittest.TestCase):
    """Tests for acorn registration mechanism."""

    def test_acorn_registry_contains_all_functions(self):
        """Test that all acorn functions are registered."""
        self.assertIn(NAMES["upn"], ACORN_REGISTRY)
        self.assertIn(NAMES["usi"], ACORN_REGISTRY)
        self.assertIn(NAMES["basics"], ACORN_REGISTRY)
        self.assertIn(NAMES["d2r"], ACORN_REGISTRY)
        self.assertIn(NAMES["r2d"], ACORN_REGISTRY)
        self.assertIn(NAMES["qc"], ACORN_REGISTRY)

    def test_registry_values_are_callable(self):
        """Test that registry values are callable functions."""
        for name, func in ACORN_REGISTRY.items():
            self.assertTrue(callable(func), f"{name} is not callable")

    def test_names_dict_completeness(self):
        """Test that NAMES dict has expected keys."""
        expected_keys = ["upn", "usi", "basics", "d2r", "r2d", "qc"]
        for key in expected_keys:
            self.assertIn(key, NAMES)


if __name__ == "__main__":
    unittest.main()

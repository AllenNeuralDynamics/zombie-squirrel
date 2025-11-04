#!/usr/bin/env python
"""Test runner that sets environment variables before importing test modules."""

import os
import sys
import unittest

os.environ["TREE_SPECIES"] = "memory"

if __name__ == "__main__":
    loader = unittest.TestLoader()
    suite = loader.discover("tests", pattern="test_*.py")
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)

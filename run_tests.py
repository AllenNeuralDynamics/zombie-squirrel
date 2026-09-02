#!/usr/bin/env python
"""Run the test suite."""

import sys

import pytest

if __name__ == "__main__":
    sys.exit(pytest.main(["tests", "-v"]))

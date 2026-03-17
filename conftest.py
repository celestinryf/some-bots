"""
Root conftest.py — shared fixtures for all test suites.
"""

import pytest

from shared.config.settings import reset_settings


@pytest.fixture(autouse=True)
def _clean_settings():  # pyright: ignore[reportUnusedFunction]
    """Reset cached settings before and after every test. Prevents state leakage."""
    reset_settings()
    yield
    reset_settings()

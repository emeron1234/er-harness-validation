"""
Test configuration file
"""

import pytest


def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line(
        "markers", "spark: mark test as requiring spark session"
    )

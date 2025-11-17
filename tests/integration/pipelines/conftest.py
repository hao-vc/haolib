"""Pytest configuration for pipeline integration tests.

Imports fixtures from storages conftest to make them available for pipeline tests.
"""

# Import all fixtures from storages conftest
from tests.integration.storages.conftest import *  # noqa: F403

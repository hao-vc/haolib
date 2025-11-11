"""Conftest for health check integration tests."""

from __future__ import annotations

from typing import cast

import pytest_asyncio
from fastapi import FastAPI
from fastapi.testclient import TestClient

from haolib.configs.health import HealthCheckConfig
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.web.health.core.checker import AbstractHealthChecker, HealthCheckMetadata, HealthCheckResult


class SimpleHealthChecker:
    """Simple health checker for testing."""

    def __init__(
        self,
        name: str,
        is_healthy: bool = True,
        critical: bool = True,
        error: str | None = None,
    ) -> None:
        """Initialize simple health checker.

        Args:
            name: Name of the checker.
            is_healthy: Whether the check should be healthy.
            critical: Whether the check is critical.
            error: Error message if unhealthy.

        """
        self.name = name
        self._is_healthy = is_healthy
        self._critical = critical
        self._error = error

    async def __call__(self) -> HealthCheckResult:
        """Execute the health check."""
        return HealthCheckResult(
            metadata=HealthCheckMetadata(
                name=self.name,
                critical=self._critical,
                description=f"Test checker: {self.name}",
            ),
            is_healthy=self._is_healthy,
            error=self._error,
        )


@pytest_asyncio.fixture
async def health_checker_healthy() -> SimpleHealthChecker:
    """Healthy health checker fixture."""
    return SimpleHealthChecker(name="healthy_check", is_healthy=True, critical=True)


@pytest_asyncio.fixture
async def health_checker_unhealthy() -> SimpleHealthChecker:
    """Unhealthy health checker fixture."""
    return SimpleHealthChecker(name="unhealthy_check", is_healthy=False, critical=True, error="Test error")


@pytest_asyncio.fixture
async def health_checker_degraded() -> SimpleHealthChecker:
    """Non-critical unhealthy checker fixture."""
    return SimpleHealthChecker(name="degraded_check", is_healthy=False, critical=False, error="Non-critical error")


@pytest_asyncio.fixture
async def app_with_health_check() -> FastAPI:
    """FastAPI app with health check endpoint."""
    app = FastAPI()
    entrypoint = FastAPIEntrypoint(app=app)
    return entrypoint.get_app()


@pytest_asyncio.fixture
async def app_with_health_checkers(
    health_checker_healthy: SimpleHealthChecker,
) -> FastAPI:
    """FastAPI app with health check endpoint and checkers."""
    app = FastAPI()
    entrypoint = FastAPIEntrypoint(app=app).setup_health_check(
        health_checkers=cast("list[AbstractHealthChecker]", [health_checker_healthy])
    )
    return entrypoint.get_app()


@pytest_asyncio.fixture
async def test_client(app_with_health_check: FastAPI) -> TestClient:
    """Test client for health check tests."""
    return TestClient(app_with_health_check)


@pytest_asyncio.fixture
async def test_client_with_checkers(app_with_health_checkers: FastAPI) -> TestClient:
    """Test client with health checkers."""
    return TestClient(app_with_health_checkers)


@pytest_asyncio.fixture
async def empty_app() -> FastAPI:
    """Empty FastAPI app fixture."""
    return FastAPI()


@pytest_asyncio.fixture
async def health_checker_healthy_simple() -> SimpleHealthChecker:
    """Simple healthy checker fixture."""
    return SimpleHealthChecker(name="test", is_healthy=True)


@pytest_asyncio.fixture
async def health_checker_unhealthy_simple() -> SimpleHealthChecker:
    """Simple unhealthy checker fixture."""
    return SimpleHealthChecker(name="unhealthy", is_healthy=False, critical=True, error="Failed")


@pytest_asyncio.fixture
async def health_checker_degraded_simple() -> SimpleHealthChecker:
    """Simple degraded checker fixture."""
    return SimpleHealthChecker(name="degraded", is_healthy=False, critical=False, error="Non-critical")


def create_entrypoint_with_checkers(
    app: FastAPI,
    checkers: list[SimpleHealthChecker] | None = None,
    config: HealthCheckConfig | None = None,
) -> FastAPIEntrypoint:
    """Helper to create entrypoint with checkers."""
    entrypoint = FastAPIEntrypoint(app=app)
    if checkers:
        cast_checkers = cast("list[AbstractHealthChecker]", checkers)
        return entrypoint.setup_health_check(health_checkers=cast_checkers, config=config)
    return entrypoint.setup_health_check(config=config)


def create_test_client_with_checkers(
    checkers: list[SimpleHealthChecker] | None = None,
    config: HealthCheckConfig | None = None,
) -> TestClient:
    """Helper to create test client with checkers."""
    app = FastAPI()
    entrypoint = create_entrypoint_with_checkers(app, checkers, config)
    return TestClient(entrypoint.get_app())

"""Integration tests for health check system."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from haolib.configs.health import HealthCheckConfig
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.web.health.core.checker import HealthCheckMetadata, HealthCheckResult
from haolib.web.health.core.status import HealthStatus
from haolib.web.health.handlers.fastapi import (
    FastAPIHealthCheckResponse,
    fastapi_default_health_check_response_factory,
    fastapi_health_check_handler_factory,
)
from tests.integration.health.conftest import (
    SimpleHealthChecker,
    create_test_client_with_checkers,
)

# Constants
HTTP_OK = 200
HTTP_CREATED = 201
HTTP_ACCEPTED = 202
HTTP_NOT_FOUND = 404
HTTP_INTERNAL_SERVER_ERROR = 500
HTTP_SERVICE_UNAVAILABLE = 503
HEALTH_PATH_DEFAULT = "/health"
HEALTH_PATH_CUSTOM = "/custom-health"
NUM_CHECKERS_SMALL = 2
NUM_CHECKERS_MEDIUM = 3
DURATION_MS_TEST = 10.5
DURATION_MS_OTHER = 5.0


class TestHealthCheckEndpoint:
    """Test health check endpoint."""

    def test_health_check_no_checkers(self, empty_app: FastAPI) -> None:
        """Test health check endpoint with no checkers."""
        entrypoint = FastAPIEntrypoint(app=empty_app).setup_health_check()
        client = TestClient(entrypoint.get_app())

        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_OK
        data = response.json()
        assert data["status"] == "healthy"
        assert data["checks"] is None

    def test_health_check_single_healthy_checker(
        self,
        test_client_with_checkers: TestClient,
    ) -> None:
        """Test health check with single healthy checker."""
        response = test_client_with_checkers.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_OK
        data = response.json()
        assert data["status"] == "healthy"
        assert data["checks"] is not None
        assert len(data["checks"]) == 1
        assert data["checks"][0]["name"] == "healthy_check"
        assert data["checks"][0]["status"] == "healthy"

    def test_health_check_unhealthy_checker(
        self,
        health_checker_unhealthy_simple: SimpleHealthChecker,
    ) -> None:
        """Test health check with unhealthy checker."""
        client = create_test_client_with_checkers(checkers=[health_checker_unhealthy_simple])

        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_SERVICE_UNAVAILABLE
        data = response.json()
        assert data["status"] == "unhealthy"
        assert data["checks"] is not None
        assert len(data["checks"]) == 1
        assert data["checks"][0]["status"] == "unhealthy"
        assert data["checks"][0]["error"] == "Failed"

    def test_health_check_degraded_status(
        self,
        health_checker_degraded_simple: SimpleHealthChecker,
    ) -> None:
        """Test health check with degraded status (non-critical failure)."""
        client = create_test_client_with_checkers(checkers=[health_checker_degraded_simple])

        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_OK  # Degraded still returns 200
        data = response.json()
        assert data["status"] == "degraded"
        assert data["checks"] is not None
        assert data["checks"][0]["status"] == "unhealthy"

    def test_health_check_multiple_checkers(self) -> None:
        """Test health check with multiple checkers."""
        checkers = [
            SimpleHealthChecker(name="check1", is_healthy=True, critical=True),
            SimpleHealthChecker(name="check2", is_healthy=True, critical=False),
            SimpleHealthChecker(name="check3", is_healthy=False, critical=False, error="Non-critical"),
        ]
        client = create_test_client_with_checkers(checkers=checkers)

        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_OK
        data = response.json()
        assert data["status"] == "degraded"
        assert len(data["checks"]) == NUM_CHECKERS_MEDIUM

    def test_health_check_custom_path(self, empty_app: FastAPI) -> None:
        """Test health check with custom path."""
        config = HealthCheckConfig(route_path=HEALTH_PATH_CUSTOM)
        entrypoint = FastAPIEntrypoint(app=empty_app).setup_health_check(config=config)
        client = TestClient(entrypoint.get_app())

        response = client.get(HEALTH_PATH_CUSTOM)
        assert response.status_code == HTTP_OK

        # Original path should not exist
        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_NOT_FOUND

    def test_health_check_custom_status_codes(
        self,
        health_checker_healthy_simple: SimpleHealthChecker,
    ) -> None:
        """Test health check with custom status codes."""
        config = HealthCheckConfig(
            status_code_healthy=HTTP_CREATED,
            status_code_unhealthy=HTTP_INTERNAL_SERVER_ERROR,
            status_code_degraded=HTTP_ACCEPTED,
        )
        client = create_test_client_with_checkers(
            checkers=[health_checker_healthy_simple],
            config=config,
        )

        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_CREATED

    def test_health_check_without_details(
        self,
        health_checker_healthy_simple: SimpleHealthChecker,
    ) -> None:
        """Test health check without including details."""
        config = HealthCheckConfig(include_details=False)
        client = create_test_client_with_checkers(
            checkers=[health_checker_healthy_simple],
            config=config,
        )

        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_OK
        data = response.json()
        assert data["status"] == "healthy"
        assert data["checks"] is None

    def test_health_check_openapi_schema(self, empty_app: FastAPI) -> None:
        """Test that OpenAPI schema is generated correctly."""
        entrypoint = FastAPIEntrypoint(app=empty_app).setup_health_check()
        client = TestClient(entrypoint.get_app())

        # Get OpenAPI schema
        response = client.get("/openapi.json")
        assert response.status_code == HTTP_OK
        schema = response.json()

        # Check that health endpoint is in schema
        assert HEALTH_PATH_DEFAULT in schema["paths"]
        health_path = schema["paths"][HEALTH_PATH_DEFAULT]
        assert "get" in health_path

        # Check response schema
        get_operation = health_path["get"]
        assert "responses" in get_operation
        assert str(HTTP_OK) in get_operation["responses"]
        assert str(HTTP_SERVICE_UNAVAILABLE) in get_operation["responses"]

        # Check response model
        assert "response_model" in get_operation or "responses" in get_operation


class TestHealthCheckHandlerFactory:
    """Test health check handler factory."""

    @pytest.mark.asyncio
    async def test_handler_factory_no_checkers(self, empty_app: FastAPI) -> None:
        """Test handler factory with no checkers."""
        handler = fastapi_health_check_handler_factory()

        @empty_app.get(HEALTH_PATH_DEFAULT)
        async def health(request: Request) -> FastAPIHealthCheckResponse:
            return await handler(request)

        client = TestClient(empty_app)
        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_OK
        data = response.json()
        assert data["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_handler_factory_with_checkers(
        self,
        empty_app: FastAPI,
        health_checker_healthy_simple: SimpleHealthChecker,
    ) -> None:
        """Test handler factory with checkers."""
        handler = fastapi_health_check_handler_factory(checkers=[health_checker_healthy_simple])

        @empty_app.get(HEALTH_PATH_DEFAULT)
        async def health(request: Request) -> FastAPIHealthCheckResponse:
            return await handler(request)

        client = TestClient(empty_app)
        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_OK
        data = response.json()
        assert data["status"] == "healthy"
        assert data["checks"] is not None

    @pytest.mark.asyncio
    async def test_handler_factory_with_config(
        self,
        empty_app: FastAPI,
        health_checker_healthy_simple: SimpleHealthChecker,
    ) -> None:
        """Test handler factory with custom config."""
        config = HealthCheckConfig(timeout_seconds=1.0, execute_parallel=True)
        handler = fastapi_health_check_handler_factory(
            checkers=[health_checker_healthy_simple],
            config=config,
        )

        @empty_app.get(HEALTH_PATH_DEFAULT)
        async def health(request: Request) -> FastAPIHealthCheckResponse:
            return await handler(request)

        client = TestClient(empty_app)
        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_OK


class TestHealthCheckResponseFactory:
    """Test health check response factory."""

    @pytest.mark.asyncio
    async def test_default_response_factory_no_results(self) -> None:
        """Test default response factory with no results."""
        response = await fastapi_default_health_check_response_factory(HealthStatus.HEALTHY, None)
        assert isinstance(response, FastAPIHealthCheckResponse)
        assert response.status == "healthy"
        assert response.checks is None

    @pytest.mark.asyncio
    async def test_default_response_factory_with_results(self) -> None:
        """Test default response factory with results."""
        results = [
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check1", critical=True),
                is_healthy=True,
                duration_ms=DURATION_MS_TEST,
            ),
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check2", critical=False),
                is_healthy=False,
                error="Failed",
                duration_ms=DURATION_MS_OTHER,
            ),
        ]
        response = await fastapi_default_health_check_response_factory(HealthStatus.DEGRADED, results)
        assert isinstance(response, FastAPIHealthCheckResponse)
        assert response.status == "degraded"
        assert response.checks is not None
        assert len(response.checks) == NUM_CHECKERS_SMALL
        assert response.checks[0].name == "check1"
        assert response.checks[0].status == "healthy"
        assert response.checks[1].name == "check2"
        assert response.checks[1].status == "unhealthy"
        assert response.checks[1].error == "Failed"


class TestHealthCheckEdgeCases:
    """Test edge cases for health checks."""

    def test_health_check_empty_checkers_list(self, empty_app: FastAPI) -> None:
        """Test health check with empty checkers list."""
        entrypoint = FastAPIEntrypoint(app=empty_app).setup_health_check(health_checkers=[])
        client = TestClient(entrypoint.get_app())

        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_OK
        data = response.json()
        assert data["status"] == "healthy"

    def test_health_check_mixed_critical_and_non_critical(self) -> None:
        """Test health check with mixed critical and non-critical failures."""
        checkers = [
            SimpleHealthChecker(name="critical_healthy", is_healthy=True, critical=True),
            SimpleHealthChecker(
                name="critical_unhealthy",
                is_healthy=False,
                critical=True,
                error="Critical failed",
            ),
            SimpleHealthChecker(
                name="non_critical_unhealthy",
                is_healthy=False,
                critical=False,
                error="Non-critical",
            ),
        ]
        client = create_test_client_with_checkers(checkers=checkers)

        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_SERVICE_UNAVAILABLE  # Critical failure takes precedence
        data = response.json()
        assert data["status"] == "unhealthy"

    def test_health_check_all_unhealthy(self) -> None:
        """Test health check when all checkers are unhealthy."""
        checkers = [
            SimpleHealthChecker(name="check1", is_healthy=False, critical=True, error="Error 1"),
            SimpleHealthChecker(name="check2", is_healthy=False, critical=True, error="Error 2"),
        ]
        client = create_test_client_with_checkers(checkers=checkers)

        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_SERVICE_UNAVAILABLE
        data = response.json()
        assert data["status"] == "unhealthy"
        assert all(check["status"] == "unhealthy" for check in data["checks"] or [])

    def test_health_check_all_non_critical_unhealthy(self) -> None:
        """Test health check when all non-critical checkers are unhealthy."""
        checkers = [
            SimpleHealthChecker(name="check1", is_healthy=False, critical=False, error="Error 1"),
            SimpleHealthChecker(name="check2", is_healthy=False, critical=False, error="Error 2"),
        ]
        client = create_test_client_with_checkers(checkers=checkers)

        response = client.get(HEALTH_PATH_DEFAULT)
        assert response.status_code == HTTP_OK  # Degraded status
        data = response.json()
        assert data["status"] == "degraded"

    def test_health_check_response_includes_all_fields(
        self,
        health_checker_healthy_simple: SimpleHealthChecker,
    ) -> None:
        """Test that health check response includes all expected fields."""
        client = create_test_client_with_checkers(checkers=[health_checker_healthy_simple])

        response = client.get(HEALTH_PATH_DEFAULT)
        data = response.json()

        # Check top-level fields
        assert "status" in data
        assert "checks" in data

        # Check individual check fields
        if data["checks"]:
            check = data["checks"][0]
            assert "name" in check
            assert "status" in check
            # Other fields may be None, but should be present
            assert "description" in check
            assert "error" in check
            assert "duration_ms" in check
            assert "details" in check

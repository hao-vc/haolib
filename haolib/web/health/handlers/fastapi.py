"""FastAPI health check handlers."""

from collections.abc import Callable, Coroutine
from typing import Any, Literal, Protocol

from fastapi import Request, Response
from pydantic import BaseModel, Field

from haolib.configs.health import HealthCheckConfig
from haolib.web.health.checkers.abstract import AbstractHealthChecker, HealthCheckResult, HealthStatus
from haolib.web.health.checkers.executor import HealthCheckExecutor


class FastAPIHealthCheckResponseItem(BaseModel):
    """Individual health check result in the response.

    Attributes:
        name: Name of the health checker.
        status: Status of this check (healthy or unhealthy).
        description: Description of the check.
        error: Error message if unhealthy.
        duration_ms: Duration in milliseconds.
        details: Additional details.

    """

    name: str = Field(description="Name of the health checker.")
    status: Literal["healthy", "unhealthy"] = Field(description="Status of this check.")
    description: str | None = Field(default=None, description="Description of the check.")
    error: str | None = Field(default=None, description="Error message if unhealthy.")
    duration_ms: float | None = Field(default=None, description="Duration in milliseconds.")
    details: dict[str, Any] | None = Field(default=None, description="Additional details.")


class FastAPIHealthCheckResponse(BaseModel):
    """Default FastAPI health check response schema.

    Attributes:
        status: Overall health status (healthy, unhealthy, or degraded).
        checks: Individual check results, or None if checks were not performed.

    """

    status: Literal["healthy", "unhealthy", "degraded"] = Field(description="Overall health status.")
    checks: list[FastAPIHealthCheckResponseItem] | None = Field(default=None, description="Individual check results.")


class HealthCheckResponseFactory(Protocol):
    """Protocol for creating health check responses.

    Implementations should convert HealthCheckResult list and overall status
    into a FastAPI-compatible response (Response or BaseModel).
    """

    async def __call__(
        self,
        overall_status: HealthStatus,
        results: list[HealthCheckResult] | None = None,
    ) -> Response | BaseModel:
        """Create a health check response.

        Args:
            overall_status: The aggregated health status.
            results: Individual health check results, or None if no checks were performed.

        Returns:
            FastAPI Response or Pydantic BaseModel for JSON response.

        """
        ...


async def fastapi_default_health_check_response_factory(
    overall_status: HealthStatus,
    results: list[HealthCheckResult] | None = None,
) -> FastAPIHealthCheckResponse:
    """Default health check response factory.

    Creates a standard JSON response with overall status and individual check results.

    Args:
        overall_status: The aggregated health status.
        results: Individual health check results, or None if no checks were performed.

    Returns:
        FastAPIHealthCheckResponse with status and check details.

    """
    return FastAPIHealthCheckResponse(
        status=overall_status.value,
        checks=[
            FastAPIHealthCheckResponseItem(
                name=result.metadata.name,
                status="healthy" if result.is_healthy else "unhealthy",
                description=result.metadata.description,
                error=result.error,
                duration_ms=result.duration_ms,
                details=result.metadata.details,
            )
            for result in (results or [])
        ]
        if results
        else None,
    )


def fastapi_health_check_handler_factory(
    checkers: list[AbstractHealthChecker] | None = None,
    config: HealthCheckConfig | None = None,
    response_factory: HealthCheckResponseFactory | None = None,
) -> Callable[[Request], Coroutine[Any, Any, FastAPIHealthCheckResponse]]:
    """Create a FastAPI health check handler.

    This factory function creates a health check handler that can be used
    as a FastAPI route handler. The handler executes health checks and returns
    a properly formatted response for OpenAPI documentation.

    Args:
        checkers: List of health checkers to execute. If None, returns healthy status.
        config: Configuration for the health check endpoint. If None, uses defaults.
        response_factory: Custom response factory. If None, uses default response factory.

    Returns:
        FastAPI route handler function.

    Example:
        ```python
        from haolib.web.health.handlers.fastapi import fastapi_health_check_handler_factory, HealthCheckConfig

        @app.get("/health")
        async def health(request: Request):
            return await fastapi_health_check_handler_factory(
                checkers=[db_checker, redis_checker],
                config=HealthCheckConfig(
                    timeout=timedelta(seconds=5),
                    execute_parallel=True,
                )
            )(request)
        ```

    """
    if config is None:
        config = HealthCheckConfig()

    executor = HealthCheckExecutor(
        timeout=config.timeout,
        execute_parallel=config.execute_parallel,
    )
    response_factory = response_factory or fastapi_default_health_check_response_factory

    async def health_handler(request: Request) -> FastAPIHealthCheckResponse:
        """Health check handler."""
        # Determine status and results
        if not checkers:
            overall_status = HealthStatus.HEALTHY
            results = None
        else:
            results = await executor.execute(checkers)
            overall_status = executor.aggregate_status(results)

        # Get response from factory
        response = await response_factory(
            overall_status,
            results if config.include_details else None,
        )

        # Ensure we return FastAPIHealthCheckResponse for proper OpenAPI generation
        if isinstance(response, FastAPIHealthCheckResponse):
            return response

        if isinstance(response, Response):
            # Custom Response returned - create fallback response
            # This should not happen with default factory, but handle gracefully
            return FastAPIHealthCheckResponse(
                status=overall_status.value,
                checks=None,
            )

        # Fallback for other BaseModel types
        return FastAPIHealthCheckResponse(
            status=overall_status.value,
            checks=None,
        )

    return health_handler

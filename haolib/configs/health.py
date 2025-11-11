"""Health check configuration."""

from pydantic import Field
from pydantic_settings import BaseSettings


class HealthCheckConfig(BaseSettings):
    """Configuration for health check endpoint.

    Attributes:
        path: Endpoint path.
        timeout_seconds: Timeout for all health checks.
        execute_parallel: Whether to execute checks in parallel.
        response_factory: Custom response factory.
        status_code_healthy: HTTP status code when healthy.
        status_code_unhealthy: HTTP status code when unhealthy.
        status_code_degraded: HTTP status code when degraded.
        include_details: Whether to include individual check details.

    """

    route_path: str = Field(default="/health", description="Endpoint path.")
    timeout_seconds: float | None = Field(default=None, description="Timeout for all health checks.")
    execute_parallel: bool = Field(default=True, description="Whether to execute checks in parallel.")
    status_code_healthy: int = Field(default=200, description="HTTP status code when healthy.")
    status_code_unhealthy: int = Field(
        default=503,
        description="HTTP status code when unhealthy.",
    )
    status_code_degraded: int = Field(default=200, description="HTTP status code when degraded.")
    include_details: bool = Field(default=True, description="Whether to include individual check details.")

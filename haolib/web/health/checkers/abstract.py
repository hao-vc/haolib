"""Core health checker abstractions."""

from typing import Any, Protocol

from pydantic import BaseModel, Field

from haolib.enums.base import BaseEnum


class HealthStatus(BaseEnum):
    """Health status enumeration.

    Values:
        HEALTHY: All checks passed successfully.
        UNHEALTHY: At least one critical check failed.
        DEGRADED: Some non-critical checks failed, but service is operational.
    """

    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"
    DEGRADED = "degraded"  # Some checks failed but service is operational


class HealthCheckMetadata(BaseModel):
    """Metadata for a health check result.

    Attributes:
        name: The name of the health checker.
        description: Optional description of what is being checked.
        critical: Whether this check is critical for service health.
            If True and check fails, overall status will be UNHEALTHY.
            If False and check fails, overall status will be DEGRADED.
        details: Additional details about the check result.

    """

    name: str = Field(description="The name of the health checker.")
    description: str | None = Field(default=None, description="Optional description of what is being checked.")
    critical: bool = Field(default=True, description="Whether this check is critical for service health.")
    details: dict[str, Any] | None = Field(default=None, description="Additional details about the check result.")


class HealthCheckResult(BaseModel):
    """Result of a health check.

    Attributes:
        metadata: Metadata about the health check.
        is_healthy: Whether the check passed.
        error: Error message if the check failed.
        duration_ms: Duration of the check in milliseconds.

    """

    metadata: HealthCheckMetadata = Field(description="Metadata about the health check.")
    is_healthy: bool = Field(description="Whether the check passed.")
    error: str | None = Field(default=None, description="Error message if the check failed.")
    duration_ms: float | None = Field(default=None, description="Duration of the check in milliseconds.")


class AbstractHealthChecker(Protocol):
    """Protocol for health checkers.

    Health checkers are callable async functions that return HealthCheckResult.
    They should be idempotent and should not have side effects.

    Example:
        ```python
        class DatabaseHealthChecker:
            def __init__(self, db: Database):
                self.db = db

            async def __call__(self) -> HealthCheckResult:
                import time
                start = time.time()
                try:
                    await self.db.execute("SELECT 1")
                    duration = (time.time() - start) * 1000
                    return HealthCheckResult(
                        metadata=HealthCheckMetadata(
                            name="database",
                            critical=True,
                            description="PostgreSQL database connection"
                        ),
                        is_healthy=True,
                        duration_ms=duration
                    )
                except Exception as e:
                    duration = (time.time() - start) * 1000
                    return HealthCheckResult(
                        metadata=HealthCheckMetadata(
                            name="database",
                            critical=True,
                            description="PostgreSQL database connection"
                        ),
                        is_healthy=False,
                        error=str(e),
                        duration_ms=duration
                    )
        ```

    """

    async def __call__(self, *args: Any, **kwargs: Any) -> HealthCheckResult:
        """Execute the health check.

        Args:
            *args: Additional positional arguments (not typically used).
            **kwargs: Additional keyword arguments (not typically used).

        Returns:
            HealthCheckResult: The result of the health check.

        """
        ...

"""Health status enumeration."""

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

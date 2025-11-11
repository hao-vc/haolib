"""Unit tests for health check system."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from haolib.web.health.core.checker import (
    HealthCheckMetadata,
    HealthCheckResult,
)
from haolib.web.health.core.executor import HealthCheckExecutor
from haolib.web.health.core.status import HealthStatus

# Constants
DELAY_SHORT = 0.05
DELAY_MEDIUM = 0.1
DELAY_LONG = 0.2
TIMEOUT_SHORT = 0.1
TIMEOUT_MEDIUM = 0.15
TIMEOUT_LONG = 0.25
DURATION_MS_PRESET = 42.0
DURATION_MS_TEST = 10.5
DURATION_MS_OTHER = 5.0
MIN_DURATION_MS = 100
NUM_CHECKERS_SMALL = 2
NUM_CHECKERS_MEDIUM = 3


class MockHealthChecker:
    """Mock health checker for testing."""

    def __init__(
        self,
        name: str,
        is_healthy: bool = True,
        delay: float = 0.0,
        error: str | None = None,
        critical: bool = True,
        duration_ms: float | None = None,
        raise_exception: bool = False,
        exception_type: type[Exception] = Exception,
    ) -> None:
        """Initialize mock health checker.

        Args:
            name: Name of the checker.
            is_healthy: Whether the check should be healthy.
            delay: Delay in seconds before returning result.
            error: Error message if unhealthy.
            critical: Whether the check is critical.
            duration_ms: Duration to set in result (if None, will be measured).
            raise_exception: Whether to raise an exception instead of returning result.
            exception_type: Type of exception to raise.

        """
        self.name = name
        self._is_healthy = is_healthy
        self._delay = delay
        self._error = error
        self._critical = critical
        self._duration_ms = duration_ms
        self._raise_exception = raise_exception
        self._exception_type = exception_type
        self.call_count = 0

    async def __call__(self, *args: Any, **kwargs: Any) -> HealthCheckResult:  # noqa: ARG002
        """Execute the health check."""
        self.call_count += 1

        if self._delay > 0:
            await asyncio.sleep(self._delay)

        if self._raise_exception:
            raise self._exception_type(self._error or "Mock exception")

        return HealthCheckResult(
            metadata=HealthCheckMetadata(
                name=self.name,
                critical=self._critical,
                description=f"Mock checker for {self.name}",
            ),
            is_healthy=self._is_healthy,
            error=self._error,
            duration_ms=self._duration_ms,
        )


class TestHealthStatus:
    """Test HealthStatus enum."""

    def test_health_status_values(self) -> None:
        """Test health status enum values."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"
        assert HealthStatus.DEGRADED.value == "degraded"


class TestHealthCheckMetadata:
    """Test HealthCheckMetadata."""

    def test_metadata_creation(self) -> None:
        """Test metadata creation."""
        metadata = HealthCheckMetadata(
            name="test",
            description="Test description",
            critical=True,
            details={"key": "value"},
        )
        assert metadata.name == "test"
        assert metadata.description == "Test description"
        assert metadata.critical is True
        assert metadata.details == {"key": "value"}

    def test_metadata_defaults(self) -> None:
        """Test metadata defaults."""
        metadata = HealthCheckMetadata(name="test")
        assert metadata.name == "test"
        assert metadata.description is None
        assert metadata.critical is True
        assert metadata.details is None


class TestHealthCheckResult:
    """Test HealthCheckResult."""

    def test_result_creation(self) -> None:
        """Test result creation."""
        metadata = HealthCheckMetadata(name="test")
        result = HealthCheckResult(
            metadata=metadata,
            is_healthy=True,
            error=None,
            duration_ms=DURATION_MS_TEST,
        )
        assert result.metadata == metadata
        assert result.is_healthy is True
        assert result.error is None
        assert result.duration_ms == DURATION_MS_TEST

    def test_unhealthy_result(self) -> None:
        """Test unhealthy result."""
        metadata = HealthCheckMetadata(name="test")
        result = HealthCheckResult(
            metadata=metadata,
            is_healthy=False,
            error="Connection failed",
            duration_ms=5.0,
        )
        assert result.is_healthy is False
        assert result.error == "Connection failed"


class TestHealthCheckExecutor:
    """Test HealthCheckExecutor."""

    @pytest.mark.asyncio
    async def test_execute_empty_list(self) -> None:
        """Test executing empty list of checkers."""
        executor = HealthCheckExecutor()
        results = await executor.execute([])
        assert results == []

    @pytest.mark.asyncio
    async def test_execute_single_healthy_checker(self) -> None:
        """Test executing single healthy checker."""
        executor = HealthCheckExecutor()
        checker = MockHealthChecker(name="test", is_healthy=True)
        results = await executor.execute([checker])
        assert len(results) == 1
        assert results[0].is_healthy is True
        assert results[0].metadata.name == "test"
        assert checker.call_count == 1

    @pytest.mark.asyncio
    async def test_execute_single_unhealthy_checker(self) -> None:
        """Test executing single unhealthy checker."""
        executor = HealthCheckExecutor()
        checker = MockHealthChecker(name="test", is_healthy=False, error="Failed")
        results = await executor.execute([checker])
        assert len(results) == 1
        assert results[0].is_healthy is False
        assert results[0].error == "Failed"

    @pytest.mark.asyncio
    async def test_execute_parallel_healthy_checkers(self) -> None:
        """Test executing multiple healthy checkers in parallel."""
        executor = HealthCheckExecutor(execute_parallel=True)
        checkers = [
            MockHealthChecker(name="checker1", is_healthy=True, delay=DELAY_MEDIUM),
            MockHealthChecker(name="checker2", is_healthy=True, delay=DELAY_MEDIUM),
            MockHealthChecker(name="checker3", is_healthy=True, delay=DELAY_MEDIUM),
        ]
        start_time = time.perf_counter()
        results = await executor.execute(checkers)
        duration = time.perf_counter() - start_time

        assert len(results) == NUM_CHECKERS_MEDIUM
        assert all(r.is_healthy for r in results)
        # Parallel execution should be faster than sequential (3 * 0.1 = 0.3)
        assert duration < TIMEOUT_LONG  # Some overhead is expected

    @pytest.mark.asyncio
    async def test_execute_sequential_checkers(self) -> None:
        """Test executing checkers sequentially."""
        executor = HealthCheckExecutor(execute_parallel=False)
        checkers = [
            MockHealthChecker(name="checker1", is_healthy=True, delay=DELAY_SHORT),
            MockHealthChecker(name="checker2", is_healthy=True, delay=DELAY_SHORT),
            MockHealthChecker(name="checker3", is_healthy=True, delay=DELAY_SHORT),
        ]
        start_time = time.perf_counter()
        results = await executor.execute(checkers)
        duration = time.perf_counter() - start_time

        assert len(results) == NUM_CHECKERS_MEDIUM
        assert all(r.is_healthy for r in results)
        # Sequential execution should take at least 3 * DELAY_SHORT
        assert duration >= NUM_CHECKERS_MEDIUM * DELAY_SHORT

    @pytest.mark.asyncio
    async def test_execute_with_timeout_parallel(self) -> None:
        """Test timeout in parallel execution."""
        executor = HealthCheckExecutor(timeout_seconds=TIMEOUT_SHORT, execute_parallel=True)
        checkers = [
            MockHealthChecker(name="checker1", is_healthy=True, delay=DELAY_SHORT),
            MockHealthChecker(name="checker2", is_healthy=True, delay=DELAY_LONG),  # Will timeout
            MockHealthChecker(name="checker3", is_healthy=True, delay=DELAY_SHORT),
        ]
        results = await executor.execute(checkers)

        assert len(results) == NUM_CHECKERS_MEDIUM
        # All should be marked as timeout
        assert all(not r.is_healthy for r in results)
        assert all("timed out" in (r.error or "") for r in results)

    @pytest.mark.asyncio
    async def test_execute_with_timeout_sequential(self) -> None:
        """Test timeout in sequential execution."""
        executor = HealthCheckExecutor(timeout_seconds=TIMEOUT_SHORT, execute_parallel=False)
        checkers = [
            MockHealthChecker(name="checker1", is_healthy=True, delay=DELAY_SHORT),
            MockHealthChecker(name="checker2", is_healthy=True, delay=DELAY_LONG),  # Will timeout
        ]
        results = await executor.execute(checkers)

        assert len(results) == NUM_CHECKERS_SMALL
        assert results[0].is_healthy is True  # First completed
        assert results[1].is_healthy is False  # Second timed out
        assert "timed out" in (results[1].error or "")

    @pytest.mark.asyncio
    async def test_execute_with_exception(self) -> None:
        """Test handling exceptions from checkers."""
        executor = HealthCheckExecutor()
        checkers = [
            MockHealthChecker(name="checker1", is_healthy=True),
            MockHealthChecker(
                name="checker2",
                raise_exception=True,
                exception_type=ValueError,
                error="Test error",
            ),
        ]
        results = await executor.execute(checkers)

        assert len(results) == NUM_CHECKERS_SMALL
        assert results[0].is_healthy is True
        assert results[1].is_healthy is False
        assert "Test error" in (results[1].error or "")

    @pytest.mark.asyncio
    async def test_execute_with_exception_parallel(self) -> None:
        """Test handling exceptions in parallel execution."""
        executor = HealthCheckExecutor(execute_parallel=True)
        checkers = [
            MockHealthChecker(name="checker1", is_healthy=True),
            MockHealthChecker(
                name="checker2",
                raise_exception=True,
                exception_type=RuntimeError,
                error="Runtime error",
            ),
        ]
        results = await executor.execute(checkers)

        assert len(results) == NUM_CHECKERS_SMALL
        assert results[0].is_healthy is True
        assert results[1].is_healthy is False
        assert "Runtime error" in (results[1].error or "")

    @pytest.mark.asyncio
    async def test_duration_measurement(self) -> None:
        """Test duration measurement."""
        executor = HealthCheckExecutor()
        checker = MockHealthChecker(name="test", is_healthy=True, delay=DELAY_MEDIUM)
        results = await executor.execute([checker])

        assert len(results) == 1
        assert results[0].duration_ms is not None
        assert results[0].duration_ms >= MIN_DURATION_MS

    @pytest.mark.asyncio
    async def test_duration_preserved_if_set(self) -> None:
        """Test that duration is preserved if already set by checker."""
        executor = HealthCheckExecutor()
        checker = MockHealthChecker(name="test", is_healthy=True, duration_ms=DURATION_MS_PRESET)
        results = await executor.execute([checker])

        assert len(results) == 1
        assert results[0].duration_ms == DURATION_MS_PRESET

    @pytest.mark.asyncio
    async def test_get_checker_metadata_with_metadata_attr(self) -> None:
        """Test extracting metadata from checker with metadata attribute."""
        executor = HealthCheckExecutor()
        metadata = HealthCheckMetadata(name="custom", critical=False)

        # Create a checker-like object with metadata attribute
        class CheckerWithMetadata:
            def __init__(self) -> None:
                self.metadata = metadata

        checker = CheckerWithMetadata()
        extracted = executor._get_checker_metadata(checker, 0)  # noqa: SLF001
        assert extracted == metadata

    @pytest.mark.asyncio
    async def test_get_checker_metadata_with_name_attr(self) -> None:
        """Test extracting metadata from checker with name attribute."""
        executor = HealthCheckExecutor()
        checker = MockHealthChecker(name="test_checker")
        extracted = executor._get_checker_metadata(checker, 0)  # noqa: SLF001

        assert extracted.name == "test_checker"
        assert extracted.critical is True

    @pytest.mark.asyncio
    async def test_get_checker_metadata_fallback(self) -> None:
        """Test fallback metadata creation."""
        executor = HealthCheckExecutor()
        checker = object()  # No metadata or name attributes
        extracted = executor._get_checker_metadata(checker, 5)  # noqa: SLF001

        assert extracted.name == "checker_5"
        assert extracted.critical is True

    def test_aggregate_status_empty_results(self) -> None:
        """Test aggregating status from empty results."""
        status = HealthCheckExecutor.aggregate_status([])
        assert status == HealthStatus.HEALTHY

    def test_aggregate_status_all_healthy(self) -> None:
        """Test aggregating status when all checks are healthy."""
        results = [
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check1", critical=True),
                is_healthy=True,
            ),
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check2", critical=False),
                is_healthy=True,
            ),
        ]
        status = HealthCheckExecutor.aggregate_status(results)
        assert status == HealthStatus.HEALTHY

    def test_aggregate_status_critical_unhealthy(self) -> None:
        """Test aggregating status when critical check is unhealthy."""
        results = [
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check1", critical=True),
                is_healthy=False,
            ),
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check2", critical=False),
                is_healthy=True,
            ),
        ]
        status = HealthCheckExecutor.aggregate_status(results)
        assert status == HealthStatus.UNHEALTHY

    def test_aggregate_status_non_critical_unhealthy(self) -> None:
        """Test aggregating status when only non-critical checks are unhealthy."""
        results = [
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check1", critical=True),
                is_healthy=True,
            ),
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check2", critical=False),
                is_healthy=False,
            ),
        ]
        status = HealthCheckExecutor.aggregate_status(results)
        assert status == HealthStatus.DEGRADED

    def test_aggregate_status_multiple_critical_unhealthy(self) -> None:
        """Test aggregating status with multiple critical unhealthy checks."""
        results = [
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check1", critical=True),
                is_healthy=False,
            ),
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check2", critical=True),
                is_healthy=False,
            ),
        ]
        status = HealthCheckExecutor.aggregate_status(results)
        assert status == HealthStatus.UNHEALTHY

    def test_aggregate_status_mixed_critical_and_non_critical(self) -> None:
        """Test aggregating status with mixed critical and non-critical failures."""
        results = [
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check1", critical=True),
                is_healthy=True,
            ),
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check2", critical=False),
                is_healthy=False,
            ),
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check3", critical=False),
                is_healthy=False,
            ),
        ]
        status = HealthCheckExecutor.aggregate_status(results)
        assert status == HealthStatus.DEGRADED

    def test_aggregate_status_critical_takes_precedence(self) -> None:
        """Test that critical failures take precedence over non-critical."""
        results = [
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check1", critical=True),
                is_healthy=False,
            ),
            HealthCheckResult(
                metadata=HealthCheckMetadata(name="check2", critical=False),
                is_healthy=False,
            ),
        ]
        status = HealthCheckExecutor.aggregate_status(results)
        assert status == HealthStatus.UNHEALTHY

"""Health check executor with timeout and parallel execution support."""

import asyncio
import time
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from haolib.web.health.checkers.abstract import (
    AbstractHealthChecker,
    HealthCheckMetadata,
    HealthCheckResult,
    HealthStatus,
)

# Constants
TIMEOUT_ERROR_MESSAGE = "Health check timed out"


class HealthCheckExecutor:
    """Executes health checks with timeout and parallel execution support.

    Example:
        ```python
        executor = HealthCheckExecutor(timeout=timedelta(seconds=5))
        results = await executor.execute([db_checker, redis_checker])
        overall_status = executor.aggregate_status(results)
        ```

    """

    def __init__(
        self,
        timeout: timedelta | None = None,
        execute_parallel: bool = True,
    ) -> None:
        """Initialize the health check executor.

        Args:
            timeout: Maximum time to wait for all checks to complete.
                If None, no timeout is applied.
            execute_parallel: Whether to execute checks in parallel.
                If False, checks are executed sequentially.

        """
        self.timeout = timeout
        self.execute_parallel = execute_parallel

    async def execute(
        self,
        checkers: Sequence[AbstractHealthChecker],
    ) -> list[HealthCheckResult]:
        """Execute health checks.

        Args:
            checkers: Sequence of health checkers to execute.
                Must not be empty.

        Returns:
            List of health check results in the same order as checkers.
            Returns empty list if checkers is empty.

        Raises:
            ValueError: If checkers is None (not empty sequence).

        """
        if not checkers:
            return []

        if self.execute_parallel:
            return await self._execute_parallel(checkers)
        return await self._execute_sequential(checkers)

    async def _execute_parallel(
        self,
        checkers: Sequence[AbstractHealthChecker],
    ) -> list[HealthCheckResult]:
        """Execute checks in parallel."""
        tasks = [asyncio.create_task(self._execute_with_timing(checker)) for checker in checkers]

        if self.timeout is not None:
            try:
                results = await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True),
                    timeout=self.timeout.total_seconds(),
                )
            except TimeoutError:
                # Cancel remaining tasks and create timeout results
                for task in tasks:
                    if not task.done():
                        task.cancel()
                # Wait for cancellations to complete
                await asyncio.gather(*tasks, return_exceptions=True)
                results = [
                    HealthCheckResult(
                        metadata=self._get_checker_metadata(checker, i),
                        is_healthy=False,
                        error=TIMEOUT_ERROR_MESSAGE,
                    )
                    for i, checker in enumerate(checkers)
                ]
        else:
            results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to unhealthy results
        final_results: list[HealthCheckResult] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                checker = checkers[i]
                final_results.append(
                    HealthCheckResult(
                        metadata=self._get_checker_metadata(checker, i),
                        is_healthy=False,
                        error=str(result),
                    )
                )
            elif isinstance(result, HealthCheckResult):
                final_results.append(result)
            else:
                # Fallback for unexpected types
                checker = checkers[i]
                final_results.append(
                    HealthCheckResult(
                        metadata=self._get_checker_metadata(checker, i),
                        is_healthy=False,
                        error=f"Unexpected result type: {type(result)}",
                    )
                )

        return final_results

    async def _execute_sequential(
        self,
        checkers: Sequence[AbstractHealthChecker],
    ) -> list[HealthCheckResult]:
        """Execute checks sequentially."""
        results: list[HealthCheckResult] = []
        for i, checker in enumerate(checkers):
            try:
                if self.timeout is not None:
                    result = await asyncio.wait_for(
                        self._execute_with_timing(checker),
                        timeout=self.timeout.total_seconds(),
                    )
                    results.append(result)
                else:
                    result = await self._execute_with_timing(checker)
                    results.append(result)
            except TimeoutError:
                results.append(
                    HealthCheckResult(
                        metadata=self._get_checker_metadata(checker, i),
                        is_healthy=False,
                        error=TIMEOUT_ERROR_MESSAGE,
                    )
                )
            except Exception as e:
                results.append(
                    HealthCheckResult(
                        metadata=self._get_checker_metadata(checker, i),
                        is_healthy=False,
                        error=str(e),
                    )
                )
        return results

    async def _execute_with_timing(self, checker: AbstractHealthChecker) -> HealthCheckResult:
        """Execute a checker and measure its duration.

        Args:
            checker: The health checker to execute.

        Returns:
            HealthCheckResult: The result with measured duration.

        """
        start_time = time.perf_counter()
        result = await checker()
        duration_ms = (time.perf_counter() - start_time) * 1000

        # Update duration if not already set by the checker
        if result.duration_ms is None:
            result.duration_ms = duration_ms

        return result

    @staticmethod
    def _get_checker_metadata(checker: Any, index: int) -> HealthCheckMetadata:
        """Extract metadata from checker or create default.

        Args:
            checker: The health checker instance.
            index: The index of the checker in the sequence.

        Returns:
            HealthCheckMetadata: Metadata for the checker.

        """
        if hasattr(checker, "metadata") and isinstance(checker.metadata, HealthCheckMetadata):
            return checker.metadata

        if hasattr(checker, "name") and isinstance(checker.name, str):
            return HealthCheckMetadata(name=checker.name, critical=True)

        return HealthCheckMetadata(name=f"checker_{index}", critical=True)

    @staticmethod
    def aggregate_status(results: list[HealthCheckResult]) -> HealthStatus:
        """Aggregate health check results into overall status.

        Rules:
        - If any critical check is unhealthy, status is UNHEALTHY
        - If any non-critical check is unhealthy, status is DEGRADED
        - Otherwise, status is HEALTHY

        Args:
            results: List of health check results.

        Returns:
            Overall health status.

        """
        if not results:
            return HealthStatus.HEALTHY

        has_unhealthy_critical = any(not result.is_healthy and result.metadata.critical for result in results)
        if has_unhealthy_critical:
            return HealthStatus.UNHEALTHY

        has_unhealthy_non_critical = any(not result.is_healthy and not result.metadata.critical for result in results)
        if has_unhealthy_non_critical:
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

"""Abstract pipeline optimizer."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from haolib.pipelines.base import Operation, Pipeline


@dataclass
class PipelineAnalysis:
    """Analysis of pipeline for optimization."""

    can_execute_on_storage: bool
    """Whether the entire pipeline can be executed on storage side."""

    optimized_operation: Operation | None = None
    """Optimized operation for execution on storage (e.g., single SQL query)."""

    remaining_operations: Sequence[Operation] = ()
    """Operations that need to be executed in Python (if hybrid approach)."""

    execution_plan: str = "python"
    """Execution plan: 'storage', 'hybrid', or 'python'."""

    sql_operations: Sequence[Operation] = ()
    """Operations that can be executed in SQL (for building optimized query)."""


class PipelineOptimizer(Protocol):
    """Pipeline optimizer for specific storage type.

    Each storage implementation can provide its own optimizer that knows
    how to optimize pipelines for that specific storage backend.
    """

    def analyze(self, pipeline: Operation[Any, Any] | Pipeline[Any, Any, Any]) -> PipelineAnalysis:
        """Analyze pipeline and propose optimization.

        Args:
            pipeline: Operation or pipeline to analyze.

        Returns:
            Analysis with execution plan.

        """
        ...

    def optimize(
        self, pipeline: Operation[Any, Any] | Pipeline[Any, Any, Any]
    ) -> Operation[Any, Any] | Pipeline[Any, Any, Any]:
        """Optimize pipeline for execution on storage.

        Args:
            pipeline: Operation or pipeline to optimize.

        Returns:
            Optimized operation (may be a single SQL query, aggregation pipeline, etc.).

        """
        ...

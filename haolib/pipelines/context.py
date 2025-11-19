"""Pipeline context for global optimization.

Provides context information about the entire pipeline to targets,
enabling global optimization across multiple targets.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from haolib.pipelines.base import Operation, Pipeline


@dataclass(frozen=True)
class PipelineContext:
    """Context information for pipeline optimization.

    Provides complete information about the pipeline being executed,
    allowing targets to perform global optimizations.

    Example:
        ```python
        # SQL Storage can see:
        # - That data will return after operations in other targets
        # - That multiple operations can be batched
        # - That intermediate operations can be optimized away

        context = PipelineContext(
            full_pipeline=pipeline,
            current_position=0,
            target_operations={sql_storage: [reado, updateo], s3_storage: [createo]},
        )
        ```

    """

    full_pipeline: Pipeline[Any, Any, Any]
    """Complete pipeline being executed."""

    current_position: int
    """Position of current operation in flattened pipeline."""

    target_operations: dict[Any, list[Operation[Any, Any]]]
    """Operations grouped by target (for optimization)."""

    def get_operations_for_target(self, target: Any) -> list[Operation[Any, Any]]:
        """Get all operations bound to a specific target.

        Args:
            target: Target to get operations for.

        Returns:
            List of operations bound to the target.

        """
        return self.target_operations.get(target, [])

    def get_future_operations(self, target: Any) -> list[Operation[Any, Any]]:
        """Get future operations bound to a specific target.

        Useful for determining if data will return to the same target,
        enabling optimizations like batching or query merging.

        Args:
            target: Target to check future operations for.

        Returns:
            List of future operations bound to the target.

        """
        operations = self.get_operations_for_target(target)
        # Return operations after current position
        return [op for idx, op in enumerate(operations) if idx > self.current_position]

    def will_return_to_target(self, target: Any) -> bool:
        """Check if pipeline will return to a specific target after current operation.

        Useful for optimizations like:
        - Delaying execution until data returns
        - Merging operations
        - Batching operations

        Args:
            target: Target to check.

        Returns:
            True if pipeline will return to target, False otherwise.

        """
        return len(self.get_future_operations(target)) > 0

"""SQLAlchemy pipeline optimizer.

Analyzes pipelines and optimizes them to execute as SQL queries when possible.
"""

from typing import Any

from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.indexes.sqlalchemy import IndexHandler
from haolib.storages.operations.base import Operation, Pipeline, TargetBoundOperation, TargetSwitch
from haolib.storages.operations.concrete import (
    CreateOperation,
    DeleteOperation,
    FilterOperation,
    MapOperation,
    ReadOperation,
    ReduceOperation,
    TransformOperation,
    UpdateOperation,
)
from haolib.storages.operations.optimizer import PipelineAnalysis
from haolib.storages.operations.optimizer.predicate_analyzer import PredicateAnalyzer
from haolib.storages.operations.optimizer.query_builder import QueryBuilder


class SQLAlchemyPipelineOptimizer:
    """Optimizer for SQLAlchemy pipelines.

    Analyzes pipelines and optimizes them to execute as SQL queries when possible.
    Can convert operations like filter, map, reduce into SQL WHERE, SELECT, and
    aggregation clauses.

    Example:
        ```python
        optimizer = SQLAlchemyPipelineOptimizer(registry)
        analysis = optimizer.analyze(pipeline)

        if analysis.execution_plan == "storage":
            # Entire pipeline can be executed in SQL
            result = await storage.execute(analysis.optimized_operation)
        elif analysis.execution_plan == "hybrid":
            # Part in SQL, part in Python
            sql_result = await storage.execute(analysis.optimized_operation)
            # Then execute remaining operations in Python
        else:
            # All in Python
            result = await storage.execute(pipeline)
        ```

    """

    def __init__(self, registry: DataTypeRegistry) -> None:
        """Initialize the optimizer.

        Args:
            registry: Data type registry for converting user types to storage models.

        """
        self._registry = registry
        self._index_handler = IndexHandler(registry)
        self._predicate_analyzer = PredicateAnalyzer()
        self._query_builder = QueryBuilder(registry)

    def analyze(self, pipeline: Any) -> PipelineAnalysis:
        """Analyze pipeline and determine optimization strategy.

        Args:
            pipeline: Operation or pipeline to analyze.

        Returns:
            Analysis with execution plan and optimized operation if possible.

        """
        # Flatten pipeline into list of operations
        operations = self._flatten_pipeline(pipeline)

        # Determine what can be executed in SQL
        sql_operations = []
        python_operations = []

        for idx, op in enumerate(operations):
            if self._can_execute_in_sql(op):
                sql_operations.append(op)
            else:
                # If we encounter an operation that cannot be executed in SQL,
                # all subsequent operations are also in Python
                python_operations = list(operations[idx:])
                break

        # Build execution plan
        if not python_operations:
            # Entire pipeline can be executed in SQL
            optimized_op = self._build_optimized_operation(sql_operations)
            return PipelineAnalysis(
                can_execute_on_storage=True,
                optimized_operation=optimized_op,
                execution_plan="storage",
                sql_operations=tuple(sql_operations),
            )

        if sql_operations:
            # Hybrid approach: part in SQL, part in Python
            optimized_op = self._build_optimized_operation(sql_operations)
            return PipelineAnalysis(
                can_execute_on_storage=False,
                optimized_operation=optimized_op,
                remaining_operations=tuple(python_operations),
                execution_plan="hybrid",
                sql_operations=tuple(sql_operations),
            )

        # All operations need to be executed in Python
        return PipelineAnalysis(
            can_execute_on_storage=False,
            remaining_operations=tuple(operations),
            execution_plan="python",
        )

    def optimize(
        self, pipeline: Operation[Any, Any] | Pipeline[Any, Any, Any]
    ) -> Operation[Any, Any] | Pipeline[Any, Any, Any]:
        """Optimize pipeline to single SQL query when possible.

        Args:
            pipeline: Operation or pipeline to optimize.

        Returns:
            Optimized operation (may be a single SQL query) or original pipeline.

        """
        analysis = self.analyze(pipeline)
        if analysis.optimized_operation:
            return analysis.optimized_operation
        return pipeline

    def _flatten_pipeline(self, operation: Operation[Any, Any] | Pipeline[Any, Any, Any]) -> list[Operation[Any, Any]]:
        """Flatten pipeline into list of operations.

        Args:
            operation: Operation or pipeline to flatten.

        Returns:
            List of operations in execution order.

        """
        operations: list[Operation] = []
        # Handle TargetBoundOperation and TargetSwitch at the top level
        if isinstance(operation, TargetBoundOperation):
            # Unwrap TargetBoundOperation - flatten its operation
            operation = operation.operation
        elif isinstance(operation, TargetSwitch):
            msg = "TargetSwitch should not be in optimizer"
            raise TypeError(msg)
        current: Operation | Pipeline = operation

        while True:
            if isinstance(current, Pipeline):
                # Add first operation
                if isinstance(current.first, Pipeline):
                    # Recursively flatten nested pipeline
                    operations.extend(self._flatten_pipeline(current.first))
                else:
                    # current.first can be Operation, TargetBoundOperation, or TargetSwitch
                    # But we only accept Operation here
                    first_op = current.first
                    if isinstance(first_op, (TargetBoundOperation, TargetSwitch)):
                        # Skip TargetBoundOperation and TargetSwitch - they're handled by executor
                        msg = "TargetBoundOperation and TargetSwitch should not be in optimizer"
                        raise TypeError(msg)
                    operations.append(first_op)

                # Move to second operation
                # current.second can be Operation, Pipeline, TargetBoundOperation, or TargetSwitch
                # But we only accept Operation or Pipeline here
                second_op = current.second
                if isinstance(second_op, (TargetBoundOperation, TargetSwitch)):
                    # Skip TargetBoundOperation and TargetSwitch - they're handled by executor
                    msg = "TargetBoundOperation and TargetSwitch should not be in optimizer"
                    raise TypeError(msg)
                # Now second_op is Operation or Pipeline, safe to assign
                current = second_op
            else:
                # This is a simple operation
                operations.append(current)
                break

        return operations

    def _can_execute_in_sql(self, operation: Operation) -> bool:
        """Check if operation can be executed in SQL.

        Args:
            operation: Operation to check.

        Returns:
            True if operation can be executed in SQL, False otherwise.

        """
        # ReadOperation and CRUD operations can always be executed in SQL
        if isinstance(operation, (ReadOperation, CreateOperation, UpdateOperation, DeleteOperation)):
            return True

        # FilterOperation can be in SQL if predicate can be converted to specifications
        if isinstance(operation, FilterOperation):
            return self._predicate_analyzer.can_convert_to_sql(operation)

        # MapOperation, ReduceOperation, TransformOperation need detailed analysis
        # For now return False - detailed analysis will be in next stage
        if isinstance(operation, (MapOperation, ReduceOperation, TransformOperation)):
            return False

        # Unknown operation - execute in Python
        return False

    def _build_optimized_operation(self, operations: list[Operation]) -> Operation | None:
        """Build optimized operation from list of operations.

        Note: This method returns a placeholder. The actual query building
        happens in build_optimized_operation_async() which requires a session.

        Args:
            operations: List of operations that can be executed in SQL.

        Returns:
            Optimized operation (ReadOperation with optimized query) or None.

        """
        if not operations:
            return None

        # Find ReadOperation as base
        read_op: ReadOperation[Any] | None = None
        for op in operations:
            if isinstance(op, ReadOperation):
                read_op = op
                break

        if not read_op:
            # If no ReadOperation, cannot optimize
            return None

        # If there are only ReadOperation (no filters), return as is
        if len(operations) == 1:
            return read_op

        # For operations with filters, we need to build query in async method
        # Return a marker that indicates we need to build optimized query
        # We'll store operations for later use
        return read_op

    async def build_optimized_operation_async(self, operations: list[Operation], session: Any) -> Operation | None:
        """Build optimized operation from list of operations (async version).

        This method actually builds the optimized SQL query using QueryBuilder.

        Args:
            operations: List of operations that can be executed in SQL.
            session: SQLAlchemy session for building queries.

        Returns:
            Optimized operation (ReadOperation with optimized query) or None.

        """
        return await self._query_builder.build_async(operations, session)

"""SQLAlchemy pipeline optimizer.

Analyzes pipelines and optimizes them to execute as SQL queries when possible.
"""

from typing import Any

from haolib.pipelines.base import Operation, Pipeline, TargetBoundOperation, TargetSwitch
from haolib.pipelines.context import PipelineContext

# Import operations lazily to avoid circular import
# Operations are imported in methods that use them
from haolib.pipelines.optimizer import PipelineAnalysis
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.indexes.sqlalchemy import IndexHandler
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

    def analyze(self, pipeline: Any, pipeline_context: PipelineContext | None = None) -> PipelineAnalysis:
        """Analyze pipeline and determine optimization strategy.

        Args:
            pipeline: Operation or pipeline to analyze.
            pipeline_context: Optional context about the entire pipeline for global optimization.

        Returns:
            Analysis with execution plan and optimized operation if possible.

        """
        # Flatten pipeline into list of operations
        operations = self._flatten_pipeline(pipeline)

        # Use pipeline_context for global optimization if available
        if pipeline_context:
            # Check if data will return to this storage (enables additional optimizations)
            # This is a placeholder for future global optimizations
            pass

        # Determine what can be executed in SQL
        sql_operations = []
        python_operations = []

        for idx, op in enumerate(operations):
            # Check if operation can be executed in SQL
            # Operations that receive previous_result (idx > 0) may not be optimizable
            has_previous = idx > 0
            can_execute_in_sql = self._can_execute_in_sql(op, has_previous=has_previous)

            if can_execute_in_sql:
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

        # Check if we can optimize reado | [filtero]* | updateo/patcho/deleteo pattern
        # This is a special case where operations can be optimized even in pipeline mode
        if sql_operations and python_operations:
            first_python_op = python_operations[0]
            first_python_idx = len(sql_operations)

            # Check if pattern is reado | [filtero]* | updateo/patcho/deleteo
            from haolib.pipelines.operations import (  # noqa: PLC0415
                DeleteOperation,
                PatchOperation,
                ReadOperation,
                UpdateOperation,
            )

            if (
                first_python_idx > 0
                and isinstance(sql_operations[0], ReadOperation)
                and isinstance(first_python_op, (UpdateOperation, PatchOperation, DeleteOperation))
            ):
                # Check if we can optimize this pattern
                can_optimize = self._can_optimize_read_update_pattern(sql_operations, first_python_op)
                if can_optimize:
                    # Can optimize: treat as storage execution plan
                    # The actual optimization will happen in build_optimized_operation_async
                    return PipelineAnalysis(
                        can_execute_on_storage=True,
                        optimized_operation=None,  # Will be built in async method
                        execution_plan="storage",
                        sql_operations=tuple(operations),  # All operations for optimization
                    )

        # Check if first python operation uses previous_result (pipeline mode)
        # If so, entire pipeline must execute in Python (cannot split)
        if python_operations:
            first_python_op = python_operations[0]
            first_python_idx = len(sql_operations)
            uses_previous_result = (
                first_python_idx > 0
                and not self._can_execute_in_sql(first_python_op, has_previous=True)
                and self._operation_uses_previous_result(first_python_op)
            )

            if uses_previous_result:
                # Operation uses previous_result - entire pipeline must be Python
                return PipelineAnalysis(
                    can_execute_on_storage=False,
                    remaining_operations=tuple(operations),
                    execution_plan="python",
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
                    # Extract operation from TargetBoundOperation or TargetSwitch
                    first_op = current.first
                    if isinstance(first_op, TargetBoundOperation) or isinstance(first_op, TargetSwitch):
                        first_op = first_op.operation
                    operations.append(first_op)

                # Move to second operation
                # current.second can be Operation, Pipeline, TargetBoundOperation, or TargetSwitch
                # Extract operation from TargetBoundOperation or TargetSwitch
                second_op = current.second
                if isinstance(second_op, TargetBoundOperation) or isinstance(second_op, TargetSwitch):
                    second_op = second_op.operation
                # Now second_op is Operation or Pipeline, safe to assign
                current = second_op
            else:
                # This is a simple operation
                operations.append(current)
                break

        return operations

    def _can_execute_in_sql(self, operation: Operation, has_previous: bool = False) -> bool:
        """Check if operation can be executed in SQL.

        Args:
            operation: Operation to check.
            has_previous: Whether operation will receive previous_result from pipeline.

        Returns:
            True if operation can be executed in SQL, False otherwise.

        """
        # Import operations lazily to avoid circular import
        from haolib.pipelines.operations import (  # noqa: PLC0415
            CreateOperation,
            DeleteOperation,
            PatchOperation,
            ReadOperation,
            UpdateOperation,
        )

        # ReadOperation can always be executed in SQL (it never receives previous_result)
        if isinstance(operation, ReadOperation):
            return True

        # CRUD operations can be executed in SQL in two cases:
        # 1. Search mode: have search_index (and data/patch for update/patch)
        # 2. Pipeline mode: receive previous_result from reado in same storage (can be optimized)
        if isinstance(operation, UpdateOperation):
            # UpdateOperation can be in SQL if:
            # - Search mode: has search_index and data
            # - Pipeline mode: receives previous_result (will be optimized with reado)
            if has_previous:
                # Pipeline mode: can be optimized if data is not callable
                # (callable data requires Python execution)
                return not callable(operation.data) if operation.data is not None else False
            # Search mode: must have both search_index and data
            return operation.search_index is not None and operation.data is not None

        if isinstance(operation, PatchOperation):
            # PatchOperation can be in SQL if:
            # - Search mode: has search_index and patch
            # - Pipeline mode: receives previous_result (will be optimized with reado)
            if has_previous:
                # Pipeline mode: can be optimized if patch is not callable
                # (patcho doesn't support callable, but check anyway)
                return not callable(operation.patch) if operation.patch is not None else False
            # Search mode: must have both search_index and patch
            return operation.search_index is not None and operation.patch is not None

        if isinstance(operation, DeleteOperation):
            # DeleteOperation can be in SQL if:
            # - Search mode: has search_index
            # - Pipeline mode: receives previous_result (will be optimized with reado)
            # Both modes can be optimized
            return True

        if isinstance(operation, CreateOperation):
            # CreateOperation can be in SQL if it has data (not using previous_result)
            # If it receives previous_result, it cannot be optimized
            if has_previous:
                # Pipeline mode: cannot be optimized (uses previous_result)
                return False
            # Search mode: can be optimized if it has data
            return bool(operation.data)

        # FilterOperation can be in SQL if predicate can be converted to specifications
        # But only if it's part of a SQL-optimizable pipeline (after ReadOperation)
        from haolib.pipelines.operations import FilterOperation  # noqa: PLC0415

        if isinstance(operation, FilterOperation):
            # FilterOperation always needs previous_result, so it can only be optimized
            # if it's part of a SQL pipeline (after ReadOperation that was optimized)
            return self._predicate_analyzer.can_convert_to_sql(operation)

        # MapOperation, ReduceOperation, TransformOperation need detailed analysis
        # For now return False - detailed analysis will be in next stage
        from haolib.pipelines.operations import (  # noqa: PLC0415
            MapOperation,
            ReduceOperation,
            TransformOperation,
        )

        if isinstance(operation, (MapOperation, ReduceOperation, TransformOperation)):
            return False

        # Unknown operation - execute in Python
        return False

    def _operation_uses_previous_result(self, operation: Operation) -> bool:
        """Check if operation uses previous_result (pipeline mode).

        Args:
            operation: Operation to check.

        Returns:
            True if operation uses previous_result instead of search_index/data.

        """
        from haolib.pipelines.operations import (  # noqa: PLC0415
            DeleteOperation,
            PatchOperation,
            UpdateOperation,
        )

        if isinstance(operation, UpdateOperation):
            # Uses previous_result if no search_index or data
            return operation.search_index is None or operation.data is None

        if isinstance(operation, PatchOperation):
            # Uses previous_result if no search_index or patch
            return operation.search_index is None or operation.patch is None

        if isinstance(operation, DeleteOperation):
            # Uses previous_result if no search_index
            return operation.search_index is None

        # Other operations don't use previous_result in this way
        return False

    def _can_optimize_read_update_pattern(self, sql_operations: list[Operation], update_op: Operation) -> bool:
        """Check if reado | [filtero]* | updateo/patcho/deleteo pattern can be optimized.

        Args:
            sql_operations: List of SQL operations (should start with ReadOperation).
            update_op: Update/Patch/Delete operation to check.

        Returns:
            True if pattern can be optimized, False otherwise.

        """
        from haolib.pipelines.operations import (  # noqa: PLC0415
            DeleteOperation,
            FilterOperation,
            PatchOperation,
            ReadOperation,
            UpdateOperation,
        )

        # First operation must be ReadOperation
        if not sql_operations or not isinstance(sql_operations[0], ReadOperation):
            return False

        # Check if all operations between read and update are FilterOperations
        for op in sql_operations[1:]:
            if not isinstance(op, FilterOperation):
                return False

        # Check type compatibility
        # For UpdateOperation, PatchOperation, DeleteOperation, we can't directly
        # check data_type since they don't have it. Instead, we rely on the fact
        # that if they're in the same pipeline, they should work with the same type.
        # The actual type check happens in QueryBuilder when building the query.
        if isinstance(update_op, UpdateOperation):
            # Check if data is callable (cannot optimize callable)
            return not callable(update_op.data) if update_op.data is not None else False

        if isinstance(update_op, PatchOperation):
            # Check if patch is callable (patcho doesn't support callable)
            return not callable(update_op.patch) if update_op.patch is not None else False

        if isinstance(update_op, DeleteOperation):
            # Type compatibility will be checked in QueryBuilder
            return True

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
        from haolib.pipelines.operations import ReadOperation  # noqa: PLC0415

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
        Supports patterns:
        - reado | [filtero]* -> optimized ReadOperation
        - reado | [filtero]* | updateo -> optimized UpdateOperation
        - reado | [filtero]* | patcho -> optimized PatchOperation
        - reado | [filtero]* | deleteo -> optimized DeleteOperation

        Args:
            operations: List of operations that can be executed in SQL.
            session: SQLAlchemy session for building queries.

        Returns:
            Optimized operation or None.

        """
        from haolib.pipelines.operations import (  # noqa: PLC0415
            DeleteOperation,
            FilterOperation,
            PatchOperation,
            ReadOperation,
            UpdateOperation,
        )

        if not operations:
            return None

        # First operation must be ReadOperation
        read_op = operations[0]
        if not isinstance(read_op, ReadOperation):
            return None

        # Separate filter operations from update/patch/delete operations
        filter_ops: list[FilterOperation] = []
        update_op: UpdateOperation[Any] | None = None
        patch_op: PatchOperation[Any] | None = None
        delete_op: DeleteOperation[Any] | None = None

        for op in operations[1:]:
            if isinstance(op, FilterOperation):
                filter_ops.append(op)
            elif isinstance(op, UpdateOperation):
                update_op = op
            elif isinstance(op, PatchOperation):
                patch_op = op
            elif isinstance(op, DeleteOperation):
                delete_op = op

        # Build optimized query based on pattern
        if update_op is not None:
            return await self._query_builder.build_update_async(read_op, filter_ops, update_op, session)

        if patch_op is not None:
            return await self._query_builder.build_patch_async(read_op, filter_ops, patch_op, session)

        if delete_op is not None:
            return await self._query_builder.build_delete_async(read_op, filter_ops, delete_op, session)

        # Default: build optimized ReadOperation (reado | [filtero]*)
        return await self._query_builder.build_async(operations, session)

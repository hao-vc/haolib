"""Executor for executable pipelines across multiple storages."""

from collections.abc import AsyncIterator
from typing import Any

from haolib.storages.operations.base import (
    Operation,
    Pipeline,
    TargetBoundOperation,
    TargetSwitch,
)
from haolib.storages.operations.concrete import (
    CreateOperation,
    FilterOperation,
    MapOperation,
    ReduceOperation,
    TransformOperation,
)
from haolib.storages.operations.concrete import (
    CreateOperation as CreateOp,
)


class ExecutablePipelineExecutor:
    """Executor for pipelines that can span multiple targets.

    Coordinates execution of operations across different targets (storages, ML models, APIs, etc.),
    handling target switches and data transfer automatically.

    Example:
        ```python
        from haolib.storages.dsl import reado, reduceo, transformo, createo
        from haolib.storages.indexes.params import ParamIndex

        # New syntax: operation ^ storage for binding, | for composition
        # Both operators have same precedence (10), so they execute left-to-right
        pipeline = (
            reado(search_index=ParamIndex(User)) ^ sql_storage
            | reduceo(lambda acc, u: acc + u.age, 0) ^ sql_storage
            | transformo(lambda total: str(total).encode())
            | createo([lambda data: data]) ^ s3_storage
        )

        executor = ExecutablePipelineExecutor()
        result = await executor.execute(pipeline)
        ```

    """

    async def execute[T_Result](self, pipeline: Pipeline[Any, Any, T_Result]) -> T_Result:
        """Execute pipeline across multiple storages.

        Args:
            pipeline: Pipeline to execute.

        Returns:
            Final pipeline result.

        Raises:
            ValueError: If no target is specified for an operation that requires it.
            RuntimeError: If pipeline execution fails.

        """
        return await self._execute_pipeline(pipeline, previous_result=None)

    async def _execute_pipeline(
        self,
        pipeline: Pipeline[Any, Any, Any],
        previous_result: Any,
    ) -> Any:
        """Execute pipeline recursively, maintaining left-to-right execution order.

        Due to Python's left-associativity of | operator, a | b | c is parsed as (a | b) | c.
        This creates a nested structure where the rightmost operation is in 'second',
        and all previous operations are nested in 'first'. We need to flatten this
        to execute operations left-to-right.

        Args:
            pipeline: Pipeline to execute.
            previous_result: Result from previous operation (for nested pipelines).

        Returns:
            Pipeline result.

        """
        # Flatten nested pipelines to execute left-to-right
        # Due to left-associativity, structure is: ((a | b) | c) | d
        # We need to extract: a, b, c, d in order
        operations = []

        def collect_operations(p: Pipeline[Any, Any, Any] | Operation[Any, Any] | TargetBoundOperation[Any]) -> None:
            """Recursively collect operations from nested Pipeline structure in left-to-right order."""
            if isinstance(p, Pipeline):
                # Recursively collect from first (which might be nested)
                if isinstance(p.first, Pipeline):
                    collect_operations(p.first)
                else:
                    # First is an operation, add it
                    operations.append(p.first)

                # Second is always an operation (or nested Pipeline - handle it)
                if isinstance(p.second, Pipeline):
                    collect_operations(p.second)
                else:
                    operations.append(p.second)
            else:
                # p is already an operation, add it
                operations.append(p)

        collect_operations(pipeline)

        # Group consecutive operations bound to the same storage for transaction grouping
        grouped_operations = self._group_operations_by_storage(operations)

        # Execute grouped operations sequentially from left to right
        result = previous_result
        for group in grouped_operations:
            if isinstance(group, list):
                # Group of operations bound to the same storage - execute as pipeline in single transaction
                # Even single operation in group should go through storage.execute() to ensure transaction consistency
                result = await self._execute_storage_group(group, result)
            else:
                # Single operation (not grouped) - execute normally
                result = await self._execute_operation(group, previous_result=result)

        return result

    async def _execute_operation(
        self,
        operation: Operation[Any, Any] | Pipeline[Any, Any, Any] | TargetBoundOperation[Any] | TargetSwitch[Any, Any],
        previous_result: Any,
    ) -> Any:
        """Execute single operation or handle target switch.

        Args:
            operation: Operation to execute.
            previous_result: Result from previous operation.

        Returns:
            Operation result.

        """
        # Handle nested pipelines
        if isinstance(operation, Pipeline):
            return await self._execute_pipeline(operation, previous_result)

        # Handle target-bound operations
        if isinstance(operation, TargetBoundOperation):
            # Handle CreateOperation - merge previous_result with data
            if isinstance(operation.operation, CreateOperation):
                # Handle S3 create result (tuples) - extract data only
                if previous_result is not None:
                    if isinstance(previous_result, list) and previous_result and isinstance(previous_result[0], tuple):
                        # previous_result is list of tuples from S3 create - extract data
                        previous_result_data = [item for item, _ in previous_result]
                    else:
                        previous_result_data = (
                            previous_result if isinstance(previous_result, list) else [previous_result]
                        )
                else:
                    previous_result_data = []

                # Process callables in operation.data
                processed_operation_data = []
                for item in operation.operation.data:
                    if callable(item):
                        # Call the function with previous_result (if available)
                        if previous_result is not None:
                            processed_operation_data.append(item(previous_result))
                        else:
                            # Callable without previous_result - just add it
                            processed_operation_data.append(item)
                    else:
                        processed_operation_data.append(item)

                # Merge: previous_result (if any) + operation.data
                # If operation.data is empty and previous_result exists, use previous_result
                # If operation.data is not empty and previous_result exists, prepend previous_result
                # Note: Empty list is still a valid result (it means "create nothing")
                if len(processed_operation_data) == 0 and len(previous_result_data) > 0:
                    # Empty data, use previous_result
                    processed_data = previous_result_data
                elif len(processed_operation_data) > 0 and len(previous_result_data) > 0:
                    # Both exist, prepend previous_result to data
                    processed_data = previous_result_data + processed_operation_data
                elif len(processed_operation_data) > 0:
                    # Only data exists
                    processed_data = processed_operation_data
                elif len(previous_result_data) > 0:
                    # Only previous_result exists
                    processed_data = previous_result_data
                elif len(processed_operation_data) == 0 and len(previous_result_data) == 0:
                    # Both are empty - this is OK, it means "create nothing" (no-op)
                    processed_data = []
                else:
                    # This should never happen, but just in case
                    msg = "CreateOperation requires either data or previous_result"
                    raise ValueError(msg)

                wrapped_op = CreateOp(data=processed_data)
                return await operation.target.execute(wrapped_op)

            # If operation needs previous_result, execute in Python
            # Operations like Filter, Map, Reduce, Transform always need previous_result
            # operation.operation is Operation or Pipeline, not TargetSwitch
            if isinstance(operation.operation, Operation) and self._operation_needs_previous_result(
                operation.operation
            ):
                if previous_result is None:
                    msg = f"{type(operation.operation).__name__} requires previous result"
                    raise ValueError(msg)
                return await self._execute_python_operation(operation.operation, previous_result)
            # Execute operation in the bound target
            return await operation.target.execute(operation.operation)

        # Handle target switches
        if isinstance(operation, TargetSwitch):
            # Execute source operation in source target
            source_result = await operation.source_target.execute(operation.source_result)

            # Handle CreateOperation - merge source_result with data
            if isinstance(operation.next_operation, CreateOperation):
                # Handle S3 create result (tuples) - extract data only
                if source_result is not None:
                    if isinstance(source_result, list) and source_result and isinstance(source_result[0], tuple):
                        # source_result is list of tuples from S3 create - extract data
                        source_result_data = [item for item, _ in source_result]
                    else:
                        source_result_data = source_result if isinstance(source_result, list) else [source_result]
                else:
                    source_result_data = []

                # Process callables in operation.next_operation.data
                processed_operation_data = []
                for item in operation.next_operation.data:
                    if callable(item):
                        # Call the function with source_result (if available)
                        if source_result is not None:
                            processed_operation_data.append(item(source_result))
                        else:
                            # Callable without source_result - just add it
                            processed_operation_data.append(item)
                    else:
                        processed_operation_data.append(item)

                # Merge: source_result (if any) + operation.data
                if not processed_operation_data and source_result_data:
                    # Empty data, use source_result
                    processed_data = source_result_data
                elif processed_operation_data and source_result_data:
                    # Both exist, prepend source_result to data
                    processed_data = source_result_data + processed_operation_data
                elif processed_operation_data:
                    # Only data exists
                    processed_data = processed_operation_data
                elif source_result_data:
                    # Only source_result exists
                    processed_data = source_result_data
                else:
                    # Neither exists - error
                    msg = "CreateOperation requires either data or previous_result"
                    raise ValueError(msg)

                wrapped_op = CreateOp(data=processed_data)
                return await operation.target_target.execute(wrapped_op)

            # For operations that need previous_result, execute in Python
            # next_operation can be Operation or Pipeline, but we only check Operation types
            if isinstance(operation.next_operation, Operation) and isinstance(
                operation.next_operation, (FilterOperation, MapOperation, ReduceOperation, TransformOperation)
            ):
                return await self._execute_python_operation(operation.next_operation, source_result)

            # For other operations, try to execute in target target
            return await operation.target_target.execute(operation.next_operation)

        # Handle regular operations - execute in Python
        # Regular operations (not bound to target) that need previous_result must have it
        # operation can be Operation, Pipeline, or TargetSwitch, but we only check Operation
        if isinstance(operation, Operation) and self._operation_needs_previous_result(operation):
            if previous_result is None:
                msg = f"Operation {type(operation).__name__} requires previous result but none is provided"
                raise ValueError(msg)
            return await self._execute_python_operation(operation, previous_result)

        # Handle CreateOperation without target - pass through data
        # CreateOperation can work without target if it has previous_result or data
        # It will just pass data through the pipeline without saving
        if isinstance(operation, CreateOperation):
            # If CreateOperation has previous_result or data, it can work without target
            # It will just pass data through the pipeline
            if previous_result is not None or len(operation.data) > 0:
                # Merge previous_result with data (same logic as in TargetBoundOperation)
                if previous_result is not None:
                    if isinstance(previous_result, list) and previous_result and isinstance(previous_result[0], tuple):
                        # previous_result is list of tuples from S3 create - extract data
                        previous_result_data = [item for item, _ in previous_result]
                    else:
                        previous_result_data = (
                            previous_result if isinstance(previous_result, list) else [previous_result]
                        )
                else:
                    previous_result_data = []

                # Process callables in operation.data
                processed_operation_data = []
                for item in operation.data:
                    if callable(item):
                        # Call the function with previous_result (if available)
                        if previous_result is not None:
                            processed_operation_data.append(item(previous_result))
                        else:
                            # Callable without previous_result - just add it
                            processed_operation_data.append(item)
                    else:
                        processed_operation_data.append(item)

                # Merge data
                if not processed_operation_data and previous_result_data:
                    # Empty data, use previous_result
                    return previous_result_data
                if processed_operation_data and previous_result_data:
                    # Both exist, prepend previous_result to data
                    return previous_result_data + processed_operation_data
                if processed_operation_data:
                    # Only data exists
                    return processed_operation_data
                if previous_result_data:
                    # Only previous_result exists
                    return previous_result_data
                # Neither exists - error
                msg = "CreateOperation requires either data or previous_result"
                raise ValueError(msg)
            # If no data and no previous_result, fall through to error below

        # For operations that don't need previous_result, we can't execute them without storage
        msg = f"Operation {type(operation).__name__} requires storage context but is not bound to any storage"
        raise ValueError(msg)

    def _group_operations_by_storage(
        self,
        operations: list[Operation[Any, Any] | TargetBoundOperation[Any] | TargetSwitch[Any, Any]],
    ) -> list[
        Operation[Any, Any] | TargetBoundOperation[Any] | TargetSwitch[Any, Any] | list[TargetBoundOperation[Any]]
    ]:
        """Group consecutive operations bound to the same storage.

        Operations bound to the same storage are grouped together so they can be
        executed in a single transaction. Operations that need previous_result
        (filter, map, reduce, transform) are not grouped as they execute in Python.

        Args:
            operations: List of operations to group.

        Returns:
            List of operations or groups of operations.

        """
        grouped: list[
            Operation[Any, Any] | TargetBoundOperation[Any] | TargetSwitch[Any, Any] | list[TargetBoundOperation[Any]]
        ] = []
        current_group: list[TargetBoundOperation[Any]] | None = None
        current_storage: Any = None

        for op in operations:
            # Check if operation is bound to a storage
            if isinstance(op, TargetBoundOperation):
                # Check if operation needs previous_result (filter, map, reduce, transform)
                # These execute in Python and cannot be grouped with storage operations
                if isinstance(op.operation, Operation) and self._operation_needs_previous_result(op.operation):
                    # End current group if exists
                    if current_group is not None:
                        grouped.append(current_group)
                        current_group = None
                        current_storage = None
                    # Add operation as-is (executes in Python)
                    grouped.append(op)
                    continue

                # Check if operation is bound to the same storage as current group
                if current_group is not None and op.target == current_storage:
                    # Add to current group
                    current_group.append(op)
                else:
                    # End current group if exists
                    if current_group is not None:
                        grouped.append(current_group)
                    # Start new group
                    current_group = [op]
                    current_storage = op.target
            elif isinstance(op, TargetSwitch):
                # TargetSwitch breaks grouping - end current group
                if current_group is not None:
                    grouped.append(current_group)
                    current_group = None
                    current_storage = None
                # Add TargetSwitch as-is
                grouped.append(op)
            else:
                # Regular operation (not bound to storage) - end current group
                if current_group is not None:
                    grouped.append(current_group)
                    current_group = None
                    current_storage = None
                # Add operation as-is
                grouped.append(op)

        # End final group if exists
        if current_group is not None:
            grouped.append(current_group)

        return grouped

    async def _execute_storage_group(
        self,
        group: list[TargetBoundOperation[Any]],
        previous_result: Any,
    ) -> Any:
        """Execute a group of operations bound to the same storage in a single transaction.

        This ensures all operations execute in a single transaction by creating
        one transaction and reusing it for all operations in the group.

        Args:
            group: List of TargetBoundOperation bound to the same storage.
            previous_result: Result from previous operation.

        Returns:
            Result of executing the group.

        """
        if not group:
            return previous_result

        # Get storage from first operation (all should have the same storage)
        storage = group[0].target

        # Check if storage supports execute_with_transaction (SQLAlchemyStorage)
        # For other storages (S3, etc.), use regular execute() which handles transactions internally
        from haolib.storages.sqlalchemy import SQLAlchemyStorage  # noqa: PLC0415

        if isinstance(storage, SQLAlchemyStorage):
            # Use single transaction for all operations in group
            txn = storage._begin_transaction()
            async with txn:
                result = previous_result
                for bound_op in group:
                    # Handle CreateOperation with previous_result
                    operation: Operation[Any, Any] | Pipeline[Any, Any, Any]
                    if isinstance(bound_op.operation, CreateOperation):
                        # Merge previous_result with data (same logic as in _execute_operation)
                        if result is not None:
                            if isinstance(result, list) and result and isinstance(result[0], tuple):
                                previous_result_data = [item for item, _ in result]
                            else:
                                previous_result_data = result if isinstance(result, list) else [result]
                        else:
                            previous_result_data = []

                        processed_operation_data = []
                        for item in bound_op.operation.data:
                            if callable(item):
                                if result is not None:
                                    processed_operation_data.append(item(result))
                                else:
                                    processed_operation_data.append(item)
                            else:
                                processed_operation_data.append(item)

                        if len(processed_operation_data) == 0 and len(previous_result_data) > 0:
                            processed_data = previous_result_data
                        elif len(processed_operation_data) > 0 and len(previous_result_data) > 0:
                            processed_data = previous_result_data + processed_operation_data
                        elif len(processed_operation_data) > 0:
                            processed_data = processed_operation_data
                        elif len(previous_result_data) > 0:
                            processed_data = previous_result_data
                        else:
                            processed_data = []

                        operation = CreateOp(data=processed_data)
                    else:
                        operation = bound_op.operation

                    # Execute operation with shared transaction
                    result = await storage.execute_with_transaction(operation, txn)

                return result
        else:
            # For non-transactional storages (S3, etc.), build pipeline and execute normally
            # This maintains backward compatibility
            first_op = group[0]
            if isinstance(first_op.operation, Pipeline):
                msg = "Pipeline inside TargetBoundOperation should not be grouped"
                raise TypeError(msg)
            first_operation: Operation[Any, Any] = first_op.operation

            # Handle CreateOperation with previous_result
            if isinstance(first_operation, CreateOperation):
                # Merge previous_result with data (same logic as in _execute_operation)
                if previous_result is not None:
                    if isinstance(previous_result, list) and previous_result and isinstance(previous_result[0], tuple):
                        previous_result_data = [item for item, _ in previous_result]
                    else:
                        previous_result_data = (
                            previous_result if isinstance(previous_result, list) else [previous_result]
                        )
                else:
                    previous_result_data = []

                processed_operation_data = []
                for item in first_operation.data:
                    if callable(item):
                        if previous_result is not None:
                            processed_operation_data.append(item(previous_result))
                        else:
                            processed_operation_data.append(item)
                    else:
                        processed_operation_data.append(item)

                if len(processed_operation_data) == 0 and len(previous_result_data) > 0:
                    processed_data = previous_result_data
                elif len(processed_operation_data) > 0 and len(previous_result_data) > 0:
                    processed_data = previous_result_data + processed_operation_data
                elif len(processed_operation_data) > 0:
                    processed_data = processed_operation_data
                elif len(previous_result_data) > 0:
                    processed_data = previous_result_data
                else:
                    processed_data = []

                first_operation = CreateOp(data=processed_data)

            # Build pipeline from all operations
            if len(group) == 1:
                # Single operation - execute directly
                return await storage.execute(first_operation)

            # Multiple operations - build pipeline
            current_pipeline: Pipeline[Any, Any, Any] = Pipeline(
                first=first_operation,
                second=group[1].operation,
            )

            # Extend pipeline with remaining operations
            for op in group[2:]:
                current_pipeline = Pipeline(
                    first=current_pipeline,
                    second=op.operation,
                )

            # Execute pipeline through storage (all in single transaction)
            return await storage.execute(current_pipeline)

    def _operation_needs_previous_result(self, operation: Operation[Any, Any]) -> bool:
        """Check if operation needs previous result.

        Args:
            operation: Operation to check.

        Returns:
            True if operation needs previous result.

        """
        return isinstance(operation, (FilterOperation, MapOperation, ReduceOperation, TransformOperation))

    async def _execute_python_operation(
        self,
        operation: Operation[Any, Any],
        previous_result: Any,
    ) -> Any:
        """Execute operation in Python context.

        Args:
            operation: Operation to execute.
            previous_result: Result from previous operation.

        Returns:
            Operation result.

        """

        if isinstance(operation, FilterOperation):
            if previous_result is None:
                msg = "FilterOperation requires previous result"
                raise ValueError(msg)
            # Collect async iterator if needed
            if isinstance(previous_result, AsyncIterator):
                items = [item async for item in previous_result]
            else:
                items = list(previous_result)
            return [item for item in items if operation.predicate(item)]

        if isinstance(operation, MapOperation):
            if previous_result is None:
                msg = "MapOperation requires previous result"
                raise ValueError(msg)
            # Collect async iterator if needed
            if isinstance(previous_result, AsyncIterator):
                items = [item async for item in previous_result]
            else:
                items = list(previous_result)
            return [operation.mapper(item, idx) for idx, item in enumerate(items)]

        if isinstance(operation, ReduceOperation):
            if previous_result is None:
                msg = "ReduceOperation requires previous result"
                raise ValueError(msg)
            # Collect async iterator if needed
            if isinstance(previous_result, AsyncIterator):
                items = [item async for item in previous_result]
            else:
                items = list(previous_result)
            result = operation.initial
            for item in items:
                result = operation.reducer(result, item)
            return result

        if isinstance(operation, TransformOperation):
            if previous_result is None:
                msg = "TransformOperation requires previous result"
                raise ValueError(msg)
            # Collect async iterator if needed
            if isinstance(previous_result, AsyncIterator):
                items = [item async for item in previous_result]
            else:
                items = previous_result
            return operation.transformer(items)

        # For other operations, we might need storage context
        # But since they're not bound to storage, we can't execute them
        msg = f"Operation {type(operation)} requires storage context but is not bound to any storage"
        raise ValueError(msg)

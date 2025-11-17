"""Executor for executable pipelines across multiple storages."""

from collections.abc import AsyncIterator
from typing import Any

from haolib.pipelines.base import (
    Operation,
    Pipeline,
    TargetBoundOperation,
    TargetSwitch,
)
from haolib.pipelines.context import PipelineContext
from haolib.pipelines.operations import (
    CreateOperation,
    FilterOperation,
    MapOperation,
    ReduceOperation,
    TransformOperation,
)
from haolib.pipelines.operations import (
    CreateOperation as CreateOp,
)


class ExecutablePipelineExecutor:
    """Coordinates execution of operations across different targets (storages, ML models, APIs, etc.),
    handling target switches and data transfer automatically.

    Example:
        ```python
        from haolib.pipelines import reduceo, transformo
        from haolib.storages.indexes.params import ParamIndex

        # Fluent API: storage methods return composites
        pipeline = (
            sql_storage.read(ParamIndex(User)).returning()
            | reduceo(lambda acc, u: acc + u.age, 0)
            | transformo(lambda total: str(total).encode())
            | s3_storage.create()
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
        # Build pipeline context for global optimization
        pipeline_context = self._build_pipeline_context(pipeline)
        return await self._execute_pipeline(pipeline, previous_result=None, pipeline_context=pipeline_context)

    def _build_pipeline_context(self, pipeline: Pipeline[Any, Any, Any]) -> PipelineContext:
        """Build pipeline context from pipeline structure.

        Args:
            pipeline: Pipeline to build context for.

        Returns:
            PipelineContext with full pipeline information.

        """
        # Flatten pipeline to get all operations
        operations: list[Operation[Any, Any] | TargetBoundOperation[Any] | TargetSwitch[Any, Any]] = []

        def collect_operations(p: Pipeline[Any, Any, Any] | Operation[Any, Any] | TargetBoundOperation[Any]) -> None:
            """Recursively collect operations from nested Pipeline structure."""
            if isinstance(p, Pipeline):
                if isinstance(p.first, Pipeline):
                    collect_operations(p.first)
                else:
                    operations.append(p.first)
                if isinstance(p.second, Pipeline):
                    collect_operations(p.second)
                else:
                    operations.append(p.second)
            else:
                operations.append(p)

        collect_operations(pipeline)

        # Group operations by target
        target_operations: dict[Any, list[Operation[Any, Any]]] = {}
        for op in operations:
            if isinstance(op, TargetBoundOperation):
                target = op.target
                if target not in target_operations:
                    target_operations[target] = []
                # Extract operation from TargetBoundOperation
                if isinstance(op.operation, Operation):
                    target_operations[target].append(op.operation)
                elif isinstance(op.operation, Pipeline):
                    # For nested pipelines, we need to extract operations
                    # For now, we'll skip nested pipelines in context
                    pass

        return PipelineContext(
            full_pipeline=pipeline,
            current_position=0,  # Will be updated during execution
            target_operations=target_operations,
        )

    async def _execute_pipeline(
        self,
        pipeline: Pipeline[Any, Any, Any],
        previous_result: Any,
        pipeline_context: PipelineContext | None = None,
    ) -> Any:
        """Execute pipeline recursively, maintaining left-to-right execution order.

        Due to Python's left-associativity of | operator, a | b | c is parsed as (a | b) | c.
        This creates a nested structure where the rightmost operation is in 'second',
        and all previous operations are nested in 'first'. We need to flatten this
        to execute operations left-to-right.

        Args:
            pipeline: Pipeline to execute.
            previous_result: Result from previous operation (for nested pipelines).
            pipeline_context: Context about the entire pipeline for global optimization.

        Returns:
            Pipeline result.

        """
        # Flatten nested pipelines to execute left-to-right
        # Due to left-associativity, structure is: ((a | b) | c) | d
        # We need to extract: a, b, c, d in order
        operations = []

        def collect_operations(
            p: Pipeline[Any, Any, Any] | Operation[Any, Any] | TargetBoundOperation[Any] | Any,
        ) -> None:
            """Recursively collect operations from nested Pipeline structure in left-to-right order."""
            # Check if p is a composite (from fluent API)
            from haolib.storages.fluent.composites import BaseComposite  # noqa: PLC0415

            if isinstance(p, BaseComposite):
                # Extract operations from composite
                ops = p._operations
                for op in ops:
                    # Bind operations to storage
                    bound_op = TargetBoundOperation(operation=op, target=p._storage)
                    operations.append(bound_op)
                return

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
        current_position = 0
        for group in grouped_operations:
            if isinstance(group, list):
                # Group of operations bound to the same storage - execute as pipeline in single transaction
                # Even single operation in group should go through storage.execute() to ensure transaction consistency
                # Update context position for this group
                if pipeline_context:
                    updated_context = PipelineContext(
                        full_pipeline=pipeline_context.full_pipeline,
                        current_position=current_position,
                        target_operations=pipeline_context.target_operations,
                    )
                else:
                    updated_context = None
                result = await self._execute_storage_group(group, result, updated_context)
                current_position += len(group)
            else:
                # Single operation (not grouped) - execute normally
                if pipeline_context:
                    updated_context = PipelineContext(
                        full_pipeline=pipeline_context.full_pipeline,
                        current_position=current_position,
                        target_operations=pipeline_context.target_operations,
                    )
                else:
                    updated_context = None
                result = await self._execute_operation(group, previous_result=result, pipeline_context=updated_context)
                current_position += 1

        return result

    async def _execute_operation(
        self,
        operation: Operation[Any, Any]
        | Pipeline[Any, Any, Any]
        | TargetBoundOperation[Any]
        | TargetSwitch[Any, Any]
        | Any,
        previous_result: Any,
        pipeline_context: PipelineContext | None = None,
    ) -> Any:
        """Execute single operation or handle target switch.

        Args:
            operation: Operation to execute.
            previous_result: Result from previous operation.
            pipeline_context: Context about the entire pipeline for global optimization.

        Returns:
            Operation result.

        """
        # Handle nested pipelines
        if isinstance(operation, Pipeline):
            return await self._execute_pipeline(operation, previous_result, pipeline_context)

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
                return await operation.target.execute(wrapped_op, pipeline_context=pipeline_context)

            # If operation needs previous_result, check if it can be executed in Python or storage
            # Operations like Filter, Map, Reduce, Transform always need previous_result and execute in Python
            # PatchOperation, UpdateOperation, DeleteOperation in pipeline mode need previous_result but execute in storage
            # operation.operation is Operation or Pipeline, not TargetSwitch
            if isinstance(operation.operation, Operation) and self._operation_needs_previous_result(
                operation.operation
            ):
                from haolib.pipelines.operations import (  # noqa: PLC0415
                    DeleteOperation,
                    PatchOperation,
                    UpdateOperation,
                )

                # PatchOperation, UpdateOperation, DeleteOperation in pipeline mode execute in storage
                if isinstance(operation.operation, (PatchOperation, UpdateOperation, DeleteOperation)):
                    # Collect AsyncIterator if needed before passing to storage
                    from collections.abc import AsyncIterator  # noqa: PLC0415

                    if isinstance(previous_result, AsyncIterator):
                        previous_result = [item async for item in previous_result]

                    # Execute in storage with previous_result and pipeline context
                    return await operation.target.execute(
                        operation.operation, previous_result=previous_result, pipeline_context=pipeline_context
                    )

                # Filter, Map, Reduce, Transform execute in Python
                if previous_result is None:
                    msg = f"{type(operation.operation).__name__} requires previous result"
                    raise ValueError(msg)
                return await self._execute_python_operation(operation.operation, previous_result)
            # Execute operation in the bound target
            return await operation.target.execute(operation.operation, pipeline_context=pipeline_context)

        # Handle target switches
        if isinstance(operation, TargetSwitch):
            # Execute source operation in source target
            source_result = await operation.source_target.execute(
                operation.source_result, pipeline_context=pipeline_context
            )

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
                        if source_result is not None:
                            processed_operation_data.append(item(source_result))
                        else:
                            processed_operation_data.append(item)
                    else:
                        processed_operation_data.append(item)

                # Merge: source_result (if any) + operation.data
                if len(processed_operation_data) == 0 and len(source_result_data) > 0:
                    processed_data = source_result_data
                elif len(processed_operation_data) > 0 and len(source_result_data) > 0:
                    processed_data = source_result_data + processed_operation_data
                elif len(processed_operation_data) > 0:
                    processed_data = processed_operation_data
                elif len(source_result_data) > 0:
                    processed_data = source_result_data
                else:
                    processed_data = []

                wrapped_op = CreateOp(data=processed_data)
                return await operation.target_target.execute(wrapped_op, pipeline_context=pipeline_context)

            # Execute next operation in target target
            return await operation.target_target.execute(
                operation.next_operation, previous_result=source_result, pipeline_context=pipeline_context
            )

        # Check if operation is a composite (from fluent API)
        # Composites should be converted to operations before reaching here
        from haolib.storages.fluent.composites import BaseComposite  # noqa: PLC0415

        if isinstance(operation, BaseComposite):
            # Composite should have been converted to operations in __or__
            # If it reaches here, extract operations and execute
            ops = operation._operations
            if len(ops) == 1:
                # Single operation - execute directly
                return await self._execute_operation(ops[0], previous_result, pipeline_context)
            # Multiple operations - build pipeline
            pipeline = ops[0]
            for op in ops[1:]:
                pipeline = Pipeline(first=pipeline, second=op)
            return await self._execute_operation(pipeline, previous_result, pipeline_context)

        # Regular operation (not bound to storage)
        # Check if it's UpdateOperation, PatchOperation, or DeleteOperation in pipeline mode
        from haolib.pipelines.operations import (  # noqa: PLC0415
            DeleteOperation,
            PatchOperation,
            UpdateOperation,
        )

        if isinstance(operation, (UpdateOperation, PatchOperation, DeleteOperation)):
            # These operations in pipeline mode need previous_result but execute in storage
            # But they're not bound to storage - this shouldn't happen
            # They should have been bound via TargetBoundOperation in __or__
            msg = (
                f"Operation {type(operation).__name__} in pipeline mode must be bound to storage. "
                f"Use storage.update() or storage.patch() or storage.delete() instead."
            )
            raise ValueError(msg)

        # Regular Python operation (Filter, Map, Reduce, Transform)
        if previous_result is None:
            msg = f"Operation {type(operation).__name__} requires previous result"
            raise ValueError(msg)
        return await self._execute_python_operation(operation, previous_result)

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
                # Check if operation needs previous_result
                from haolib.pipelines.operations import (  # noqa: PLC0415
                    DeleteOperation,
                    FilterOperation,
                    MapOperation,
                    PatchOperation,
                    ReduceOperation,
                    TransformOperation,
                    UpdateOperation,
                )

                # Python operations (Filter, Map, Reduce, Transform) execute in Python
                # UpdateOperation, PatchOperation, DeleteOperation in pipeline mode execute in storage
                if isinstance(op.operation, (FilterOperation, MapOperation, ReduceOperation, TransformOperation)):
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
        pipeline_context: PipelineContext | None = None,
    ) -> Any:
        """Execute a group of operations bound to the same storage in a single transaction.

        This ensures all operations execute in a single transaction by creating
        one transaction and reusing it for all operations in the group.

        Args:
            group: List of TargetBoundOperation bound to the same storage.
            previous_result: Result from previous operation.
            pipeline_context: Context about the entire pipeline for global optimization.

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
                    # PatchOperation, UpdateOperation, DeleteOperation in pipeline mode need previous_result
                    from haolib.pipelines.operations import (  # noqa: PLC0415
                        DeleteOperation,
                        PatchOperation,
                        UpdateOperation,
                    )

                    if isinstance(operation, (PatchOperation, UpdateOperation, DeleteOperation)):
                        # These operations in pipeline mode need previous_result
                        # Collect AsyncIterator if needed
                        from collections.abc import AsyncIterator  # noqa: PLC0415

                        if isinstance(result, AsyncIterator):
                            result = [item async for item in result]

                        result = await storage._executor._execute_operation(operation, txn, previous_result=result)
                    elif self._operation_needs_previous_result(operation):
                        result = await storage._executor._execute_operation(operation, txn, previous_result=result)
                    else:
                        # Operation doesn't need previous_result (e.g., ReadOperation, CreateOperation)
                        result = await storage._executor._execute_operation(operation, txn, previous_result=None)

                    # Handle AsyncIterator from ReadOperation - collect it before passing to next operation
                    from collections.abc import AsyncIterator  # noqa: PLC0415

                    if isinstance(result, AsyncIterator):
                        # Collect AsyncIterator into list for next operation
                        result = [item async for item in result]

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
                return await storage.execute(first_operation, pipeline_context=pipeline_context)

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
            return await storage.execute(current_pipeline, pipeline_context=pipeline_context)

    def _operation_needs_previous_result(self, operation: Operation[Any, Any]) -> bool:
        """Check if operation needs previous result.

        Args:
            operation: Operation to check.

        Returns:
            True if operation needs previous result.

        """
        from haolib.pipelines.operations import (  # noqa: PLC0415
            DeleteOperation,
            PatchOperation,
            UpdateOperation,
        )

        # Python operations always need previous_result
        if isinstance(operation, (FilterOperation, MapOperation, ReduceOperation, TransformOperation)):
            return True

        # UpdateOperation, PatchOperation, DeleteOperation need previous_result if in pipeline mode
        # (i.e., search_index is None or data/patch is None)
        if isinstance(operation, UpdateOperation):
            return operation.search_index is None or operation.data is None
        if isinstance(operation, PatchOperation):
            return operation.search_index is None or operation.patch is None
        if isinstance(operation, DeleteOperation):
            return operation.search_index is None

        return False

    async def _execute_python_operation(
        self,
        operation: Operation[Any, Any],
        previous_result: Any,
    ) -> Any:
        """Execute operation in Python (not in storage).

        Args:
            operation: Operation to execute.
            previous_result: Previous operation result.

        Returns:
            Operation result.

        """
        if isinstance(operation, FilterOperation):
            # Filter operation
            if isinstance(previous_result, AsyncIterator):
                # Handle AsyncIterator from ReadOperation
                filtered = []
                async for item in previous_result:
                    if operation.predicate(item):
                        filtered.append(item)
                return filtered
            if isinstance(previous_result, list):
                return [item for item in previous_result if operation.predicate(item)]
            # Single item
            if operation.predicate(previous_result):
                return previous_result
            return None

        if isinstance(operation, MapOperation):
            # Map operation
            if isinstance(previous_result, AsyncIterator):
                # Handle AsyncIterator from ReadOperation
                mapped = []
                async for item in previous_result:
                    mapped.append(operation.mapper(item, len(mapped)))
                return mapped
            if isinstance(previous_result, list):
                return [operation.mapper(item, idx) for idx, item in enumerate(previous_result)]
            # Single item
            return operation.mapper(previous_result, 0)

        if isinstance(operation, ReduceOperation):
            # Reduce operation
            if isinstance(previous_result, AsyncIterator):
                # Handle AsyncIterator from ReadOperation
                accumulator = operation.initial
                async for item in previous_result:
                    accumulator = operation.reducer(accumulator, item)
                return accumulator
            if isinstance(previous_result, list):
                accumulator = operation.initial
                for item in previous_result:
                    accumulator = operation.reducer(accumulator, item)
                return accumulator
            # Single item
            return operation.reducer(operation.initial, previous_result)

        if isinstance(operation, TransformOperation):
            # Transform operation
            # TransformOperation always receives a list, even for single items
            if isinstance(previous_result, AsyncIterator):
                # Handle AsyncIterator from ReadOperation
                items = [item async for item in previous_result]
                return operation.transformer(items)
            if isinstance(previous_result, list):
                return operation.transformer(previous_result)
            # Single item - wrap in list for transformer
            return operation.transformer([previous_result])

        if isinstance(operation, CreateOperation):
            # CreateOperation without target - pass through previous_result
            # If operation.data is empty, use previous_result
            # If operation.data is not empty, merge with previous_result
            if previous_result is not None:
                if isinstance(previous_result, AsyncIterator):
                    previous_result_data = [item async for item in previous_result]
                elif isinstance(previous_result, list):
                    previous_result_data = previous_result
                else:
                    previous_result_data = [previous_result]
            else:
                previous_result_data = []

            # Process callables in operation.data
            processed_operation_data = []
            for item in operation.data:
                if callable(item):
                    if previous_result is not None:
                        processed_operation_data.append(item(previous_result))
                    else:
                        processed_operation_data.append(item)
                else:
                    processed_operation_data.append(item)

            # Merge: previous_result (if any) + operation.data
            if len(processed_operation_data) == 0 and len(previous_result_data) > 0:
                return previous_result_data
            if len(processed_operation_data) > 0 and len(previous_result_data) > 0:
                return previous_result_data + processed_operation_data
            if len(processed_operation_data) > 0:
                return processed_operation_data
            if len(previous_result_data) > 0:
                return previous_result_data
            return []

        msg = f"Unsupported Python operation: {type(operation).__name__}"
        raise TypeError(msg)

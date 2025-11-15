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

        # Execute operations sequentially from left to right
        result = previous_result
        for idx, op in enumerate(operations):
            # First operation doesn't need previous_result unless it's explicitly provided
            # op can be Operation, TargetBoundOperation, or TargetSwitch
            # For checking if it needs previous_result, we need to unwrap if needed
            op_for_check: Operation[Any, Any] | None = None
            if isinstance(op, TargetBoundOperation):
                # TargetBoundOperation.operation is Operation or Pipeline, not TargetSwitch
                if isinstance(op.operation, Operation):
                    op_for_check = op.operation
            elif isinstance(op, Operation):
                op_for_check = op
            # TargetSwitch doesn't need previous_result check here

            if idx == 0 and result is None:
                if op_for_check is None or not self._operation_needs_previous_result(op_for_check):
                    result = await self._execute_operation(op, previous_result=None)
                else:
                    result = await self._execute_operation(op, previous_result=result)
            else:
                # All subsequent operations or operations that need previous_result
                result = await self._execute_operation(op, previous_result=result)

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
            # Handle CreateOperation with callable data
            if isinstance(operation.operation, CreateOperation) and previous_result is not None:
                # Check if data contains callables that need to be called with previous_result
                processed_data = []
                for item in operation.operation.data:
                    if callable(item):
                        # Call the function with previous_result
                        processed_data.append(item(previous_result))
                    else:
                        processed_data.append(item)

                # If no data was provided or all were callables, use previous_result
                if not processed_data and previous_result is not None:
                    processed_data = previous_result if isinstance(previous_result, list) else [previous_result]

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

            # Prepare data for target
            # For CreateOperation, we need to handle the data properly
            if isinstance(operation.next_operation, CreateOperation):
                # Check if data contains callables that need to be called with source_result
                processed_data = []
                for item in operation.next_operation.data:
                    if callable(item):
                        # Call the function with source_result
                        processed_data.append(item(source_result))
                    else:
                        processed_data.append(item)

                # If no data was provided or all were callables, use source_result
                if not processed_data and source_result is not None:
                    processed_data = source_result if isinstance(source_result, list) else [source_result]

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

        # For operations that don't need previous_result, we can't execute them without storage
        msg = f"Operation {type(operation).__name__} requires storage context but is not bound to any storage"
        raise ValueError(msg)

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

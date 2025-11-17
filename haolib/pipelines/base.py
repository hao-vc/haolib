"""Base operations for data pipelines.

Supports executable pipelines across multiple targets (storages, ML models, APIs, etc.)
with target binding syntax. The ^ operator semantically means "send data to this target".

Example:
        ```python
        from haolib.pipelines import reduceo, transformo
        from haolib.storages.indexes.params import ParamIndex

        # Fluent API: storage methods return composites that can be chained
        pipeline = (
            sql_storage.read(ParamIndex(User)).returning()
            | reduceo(lambda acc, u: acc + u.age, 0)
            | transformo(lambda total: str(total).encode())  # Executes in Python
            | s3_storage.create()
        )

        # Future: ML model target
        # pipeline = (
        #     sql_storage.read(...).returning()
        #     | transformo(...)  # Prepare features
        #     | ml_model.predict()  # Send to ML model
        #     | s3_storage.create()
        # )

        # Execute pipeline directly
        result = await pipeline.execute()
        ```

"""

from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Union, overload

if TYPE_CHECKING:
    from haolib.pipelines.operations import (
        CreateOperation,
        FilterOperation,
        MapOperation,
        ReduceOperation,
        TransformOperation,
    )
    from haolib.storages.targets.abstract import AbstractDataTarget
else:
    from typing import Protocol

    class AbstractDataTarget(Protocol):  # noqa: D101
        async def execute[T_Result](  # noqa: D102
            self,
            operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result],
        ) -> T_Result: ...


# Type alias for operations that can be composed
OperationLike = Union["Operation[Any, Any]", "Pipeline[Any, Any, Any]", "TargetBoundOperation[Any]"]


@dataclass(frozen=True)
class Operation[T_Data, T_Result](ABC):
    """Base operation class.

    Operations don't know how to execute themselves. Storage does.
    Operations are immutable and composable via | operator.

    Each concrete operation should inherit from this class and define
    its own typed parameters.

    Example:
        ```python
        from haolib.pipelines.operations import ReadOperation, FilterOperation

        op1 = ReadOperation(search_index=user_index)
        op2 = FilterOperation(predicate=lambda u: u.age >= 18)
        pipeline = op1 | op2
        ```

    """

    def __or__[T_NextResult](
        self,
        other: Operation[Any, T_NextResult] | TargetBoundOperation[Any],
    ) -> Pipeline[Any, T_Result, T_NextResult]:
        """Compose operations via | operator.

        Args:
            other: Next operation in the pipeline.

        Returns:
            Pipeline combining both operations.

        Example:
            ```python
            from haolib.pipelines import filtero
            pipeline = sql_storage.read(...).returning() | filtero(lambda u: u.age >= 18)
            ```

        """
        return Pipeline(
            first=self,
            second=other,
        )

    def __xor__(self, target: AbstractDataTarget) -> TargetBoundOperation[T_Result]:
        """Send operation to data target.

        Semantically means "send data to this target" (storage, ML model, API, etc.).

        Args:
            target: Target where operation should be executed.
                   Can be storage, ML model, API, or any other data target.

        Returns:
            TargetBoundOperation that knows where to execute.

        Example:
            ```python
            from haolib.storages.indexes.params import ParamIndex

            # Storage target (using fluent API)
            bound_op = sql_storage.read(ParamIndex(User)).returning()

            # ML Model target (future)
            # bound_op = ml_model.predict(...)
            ```

        """
        return TargetBoundOperation(operation=self, target=target)


@dataclass(frozen=True)
class Pipeline[T_Data, T_FirstResult, T_SecondResult]:
    """Pipeline of operations.

    Represents a composition of operations that will be executed sequentially.
    Results from previous operations can be passed to next operations if they support it.

    Example:
        ```python
        from haolib.pipelines.operations import ReadOperation, FilterOperation

        pipeline = (
            ReadOperation(search_index=user_index)
            | FilterOperation(predicate=lambda u: u.age >= 18)
        )
        ```

    """

    first: (
        Operation[T_Data, T_FirstResult]
        | Pipeline[Any, Any, T_FirstResult]
        | TargetBoundOperation[T_FirstResult]
        | TargetSwitch[Any, T_FirstResult]
    )
    """First operation in the pipeline."""

    second: (
        Operation[Any, T_SecondResult]
        | Pipeline[Any, Any, T_SecondResult]
        | TargetBoundOperation[T_SecondResult]
        | TargetSwitch[Any, T_SecondResult]
    )
    """Second operation in the pipeline."""

    def __or__(
        self,
        other: Operation[Any, Any] | Pipeline[Any, Any, Any] | TargetBoundOperation[Any] | Any,
    ) -> Pipeline[Any, Any, Any]:
        """Continue the pipeline.

        Args:
            other: Next operation to add to the pipeline.

        Returns:
            Extended pipeline.

        """
        # Handle composites from fluent API
        from haolib.storages.fluent.composites import BaseComposite  # noqa: PLC0415

        if isinstance(other, BaseComposite):
            # Extract operations from composite and build pipeline
            other_ops = other._operations
            if len(other_ops) == 1:
                # Single operation - bind to storage
                second_op: TargetBoundOperation[Any] = TargetBoundOperation(
                    operation=other_ops[0], target=other._storage
                )
            else:
                # Multiple operations - build pipeline and bind to storage
                other_pipeline: Pipeline[Any, Any, Any] | Operation[Any, Any] = other_ops[0]
                for op in other_ops[1:]:
                    other_pipeline = Pipeline(first=other_pipeline, second=op)
                second_op = TargetBoundOperation(operation=other_pipeline, target=other._storage)
            return Pipeline(first=self, second=second_op)

        # Check if operation needs previous_result (Python operations)
        if isinstance(other, Operation):
            from haolib.pipelines.operations import (  # noqa: PLC0415
                DeleteOperation,
                FilterOperation,
                MapOperation,
                PatchOperation,
                ReduceOperation,
                TransformOperation,
                UpdateOperation,
            )

            # Python operations (Filter, Map, Reduce, Transform) should not be bound to storage
            if isinstance(other, (FilterOperation, MapOperation, ReduceOperation, TransformOperation)):
                return Pipeline(first=self, second=other)  # type: ignore[arg-type]

        return Pipeline(
            first=self,
            second=other,
        )

    def __xor__(self, target: AbstractDataTarget) -> TargetBoundOperation[T_SecondResult]:
        """Send pipeline result to data target.

        Semantically means "send pipeline result to this target".

        Args:
            target: Target where pipeline result should be executed.

        Returns:
            TargetBoundOperation for the pipeline result.

        Example:
            ```python
            from haolib.pipelines import filtero
            pipeline = (
                sql_storage.read(user_index).returning()
                | filtero(lambda u: u.age >= 18)
            )
            ```

        """
        return TargetBoundOperation(operation=self, target=target)

    def validate(self) -> None:
        """Validate pipeline structure before execution.

        Raises:
            PipelineValidationError: If pipeline structure is invalid.

        Example:
            ```python
            from haolib.pipelines import filtero
            pipeline = (
                sql_storage.read(...).returning()
                | filtero(lambda u: u.age >= 18)
                | s3_storage.create()
            )

            # Validate before execution
            pipeline.validate()
            result = await pipeline.execute()
            ```

        """
        from haolib.pipelines.validator import PipelineValidator  # noqa: PLC0415

        validator = PipelineValidator()
        validator.validate(self)

    async def execute(self) -> T_SecondResult:
        """Execute pipeline across multiple targets.

        Automatically validates pipeline before execution.
        Detects which target to use for each operation,
        optimizes operations on target side when possible, and coordinates
        data transfer between targets.

        Targets can be storages, ML models, APIs, or any other data processing target.

        Returns:
            Final pipeline result.

        Raises:
            PipelineValidationError: If pipeline structure is invalid.
            ValueError: If no target is specified for an operation that requires it.
            RuntimeError: If pipeline execution fails.

        Example:
            ```python
            from haolib.pipelines import reduceo, transformo
            from haolib.storages.indexes.params import ParamIndex

            # Fluent API: storage methods return composites
            pipeline = (
                sql_storage.read(ParamIndex(User)).returning()
                | reduceo(lambda acc, u: acc + u.age, 0)
                | transformo(lambda total: str(total).encode())  # Executes in Python
                | s3_storage.create()
            )

            # Execute pipeline directly (validation happens automatically)
            result = await pipeline.execute()
            ```

        """
        # Validate before execution
        self.validate()

        from haolib.pipelines.executor import ExecutablePipelineExecutor  # noqa: PLC0415

        executor = ExecutablePipelineExecutor()
        return await executor.execute(self)


@dataclass(frozen=True)
class TargetBoundOperation[T_Result]:
    """Operation bound to a specific data target.

    Created when using ^ operator to send operation to a target.
    Operations without target binding execute in Python.

    Targets can be storages, ML models, APIs, or any other data processing target.
    The ^ operator semantically means "send data to this target".

    Example:
            ```python
            from haolib.storages.indexes.params import ParamIndex

            # Storage target (using fluent API)
            bound_op = sql_storage.read(ParamIndex(User)).returning()

            # ML Model target (future)
            # bound_op = ml_model.predict(...)
            ```

    """

    operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result]
    """Operation to execute."""
    target: AbstractDataTarget
    """Target where operation should be executed."""

    async def execute(self) -> T_Result:
        """Execute the bound operation in the target.

        Allows direct execution of bound operations without needing to call
        storage.execute() explicitly.

        Returns:
            Result of operation execution.

        Raises:
            TargetError: If target operation fails.
            TypeError: If operation type is not supported.

        Example:
            ```python
            from haolib.storages.indexes.params import ParamIndex

            # Read operation (using fluent API)
            users = await sql_storage.read(ParamIndex(User)).returning().execute()

            # Create operation
            await sql_storage.create([user1, user2]).returning().execute()

            # Update operation
            await sql_storage.read(ParamIndex(User, id=1)).update(User(...)).returning().execute()

            # Delete operation
            await sql_storage.read(ParamIndex(User, id=1)).delete().execute()
            ```

        """
        return await self.target.execute(self.operation)

    def __or__[T_NextResult](
        self,
        other: Operation[Any, T_NextResult] | TargetBoundOperation[Any],
    ) -> Pipeline[Any, T_Result, T_NextResult]:
        """Continue pipeline after target-bound operation.

        Args:
            other: Next operation in the pipeline.

        Returns:
            Pipeline combining operations.

        """
        # If next operation is also bound to target, check if we need to switch
        if isinstance(other, TargetBoundOperation):
            if other.target != self.target:
                # Target switch needed
                # self.operation can be Operation, Pipeline, or TargetSwitch
                # TargetSwitch.source_result expects Operation or Pipeline, not TargetSwitch
                # So we need to unwrap if it's a TargetSwitch
                source_op = self.operation
                if isinstance(source_op, TargetSwitch):
                    # If source is already a TargetSwitch, use its source_result
                    source_op = source_op.source_result

                return Pipeline(
                    first=self,
                    second=TargetSwitch(
                        source_result=source_op,
                        source_target=self.target,
                        target_target=other.target,
                        next_operation=other.operation,
                    ),
                )
            # Same target - continue normally
            return Pipeline(first=self, second=other)

        # Next operation is not bound to target
        # Check if it needs previous_result (Python operations)
        from haolib.pipelines.operations import (  # noqa: PLC0415
            DeleteOperation,
            FilterOperation,
            MapOperation,
            PatchOperation,
            ReduceOperation,
            TransformOperation,
            UpdateOperation,
        )

        if isinstance(other, (FilterOperation, MapOperation, ReduceOperation, TransformOperation)):
            # Python operations - don't bind to target
            return Pipeline(first=self, second=other)  # type: ignore[arg-type]

        # Storage operations - bind to same target
        bound_other = TargetBoundOperation(operation=other, target=self.target)
        return Pipeline(first=self, second=bound_other)

    def __xor__(self, target: AbstractDataTarget) -> TargetBoundOperation[T_Result]:
        """Rebind to different target.

        Args:
            target: New target to bind to.

        Returns:
            New TargetBoundOperation with different target.

        """
        return TargetBoundOperation(operation=self.operation, target=target)


@dataclass(frozen=True)
class TargetSwitch[T_Data, T_Result]:
    """Operation that switches execution context to another target.

    Created automatically when pipeline switches between targets.
    Handles data transfer and context switching.

    Targets can be storages, ML models, APIs, or any other data processing target.

    """

    source_result: Operation[Any, T_Data] | Pipeline[Any, Any, T_Data]
    """Source operation that produced the data."""
    source_target: AbstractDataTarget
    """Target where source operation was executed."""
    target_target: AbstractDataTarget
    """Target where next operation will be executed."""
    next_operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result]
    """Next operation to execute in target target."""

    def __or__[T_NextResult](
        self,
        other: Operation[Any, T_NextResult] | TargetBoundOperation[Any],
    ) -> Pipeline[Any, T_Result, T_NextResult]:
        """Continue pipeline after target switch.

        Args:
            other: Next operation in the pipeline.

        Returns:
            Extended pipeline.

        """
        return Pipeline(first=self, second=other)

    def __xor__(self, target: AbstractDataTarget) -> TargetBoundOperation[T_Result]:
        """Bind target switch result to another target.

        Args:
            target: Target to bind to.

        Returns:
            TargetBoundOperation with next_operation bound to target.

        Note:
            TargetSwitch is typically created automatically during pipeline composition.
            Binding a TargetSwitch to a target binds its next_operation to that target.
            TargetSwitch itself should not be wrapped in TargetBoundOperation.

        """
        # Bind the next_operation to the target, not the switch itself
        # TargetSwitch should not be wrapped in TargetBoundOperation
        if isinstance(self.next_operation, TargetBoundOperation):
            # Already bound, just return it
            return self.next_operation
        return TargetBoundOperation(operation=self.next_operation, target=target)

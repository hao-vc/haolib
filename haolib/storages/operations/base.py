"""Base operations for data pipelines.

Supports executable pipelines across multiple targets (storages, ML models, APIs, etc.)
with target binding syntax. The ^ operator semantically means "send data to this target".

Example:
        ```python
        from haolib.storages.dsl import reado, reduceo, transformo, createo
        from haolib.storages.indexes.params import ParamIndex

        # New syntax: operation ^ target for binding, | for composition
        # Both operators have same precedence (10), so they execute left-to-right
        pipeline = (
            reado(search_index=ParamIndex(User)) ^ sql_storage
            | reduceo(lambda acc, u: acc + u.age, 0) ^ sql_storage
            | transformo(lambda total: str(total).encode())  # Executes in Python
            | createo([lambda data: data]) ^ s3_storage
        )

        # Future: ML model target
        # pipeline = (
        #     reado(...) ^ sql_storage
        #     | transformo(...)  # Prepare features
        #     | predicto() ^ ml_model  # Send to ML model
        #     | createo(...) ^ s3_storage
        # )

        # Execute pipeline directly
        result = await pipeline.execute()
        ```

"""

from abc import ABC
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Union

if TYPE_CHECKING:
    from haolib.storages.targets.abstract import AbstractDataTarget

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
        from haolib.storages.operations.concrete import ReadOperation, FilterOperation

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
            pipeline = reado(...) ^ sql_storage | filtero(lambda u: u.age >= 18)
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
            from haolib.storages.dsl import reado
            from haolib.storages.indexes.params import ParamIndex

            # Storage target
            bound_op = reado(search_index=ParamIndex(User)) ^ sql_storage

            # ML Model target (future)
            # bound_op = reado(...) ^ ml_model
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
        from haolib.storages.operations.concrete import ReadOperation, FilterOperation

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

    def __or__[T_NextResult](
        self,
        other: Operation[Any, T_NextResult] | Pipeline[Any, Any, T_NextResult] | TargetBoundOperation[Any],
    ) -> Pipeline[Any, T_SecondResult, T_NextResult]:
        """Continue the pipeline.

        Args:
            other: Next operation to add to the pipeline.

        Returns:
            Extended pipeline.

        """
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
            pipeline = (
                reado(search_index=user_index)
                | filtero(lambda u: u.age >= 18)
            ) ^ sql_storage
            ```

        """
        return TargetBoundOperation(operation=self, target=target)

    def validate(self) -> None:
        """Validate pipeline structure before execution.

        Raises:
            PipelineValidationError: If pipeline structure is invalid.

        Example:
            ```python
            pipeline = (
                reado(...) ^ sql_storage
                | filtero(lambda u: u.age >= 18)
                | createo(...) ^ s3_storage
            )

            # Validate before execution
            pipeline.validate()
            result = await pipeline.execute()
            ```

        """
        from haolib.storages.operations.validator import PipelineValidator  # noqa: PLC0415

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
            from haolib.storages.dsl import reado, reduceo, transformo, createo
            from haolib.storages.indexes.params import ParamIndex

            # New syntax: operation ^ storage for binding, | for composition
            # Both operators have same precedence (10), so they execute left-to-right
            pipeline = (
                reado(search_index=ParamIndex(User)) ^ sql_storage
                | reduceo(lambda acc, u: acc + u.age, 0) ^ sql_storage
                | transformo(lambda total: str(total).encode())  # Executes in Python
                | createo([lambda data: data]) ^ s3_storage
            )

            # Execute pipeline directly (validation happens automatically)
            result = await pipeline.execute()
            ```

        """
        # Validate before execution
        self.validate()

        from haolib.storages.operations.executor import ExecutablePipelineExecutor  # noqa: PLC0415

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
        from haolib.storages.dsl import reado
        from haolib.storages.indexes.params import ParamIndex

        # Storage target
        bound_op = reado(search_index=ParamIndex(User)) ^ sql_storage

        # ML Model target (future)
        # bound_op = reado(...) ^ ml_model
        ```

    """

    operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result]
    """Operation to execute."""
    target: AbstractDataTarget
    """Target where operation should be executed."""

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

        # Next operation is not bound to target - execute in Python
        return Pipeline(first=self, second=other)

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

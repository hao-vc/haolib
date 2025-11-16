"""Pipeline validator for early error detection."""

from typing import Any

from haolib.storages.operations.base import (
    Operation,
    Pipeline,
    TargetBoundOperation,
    TargetSwitch,
)
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


class PipelineValidationError(ValueError):
    """Error raised when pipeline validation fails.

    Provides information about which operation in the pipeline failed validation.

    Example:
        ```python
        try:
            pipeline.validate()
        except PipelineValidationError as e:
            print(f"Pipeline invalid at operation {e.operation_index}: {e}")
        ```

    """

    def __init__(self, message: str, operation_index: int | None = None) -> None:
        """Initialize validation error.

        Args:
            message: Error message.
            operation_index: Index of operation that failed validation (0-based).

        """
        super().__init__(message)
        self.operation_index = operation_index


class PipelineValidator:
    """Validates pipeline structure before execution.

    Catches errors early, before any operations are executed.
    This provides better UX and prevents partial execution failures.

    Example:
        ```python
        validator = PipelineValidator()
        try:
            validator.validate(pipeline)
            result = await pipeline.execute()
        except PipelineValidationError as e:
            print(f"Pipeline invalid: {e}")
        ```

    """

    def validate(self, pipeline: Pipeline[Any, Any, Any]) -> None:
        """Validate entire pipeline structure.

        Args:
            pipeline: Pipeline to validate.

        Raises:
            PipelineValidationError: If pipeline structure is invalid.

        Example:
            ```python
            validator = PipelineValidator()
            try:
                validator.validate(pipeline)
                result = await pipeline.execute()
            except PipelineValidationError as e:
                print(f"Pipeline invalid: {e}")
            ```

        """
        operations = self._flatten_pipeline(pipeline)

        for idx, op in enumerate(operations):
            self._validate_operation(op, idx, operations)

    def _flatten_pipeline(
        self,
        pipeline: Pipeline[Any, Any, Any] | Operation[Any, Any] | TargetBoundOperation[Any],
    ) -> list[Operation[Any, Any] | TargetBoundOperation[Any] | TargetSwitch[Any, Any]]:
        """Flatten nested pipeline structure.

        Args:
            pipeline: Pipeline to flatten.

        Returns:
            List of operations in left-to-right order.

        """
        operations = []

        def collect(
            p: Pipeline[Any, Any, Any] | Operation[Any, Any] | TargetBoundOperation[Any],
        ) -> None:
            if isinstance(p, Pipeline):
                if isinstance(p.first, Pipeline):
                    collect(p.first)
                else:
                    operations.append(p.first)

                if isinstance(p.second, Pipeline):
                    collect(p.second)
                else:
                    operations.append(p.second)
            else:
                operations.append(p)

        collect(pipeline)
        return operations

    def _validate_operation(
        self,
        operation: Operation[Any, Any] | TargetBoundOperation[Any] | TargetSwitch[Any, Any],
        index: int,
        all_operations: list[Any],  # noqa: ARG002
    ) -> None:
        """Validate single operation in context of pipeline.

        Args:
            operation: Operation to validate.
            index: Index of operation in pipeline.
            all_operations: All operations in pipeline.

        Raises:
            PipelineValidationError: If operation is invalid.

        """
        # Unwrap TargetBoundOperation for validation
        if isinstance(operation, TargetBoundOperation):
            actual_op = operation.operation
            has_target = True
            target = operation.target
        elif isinstance(operation, TargetSwitch):
            # Validate target switch
            if not isinstance(operation.next_operation, TargetBoundOperation):
                msg = f"TargetSwitch at index {index} must have next_operation bound to target"
                raise PipelineValidationError(
                    msg,
                    operation_index=index,
                )
            actual_op = operation.next_operation.operation
            has_target = True
            target = operation.target_target
        else:
            actual_op = operation
            has_target = False
            target = None

        # Check if operation needs previous_result
        # actual_op can be Operation, Pipeline, or TargetSwitch, but we only check Operation
        if isinstance(actual_op, Operation):
            needs_previous = self._operation_needs_previous_result(actual_op)
            needs_target = self._operation_needs_target(actual_op)
        else:
            # Pipeline or TargetSwitch don't need previous_result or target in this context
            needs_previous = False
            needs_target = False

        # Check if operation will receive previous_result (not first operation)
        receives_previous = index > 0

        # Validate previous_result requirement
        if needs_previous and index == 0:
            op_name = type(actual_op).__name__ if isinstance(actual_op, Operation) else "Operation"
            msg = (
                f"Operation {op_name} at index {index} requires previous result but is the first operation in pipeline"
            )
            raise PipelineValidationError(
                msg,
                operation_index=index,
            )

        # Validate that operations requiring previous_result should NOT be bound to target
        # These operations (Filter, Map, Reduce, Transform) execute in Python, not in storage
        if needs_previous and has_target:
            op_name = type(actual_op).__name__ if isinstance(actual_op, Operation) else "Operation"
            target_name = type(target).__name__ if target else "target"
            msg = (
                f"Operation {op_name} at index {index} requires previous result and executes in Python. "
                f"It should not be bound to {target_name} using ^ operator. "
                f"Remove the ^ {target_name} binding - the operation will execute in Python automatically."
            )
            raise PipelineValidationError(
                msg,
                operation_index=index,
            )

        # Validate target requirement
        # Operations that require target (Read, Create, Update, Delete) must be bound to target
        # Exception: CreateOperation can receive previous_result and use it as data
        # But ReadOperation, UpdateOperation, DeleteOperation always need target
        if needs_target and not has_target:
            # CreateOperation can work without target if it receives previous_result OR has data
            if isinstance(actual_op, CreateOperation):
                if receives_previous:
                    # This is OK - CreateOperation can use previous_result as data
                    pass
                elif actual_op.data:
                    # This is OK - CreateOperation has explicit data
                    pass
                else:
                    # CreateOperation has no data and no previous_result - error
                    op_name = type(actual_op).__name__ if isinstance(actual_op, Operation) else "Operation"
                    msg = (
                        f"Operation {op_name} at index {index} has no data and no previous_result. "
                        f"Either provide data to createo() or ensure it receives previous_result from pipeline."
                    )
                    raise PipelineValidationError(
                        msg,
                        operation_index=index,
                    )
            else:
                # All other target-requiring operations must have target
                op_name = type(actual_op).__name__ if isinstance(actual_op, Operation) else "Operation"
                msg = f"Operation {op_name} at index {index} requires target binding but is not bound to any target"
                raise PipelineValidationError(
                    msg,
                    operation_index=index,
                )

    def _operation_needs_previous_result(self, operation: Operation[Any, Any]) -> bool:
        """Check if operation needs previous result.

        Args:
            operation: Operation to check.

        Returns:
            True if operation needs previous result.

        """
        return isinstance(
            operation,
            (FilterOperation, MapOperation, ReduceOperation, TransformOperation),
        )

    def _operation_needs_target(self, operation: Operation[Any, Any]) -> bool:
        """Check if operation needs target binding.

        Args:
            operation: Operation to check.

        Returns:
            True if operation needs target binding.

        """
        return isinstance(
            operation,
            (ReadOperation, CreateOperation, UpdateOperation, DeleteOperation),
        )

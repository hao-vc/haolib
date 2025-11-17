"""Operation composites for fluent storage API."""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING, Any, TypeVar, overload

if TYPE_CHECKING:
    from haolib.pipelines.base import Operation, Pipeline
    from haolib.pipelines.operations import (
        CreateOperation,
        DeleteOperation,
        FilterOperation,
        MapOperation,
        PatchOperation,
        ReadOperation,
        ReduceOperation,
        TransformOperation,
        UpdateOperation,
    )
    from haolib.storages.targets.abstract import AbstractDataTarget
from haolib.pipelines.base import Operation, Pipeline, TargetBoundOperation

# Import operations lazily to avoid circular import
# Operations are imported in methods that use them

T_Data = TypeVar("T_Data")
T_Result = TypeVar("T_Result")


class BaseComposite:
    """Base class for operation composites."""

    def __init__(
        self,
        storage: AbstractDataTarget,
        operations: list[Operation[Any, Any]],
        return_data: bool = False,
    ) -> None:
        """Initialize composite.

        Args:
            storage: Storage where operations will be executed.
            operations: List of operations in the chain.
            return_data: Whether to return data from operations.

        """
        self._storage = storage
        self._operations = operations
        self._return_data = return_data

    def returning(self) -> BaseComposite:
        """Mark that data should be returned (idempotent).

        Returns:
            New composite with return_data=True.

        """
        if self._return_data:
            return self  # Idempotent

        # Create new instance with return_data=True
        # For specific composites, we need to preserve their specific attributes
        if isinstance(self, ReadComposite):
            return ReadComposite(
                storage=self._storage,
                operation=self._read_op,
                return_data=True,
            )
        if isinstance(self, CreateComposite):
            return CreateComposite(
                storage=self._storage,
                operation=self._create_op,
                return_data=True,
            )
        if isinstance(self, UpdateComposite):
            return UpdateComposite(
                storage=self._storage,
                operation=self._update_op,
                return_data=True,
            )
        if isinstance(self, PatchComposite):
            return PatchComposite(
                storage=self._storage,
                operation=self._patch_op,
                return_data=True,
            )
        if isinstance(self, DeleteComposite):
            return DeleteComposite(
                storage=self._storage,
                operation=self._delete_op,
                return_data=True,
            )
        if isinstance(self, ReadUpdateComposite):
            return ReadUpdateComposite(
                storage=self._storage,
                read_op=self._read_op,
                update_op=self._update_op,
                return_data=True,
            )
        if isinstance(self, ReadPatchComposite):
            return ReadPatchComposite(
                storage=self._storage,
                read_op=self._read_op,
                patch_op=self._patch_op,
                return_data=True,
            )
        if isinstance(self, ReadDeleteComposite):
            return ReadDeleteComposite(
                storage=self._storage,
                read_op=self._read_op,
                delete_op=self._delete_op,
                return_data=True,
            )
        if isinstance(self, ReadUpdateDeleteComposite):
            return ReadUpdateDeleteComposite(
                storage=self._storage,
                read_op=self._read_op,
                update_op=self._update_op,
                delete_op=self._delete_op,
                return_data=True,
            )
        # Fallback for BaseComposite
        return BaseComposite(
            storage=self._storage,
            operations=self._operations,
            return_data=True,
        )

    def _build_pipeline(self) -> Pipeline[Any, Any, Any] | Operation[Any, Any] | TargetBoundOperation[Any]:
        """Build pipeline from operations.

        Returns:
            Pipeline combining all operations, or single operation if only one.
            Operations are bound to storage via TargetBoundOperation.

        """
        if not self._operations:
            msg = "No operations in composite"
            raise ValueError(msg)

        # Bind all operations to storage
        bound_operations = [TargetBoundOperation(operation=op, target=self._storage) for op in self._operations]

        if len(bound_operations) == 1:
            # Single operation - return as TargetBoundOperation
            return bound_operations[0]

        # Multiple operations - build nested pipeline
        pipeline: Pipeline[Any, Any, Any] | TargetBoundOperation[Any] = bound_operations[0]
        for bound_op in bound_operations[1:]:
            pipeline = Pipeline(first=pipeline, second=bound_op)  # type: ignore[arg-type]

        return pipeline

    def _operation_needs_previous_result(self, operation: Operation[Any, Any]) -> bool:
        """Check if operation needs previous result.

        Args:
            operation: Operation to check.

        Returns:
            True if operation needs previous result.

        """
        # Import lazily to avoid circular import
        from haolib.pipelines.operations import (  # noqa: PLC0415
            FilterOperation,
            MapOperation,
            ReduceOperation,
            TransformOperation,
        )

        return isinstance(
            operation,
            (FilterOperation, MapOperation, ReduceOperation, TransformOperation),
        )

    def __or__(
        self,
        other: Operation[Any, Any] | BaseComposite,
    ) -> Pipeline[Any, Any, Any]:
        """Compose with next operation.

        Args:
            other: Next operation or composite.

        Returns:
            Pipeline combining this composite with next operation.

        Raises:
            ValueError: If next operation needs previous_result but data is not returned.

        """
        # Build pipeline from dot-notation operations
        storage_pipeline_or_op = self._build_pipeline()

        # Check if next operation needs previous_result
        if isinstance(other, BaseComposite):
            # Other is a composite - extract its operations
            other_ops = other._operations
            if other_ops:
                first_other_op = other_ops[0]
                if self._operation_needs_previous_result(first_other_op):
                    if not self._return_data:
                        msg = (
                            f"Operation {type(first_other_op).__name__} requires previous_result, "
                            f"but previous operation does not return data. "
                            f"Add .returning() to return data."
                        )
                        raise ValueError(msg)
        elif isinstance(other, Operation):
            if self._operation_needs_previous_result(other):
                if not self._return_data:
                    msg = (
                        f"Operation {type(other).__name__} requires previous_result, "
                        f"but previous operation does not return data. "
                        f"Add .returning() to return data."
                    )
                    raise ValueError(msg)

        # Compose pipelines
        # Extract operations from composites for pipeline composition
        if isinstance(storage_pipeline_or_op, TargetBoundOperation) or isinstance(storage_pipeline_or_op, Pipeline):
            first_op = storage_pipeline_or_op
        else:
            # Single operation - bind to storage
            first_op = TargetBoundOperation(operation=storage_pipeline_or_op, target=self._storage)

        second_op: Operation[Any, Any] | TargetBoundOperation[Any] | Pipeline[Any, Any, Any]

        if isinstance(other, BaseComposite):
            # Extract operations from other composite and build pipeline
            other_ops = other._operations
            if len(other_ops) == 1:
                # Single operation - bind to its storage
                second_op = TargetBoundOperation(operation=other_ops[0], target=other._storage)
            else:
                # Multiple operations - build pipeline and bind to storage
                other_pipeline: Pipeline[Any, Any, Any] | Operation[Any, Any] = other_ops[0]
                for op in other_ops[1:]:
                    other_pipeline = Pipeline(first=other_pipeline, second=op)  # type: ignore[arg-type]
                second_op = TargetBoundOperation(operation=other_pipeline, target=other._storage)
        elif isinstance(other, Operation):
            # Check if operation needs previous_result (Python operations)
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
                second_op = other
            else:
                # Storage operations - always bind to storage
                # UpdateOperation, PatchOperation, DeleteOperation in pipeline mode need previous_result
                # but they still execute in storage, so bind them
                second_op = TargetBoundOperation(operation=other, target=self._storage)
        else:
            # Already a TargetBoundOperation or Pipeline
            second_op = other

        return Pipeline(first=first_op, second=second_op)

    async def execute(self) -> Any:
        """Execute the composite operation.

        Returns:
            Result of execution. If return_data=True, returns list of data.
            Otherwise, returns None or AsyncIterator.

        """
        # Build pipeline from operations (without binding, as storage is already known)
        if not self._operations:
            msg = "No operations in composite"
            raise ValueError(msg)

        if len(self._operations) == 1:
            # Single operation - execute directly
            pipeline_or_op: Operation[Any, Any] | Pipeline[Any, Any, Any] = self._operations[0]
        else:
            # Multiple operations - build nested pipeline
            pipeline: Pipeline[Any, Any, Any] | Operation[Any, Any] = self._operations[0]
            for op in self._operations[1:]:
                pipeline = Pipeline(first=pipeline, second=op)  # type: ignore[arg-type]
            pipeline_or_op = pipeline

        result = await self._storage.execute(pipeline_or_op)

        if self._return_data:
            # Collect AsyncIterator into list if needed
            if isinstance(result, AsyncIterator):
                return [item async for item in result]
            return result

        return result


class ReadComposite[T_Data](BaseComposite):
    """Composite for read operations."""

    def __init__(
        self,
        storage: AbstractDataTarget,
        operation: ReadOperation[T_Data],
        return_data: bool = False,
    ) -> None:
        """Initialize read composite.

        Args:
            storage: Storage where operation will be executed.
            operation: Read operation.
            return_data: Whether to return data.

        """
        super().__init__(storage, [operation], return_data)
        self._read_op = operation

    def __or__(
        self,
        other: Operation[Any, Any] | BaseComposite,
    ) -> Pipeline[Any, Any, Any]:
        """Compose with next operation, preserving type information."""
        return super().__or__(other)

    def update(
        self,
        data: T_Data | Callable[[T_Data], T_Data] | None = None,
    ) -> ReadUpdateComposite[T_Data]:
        """Chain update after read.

        Args:
            data: Data to update with.

        Returns:
            ReadUpdateComposite.

        Raises:
            TypeError: If storage does not support update.

        """
        from haolib.storages.fluent.protocols import UpdateOperatable

        if not isinstance(self._storage, UpdateOperatable):
            msg = f"{type(self._storage).__name__} does not support update"
            raise TypeError(msg)

        from haolib.pipelines.operations import UpdateOperation  # noqa: PLC0415

        update_op: UpdateOperation[T_Data] = UpdateOperation(data=data)
        return ReadUpdateComposite(
            storage=self._storage,
            read_op=self._read_op,
            update_op=update_op,
            return_data=self._return_data,
        )

    def patch(
        self,
        patch: dict[str, Any] | Any | None = None,
    ) -> ReadPatchComposite[T_Data]:
        """Chain patch after read.

        Args:
            patch: Patch to apply.

        Returns:
            ReadPatchComposite.

        Raises:
            TypeError: If storage does not support patch.

        """
        from haolib.storages.fluent.protocols import PatchOperatable

        if not isinstance(self._storage, PatchOperatable):
            msg = f"{type(self._storage).__name__} does not support patch"
            raise TypeError(msg)

        from haolib.pipelines.operations import PatchOperation  # noqa: PLC0415

        patch_op: PatchOperation[T_Data] = PatchOperation(patch=patch)
        return ReadPatchComposite(
            storage=self._storage,
            read_op=self._read_op,
            patch_op=patch_op,
            return_data=self._return_data,
        )

    def delete(self) -> ReadDeleteComposite[T_Data]:
        """Chain delete after read.

        Returns:
            ReadDeleteComposite.

        Raises:
            TypeError: If storage does not support delete.

        """
        from haolib.storages.fluent.protocols import DeleteOperatable

        if not isinstance(self._storage, DeleteOperatable):
            msg = f"{type(self._storage).__name__} does not support delete"
            raise TypeError(msg)

        from haolib.pipelines.operations import DeleteOperation  # noqa: PLC0415

        delete_op: DeleteOperation[T_Data] = DeleteOperation()
        return ReadDeleteComposite(
            storage=self._storage,
            read_op=self._read_op,
            delete_op=delete_op,
            return_data=self._return_data,
        )


class CreateComposite[T_Data](BaseComposite):
    """Composite for create operations."""

    def __init__(
        self,
        storage: AbstractDataTarget,
        operation: CreateOperation[T_Data],
        return_data: bool = False,
    ) -> None:
        """Initialize create composite.

        Args:
            storage: Storage where operation will be executed.
            operation: Create operation.
            return_data: Whether to return data.

        """
        super().__init__(storage, [operation], return_data)
        self._create_op = operation

    def __or__(
        self,
        other: Operation[Any, Any] | BaseComposite,
    ) -> Pipeline[Any, Any, Any]:
        """Compose with next operation, preserving type information."""
        return super().__or__(other)


class UpdateComposite[T_Data](BaseComposite):
    """Composite for update operations."""

    def __init__(
        self,
        storage: AbstractDataTarget,
        operation: UpdateOperation[T_Data],
        return_data: bool = False,
    ) -> None:
        """Initialize update composite.

        Args:
            storage: Storage where operation will be executed.
            operation: Update operation.
            return_data: Whether to return data.

        """
        super().__init__(storage, [operation], return_data)
        self._update_op = operation


class PatchComposite[T_Data](BaseComposite):
    """Composite for patch operations."""

    def __init__(
        self,
        storage: AbstractDataTarget,
        operation: PatchOperation[T_Data],
        return_data: bool = False,
    ) -> None:
        """Initialize patch composite.

        Args:
            storage: Storage where operation will be executed.
            operation: Patch operation.
            return_data: Whether to return data.

        """
        super().__init__(storage, [operation], return_data)
        self._patch_op = operation


class DeleteComposite[T_Data](BaseComposite):
    """Composite for delete operations."""

    def __init__(
        self,
        storage: AbstractDataTarget,
        operation: DeleteOperation[T_Data],
        return_data: bool = False,
    ) -> None:
        """Initialize delete composite.

        Args:
            storage: Storage where operation will be executed.
            operation: Delete operation.
            return_data: Whether to return data.

        """
        super().__init__(storage, [operation], return_data)
        self._delete_op = operation


class ReadUpdateComposite[T_Data](BaseComposite):
    """Composite for read + update operations."""

    def __init__(
        self,
        storage: AbstractDataTarget,
        read_op: ReadOperation[T_Data],
        update_op: UpdateOperation[T_Data],
        return_data: bool = False,
    ) -> None:
        """Initialize read+update composite.

        Args:
            storage: Storage where operations will be executed.
            read_op: Read operation.
            update_op: Update operation.
            return_data: Whether to return data.

        """
        super().__init__(storage, [read_op, update_op], return_data)
        self._read_op = read_op
        self._update_op = update_op

    def delete(self) -> ReadUpdateDeleteComposite[T_Data]:
        """Chain delete after read+update.

        Returns:
            ReadUpdateDeleteComposite.

        Raises:
            TypeError: If storage does not support delete.

        """
        from haolib.storages.fluent.protocols import DeleteOperatable

        if not isinstance(self._storage, DeleteOperatable):
            msg = f"{type(self._storage).__name__} does not support delete"
            raise TypeError(msg)

        from haolib.pipelines.operations import DeleteOperation  # noqa: PLC0415

        delete_op: DeleteOperation[T_Data] = DeleteOperation()
        return ReadUpdateDeleteComposite(
            storage=self._storage,
            read_op=self._read_op,
            update_op=self._update_op,
            delete_op=delete_op,
            return_data=self._return_data,
        )


class ReadPatchComposite[T_Data](BaseComposite):
    """Composite for read + patch operations."""

    def __init__(
        self,
        storage: AbstractDataTarget,
        read_op: ReadOperation[T_Data],
        patch_op: PatchOperation[T_Data],
        return_data: bool = False,
    ) -> None:
        """Initialize read+patch composite.

        Args:
            storage: Storage where operations will be executed.
            read_op: Read operation.
            patch_op: Patch operation.
            return_data: Whether to return data.

        """
        super().__init__(storage, [read_op, patch_op], return_data)
        self._read_op = read_op
        self._patch_op = patch_op


class ReadDeleteComposite[T_Data](BaseComposite):
    """Composite for read + delete operations."""

    def __init__(
        self,
        storage: AbstractDataTarget,
        read_op: ReadOperation[T_Data],
        delete_op: DeleteOperation[T_Data],
        return_data: bool = False,
    ) -> None:
        """Initialize read+delete composite.

        Args:
            storage: Storage where operations will be executed.
            read_op: Read operation.
            delete_op: Delete operation.
            return_data: Whether to return data.

        """
        super().__init__(storage, [read_op, delete_op], return_data)
        self._read_op = read_op
        self._delete_op = delete_op


class ReadUpdateDeleteComposite[T_Data](BaseComposite):
    """Composite for read + update + delete operations."""

    def __init__(
        self,
        storage: AbstractDataTarget,
        read_op: ReadOperation[T_Data],
        update_op: UpdateOperation[T_Data],
        delete_op: DeleteOperation[T_Data],
        return_data: bool = False,
    ) -> None:
        """Initialize read+update+delete composite.

        Args:
            storage: Storage where operations will be executed.
            read_op: Read operation.
            update_op: Update operation.
            delete_op: Delete operation.
            return_data: Whether to return data.

        """
        super().__init__(storage, [read_op, update_op, delete_op], return_data)
        self._read_op = read_op
        self._update_op = update_op
        self._delete_op = delete_op

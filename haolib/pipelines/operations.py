"""Concrete operation classes for type-safe storage operations.

Each operation type has its own class with typed parameters.
This allows for better type safety and IDE autocomplete.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar, overload

if TYPE_CHECKING:
    from haolib.pipelines.base import Operation, Pipeline, TargetBoundOperation
    from haolib.storages.indexes.abstract import SearchIndex
    from haolib.storages.targets.abstract import AbstractDataTarget
else:
    # Import SearchIndex for runtime (needed for isinstance checks)
    from haolib.storages.indexes.abstract import SearchIndex

# Import Operation for runtime (needed for class inheritance)
# Must be imported after TYPE_CHECKING block to avoid circular import
from haolib.pipelines.base import Operation, Pipeline, TargetBoundOperation

T_Data = TypeVar("T_Data")


@dataclass(frozen=True)
class CreateOperation[T_Data](Operation[Iterable[T_Data], list[T_Data]]):
    """Create data in storage.

    Example:
        ```python
        users = [User(id=1, name="Alice"), User(id=2, name="Bob")]
        await storage.execute(CreateOperation(data=users))
        ```

    """

    data: list[T_Data]
    """Data to create."""

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: FilterOperation[T_Data],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], list[T_Data]]: ...

    @overload  # type: ignore[override]
    def __or__[T_Result](
        self,
        other: MapOperation[T_Data, T_Result],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__[T_Result](
        self,
        other: ReduceOperation[T_Data, T_Result],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], T_Result]: ...

    @overload  # type: ignore[override]
    def __or__[T_Result](
        self,
        other: TransformOperation[list[T_Data], T_Result],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], T_Result]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: TargetBoundOperation[list[T_Data]],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], list[T_Data]]: ...

    def __or__[T_NextResult](
        self,
        other: Operation[Any, T_NextResult] | TargetBoundOperation[Any],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], T_NextResult]:
        """Compose with next operation, preserving type information."""
        from haolib.pipelines.base import Pipeline  # noqa: PLC0415

        return Pipeline(first=self, second=other)


@dataclass(frozen=True)
class ReadOperation[T_Data](Operation[Any, AsyncIterator[T_Data]]):
    """Read data from storage with type-safe index.

    Example:
        ```python
        from haolib.storages.indexes import index

        user_index = index(User, age=25)
        async for user in await storage.execute(ReadOperation(search_index=user_index)):
            print(user)
        ```

    """

    search_index: SearchIndex[T_Data]
    """Typed search index. Must contain data_type."""

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: FilterOperation[T_Data],
    ) -> Pipeline[Any, AsyncIterator[T_Data], list[T_Data]]: ...

    @overload  # type: ignore[override]
    def __or__[T_Result](
        self,
        other: MapOperation[T_Data, T_Result],
    ) -> Pipeline[Any, AsyncIterator[T_Data], list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__[T_Result](
        self,
        other: ReduceOperation[T_Data, T_Result],
    ) -> Pipeline[Any, AsyncIterator[T_Data], T_Result]: ...

    @overload  # type: ignore[override]
    def __or__[T_Result](
        self,
        other: TransformOperation[AsyncIterator[T_Data], T_Result],
    ) -> Pipeline[Any, AsyncIterator[T_Data], T_Result]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: CreateOperation[T_Data],
    ) -> Pipeline[Any, AsyncIterator[T_Data], list[T_Data]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: PatchOperation[T_Data],
    ) -> Pipeline[Any, AsyncIterator[T_Data], list[T_Data]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: UpdateOperation[T_Data],
    ) -> Pipeline[Any, AsyncIterator[T_Data], list[T_Data]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: TargetBoundOperation[AsyncIterator[T_Data]],
    ) -> Pipeline[Any, AsyncIterator[T_Data], AsyncIterator[T_Data]]: ...

    def __or__[T_NextResult](
        self,
        other: Operation[Any, T_NextResult] | TargetBoundOperation[Any],
    ) -> Pipeline[Any, AsyncIterator[T_Data], T_NextResult]:
        """Compose with next operation, preserving type information."""
        from haolib.pipelines.base import Pipeline  # noqa: PLC0415

        return Pipeline(first=self, second=other)


@dataclass(frozen=True)
class PatchOperation[T_Data](Operation[Any, list[T_Data]]):
    """Partial update operation (PATCH).

    Updates only specified fields. For HTTP, uses PATCH method.
    For SQL, updates only specified columns.

    Can work in two modes:
    1. Search mode: uses search_index to find data to update
    2. Pipeline mode: uses previous_result as data to update

    Example:
        ```python
        from haolib.storages.indexes import index

        # Search mode
        user_index = index(User, id=123)
        await storage.execute(PatchOperation(
            search_index=user_index,
            patch={"is_active": True}
        ))

        # Pipeline mode (using fluent API)
        storage.read(user_index).returning() | storage.patch({"is_active": True})
        ```

    """

    search_index: SearchIndex[T_Data] | None = None
    """Index for finding data to update (if no previous_result)."""

    patch: dict[str, Any] | None = None
    """Partial update specification (if no previous_result)."""


@dataclass(frozen=True)
class UpdateOperation[T_Data](Operation[Any, list[T_Data]]):
    """Full update operation (PUT).

    Replaces entire object. For HTTP, uses PUT method.
    For SQL, updates all fields.

    Can work in two modes:
    1. Search mode: uses search_index to find data, data to update
    2. Pipeline mode: uses previous_result as data to update

    Example:
        ```python
        from haolib.storages.indexes import index

        # Search mode
        user_index = index(User, id=123)
        await storage.execute(UpdateOperation(
            search_index=user_index,
            data=User(id=123, name="John", email="john@example.com", is_active=True)
        ))

        # Pipeline mode (using fluent API)
        storage.read(user_index).returning() | storage.update(data=lambda u: User(id=u.id, name=u.name.upper()))
        ```

    """

    search_index: SearchIndex[T_Data] | None = None
    """Index for finding data to update (if no previous_result)."""

    data: T_Data | Callable[[T_Data], T_Data] | None = None
    """Full replacement data or transform function (if no previous_result)."""


@dataclass(frozen=True)
class DeleteOperation[T_Data](Operation[Any, int]):
    """Delete data from storage with type-safe index.

    Can work in two modes:
    1. Search mode: uses search_index to find data to delete
    2. Pipeline mode: uses previous_result as data to delete

    Example:
        ```python
        from haolib.storages.indexes import index

        # Search mode
        user_index = index(User, id=123)
        deleted_count = await storage.execute(DeleteOperation(search_index=user_index))

        # Pipeline mode (using fluent API)
        storage.read(user_index).returning() | storage.delete()
        ```

    """

    search_index: SearchIndex[T_Data] | None = None
    """Typed search index for finding data to delete (if no previous_result)."""


@dataclass(frozen=True)
class FilterOperation[T_Data](Operation[Iterable[T_Data], list[T_Data]]):
    """Filter data.

    Example:
        ```python
        pipeline = (
            ReadOperation(search_index=user_index)
            | FilterOperation(predicate=lambda u: u.age >= 18)
        )
        results = await storage.execute(pipeline)
        ```

    """

    predicate: Callable[[T_Data], bool]
    """Function that returns True for items to keep."""

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: FilterOperation[T_Data],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], list[T_Data]]: ...

    @overload  # type: ignore[override]
    def __or__[T_Result](
        self,
        other: MapOperation[T_Data, T_Result],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__[T_Result](
        self,
        other: ReduceOperation[T_Data, T_Result],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], T_Result]: ...

    @overload  # type: ignore[override]
    def __or__[T_Result](
        self,
        other: TransformOperation[list[T_Data], T_Result],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], T_Result]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: CreateOperation[T_Data],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], list[T_Data]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: PatchOperation[T_Data],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], list[T_Data]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: UpdateOperation[T_Data],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], list[T_Data]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: TargetBoundOperation[list[T_Data]],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], list[T_Data]]: ...

    def __or__[T_NextResult](
        self,
        other: Operation[Any, T_NextResult] | TargetBoundOperation[Any],
    ) -> Pipeline[Iterable[T_Data], list[T_Data], T_NextResult]:
        """Compose with next operation, preserving type information."""
        from haolib.pipelines.base import Pipeline  # noqa: PLC0415

        return Pipeline(first=self, second=other)


@dataclass(frozen=True)
class MapOperation[T_Data, T_Result](Operation[Iterable[T_Data], list[T_Result]]):
    """Apply function to each element.

    Example:
        ```python
        pipeline = (
            ReadOperation(search_index=user_index)
            | MapOperation(mapper=lambda user, idx: user.email)
        )
        emails = await storage.execute(pipeline)
        ```

    """

    mapper: Callable[[T_Data, int], T_Result]
    """Function (item, index) -> result."""

    @overload  # type: ignore[override]
    def __or__[T_NextResult](
        self,
        other: MapOperation[T_Result, T_NextResult],
    ) -> Pipeline[Iterable[T_Data], list[T_Result], list[T_NextResult]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: FilterOperation[T_Result],
    ) -> Pipeline[Iterable[T_Data], list[T_Result], list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__[T_ReduceResult](
        self,
        other: ReduceOperation[T_Result, T_ReduceResult],
    ) -> Pipeline[Iterable[T_Data], list[T_Result], T_ReduceResult]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: ReduceOperation[Any, Any],
    ) -> Pipeline[Iterable[T_Data], list[T_Result], Any]: ...

    @overload  # type: ignore[override]
    def __or__[T_TransformResult](
        self,
        other: TransformOperation[list[T_Result], T_TransformResult],
    ) -> Pipeline[Iterable[T_Data], list[T_Result], T_TransformResult]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: CreateOperation[T_Result],
    ) -> Pipeline[Iterable[T_Data], list[T_Result], list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: PatchOperation[T_Result],
    ) -> Pipeline[Iterable[T_Data], list[T_Result], list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: UpdateOperation[T_Result],
    ) -> Pipeline[Iterable[T_Data], list[T_Result], list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: TargetBoundOperation[list[T_Result]],
    ) -> Pipeline[Iterable[T_Data], list[T_Result], list[T_Result]]: ...

    def __or__[T_NextResult](
        self,
        other: Operation[Any, T_NextResult] | TargetBoundOperation[Any],
    ) -> Pipeline[Iterable[T_Data], list[T_Result], T_NextResult]:
        """Compose with next operation, preserving type information."""
        from haolib.pipelines.base import Pipeline  # noqa: PLC0415

        return Pipeline(first=self, second=other)


@dataclass(frozen=True)
class ReduceOperation[T_Data, T_Result](Operation[Iterable[T_Data], T_Result]):
    """Reduce collection to a single value.

    Example:
        ```python
        pipeline = (
            ReadOperation(search_index=user_index)
            | MapOperation(mapper=lambda u, idx: u.age)
            | ReduceOperation(reducer=lambda acc, age: acc + age, initial=0)
        )
        total_age = await storage.execute(pipeline)
        ```

    """

    reducer: Callable[[T_Result, T_Data], T_Result]
    """Function (accumulator, item) -> accumulator."""

    initial: T_Result
    """Initial value for accumulator."""

    @overload  # type: ignore[override]
    def __or__[T_TransformResult](
        self,
        other: TransformOperation[T_Result, T_TransformResult],
    ) -> Pipeline[Iterable[T_Data], T_Result, T_TransformResult]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: FilterOperation[T_Result],
    ) -> Pipeline[Iterable[T_Data], T_Result, list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__[T_MapResult](
        self,
        other: MapOperation[T_Result, T_MapResult],
    ) -> Pipeline[Iterable[T_Data], T_Result, list[T_MapResult]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: CreateOperation[T_Result],
    ) -> Pipeline[Iterable[T_Data], T_Result, list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: TargetBoundOperation[T_Result],
    ) -> Pipeline[Iterable[T_Data], T_Result, T_Result]: ...

    def __or__[T_NextResult](
        self,
        other: Operation[Any, T_NextResult] | TargetBoundOperation[Any],
    ) -> Pipeline[Iterable[T_Data], T_Result, T_NextResult]:
        """Compose with next operation, preserving type information."""
        from haolib.pipelines.base import Pipeline  # noqa: PLC0415

        return Pipeline(first=self, second=other)


@dataclass(frozen=True)
class TransformOperation[T_Data, T_Result](Operation[T_Data, T_Result]):
    """Transform data.

    Example:
        ```python
        pipeline = (
            ReadOperation(search_index=user_index)
            | TransformOperation(transformer=lambda users: [u.model_dump() for u in users])
        )
        result = await storage.execute(pipeline)
        ```

    """

    transformer: Callable[[T_Data], T_Result]
    """Function for transformation."""

    @overload  # type: ignore[override]
    def __or__[T_NextResult](
        self,
        other: TransformOperation[T_Result, T_NextResult],
    ) -> Pipeline[T_Data, T_Result, T_NextResult]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: FilterOperation[T_Result],
    ) -> Pipeline[T_Data, T_Result, list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__[T_MapResult](
        self,
        other: MapOperation[T_Result, T_MapResult],
    ) -> Pipeline[T_Data, T_Result, list[T_MapResult]]: ...

    @overload  # type: ignore[override]
    def __or__[T_ReduceResult](
        self,
        other: ReduceOperation[T_Result, T_ReduceResult],
    ) -> Pipeline[T_Data, T_Result, T_ReduceResult]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: CreateOperation[T_Result],
    ) -> Pipeline[T_Data, T_Result, list[T_Result]]: ...

    @overload  # type: ignore[override]
    def __or__(
        self,
        other: TargetBoundOperation[T_Result],
    ) -> Pipeline[T_Data, T_Result, T_Result]: ...

    def __or__[T_NextResult](
        self,
        other: Operation[Any, T_NextResult] | TargetBoundOperation[Any],
    ) -> Pipeline[T_Data, T_Result, T_NextResult]:
        """Compose with next operation, preserving type information."""
        from haolib.pipelines.base import Pipeline  # noqa: PLC0415

        return Pipeline(first=self, second=other)

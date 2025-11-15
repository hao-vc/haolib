"""Concrete operation classes for type-safe storage operations.

Each operation type has its own class with typed parameters.
This allows for better type safety and IDE autocomplete.
"""

from collections.abc import AsyncIterator, Callable, Iterable
from dataclasses import dataclass
from typing import Any, TypeVar

from haolib.storages.indexes.abstract import SearchIndex
from haolib.storages.operations.base import Operation

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


@dataclass(frozen=True)
class UpdateOperation[T_Data](Operation[Any, list[T_Data]]):
    """Update data in storage with type-safe patch.

    Patch can be:
    - dict: Simple field updates
    - Callable: Transform function

    Example:
        ```python
        from haolib.storages.indexes import index

        user_index = index(User, id=123)
        await storage.execute(UpdateOperation(
            search_index=user_index,
            patch={"is_active": True}
        ))
        ```

    """

    search_index: SearchIndex[T_Data]
    """Index for finding data to update."""

    patch: dict[str, Any] | Callable[[T_Data], T_Data]
    """Update specification (normalized to dict or callable)."""


@dataclass(frozen=True)
class DeleteOperation[T_Data](Operation[Any, int]):
    """Delete data from storage with type-safe index.

    Example:
        ```python
        from haolib.storages.indexes import index

        user_index = index(User, id=123)
        deleted_count = await storage.execute(DeleteOperation(search_index=user_index))
        ```

    """

    search_index: SearchIndex[T_Data]
    """Typed search index for finding data to delete."""


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

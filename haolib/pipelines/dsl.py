"""DSL functions for creating pipeline operations.

These functions provide a convenient way to create Python-side operations
(filter, map, reduce, transform) for use in pipelines.

CRUD operations (create, read, update, patch, delete) are now available
through the fluent API on storage objects:
- storage.create() instead of createo()
- storage.read() instead of reado()
- storage.update() instead of updateo()
- storage.patch() instead of patcho()
- storage.delete() instead of deleteo()

Example:
    ```python
    from haolib.pipelines import filtero, mapo, reduceo, transformo
    from haolib.storages.indexes.params import ParamIndex

    # Pipeline with Python operations
    pipeline = (
        storage.read(ParamIndex(User, age=25)).returning()
        | filtero(lambda u: u.age >= 18)
        | mapo(lambda u, _idx: u.name)
        | reduceo(lambda acc, name: acc + name, "")
    )
    result = await pipeline.execute()
    ```

"""

from collections.abc import Callable
from typing import Any, TypeVar

from haolib.pipelines.operations import (
    FilterOperation,
    MapOperation,
    ReduceOperation,
    TransformOperation,
)

T_Data = TypeVar("T_Data")
T_Result = TypeVar("T_Result")


def filtero[T_Data](
    predicate: Callable[[Any], bool],
) -> FilterOperation[T_Data]:
    """Filter data.

    Args:
        predicate: Function that returns True for items to keep.

    Returns:
        FilterOperation for filtering data.

    Example:
        ```python
        # Filter users by age
        pipeline = (
            storage.read(ParamIndex(User)).returning()
            | filtero(lambda u: u.age >= 18)
        )
        adults = await pipeline.execute()
        ```

    """
    return FilterOperation(predicate=predicate)  # type: ignore[arg-type]


def mapo[T_Data, T_Result](
    mapper: Callable[[Any, int], Any],
) -> MapOperation[T_Data, T_Result]:
    """Map data to another form.

    Args:
        mapper: Function that transforms each item. Receives (item, index).

    Returns:
        MapOperation for mapping data.

    Example:
        ```python
        # Extract names from users
        pipeline = (
            storage.read(ParamIndex(User)).returning()
            | mapo(lambda u, _idx: u.name)
        )
        names = await pipeline.execute()
        ```

    """
    return MapOperation(mapper=mapper)  # type: ignore[arg-type]


def reduceo[T_Data, T_Result](
    reducer: Callable[[Any, Any], Any],
    initial: T_Result,
) -> ReduceOperation[T_Data, T_Result]:
    """Reduce data to a single value.

    Args:
        reducer: Function that accumulates values. Receives (accumulator, item).
        initial: Initial value for accumulation.

    Returns:
        ReduceOperation for reducing data.

    Example:
        ```python
        # Sum ages of users
        pipeline = (
            storage.read(ParamIndex(User)).returning()
            | reduceo(lambda acc, u: acc + u.age, 0)
        )
        total_age = await pipeline.execute()
        ```

    """
    return ReduceOperation(reducer=reducer, initial=initial)  # type: ignore[arg-type]


def transformo[T_Data, T_Result](
    transformer: Callable[[list[Any]], Any],
) -> TransformOperation[list[T_Data], T_Result]:
    """Transform entire collection.

    Args:
        transformer: Function that transforms the entire list.

    Returns:
        TransformOperation for transforming collections.

    Example:
        ```python
        # Calculate statistics
        pipeline = (
            storage.read(ParamIndex(User)).returning()
            | transformo(lambda users: {
                "count": len(users),
                "avg_age": sum(u.age for u in users) / len(users) if users else 0
            })
        )
        stats = await pipeline.execute()
        ```

    """
    return TransformOperation(transformer=transformer)  # type: ignore[arg-type]

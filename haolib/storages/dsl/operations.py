"""DSL functions for creating storage operations.

These functions provide a convenient way to create operations with minimal code.
Operations created with these functions are just data structures that will be
executed by the storage implementation.

Example:
    ```python
    from haolib.storages.dsl import createo, reado, filtero
    from haolib.storages.indexes import index

    # Simple operations
    await storage.execute(createo([user1, user2]))

    user_index = index(User, age=25)
    async for user in await storage.execute(reado(search_index=user_index)):
        print(user)

    # Pipeline
    pipeline = (
        createo([user1, user2])
        | reado(search_index=index(User))
        | filtero(lambda u: u.age >= 18)
    )
    await storage.execute(pipeline)
    ```

"""

from collections.abc import Callable, Iterable
from typing import TYPE_CHECKING, Any, TypeVar, overload

if TYPE_CHECKING:
    from typing import cast

from haolib.storages.dsl.patch import normalize_patch
from haolib.storages.indexes.abstract import SearchIndex
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

T_Data = TypeVar("T_Data")


def createo(
    data: Iterable[Any] | None = None,
) -> CreateOperation[Any]:
    """Create data in storage.

    Args:
        data: Optional data to create. If not provided and operation is in pipeline,
              previous_result will be used. If provided and operation is in pipeline,
              previous_result will be prepended to data.

    Returns:
        CreateOperation for creating data.

    Example:
        ```python
        # Create with explicit data
        users = [User(id=1, name="Alice"), User(id=2, name="Bob")]
        await storage.execute(createo(users))

        # Create using previous_result from pipeline
        pipeline = (
            reado(search_index=ParamIndex(User)) ^ sql_storage
            | createo() ^ s3_storage  # Uses previous_result
        )

        # Create with both previous_result and additional data
        pipeline = (
            reado(search_index=ParamIndex(User)) ^ sql_storage
            | createo([extra_user]) ^ s3_storage  # previous_result + [extra_user]
        )
        ```

    """
    return CreateOperation(data=list(data) if data is not None else [])


def reado[T_Data](
    search_index: SearchIndex[T_Data],
) -> ReadOperation[T_Data]:
    """Read data from storage with type-safe index.

    Args:
        search_index: Typed search index. Must contain data_type.

    Returns:
        ReadOperation returning AsyncIterator[T_Data].

    Example:
        ```python
        from haolib.storages.indexes import index

        # With parameter-based index
        user_index = index(User, age=25, is_active=True)
        async for user in await storage.execute(reado(search_index=user_index)):
            print(user)

        # With predefined index from registry
        index = storage.data_type_registry.get_index(User, "by_email")
        async for user in await storage.execute(reado(search_index=index)):
            print(user)
        ```

    """
    return ReadOperation(search_index=search_index)


def updateo[T_Data](
    search_index: SearchIndex[T_Data],
    patch: (
        dict[str, Any]
        | Any  # Pydantic BaseModel or dataclass instance
        | Callable[[T_Data], T_Data]
    ),
) -> UpdateOperation[T_Data]:
    """Update data in storage with type-safe patch.

    Patch can be:
    - dict: Simple field updates
    - Pydantic BaseModel: Type-safe updates (only set fields are updated)
    - dataclass: Type-safe updates
    - Callable: Transform function

    Args:
        search_index: Index for finding data to update.
        patch: Update specification.

    Returns:
        Operation returning updated data.

    Example:
        ```python
        from haolib.storages.indexes import index
        from pydantic import BaseModel

        # Pydantic model (only specified fields are updated)
        class UserUpdate(BaseModel):
            is_active: bool = True
            last_login: datetime | None = None

        user_index = index(User, id=123)
        await storage.execute(updateo(
            search_index=user_index,
            patch=UserUpdate(is_active=True)  # Only is_active will be updated
        ))

        # Dataclass
        @dataclass
        class UserUpdate:
            is_active: bool
            last_login: datetime | None = None

        await storage.execute(updateo(
            search_index=user_index,
            patch=UserUpdate(is_active=True)
        ))

        # Dict (as before)
        await storage.execute(updateo(
            search_index=user_index,
            patch={"is_active": True, "last_login": datetime.now()}
        ))

        # Transform function
        await storage.execute(updateo(
            search_index=user_index,
            patch=lambda user: User(
                **user.model_dump(),
                is_active=True,
                last_login=datetime.now()
            )
        ))

        # In pipeline (data from previous operation)
        pipeline = reado(search_index=user_index) | updateo(
            search_index=user_index,
            patch={"is_active": True}
        )
        ```

    """
    normalized_patch = normalize_patch(patch)

    return UpdateOperation(
        search_index=search_index,
        patch=normalized_patch,
    )


def deleteo[T_Data](
    search_index: SearchIndex[T_Data],
) -> DeleteOperation[T_Data]:
    """Delete data from storage with type-safe index.

    Args:
        search_index: Typed search index for finding data to delete.

    Returns:
        DeleteOperation returning number of deleted items.

    Example:
        ```python
        from haolib.storages.indexes import index

        # With parameter-based index
        user_index = index(User, id=123)
        deleted_count = await storage.execute(deleteo(search_index=user_index))

        # With predefined index from registry
        index = storage.data_type_registry.get_index(User, "by_email")
        deleted_count = await storage.execute(deleteo(search_index=index))
        ```

    """
    return DeleteOperation(search_index=search_index)


# Put Any overload first so pyright uses it when type cannot be inferred
# This ensures lambda parameters are typed as Any, not object
@overload
def filtero(
    predicate: Callable[[Any], bool],  # pyright: ignore[reportInvalidTypeVarUse]
) -> FilterOperation[Any]: ...


@overload
def filtero[T_Data](
    predicate: Callable[[T_Data], bool],
) -> FilterOperation[T_Data]: ...


def filtero(
    predicate: Callable[[Any], bool],
) -> FilterOperation[Any]:
    """Filter data.

    Args:
        predicate: Function that returns True for items to keep.

    Returns:
        FilterOperation for filtering data.

    Example:
        ```python
        # Filter users by age
        pipeline = reado(search_index=user_index) | filtero(lambda u: u.age >= 18)
        results = await storage.execute(pipeline)
        ```

    """
    return FilterOperation(predicate=predicate)


# Put Any overload first so pyright uses it when type cannot be inferred
# This ensures lambda parameters are typed as Any, not object
@overload
def mapo(
    mapper: Callable[[Any, int], Any],  # pyright: ignore[reportInvalidTypeVarUse]
) -> MapOperation[Any, Any]: ...


@overload
def mapo[T_Data, T_Result](
    mapper: Callable[[T_Data, int], T_Result],
) -> MapOperation[T_Data, T_Result]: ...


def mapo[T_Data, T_Result](
    mapper: Callable[[T_Data, int], T_Result],
) -> MapOperation[T_Data, T_Result]:
    """Apply function to each element.

    Args:
        mapper: Function (item, index) -> result.

    Returns:
        MapOperation for mapping data.

    Example:
        ```python
        # Extract emails
        pipeline = reado(search_index=user_index) | mapo(lambda user, idx: user.email)
        emails = await storage.execute(pipeline)
        ```

    """
    return MapOperation(mapper=mapper)


# Put Any overload first so pyright uses it when type cannot be inferred
# This ensures lambda parameters are typed as Any, not object
@overload
def reduceo(
    reducer: Callable[[Any, Any], Any],  # pyright: ignore[reportInvalidTypeVarUse]
    initial: Any,
) -> ReduceOperation[Any, Any]: ...


@overload
def reduceo[T_Data, T_Result](
    reducer: Callable[[T_Result, T_Data], T_Result],
    initial: T_Result,
) -> ReduceOperation[T_Data, T_Result]: ...


def reduceo[T_Data, T_Result](
    reducer: Callable[[T_Result, T_Data], T_Result],
    initial: T_Result,
) -> ReduceOperation[T_Data, T_Result]:
    """Reduce collection to a single value.

    Args:
        reducer: Function (accumulator, item) -> accumulator.
        initial: Initial value for accumulator.

    Returns:
        ReduceOperation for reducing data.

    Example:
        ```python
        # Sum ages
        pipeline = (
            reado(search_index=user_index)
            | mapo(lambda u, idx: u.age)
            | reduceo(lambda acc, age: acc + age, 0)
        )
        total_age = await storage.execute(pipeline)
        ```

    """
    return ReduceOperation(reducer=reducer, initial=initial)


# Put Any overload first so pyright uses it when type cannot be inferred
# This ensures lambda parameters are typed as Any, not object
@overload
def transformo(
    transformer: Callable[[Any], Any],  # pyright: ignore[reportInvalidTypeVarUse]
) -> TransformOperation[Any, Any]: ...


@overload
def transformo[T_Data, T_Result](
    transformer: Callable[[T_Data], T_Result],
) -> TransformOperation[T_Data, T_Result]: ...


def transformo[T_Data, T_Result](
    transformer: Callable[[T_Data], T_Result],
) -> TransformOperation[T_Data, T_Result]:
    """Transform data.

    Args:
        transformer: Function for transformation.

    Returns:
        TransformOperation for transforming data.

    Example:
        ```python
        # Transform to dictionaries
        pipeline = (
            reado(search_index=user_index)
            | transformo(lambda users: [u.model_dump() for u in users])
        )
        result = await storage.execute(pipeline)
        ```

    """
    return TransformOperation(transformer=transformer)

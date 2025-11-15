"""Decorators for creating type-safe index functions.

Provides decorators to create index functions with automatic type detection
and parameter passing. Works with all index types.
"""

import dataclasses
from collections.abc import Callable
from inspect import signature
from typing import Any, ParamSpec, TypeVar, get_args, get_origin, get_type_hints

from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.indexes.abstract import SearchIndex
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.indexes.path import PathIndex
from haolib.storages.indexes.sql import SQLQueryIndex
from haolib.storages.indexes.vector import VectorSearchIndex

T_Data = TypeVar("T_Data")
P = ParamSpec("P")

# Registry of known index types
_INDEX_TYPES = {
    SQLQueryIndex,
    VectorSearchIndex,
    PathIndex,
    ParamIndex,
}


def _get_index_type_from_hint(return_type: Any) -> type[SearchIndex[Any]] | None:
    """Extract index type from return type hint.

    Examples:
        SQLQueryIndex[User] -> SQLQueryIndex
        SearchIndex[User] -> None (too generic)

    """
    origin = get_origin(return_type)
    if origin is None:
        if return_type in _INDEX_TYPES:
            return return_type
        return None

    if origin in _INDEX_TYPES:
        return origin

    return None


def _get_data_type_from_hint(return_type: Any) -> type[Any] | None:
    """Extract data type from return type hint.

    Examples:
        SQLQueryIndex[User] -> User
        SearchIndex[User] -> User

    """
    args = get_args(return_type)
    if args and len(args) > 0:
        return args[0]
    return None


def index[T_Data](
    data_type: type[T_Data] | None = None,
    index_name: str | None = None,
    *,
    registry: DataTypeRegistry | None = None,
) -> Callable[[Callable[..., SearchIndex[T_Data] | None]], Callable[..., SearchIndex[T_Data]]]:
    """Decorator to create type-safe index functions for any index type.

    Works with all index types: SQLQueryIndex, VectorSearchIndex, PathIndex, ParamIndex.

    Function can:
    1. Explicitly return an index (allows changing implementation)
    2. Return None - decorator will auto-create index from function parameters

    Args:
        data_type: Data type for the index (auto-detected from return type if None).
        index_name: Index name (auto-detected from function name if None).
        registry: Optional registry to register the index.

    Returns:
        Decorated function that returns SearchIndex.

    Example:
        # SQLQueryIndex - explicit creation (can change implementation)
        @index(User, "by_email")
        def by_email(email: str) -> SQLQueryIndex[User]:
            return SQLQueryIndex(
                data_type=User,
                index_name="by_email",
                query=select(UserModel).where(UserModel.email == email)
            )

        # VectorSearchIndex - automatic creation from parameters
        @index(Article, "semantic")
        def semantic_search(query_text: str, limit: int = 10) -> VectorSearchIndex[Article]:
            # *Decorator will create VectorSearchIndex(query_text=query_text, limit=limit, ...)
            pass

        # PathIndex - automatic creation
        @index(Document, "by_path")
        def by_path(path: str) -> PathIndex[Document]:
            # Decorator will create PathIndex(path=path, ...)
            pass

        # ParamIndex - automatic creation
        @index(User, "by_params")
        def by_params(age: int, is_active: bool = True) -> ParamIndex[User]:
            # Decorator will create ParamIndex(age=age, is_active=is_active, ...)
            pass

        # Usage
        idx = by_email("john@example.com")  # SQLQueryIndex[User]
        idx2 = semantic_search("machine learning", limit=20)  # VectorSearchIndex[Article]

    """

    def decorator(func: Callable[..., SearchIndex[T_Data] | None]) -> Callable[..., SearchIndex[T_Data]]:
        sig = signature(func)
        hints = get_type_hints(func, include_extras=True)

        # Get return type
        return_type = hints.get("return", None)

        # Determine index type
        index_type = _get_index_type_from_hint(return_type) if return_type else None

        # Determine data_type
        resolved_data_type = data_type
        if not resolved_data_type and return_type:
            resolved_data_type = _get_data_type_from_hint(return_type)

        if not resolved_data_type:
            msg = (
                f"Cannot determine data_type for {func.__name__}. "
                f"Please specify data_type explicitly or use return type hint like SQLQueryIndex[User]."
            )
            raise ValueError(msg)

        # Determine index_name
        name = index_name or func.__name__

        def wrapper(*args: Any, **kwargs: Any) -> SearchIndex[T_Data]:
            # Call function
            result = func(*args, **kwargs)

            # If function returned an index - use it (can change implementation)
            if isinstance(result, SearchIndex):
                return result

            # If function returned None or nothing - create index automatically
            if index_type is None:
                # Default to ParamIndex
                return ParamIndex(data_type=resolved_data_type, index_name=name, **kwargs)

            # Create index of the required type from function parameters
            # Function parameters are passed to index constructor
            index_params = {"data_type": resolved_data_type, "index_name": name, **kwargs}

            # Check that all required fields are present
            # (for dataclass these are fields without default)
            if dataclasses.is_dataclass(index_type):
                fields = dataclasses.fields(index_type)
                required_fields = {
                    f.name
                    for f in fields
                    if f.default == dataclasses.MISSING and f.default_factory == dataclasses.MISSING
                }
                required_fields.discard("data_type")  # We always pass
                required_fields.discard("index_name")  # We always pass

                missing = required_fields - set(index_params.keys())
                if missing:
                    msg = (
                        f"Cannot auto-create {index_type.__name__}: "
                        f"missing required parameters: {missing}. "
                        f"Function should explicitly return the index or provide these parameters."
                    )
                    raise ValueError(msg)

            # Create index
            try:
                return index_type(**index_params)
            except TypeError as e:
                msg = (
                    f"Cannot auto-create {index_type.__name__} from function parameters. "
                    f"Function should explicitly return the index. Error: {e}"
                )
                raise ValueError(msg) from e

        # Save metadata using magic attributes
        wrapper.__haolib_index_data_type__ = resolved_data_type  # type: ignore[attr-defined]
        wrapper.__haolib_index_name__ = name  # type: ignore[attr-defined]
        wrapper.__haolib_index_registry__ = registry  # type: ignore[attr-defined]
        wrapper.__haolib_index_type__ = index_type  # type: ignore[attr-defined]
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper.__signature__ = sig  # type: ignore[attr-defined]
        wrapper.__annotations__ = func.__annotations__

        # Register index in registry if provided
        if registry is not None:
            registry.register_index(resolved_data_type, name, wrapper)

        return wrapper

    return decorator


def indexes[T_Data](
    data_type: type[T_Data],
    *,
    registry: DataTypeRegistry | None = None,
) -> Callable[[type], type]:
    """Decorator for class with index methods.

    Args:
        data_type: Data type for all indexes in the class.
        registry: Optional registry to register indexes.

    Returns:
        Decorated class.

    Example:
        @indexes(User)
        class UserIndexes:
            @index()  # index_name = "by_email"
            def by_email(self, email: str) -> SQLQueryIndex[User]:
                return SQLQueryIndex(...)

            @index("by_age")
            def by_age(self, age: int) -> ParamIndex[User]:
                # Automatic ParamIndex creation
                pass

        # Usage
        user_indexes = UserIndexes()
        idx = user_indexes.by_email("john@example.com")

    """

    def decorator(cls: type) -> type:
        cls.__haolib_index_data_type__ = data_type  # type: ignore[attr-defined]
        cls.__haolib_index_registry__ = registry  # type: ignore[attr-defined]

        # Register all index methods that were decorated with @index
        if registry is not None:
            for attr_name in dir(cls):
                if attr_name.startswith("_"):
                    continue
                attr = getattr(cls, attr_name)
                if callable(attr) and hasattr(attr, "__haolib_index_name__"):
                    index_name = attr.__haolib_index_name__  # type: ignore[attr-defined]
                    # Register the method (will be called with instance)
                    registry.register_index(data_type, index_name, attr)  # type: ignore[arg-type]

        return cls

    return decorator

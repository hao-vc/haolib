"""Data type registry for the storages."""

from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass
from functools import wraps
from typing import TYPE_CHECKING, Any, ParamSpec, TypeVar

if TYPE_CHECKING:
    from haolib.storages.indexes.abstract import SearchIndex

P = ParamSpec("P")
T_Storage = TypeVar("T_Storage")
T_User = TypeVar("T_User")
T_Data = TypeVar("T_Data")


@dataclass(frozen=True)
class TypeRegistration[T_Storage, T_User]:
    """Registration entry for a data type mapping."""

    storage_type: type[T_Storage]
    user_type: type[T_User]
    to_storage: Callable[[T_User], T_Storage]
    from_storage: Callable[[T_Storage], T_User]


class DataTypeRegistry:
    """Registry for data types."""

    def __init__(self) -> None:
        self._storage_to_users: dict[type, list[TypeRegistration]] = defaultdict(list)
        self._user_to_storages: dict[type, list[TypeRegistration]] = defaultdict(list)
        self._indexes: dict[type, dict[str, Callable[..., SearchIndex[Any]]]] = defaultdict(dict)
        # Temporary storage for to_storage/from_storage pairs before both are registered
        self._pending_mappings: dict[tuple[type, type], dict[str, Callable[..., Any]]] = {}

    def register[T_Storage, T_User](
        self,
        storage_type: type[T_Storage],
        user_type: type[T_User],
        to_storage: Callable[[T_User], T_Storage],
        from_storage: Callable[[T_Storage], T_User],
    ) -> None:
        """Register type mapping - allows multiple mappings."""
        registration = TypeRegistration(
            storage_type=storage_type,
            user_type=user_type,
            to_storage=to_storage,
            from_storage=from_storage,
        )
        self._storage_to_users[storage_type].append(registration)
        self._user_to_storages[user_type].append(registration)

    def maps_to[T_Storage, T_User](
        self,
        user_type: type[T_User],
        to_storage: Callable[[T_User], T_Storage] | None = None,
        from_storage: Callable[[T_Storage], T_User] | None = None,
    ) -> Callable[..., type[T_Storage]]:
        """Decorator to register storage type mapping to user type.

        Decorator goes ON storage type (SQL model, etc.), not on user type.

        Usage:
            @registry.maps_to(User)
            class UserModel(SQLAlchemyMappedModel[User]):
                __tablename__ = "users"
                id: Mapped[int] = mapped_column(primary_key=True)
                name: Mapped[str] = mapped_column(String(255))

                @classmethod
                def create_from(cls, from_value: User) -> Self:
                    return cls(**from_value.model_dump())

                def convert(self) -> User:
                    return User(id=self.id, name=self.name)

            # bytes -> str
            @registry.maps_to(str)
            class BytesStorage:
                pass

            registry.register(
                storage_type=bytes,
                user_type=str,
                to_storage=lambda s: s.encode(),
                from_storage=lambda b: b.decode(),
            )
        """

        def decorator(storage_type: type[T_Storage]) -> type[T_Storage]:
            # Определяем функции преобразования
            if to_storage and from_storage:
                # Кастомные функции
                self.register(
                    storage_type=storage_type,
                    user_type=user_type,
                    to_storage=to_storage,
                    from_storage=from_storage,
                )
            else:
                msg = (
                    f"Cannot automatically determine conversion for {storage_type} -> {user_type}. "
                    "Please provide to_storage and from_storage functions."
                )
                raise ValueError(msg)

            return storage_type

        return decorator

    def get_for_storage_type(
        self,
        storage_type: type,
        user_type: type | None = None,
    ) -> TypeRegistration | None:
        """Get registration for storage type.

        Args:
            storage_type: Storage type to find mapping for.
            user_type: Optional user type to resolve conflict.
                      If None and multiple mappings exist, returns first.

        Returns:
            TypeRegistration or None if not found.

        Raises:
            ValueError: If multiple mappings exist and user_type not specified.

        """
        registrations = self._storage_to_users.get(storage_type, [])

        if not registrations:
            return None

        if len(registrations) == 1:
            return registrations[0]

        # Несколько маппингов - нужен user_type для разрешения
        if user_type is None:
            msg = (
                f"Multiple mappings found for {storage_type}. "
                f"Please specify user_type. Available: {[r.user_type for r in registrations]}"
            )
            raise ValueError(msg)

        # Ищем по user_type
        for reg in registrations:
            if reg.user_type == user_type:
                return reg

        msg = f"No mapping found for {storage_type} -> {user_type}. Available: {[r.user_type for r in registrations]}"
        raise ValueError(msg)

    def get_for_user_type(
        self,
        user_type: type,
        storage_type: type | None = None,
    ) -> TypeRegistration | None:
        """Get registration for user type.

        Args:
            user_type: User type to find mapping for.
            storage_type: Optional storage type to resolve conflict.
                        If None and multiple mappings exist, returns first.

        Returns:
            TypeRegistration or None if not found.

        Raises:
            ValueError: If multiple mappings exist and storage_type not specified.

        """
        registrations = self._user_to_storages.get(user_type, [])

        if not registrations:
            return None

        if len(registrations) == 1:
            return registrations[0]

        # Несколько маппингов - нужен storage_type для разрешения
        if storage_type is None:
            msg = (
                f"Multiple mappings found for {user_type}. "
                f"Please specify storage_type. Available: {[r.storage_type for r in registrations]}"
            )
            raise ValueError(msg)

        # Ищем по storage_type
        for reg in registrations:  # Один storage_type -> список user_type (может быть несколько)
            if reg.storage_type == storage_type:
                return reg

        msg = (
            f"No mapping found for {user_type} -> {storage_type}. Available: {[r.storage_type for r in registrations]}"
        )
        raise ValueError(msg)

    def register_index[T_Data](
        self,
        data_type: type[T_Data],
        index_name: str,
        index_func: Callable[..., SearchIndex[T_Data]],
    ) -> None:
        """Register an index function.

        Args:
            data_type: Data type for the index.
            index_name: Name of the index.
            index_func: Function that returns SearchIndex when called.

        Raises:
            ValueError: If index with same name already registered.

        Example:
            @index(User, "by_email", registry=registry)
            def by_email(email: str) -> SQLQueryIndex[User]:
                return SQLQueryIndex(...)

            # Or manually:
            registry.register_index(User, "by_email", by_email)

        """
        if index_name in self._indexes[data_type]:
            msg = f"Index {index_name!r} for {data_type.__name__} is already registered"
            raise ValueError(msg)
        self._indexes[data_type][index_name] = index_func  # type: ignore[assignment]

    def get_index[T_Data](
        self,
        data_type: type[T_Data],
        index_name: str,
    ) -> Callable[..., SearchIndex[T_Data]] | None:
        """Get registered index function.

        Args:
            data_type: Data type for the index.
            index_name: Name of the index.

        Returns:
            Index function or None if not found.

        Example:
            index_func = registry.get_index(User, "by_email")
            if index_func:
                idx = index_func("john@example.com")
                await storage.execute(reado(search_index=idx))

        """
        return self._indexes.get(data_type, {}).get(index_name)  # type: ignore[return-value]

    def list_indexes(self, data_type: type) -> list[str]:
        """List all registered index names for a data type.

        Args:
            data_type: Data type to list indexes for.

        Returns:
            List of index names.

        Example:
            index_names = registry.list_indexes(User)
            # ['by_email', 'by_age', 'by_name']

        """
        return list(self._indexes.get(data_type, {}).keys())

    def to_storage[T_User, T_Storage](
        self,
        user_type: type[T_User],
        storage_type: type[T_Storage],
    ) -> Callable[[Callable[[T_User], T_Storage]], Callable[[T_User], T_Storage]]:
        """Decorator to register to_storage mapping function.

        FastAPI-style decorator for registering domain-to-storage conversion.

        Args:
            user_type: Domain type (e.g., User).
            storage_type: Storage type (e.g., UserModel).

        Returns:
            Decorator function.

        Example:
            ```python
            registry = DataTypeRegistry()

            @registry.to_storage(User, UserModel)
            def to_storage(user: User) -> UserModel:
                return UserModel(id=user.id, name=user.name, age=user.age, email=user.email)
            ```

        """
        key = (user_type, storage_type)

        def decorator(func: Callable[[T_User], T_Storage]) -> Callable[[T_User], T_Storage]:
            # Store the function temporarily
            if key not in self._pending_mappings:
                self._pending_mappings[key] = {}
            self._pending_mappings[key]["to_storage"] = func

            # If from_storage is already registered, complete the registration
            if "from_storage" in self._pending_mappings.get(key, {}):
                from_storage_func = self._pending_mappings[key]["from_storage"]
                self.register(
                    storage_type=storage_type,
                    user_type=user_type,
                    to_storage=func,
                    from_storage=from_storage_func,
                )
                # Clean up temporary storage
                del self._pending_mappings[key]

            @wraps(func)
            def wrapper(user: T_User) -> T_Storage:
                return func(user)

            return wrapper

        return decorator

    def from_storage[T_User, T_Storage](
        self,
        user_type: type[T_User],
        storage_type: type[T_Storage],
    ) -> Callable[[Callable[[T_Storage], T_User]], Callable[[T_Storage], T_User]]:
        """Decorator to register from_storage mapping function.

        FastAPI-style decorator for registering storage-to-domain conversion.

        Args:
            user_type: Domain type (e.g., User).
            storage_type: Storage type (e.g., UserModel).

        Returns:
            Decorator function.

        Example:
            ```python
            registry = DataTypeRegistry()

            @registry.from_storage(User, UserModel)
            def from_storage(model: UserModel) -> User:
                return User(id=model.id, name=model.name, age=model.age, email=model.email)
            ```

        """
        key = (user_type, storage_type)

        def decorator(func: Callable[[T_Storage], T_User]) -> Callable[[T_Storage], T_User]:
            # Store the function temporarily
            if key not in self._pending_mappings:
                self._pending_mappings[key] = {}
            self._pending_mappings[key]["from_storage"] = func

            # If to_storage is already registered, complete the registration
            if "to_storage" in self._pending_mappings.get(key, {}):
                to_storage_func = self._pending_mappings[key]["to_storage"]
                self.register(
                    storage_type=storage_type,
                    user_type=user_type,
                    to_storage=to_storage_func,
                    from_storage=func,
                )
                # Clean up temporary storage
                del self._pending_mappings[key]

            @wraps(func)
            def wrapper(model: T_Storage) -> T_User:
                return func(model)

            return wrapper

        return decorator

    def index[T_Data](
        self,
        data_type: type[T_Data],
    ) -> Callable[[Callable[P, "SearchIndex[T_Data]"]], Callable[P, "SearchIndex[T_Data]"]]:
        """Decorator to register index function.

        FastAPI-style decorator for registering search indexes.
        Automatically extracts index name from function name.

        Args:
            data_type: Data type for the index (e.g., User).

        Returns:
            Decorator function.

        Example:
            ```python
            registry = DataTypeRegistry()

            @registry.index(User)
            def by_email(email: str) -> SQLQueryIndex[User]:
                return SQLQueryIndex(
                    query=select(UserModel).where(UserModel.email == email)
                )

            # Usage
            idx = by_email("john@example.com")
            # Or through registry
            idx = registry.get_index(User, "by_email")("john@example.com")
            ```

        """
        from haolib.storages.indexes.abstract import SearchIndex

        def decorator(func: Callable[P, SearchIndex[T_Data]]) -> Callable[P, SearchIndex[T_Data]]:
            # Extract index name from function name
            index_name = func.__name__

            # Register the index
            self.register_index(data_type, index_name, func)

            @wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> SearchIndex[T_Data]:
                return func(*args, **kwargs)

            return wrapper

        return decorator

"""Protocols for fluent storage API operations."""

from collections.abc import AsyncIterator, Callable
from typing import Any, Protocol, TypeVar, runtime_checkable

from haolib.storages.fluent.composites import DeleteComposite, PatchComposite, UpdateComposite
from haolib.storages.indexes.abstract import SearchIndex

T_Data = TypeVar("T_Data")


@runtime_checkable
class ReadOperatable(Protocol):
    """Storage that supports read operations."""

    def read[T_Data](self, index: SearchIndex[T_Data]) -> ReadComposite[T_Data]: ...


@runtime_checkable
class CreateOperatable(Protocol):
    """Storage that supports create operations."""

    def create[T_Data](self, data: list[T_Data] | None = None) -> CreateComposite[T_Data]: ...


@runtime_checkable
class UpdateOperatable(Protocol):
    """Storage that supports update operations."""

    def update[T_Data](
        self,
        data: T_Data | dict[str, Any] | Callable[[T_Data], T_Data] | None = None,
    ) -> UpdateComposite[T_Data]: ...


@runtime_checkable
class PatchOperatable(Protocol):
    """Storage that supports patch operations."""

    def patch[T_Data](
        self,
        patch: dict[str, Any] | Any | None = None,
    ) -> PatchComposite[T_Data]: ...


@runtime_checkable
class DeleteOperatable(Protocol):
    """Storage that supports delete operations."""

    def delete[T_Data](self) -> DeleteComposite[T_Data]: ...

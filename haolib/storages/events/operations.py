"""Storage operation events."""

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from haolib.components.events import ComponentEventResult
from haolib.storages.abstract import AbstractStorage
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
from haolib.storages.transactions import Transaction


# Helper function for default composer
def _default_composer(
    event: Any,
) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
    """Default composer function for events."""
    return lambda results: results[-1] if results else ComponentEventResult(event=event, result=None)


# Create events
@dataclass(frozen=True)
class BeforeCreateEvent:
    """Event emitted before create operation."""

    component: AbstractStorage
    operation: CreateOperation[Any]
    transaction: Transaction | None = None
    identifier: str = "storage.before_create"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


@dataclass(frozen=True)
class AfterCreateEvent:
    """Event emitted after create operation."""

    component: AbstractStorage
    operation: CreateOperation[Any]
    result: list[Any]
    transaction: Transaction | None = None
    identifier: str = "storage.after_create"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


# Read events
@dataclass(frozen=True)
class BeforeReadEvent:
    """Event emitted before read operation."""

    component: AbstractStorage
    operation: ReadOperation[Any]
    transaction: Transaction | None = None
    identifier: str = "storage.before_read"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


@dataclass(frozen=True)
class AfterReadEvent:
    """Event emitted after read operation."""

    component: AbstractStorage
    operation: ReadOperation[Any]
    result: Any  # AsyncIterator
    transaction: Transaction | None = None
    identifier: str = "storage.after_read"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


# Update events
@dataclass(frozen=True)
class BeforeUpdateEvent:
    """Event emitted before update operation."""

    component: AbstractStorage
    operation: UpdateOperation[Any]
    transaction: Transaction | None = None
    identifier: str = "storage.before_update"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


@dataclass(frozen=True)
class AfterUpdateEvent:
    """Event emitted after update operation."""

    component: AbstractStorage
    operation: UpdateOperation[Any]
    result: list[Any]
    transaction: Transaction | None = None
    identifier: str = "storage.after_update"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


# Delete events
@dataclass(frozen=True)
class BeforeDeleteEvent:
    """Event emitted before delete operation."""

    component: AbstractStorage
    operation: DeleteOperation[Any]
    transaction: Transaction | None = None
    identifier: str = "storage.before_delete"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


@dataclass(frozen=True)
class AfterDeleteEvent:
    """Event emitted after delete operation."""

    component: AbstractStorage
    operation: DeleteOperation[Any]
    result: int
    transaction: Transaction | None = None
    identifier: str = "storage.after_delete"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


# Filter events
@dataclass(frozen=True)
class BeforeFilterEvent:
    """Event emitted before filter operation."""

    component: AbstractStorage
    operation: FilterOperation[Any]
    input_data: Any
    transaction: Transaction | None = None
    identifier: str = "storage.before_filter"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


@dataclass(frozen=True)
class AfterFilterEvent:
    """Event emitted after filter operation."""

    component: AbstractStorage
    operation: FilterOperation[Any]
    result: list[Any]
    transaction: Transaction | None = None
    identifier: str = "storage.after_filter"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


# Map events
@dataclass(frozen=True)
class BeforeMapEvent:
    """Event emitted before map operation."""

    component: AbstractStorage
    operation: MapOperation[Any, Any]
    input_data: Any
    transaction: Transaction | None = None
    identifier: str = "storage.before_map"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


@dataclass(frozen=True)
class AfterMapEvent:
    """Event emitted after map operation."""

    component: AbstractStorage
    operation: MapOperation[Any, Any]
    result: list[Any]
    transaction: Transaction | None = None
    identifier: str = "storage.after_map"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


# Reduce events
@dataclass(frozen=True)
class BeforeReduceEvent:
    """Event emitted before reduce operation."""

    component: AbstractStorage
    operation: ReduceOperation[Any, Any]
    input_data: Any
    transaction: Transaction | None = None
    identifier: str = "storage.before_reduce"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


@dataclass(frozen=True)
class AfterReduceEvent:
    """Event emitted after reduce operation."""

    component: AbstractStorage
    operation: ReduceOperation[Any, Any]
    result: Any
    transaction: Transaction | None = None
    identifier: str = "storage.after_reduce"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


# Transform events
@dataclass(frozen=True)
class BeforeTransformEvent:
    """Event emitted before transform operation."""

    component: AbstractStorage
    operation: TransformOperation[Any, Any]
    input_data: Any
    transaction: Transaction | None = None
    identifier: str = "storage.before_transform"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)


@dataclass(frozen=True)
class AfterTransformEvent:
    """Event emitted after transform operation."""

    component: AbstractStorage
    operation: TransformOperation[Any, Any]
    result: Any
    transaction: Transaction | None = None
    identifier: str = "storage.after_transform"

    @property
    def composer(
        self,
    ) -> Callable[[list[ComponentEventResult[Any, Any]]], ComponentEventResult[Any, Any]]:
        """Composer function."""
        return _default_composer(self)

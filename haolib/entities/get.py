"""Entities get."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any, Self

from haolib.batches.entities import EntityBatch
from haolib.entities.base import BaseEntity, HasId

if TYPE_CHECKING:
    from collections.abc import Sequence


class BaseEntityGet[T_Id, T_Entity: BaseEntity](HasId[T_Id]):
    """Base entity get."""

    @classmethod
    @abc.abstractmethod
    async def from_entity(cls, entity: T_Entity, *args: Any, **kwargs: Any) -> Self:
        """Get entity get from entity."""


class BaseBulkEntityGet[T_Id, T_Entity: BaseEntity, T_EntityGet: BaseEntityGet]:
    """Base bulk entity get."""

    entities: Sequence[T_EntityGet]

    @classmethod
    @abc.abstractmethod
    async def from_batch(cls, batch: EntityBatch[T_Id, T_Entity], *args: Any, **kwargs: Any) -> Self:
        """Get bulk entity get from batch."""

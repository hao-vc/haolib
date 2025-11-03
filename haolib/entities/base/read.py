"""Entities read."""

import abc
from typing import Any, Self

from haolib.batches.batch import Batch
from haolib.entities.base import BaseEntity, HasId


class BaseEntityRead[T_Id, T_Entity: BaseEntity](HasId[T_Id]):
    """Base entity read."""

    @classmethod
    @abc.abstractmethod
    async def from_entity(cls, entity: T_Entity, *args: Any, **kwargs: Any) -> Self:
        """Get entity read from entity."""


class BaseBulkEntityRead[T_Id, T_Entity: BaseEntity, T_EntityRead: BaseEntityRead]:
    """Base bulk entity read."""

    @classmethod
    @abc.abstractmethod
    async def from_batch(cls, batch: Batch[T_Id, T_Entity], *args: Any, **kwargs: Any) -> Self:
        """Get bulk entity read from batch."""

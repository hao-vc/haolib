"""Entities create."""

import abc
from typing import TYPE_CHECKING, Any

from haolib.batches.batch import Batch
from haolib.entities.base import BaseEntity

if TYPE_CHECKING:
    from collections.abc import Iterable


class BaseEntityCreate[T_Id, T_Entity: BaseEntity](abc.ABC):
    """Base entity create."""

    @abc.abstractmethod
    async def create_entity(self, *args: Any, **kwargs: Any) -> T_Entity:
        """Create entity and return the created entity."""


class BaseBulkEntityCreate[T_Id, T_Entity: BaseEntity, T_EntityCreate: BaseEntityCreate](abc.ABC):
    """Base bulk entity create."""

    @abc.abstractmethod
    async def get_entity_creates(self) -> Iterable[T_EntityCreate]:
        """Get entities to create."""

    async def create_batch(self, *args: Any, **kwargs: Any) -> Batch[T_Id, T_Entity]:
        """Create entities and return batch of the created entities."""

        return Batch[T_Id, T_Entity](key_getter=lambda entity: entity.id).merge_list(
            [await entity_create.create_entity(*args, **kwargs) for entity_create in await self.get_entity_creates()]
        )

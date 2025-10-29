"""Entities create."""

import abc
from typing import TYPE_CHECKING, Any

from haolib.batches.entities import EntityBatch
from haolib.entities.base import BaseEntity

if TYPE_CHECKING:
    from collections.abc import Iterable


class BaseEntityCreate[T_Id, T_Entity: BaseEntity]:
    """Base entity create."""

    @abc.abstractmethod
    async def create_entity(self, *args: Any, **kwargs: Any) -> T_Entity:
        """Create entity and return the created entity."""


class BaseBulkEntityCreate[T_Id, T_Entity: BaseEntity, T_EntityCreate: BaseEntityCreate]:
    """Base bulk entity create."""

    @abc.abstractmethod
    async def get_entities(self) -> Iterable[T_EntityCreate]:
        """Get entities to create."""

    async def create_batch(self, *args: Any, **kwargs: Any) -> EntityBatch[T_Id, T_Entity]:
        """Create entities and return batch of the created entities."""

        return EntityBatch[T_Id, T_Entity]().merge_list(
            [await entity_create.create_entity(*args, **kwargs) for entity_create in await self.get_entities()]
        )

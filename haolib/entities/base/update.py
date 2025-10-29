"""Entities update."""

import abc
from collections.abc import Iterable
from typing import Any

from haolib.batches.entities import EntityBatch
from haolib.entities.base import BaseEntity, HasId


class BaseEntityUpdate[T_Id, T_Entity: BaseEntity](HasId[T_Id]):
    """Base entity update."""

    @abc.abstractmethod
    async def update_entity(self, entity: T_Entity, *args: Any, **kwargs: Any) -> T_Entity:
        """Update entity and return the updated entity."""


class BaseBulkEntityUpdate[T_Id, T_Entity: BaseEntity, T_EntityUpdate: BaseEntityUpdate]:
    """Base bulk entity update."""

    @abc.abstractmethod
    async def get_entities(self) -> Iterable[T_EntityUpdate]:
        """Get entities to update."""

    async def update_batch(
        self,
        batch: EntityBatch[T_Id, T_Entity],
        *args: Any,
        **kwargs: Any,
    ) -> EntityBatch[T_Id, T_Entity]:
        """Update entities in batch and return the updated batch."""

        return EntityBatch[T_Id, T_Entity]().merge_list(
            [
                await entity.update_entity(batch.get_by_key(entity.id, exception=ValueError), *args, **kwargs)
                for entity in await self.get_entities()
            ]
        )

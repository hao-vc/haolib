"""Entities update."""

import abc
from typing import TYPE_CHECKING, Any

from haolib.batches.batch import Batch
from haolib.entities.base import BaseEntity, HasId

if TYPE_CHECKING:
    from collections.abc import Iterable


class BaseEntityUpdate[T_Id, T_Entity: BaseEntity](HasId[T_Id], abc.ABC):
    """Base entity update."""

    @abc.abstractmethod
    async def update_entity(self, entity: T_Entity, *args: Any, **kwargs: Any) -> T_Entity:
        """Update entity and return the updated entity."""


class BaseBulkEntityUpdate[T_Id, T_Entity: BaseEntity, T_EntityUpdate: BaseEntityUpdate](abc.ABC):
    """Base bulk entity update."""

    @abc.abstractmethod
    async def get_entity_updates(self) -> Iterable[T_EntityUpdate]:
        """Get entities to update."""

    async def update_batch(
        self,
        batch: Batch[T_Id, T_Entity],
        *args: Any,
        **kwargs: Any,
    ) -> Batch[T_Id, T_Entity]:
        """Update entities in batch and return the updated batch."""

        return Batch[T_Id, T_Entity](key_getter=lambda entity: entity.id).merge_list(
            [
                await entity_update.update_entity(
                    batch.get_by_key(entity_update.id, exception=ValueError), *args, **kwargs
                )
                for entity_update in await self.get_entity_updates()
            ]
        )

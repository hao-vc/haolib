"""Entities update."""

from __future__ import annotations

import abc
from typing import TYPE_CHECKING, Any

from haolib.batches.entities import EntityBatch
from haolib.entities.base import BaseEntity, HasId

if TYPE_CHECKING:
    from collections.abc import Sequence


class BaseEntityUpdate[T_Id, T_Entity: BaseEntity](HasId[T_Id]):
    """Base entity update."""

    @abc.abstractmethod
    async def update_entity(self, entity: T_Entity, *args: Any, **kwargs: Any) -> T_Entity:
        """Update entity."""


class BaseBulkEntityUpdate[T_Id, T_Entity: BaseEntity]:
    """Base bulk entity update."""

    entities: Sequence[BaseEntityUpdate[T_Id, T_Entity]]

    async def update_batch(
        self,
        batch: EntityBatch[T_Id, T_Entity],
        *args: Any,  # noqa: ARG002
        **kwargs: Any,  # noqa: ARG002
    ) -> EntityBatch[T_Id, T_Entity]:
        """Update entities."""

        return EntityBatch([await entity.update_entity((await batch.to_dict())[entity.id]) for entity in self.entities])

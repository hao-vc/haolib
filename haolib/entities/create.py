"""Entities create."""

from __future__ import annotations

import abc
from typing import Any

from haolib.batches.entities import EntityBatch
from haolib.entities.base import BaseEntity


class BaseEntityCreate[T_Id, T_Entity: BaseEntity]:
    """Base entity create."""

    @abc.abstractmethod
    async def create_entity(self, *args: Any, **kwargs: Any) -> T_Entity:
        """Create entity."""


class BaseBulkEntityCreate[T_Id, T_Entity: BaseEntity, T_EntityCreate: BaseEntityCreate]:
    """Base bulk entity create."""

    entities: list[T_EntityCreate]

    async def create_batch(self, *args: Any, **kwargs: Any) -> EntityBatch[T_Id, T_Entity]:
        """Create entities."""

        return EntityBatch().merge_list(
            [await entity_create.create_entity(*args, **kwargs) for entity_create in self.entities]
        )

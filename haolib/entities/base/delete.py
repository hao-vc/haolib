"""Entities delete."""

import abc
from collections.abc import Iterable

from haolib.entities.base import BaseEntity


class BaseEntityDelete[T_Id, T_Entity: BaseEntity]:
    """Base entity delete."""

    id: T_Id


class BaseBulkEntityDelete[T_Id, T_Entity: BaseEntity, T_EntityDelete: BaseEntityDelete]:
    """Base bulk entity delete."""

    @abc.abstractmethod
    async def get_entity_deletes(self) -> Iterable[T_EntityDelete]:
        """Get entities to delete."""

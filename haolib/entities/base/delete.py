"""Entities delete."""

import abc
from collections.abc import Iterable

from haolib.entities.base import BaseEntity, HasId


class BaseEntityDelete[T_Id, T_Entity: BaseEntity](HasId[T_Id], abc.ABC):
    """Base entity delete."""


class BaseBulkEntityDelete[T_Id, T_Entity: BaseEntity, T_EntityDelete: BaseEntityDelete](abc.ABC):
    """Base bulk entity delete."""

    @abc.abstractmethod
    async def get_entity_deletes(self) -> Iterable[T_EntityDelete]:
        """Get entities to delete."""

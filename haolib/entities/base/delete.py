"""Entities delete."""

from haolib.entities.base import BaseEntity


class BaseEntityDelete[T_Id, T_Entity: BaseEntity]:
    """Base entity delete."""


class BaseBulkEntityDelete[T_Id, T_Entity: BaseEntity, T_EntityDelete: BaseEntityDelete]:
    """Base bulk entity delete."""

    entities: list[T_EntityDelete]

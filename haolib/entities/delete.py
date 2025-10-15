"""Entities delete."""

from haolib.entities.base import BaseEntity


class BaseEntityDelete[T_Id, T_Entity: BaseEntity]:
    """Base entity delete."""


class BaseBulkEntityDelete[T_Id]:
    """Base bulk entity delete."""

"""Base entity."""

import abc
from datetime import datetime


class HasId[T_Id]:
    """Has id."""

    id: T_Id


class BaseEntity[T_Id](HasId[T_Id], abc.ABC):
    """Base entity."""


class BaseEntityDateTimeMixin:
    """Base entity with date time."""

    created_at: datetime
    updated_at: datetime

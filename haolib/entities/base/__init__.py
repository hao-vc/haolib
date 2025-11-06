"""Entities base."""

import abc


class HasId[T_Id](abc.ABC):
    """Has id."""

    id: T_Id


class BaseEntity[T_Id](HasId[T_Id], abc.ABC):
    """Entity."""

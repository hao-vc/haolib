"""Entities base."""

import abc
from typing import Protocol


class HasId[T_Id](Protocol):
    """Has id."""

    id: T_Id


class BaseEntity[T_Id](HasId[T_Id], abc.ABC):
    """Entity."""

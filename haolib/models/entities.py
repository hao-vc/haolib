"""Models for entities."""

import abc
from typing import Self

from sqlalchemy.orm import Mapped

from haolib.entities.base import BaseEntity
from haolib.models.base import AbstractModel


class EntityModel[T_Id, T_Entity: BaseEntity](AbstractModel):
    """Model for entities."""

    id: Mapped[T_Id]

    @classmethod
    @abc.abstractmethod
    def from_entity(cls, entity: T_Entity) -> Self:
        """Create a model instance from an entity."""

    @abc.abstractmethod
    def to_entity(self) -> T_Entity:
        """Convert the model to an entity."""

    @abc.abstractmethod
    def update_from_entity(self, entity: T_Entity) -> Self:
        """Update the model from an entity."""

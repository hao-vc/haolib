"""Models for entities."""

import abc
from typing import TYPE_CHECKING, Any, ClassVar, Self

if TYPE_CHECKING:
    from sqlalchemy.orm import Mapped

from haolib.models.base import AbstractModel


class BaseEntityModel[T_Id, T_Entity](AbstractModel):
    """Model for entities."""

    __abstract__: ClassVar[bool] = True

    if TYPE_CHECKING:
        id: Mapped[T_Id]

    @classmethod
    @abc.abstractmethod
    def from_entity(cls, entity: T_Entity, *args: Any, **kwargs: Any) -> Self:
        """Create a model instance from an entity."""

    @abc.abstractmethod
    def to_entity(self, *args: Any, **kwargs: Any) -> T_Entity:
        """Convert the model to an entity."""

    @abc.abstractmethod
    def update_from_entity(self, entity: T_Entity, *args: Any, **kwargs: Any) -> Self:
        """Update the model from an entity."""

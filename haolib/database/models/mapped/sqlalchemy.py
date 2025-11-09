"""SQLAlchemy mapped models."""

import abc
from typing import Any, ClassVar, Self

from haolib.database.models.base.sqlalchemy import SQLAlchemyBaseModel


class SQLAlchemyMappedModel[T_MappedTo](SQLAlchemyBaseModel):
    """SQLAlchemy mapped model."""

    __abstract__: ClassVar[bool] = True

    @classmethod
    @abc.abstractmethod
    def create_from(cls, from_value: T_MappedTo, *args: Any, **kwargs: Any) -> Self:
        """Create a model instance from a from value."""

    @abc.abstractmethod
    def convert(self, *args: Any, **kwargs: Any) -> T_MappedTo:
        """Convert the model to a mapped to value."""


class SQLAlchemyUpdateableMappedModel[T_MappedTo](SQLAlchemyBaseModel):
    """SQLAlchemy updateable mapped model."""

    __abstract__: ClassVar[bool] = True

    @abc.abstractmethod
    def update_from(self, from_value: T_MappedTo, *args: Any, **kwargs: Any) -> Self:
        """Update the model from a from value."""

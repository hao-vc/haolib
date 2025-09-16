"""Base abstractions for database models.

This module provides the foundation for all database models in the application,
with common methods and behaviors for consistent data handling.
"""

import typing as t
from typing import Any, ClassVar

from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeBase


class AbstractModel(DeclarativeBase):
    """Base abstract model for database entities.

    Provides common functionality for all database models, including
    serialization, deserialization, and basic representation methods.

    All models extending this class inherit SQLAlchemy's declarative base
    and gain common utility methods.

    Attributes:
        __abstract__: SQLAlchemy marker indicating this is an abstract base class

    """

    __abstract__: ClassVar[bool] = True

    def __repr__(self) -> str:
        """Generate a string representation of the model.

        Returns:
            A string representation including the class name and primary key values

        """
        _repr = f"<{self.__class__.__name__} "
        for name in self._get_primary_keys():
            _repr += f"{name}={self._get_key_value(name)}, "
        return _repr[:-2] + ">"

    def __str__(self) -> str:
        """Return a string representation of the model.

        Returns:
            The result of __repr__ for consistency

        """
        return self.__repr__()

    def to_dict(self) -> dict[str, Any]:
        """Convert the model to a dictionary.

        Returns:
            A dictionary representation of the model's attributes

        """
        return self.__dict__

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> t.Self:
        """Create a model instance from a dictionary.

        Args:
            data: Dictionary containing attribute values

        Returns:
            A new instance of the model

        """
        return cls(**data)

    @classmethod
    def from_schema(cls, model: BaseModel) -> t.Self:
        """Create a model instance from a Pydantic schema.

        Args:
            model: A Pydantic model instance

        Returns:
            A new instance of the database model

        """
        return cls.from_dict(model.model_dump())

    @classmethod
    def _get_primary_keys(cls) -> list[str]:
        """Get the names of the model's primary key columns.

        Returns:
            A list of primary key column names

        """
        return [i.name for i in cls.__table__.primary_key.columns.values()]  # type: ignore[attr-defined]

    def _get_key_value(self, name: str) -> Any:
        """Get the value of a specific attribute.

        Args:
            name: The name of the attribute

        Returns:
            The value of the specified attribute

        """
        return getattr(self, name)

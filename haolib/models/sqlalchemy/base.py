"""Base abstractions for database models.

This module provides the foundation for all database models in the application,
with common methods and behaviors for consistent data handling.
"""

from typing import Any, ClassVar

from sqlalchemy.orm import DeclarativeBase


class SQLAlchemyBaseModel(DeclarativeBase):
    """Base declarative base model for database entities.

    Provides common functionality for all database models, including
    serialization, deserialization, and basic representation methods.

    All models extending this class inherit SQLAlchemy's declarative base
    and gain common utility methods.


    """

    __abstract__: ClassVar[bool] = True

    def __repr__(self) -> str:
        """Generate a string representation of the model.

        Returns:
            str: A string representation including the class name and primary key values

        """
        _repr = f"<{self.__class__.__name__} "
        for name in self._get_primary_keys():
            _repr += f"{name}={self._get_key_value(name)}, "
        return _repr[:-2] + ">"

    def __str__(self) -> str:
        """Return a string representation of the model.

        Returns:
            str: The result of __repr__ for consistency

        """
        return self.__repr__()

    def to_dict(self) -> dict[str, Any]:
        """Convert the model to a dictionary.

        Returns:
            dict[str, Any]: A dictionary representation of the model's attributes

        """
        return self.__dict__

    @classmethod
    def _get_primary_keys(cls) -> list[str]:
        """Get the names of the model's primary key columns.

        Returns:
            list[str]: A list of primary key column names

        """
        return [i.name for i in cls.__table__.primary_key.columns.values()]  # type: ignore[attr-defined]

    def _get_key_value(self, name: str) -> Any:
        """Get the value of a specific attribute.

        Args:
            name: The name of the attribute

        Returns:
            Any: The value of the specified attribute

        """
        return getattr(self, name)

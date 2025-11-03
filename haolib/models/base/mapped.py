"""Models for entities."""

import abc
from typing import Any, Protocol, Self


class BaseCreatableFrom[T_From](Protocol):
    """Model for creatable from mapped."""

    @classmethod
    @abc.abstractmethod
    def create_from(cls, from_value: T_From, *args: Any, **kwargs: Any) -> Self:
        """Create a model instance from a from value."""


class BaseConvertable[T_To](Protocol):
    """Model for convertable."""

    @abc.abstractmethod
    def convert(self, *args: Any, **kwargs: Any) -> T_To:
        """Convert the model."""


class BaseUpdateableFrom[T_From](Protocol):
    """Model for updatable from mapped."""

    @abc.abstractmethod
    def update_from(self, from_value: T_From, *args: Any, **kwargs: Any) -> Self:
        """Update the model from a from value."""


class BaseMappedModel[T_MappedTo](BaseCreatableFrom[T_MappedTo], BaseConvertable[T_MappedTo]):
    """Mapped model."""


class BaseUpdateableMappedModel[T_MappedTo](BaseMappedModel[T_MappedTo], BaseUpdateableFrom[T_MappedTo]):
    """Updateable mapped model."""

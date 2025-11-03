"""Models for entities."""

from typing import Any, Protocol, Self


class AbstractCreatableFrom[T_From](Protocol):
    """Model for creatable from mapped."""

    @classmethod
    def create_from(cls, from_value: T_From, *args: Any, **kwargs: Any) -> Self:
        """Create a model instance from a from value."""
        ...


class AbstractConvertable[T_To](Protocol):
    """Model for convertable."""

    def convert(self, *args: Any, **kwargs: Any) -> T_To:
        """Convert the model."""
        ...


class AbstractUpdateableFrom[T_From](Protocol):
    """Model for updatable from mapped."""

    def update_from(self, from_value: T_From, *args: Any, **kwargs: Any) -> Self:
        """Update the model from a from value."""
        ...


class BaseMappedModel[T_MappedTo](AbstractCreatableFrom[T_MappedTo], AbstractConvertable[T_MappedTo]):
    """Mapped model."""


class BaseUpdateableMappedModel[T_MappedTo](BaseMappedModel[T_MappedTo], AbstractUpdateableFrom[T_MappedTo]):
    """Updateable mapped model."""

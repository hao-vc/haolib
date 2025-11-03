"""Models for entities."""

from typing import Any, Protocol, Self


class AbstractCreatableFrom[T_From](Protocol):
    """Abstract model for creatable from mapped."""

    @classmethod
    def create_from(cls, from_value: T_From, *args: Any, **kwargs: Any) -> Self:
        """Create a model instance from a from value."""
        ...


class AbstractConvertable[T_To](Protocol):
    """Abstract model for convertable."""

    def convert(self, *args: Any, **kwargs: Any) -> T_To:
        """Convert the model."""
        ...


class AbstractUpdateableFrom[T_From](Protocol):
    """Abstract model for updatable from mapped."""

    def update_from(self, from_value: T_From, *args: Any, **kwargs: Any) -> Self:
        """Update the model from a from value."""
        ...


class AbstractMappedModel[T_MappedTo](AbstractCreatableFrom[T_MappedTo], AbstractConvertable[T_MappedTo]):
    """Abstract mapped model."""


class AbstractUpdateableMappedModel[T_MappedTo](AbstractMappedModel[T_MappedTo], AbstractUpdateableFrom[T_MappedTo]):
    """Abstract updateable mapped model."""

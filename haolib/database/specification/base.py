"""Specification pattern implementation."""

import re
from abc import ABC, abstractmethod
from typing import Any, Self

from haolib.enums.filter import OrderByType
from haolib.utils.rattrs import rgetattr


class BaseSpecification(ABC):
    """The base specification class to implement the Specification pattern."""

    field: str
    value: Any

    def __init__(self, field: str, value: Any) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (Any): A value of the field to use in is_satisfied_by.

        """
        self.field = field
        self.value = value

    @abstractmethod
    def is_satisfied_by(self, obj: Any) -> bool:
        """Whether the obj specifies the specification condition.

        Args:
            obj (Any): Any object.

        Returns:
            bool: The result of applying the predicate to the object.

        """


class InvertibleSpecification(BaseSpecification):
    """Specification that can be inverted."""

    is_inverted: bool

    def __init__(self, field: str, value: Any, is_inverted: bool = False) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (Any): A value of the field to use in is_satisfied_by.
            is_inverted (bool, optional): Whether the specification is inverted. Defaults to False.

        """
        super().__init__(field, value)
        self.is_inverted = is_inverted

    def __invert__(self) -> Self:
        """Invert the specification."""
        self.is_inverted = not self.is_inverted
        return self


class EqualsSpecification(InvertibleSpecification):
    """Specification that checks if the field of an object is equal to a value.

    Might be inverted using the `~` operator.

    Example:
        >>> spec = EqualsSpecification("name", "John")
        >>> spec.is_satisfied_by(TestObject(name="John"))
        True
        >>> ~spec.is_satisfied_by(TestObject(name="John"))
        False

    """

    def __init__(self, field: str, value: Any, is_inverted: bool = False) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (Any): A value of the field to use in is_satisfied_by.
            is_inverted (bool, optional): Whether the specification is inverted. Defaults to False.

        """
        super().__init__(field, value, is_inverted)

    def is_satisfied_by(self, obj: Any) -> bool:
        """Whether the obj specifies the equals specification condition.

        Args:
            obj (Any): Any object.

        Returns:
            bool: The result of applying the predicate to the object.

        """
        return (
            rgetattr(obj, self.field) == self.value if not self.is_inverted else rgetattr(obj, self.field) != self.value
        )


class GreaterThanSpecification(InvertibleSpecification):
    """Specification that checks if the field of an object is greater than a value."""

    def __init__(self, field: str, value: Any, is_inverted: bool = False) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (Any): A value of the field to use in is_satisfied_by.
            is_inverted (bool, optional): Whether the specification is inverted. Defaults to False.

        """
        super().__init__(field, value, is_inverted)

    def is_satisfied_by(self, obj: Any) -> bool:
        """Whether the obj specifies the greater than specification condition.

        Args:
            obj (Any): Any object.

        Returns:
            bool: The result of applying the predicate to the object.

        """
        return (
            rgetattr(obj, self.field) > self.value if not self.is_inverted else rgetattr(obj, self.field) <= self.value
        )


class LessThanSpecification(InvertibleSpecification):
    """Specification that checks if the field of an object is less than a value."""

    def __init__(self, field: str, value: Any, is_inverted: bool = False) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (Any): A value of the field to use in is_satisfied_by.
            is_inverted (bool, optional): Whether the specification is inverted. Defaults to False.

        """
        super().__init__(field, value, is_inverted)

    def is_satisfied_by(self, obj: Any) -> bool:
        """Whether the obj specifies the less than specification condition.

        Args:
            obj (Any): Any object.

        Returns:
            bool: The result of applying the predicate to the object.

        """
        return (
            rgetattr(obj, self.field) < self.value if not self.is_inverted else rgetattr(obj, self.field) >= self.value
        )


class GreaterThanOrEqualsToSpecification(InvertibleSpecification):
    """Specification that checks if the field of an object is greater than or equals to a value."""

    def __init__(self, field: str, value: Any, is_inverted: bool = False) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (Any): A value of the field to use in is_satisfied_by.
            is_inverted (bool, optional): Whether the specification is inverted. Defaults to False.

        """
        super().__init__(field, value, is_inverted)

    def is_satisfied_by(self, obj: Any) -> bool:
        """Whether the obj specifies the greater than or equals to specification condition.

        Args:
            obj (Any): Any object.

        Returns:
            bool: The result of applying the predicate to the object.

        """
        return (
            rgetattr(obj, self.field) >= self.value if not self.is_inverted else rgetattr(obj, self.field) < self.value
        )


class LessThanOrEqualsToSpecification(InvertibleSpecification):
    """Specification that checks if the field of an object is less than or equals to a value."""

    def __init__(self, field: str, value: Any, is_inverted: bool = False) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (Any): A value of the field to use in is_satisfied_by.
            is_inverted (bool, optional): Whether the specification is inverted. Defaults to False.

        """
        super().__init__(field, value, is_inverted)

    def is_satisfied_by(self, obj: Any) -> bool:
        """Whether the obj specifies the less than or equals to specification condition.

        Args:
            obj (Any): Any object.

        Returns:
            bool: The result of applying the predicate to the object.

        """
        return (
            rgetattr(obj, self.field) <= self.value if not self.is_inverted else rgetattr(obj, self.field) > self.value
        )


class InListSpecification(InvertibleSpecification):
    """Specification that checks if the field of an object is in a value, where value is a list."""

    def __init__(self, field: str, value: list[Any], is_inverted: bool = False) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (Any): A value of the field to use in is_satisfied_by.
            is_inverted (bool, optional): Whether the specification is inverted. Defaults to False.

        """
        super().__init__(field, value, is_inverted)

    def is_satisfied_by(self, obj: Any) -> bool:
        """Whether the obj specifies the in list specification condition.

        Args:
            obj (Any): Any object.

        Returns:
            bool: The result of applying the predicate to the object.

        """

        return (
            rgetattr(obj, self.field) in self.value
            if not self.is_inverted
            else rgetattr(obj, self.field) not in self.value
        )


class SubListSpecification(InvertibleSpecification):
    """Specification that checks if the field of an object is a sublist of a value, where value is a list."""

    def __init__(self, field: str, value: list[Any], is_inverted: bool = False) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (Any): A value of the field to use in is_satisfied_by.
            is_inverted (bool, optional): Whether the specification is inverted. Defaults to False.

        """
        super().__init__(field, value, is_inverted)

    def is_satisfied_by(self, obj: Any) -> bool:
        """Whether the obj specifies the sublist specification condition.

        Args:
            obj (Any): Any object.

        Returns:
            bool: The result of applying the predicate to the object.

        """

        return (
            set(self.value).issubset(set(rgetattr(obj, self.field)))
            if not self.is_inverted
            else not set(self.value).issubset(set(rgetattr(obj, self.field)))
        )


class LikeSpecification(InvertibleSpecification):
    """Specification that checks if the field is like a value. Works as the SQL LIKE operator."""

    def __init__(self, field: str, value: str, is_inverted: bool = False) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (str): A value of the field to use in is_satisfied_by.
            is_inverted (bool, optional): Whether the specification is inverted. Defaults to False.

        """
        super().__init__(field, value, is_inverted)

    def is_satisfied_by(self, obj: Any) -> bool:
        """Whether the obj specifies the like specification condition.

        Args:
            obj (Any): Any object.

        Returns:
            bool: The result of applying the predicate to the object.

        """
        return (
            re.match(self.value.replace("%", ".*").replace(".", r"\."), rgetattr(obj, self.field)) is not None
            if not self.is_inverted
            else re.match(self.value.replace("%", ".*").replace(".", r"\."), rgetattr(obj, self.field)) is None
        )


class ILikeSpecification(InvertibleSpecification):
    """Specification that checks if the field is ilike a value. Works as the SQL ILIKE operator."""

    def __init__(self, field: str, value: str, is_inverted: bool = False) -> None:
        """Initialize the specification object.

        Args:
            field (str): A field to use in is_satisfied_by.
            value (str): A value of the field to use in is_satisfied_by.
            is_inverted (bool, optional): Whether the specification is inverted. Defaults to False.

        """
        super().__init__(field, value, is_inverted)

    def is_satisfied_by(self, obj: Any) -> bool:
        """Whether the obj specifies the ilike specification condition.

        Args:
            obj (Any): Any object.

        Returns:
            bool: The result of applying the predicate to the object.

        """
        return (
            re.match(self.value.lower().replace("%", ".*").replace(".", r"\."), rgetattr(obj, self.field).lower())
            is not None
            if not self.is_inverted
            else re.match(self.value.lower().replace("%", ".*").replace(".", r"\."), rgetattr(obj, self.field).lower())
            is None
        )


class OrderBySpecification:
    """Order by specification."""

    field: str
    type: OrderByType

    def __init__(self, field: str, type: OrderByType) -> None:
        self.field = field
        self.type = type

"""Recursive attributes for objects."""

import functools
from typing import Any


def rgetattr(obj: object, attr: str, *args: list[Any]) -> Any:
    """Recursively get an attribute from an object.

    Args:
        obj (objecy): Object to get the attribute from.
        attr (str): Attribute to get.
        *args (list[Any]): Arguments to pass to getattr.

    Returns:
        Any: The value of the attribute.

    """

    def _getattr(obj: object, attr: str) -> Any:
        return getattr(obj, attr, *args)

    # Type ignoring since mypy doesn't understand that the attribute can only be a string
    return functools.reduce(_getattr, [obj, *attr.split(".")])  # type: ignore[arg-type]

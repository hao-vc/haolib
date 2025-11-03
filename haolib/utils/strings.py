"""String case utils."""

import re


def to_constant_case(string: str) -> str:
    """Convert string to constant case."""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", string).upper()

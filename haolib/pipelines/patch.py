"""Patch normalization for update operations."""

import dataclasses
from collections.abc import Callable, Mapping
from typing import Any

from pydantic import BaseModel


def normalize_patch(patch: Any) -> dict[str, Any] | Callable:
    """Normalize patch to dict or keep as callable.

    Supports:
    - dict/Mapping: Simple field updates
    - Pydantic BaseModel: Type-safe updates (only set fields are updated)
    - dataclass: Type-safe updates
    - Callable: Transform function (kept as-is)

    Args:
        patch: Patch specification.

    Returns:
        Normalized patch (dict or callable).

    Raises:
        TypeError: If patch type is not supported.

    Example:
        ```python
        # Pydantic model
        class UserUpdate(BaseModel):
            is_active: bool
        normalize_patch(UserUpdate(is_active=True))  # {"is_active": True}

        # Dataclass
        @dataclass
        class UserUpdate:
            is_active: bool
        normalize_patch(UserUpdate(is_active=True))  # {"is_active": True}

        # Dict
        normalize_patch({"is_active": True})  # {"is_active": True}

        # Function (kept as-is)
        normalize_patch(lambda u: User(**u.model_dump(), is_active=True))  # function
        ```

    """
    # If it's a callable (function), keep it as-is
    if callable(patch) and not isinstance(patch, type):
        return patch

    # Pydantic model
    if isinstance(patch, BaseModel):
        # exclude_unset=True - only explicitly set fields
        # exclude_none=False - include None values
        return patch.model_dump(exclude_unset=True, exclude_none=False)

    # Dataclass instance
    if dataclasses.is_dataclass(patch) and not isinstance(patch, type):
        return dataclasses.asdict(patch)

    # Dict or Mapping
    if isinstance(patch, Mapping):
        return dict(patch)

    msg = f"Patch must be dict, BaseModel, dataclass, or callable, got {type(patch)}"
    raise TypeError(msg)

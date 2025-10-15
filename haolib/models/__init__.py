"""Database model abstractions and base classes.

This package provides base classes for database models that can be
used across the application to ensure consistent data structures
and behavior.
"""

from haolib.models.base import AbstractModel
from haolib.models.entities import EntityModel

__all__ = ["AbstractModel", "EntityModel"]

"""Database model abstractions and base classes.

This package provides base classes for database models that can be
used across the application to ensure consistent data structures
and behavior.
"""

from haolib.database.models.base.sqlalchemy import SQLAlchemyBaseModel

__all__ = ["SQLAlchemyBaseModel"]

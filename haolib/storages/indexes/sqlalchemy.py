"""Index handler for SQLAlchemy storage.

Converts various index types to SQLAlchemy queries.
"""

from typing import Any

from sqlalchemy import Select, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import DeclarativeBase

from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.indexes.abstract import SearchIndex
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.indexes.sql import SQLQueryIndex


class IndexHandler:
    """Handler for converting indexes to SQLAlchemy queries."""

    def __init__(self, registry: DataTypeRegistry) -> None:
        """Initialize the index handler.

        Args:
            registry: Data type registry for converting user types to storage models.

        """
        self._registry = registry

    async def build_query[T_Data](
        self,
        index: SearchIndex[T_Data],
        session: AsyncSession,
    ) -> Select[tuple[Any]]:
        """Convert index to SQLAlchemy query.

        Args:
            index: Search index to convert.
            session: SQLAlchemy session (for potential query execution).

        Returns:
            SQLAlchemy Select query.

        Raises:
            TypeError: If index type is not supported.
            ValueError: If storage model is not registered for data type.

        """
        if isinstance(index, SQLQueryIndex):
            # Already a SQLAlchemy query
            return index.query

        if isinstance(index, ParamIndex):
            return await self._build_param_index_query(index, session)

        msg = f"Unsupported index type: {type(index)}"
        raise TypeError(msg)

    async def _build_param_index_query[T_Data](
        self,
        index: ParamIndex[T_Data],
        session: AsyncSession,  # noqa: ARG002
    ) -> Select[tuple[Any]]:
        """Build query from ParamIndex.

        Args:
            index: ParamIndex to convert.
            session: SQLAlchemy session.

        Returns:
            SQLAlchemy Select query.

        Raises:
            ValueError: If storage model is not registered for data type.

        """
        # Get storage model from registry
        registration = self._registry.get_for_user_type(index.data_type)
        if not registration:
            msg = f"No storage model registered for {index.data_type}"
            raise ValueError(msg)

        model: type[DeclarativeBase] = registration.storage_type
        query: Select[tuple[Any]] = select(model)

        # Apply parameters as filters
        for key, value in index.params.items():
            if hasattr(model, key):
                column = getattr(model, key)
                query = query.where(column == value)

        return query

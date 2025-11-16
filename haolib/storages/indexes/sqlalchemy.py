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

    def _extract_model_from_query(self, query: Select[Any]) -> type[DeclarativeBase] | None:
        """Extract storage model from SQLAlchemy Select query.

        Args:
            query: SQLAlchemy Select query.

        Returns:
            Storage model type, or None if cannot be extracted.

        """
        # Try to get model from query.get_final_froms() (tables/models in FROM clause)
        froms = query.get_final_froms() if hasattr(query, "get_final_froms") else getattr(query, "froms", None)
        if froms:
            for from_item in froms:
                # Check if it's a mapped class (DeclarativeBase)
                if isinstance(from_item, type) and issubclass(from_item, DeclarativeBase):
                    return from_item
                # Check if it's a Table and get mapped class from it
                if hasattr(from_item, "entity") and isinstance(from_item.entity, type):
                    if issubclass(from_item.entity, DeclarativeBase):
                        return from_item.entity

        # Try to get from column_descriptions (for Select queries)
        if hasattr(query, "column_descriptions") and query.column_descriptions:
            for desc in query.column_descriptions:
                if "entity" in desc and desc["entity"] is not None:
                    entity = desc["entity"]
                    if isinstance(entity, type) and issubclass(entity, DeclarativeBase):
                        return entity

        # Try to get from selected_columns
        if hasattr(query, "selected_columns"):
            for col in query.selected_columns:
                if hasattr(col, "entity") and col.entity is not None:
                    entity = col.entity
                    if isinstance(entity, type) and issubclass(entity, DeclarativeBase):
                        return entity

        return None

    def get_data_type_from_query(self, query: Select[Any]) -> type[Any]:
        """Get user data type from SQLAlchemy query.

        Extracts storage model from query and finds corresponding user type from registry.

        Args:
            query: SQLAlchemy Select query.

        Returns:
            User data type.

        Raises:
            ValueError: If cannot extract model from query or model is not registered.

        """
        # Extract storage model from query
        storage_model = self._extract_model_from_query(query)
        if storage_model is None:
            msg = "Cannot extract model type from SQLAlchemy query. Please ensure query uses a mapped model (e.g., select(UserModel))."
            raise ValueError(msg)

        # Find user type from registry
        registration = self._registry.get_for_storage_type(storage_model)
        if not registration:
            msg = f"Storage model {storage_model} is not registered in DataTypeRegistry"
            raise ValueError(msg)

        return registration.user_type

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

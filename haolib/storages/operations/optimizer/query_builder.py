"""Query builder for constructing optimized SQL queries from operations.

Builds optimized SQLAlchemy queries by combining ReadOperation with FilterOperation
predicates converted to SQL conditions. Also supports building UPDATE and DELETE
queries from reado | updateo/patcho/deleteo patterns.
"""

from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, update
from sqlalchemy.orm import DeclarativeBase

from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.indexes.sql import SQLQueryIndex
from haolib.storages.indexes.sqlalchemy import IndexHandler

if TYPE_CHECKING:
    from haolib.pipelines.operations import (
        DeleteOperation,
        PatchOperation,
        ReadOperation,
        UpdateOperation,
    )

# Import operations lazily to avoid circular import
# Operations are imported in methods that use them
from haolib.storages.operations.optimizer.predicate_analyzer import PredicateAnalyzer


class QueryBuilder:
    """Builds optimized SQL queries from operations.

    Combines ReadOperation with FilterOperation predicates to create
    a single optimized SQL query.

    Example:
        ```python
        builder = QueryBuilder(registry)
        read_op = ReadOperation(search_index=user_index)
        filter_op = FilterOperation(predicate=lambda u: u.age >= 18)
        optimized_query = builder.build([read_op, filter_op])
        ```

    """

    def __init__(self, registry: DataTypeRegistry) -> None:
        """Initialize the query builder.

        Args:
            registry: Data type registry for converting user types to storage models.

        """
        self._registry = registry
        self._predicate_analyzer = PredicateAnalyzer()

    async def build_async(self, operations: list[Any], session: Any) -> ReadOperation[Any] | None:
        """Build optimized ReadOperation from list of operations (async version).

        Args:
            operations: List of operations to optimize (must start with ReadOperation).
            session: SQLAlchemy session for building queries.

        Returns:
            Optimized ReadOperation with SQLQueryIndex, or None if cannot optimize.

        """
        from haolib.pipelines.operations import FilterOperation, ReadOperation  # noqa: PLC0415

        if not operations:
            return None

        # First operation must be ReadOperation
        read_op = operations[0]
        if not isinstance(read_op, ReadOperation):
            return None

        # Build base query from ReadOperation index
        index_handler = IndexHandler(self._registry)
        base_query = await index_handler.build_query(read_op.search_index, session)

        # Get storage model
        # For SQLQueryIndex, extract data_type from query
        if isinstance(read_op.search_index, SQLQueryIndex):
            # Extract model from query
            storage_model = index_handler._extract_model_from_query(base_query)
            if storage_model is None:
                return None
            registration = self._registry.get_for_storage_type(storage_model)
        else:
            registration = self._registry.get_for_user_type(read_op.search_index.data_type)

        if not registration:
            return None

        model: type[DeclarativeBase] = registration.storage_type

        # Start with base query
        query = base_query

        # Apply FilterOperation predicates as SQL conditions
        for op in operations[1:]:
            if isinstance(op, FilterOperation):
                conditions = self._predicate_analyzer.extract_conditions(op.predicate, model)
                if conditions:
                    for condition in conditions:
                        query = query.where(condition)
                else:
                    # Cannot convert predicate to SQL - cannot optimize
                    return None

        # Create optimized ReadOperation with SQLQueryIndex
        optimized_index = SQLQueryIndex[Any](query=query)

        return ReadOperation(search_index=optimized_index)

    async def build_update_async(
        self,
        read_op: ReadOperation[Any],
        filter_ops: list[Any],
        update_op: UpdateOperation[Any],
        session: Any,
    ) -> UpdateOperation[Any] | None:
        """Build optimized UPDATE query from reado | [filtero]* | updateo pattern.

        Args:
            read_op: ReadOperation to extract WHERE conditions from.
            filter_ops: List of FilterOperation to add additional conditions.
            update_op: UpdateOperation with data to update.
            session: SQLAlchemy session for building queries.

        Returns:
            Optimized UpdateOperation with SQLQueryIndex, or None if cannot optimize.

        """
        from haolib.pipelines.operations import FilterOperation, UpdateOperation  # noqa: PLC0415

        # Check if data is callable - cannot optimize callable in SQL
        if callable(update_op.data):
            return None

        # Check type compatibility - both should work with same data type
        # We'll verify this by checking if they use the same registry entry

        # Build base query from ReadOperation index
        index_handler = IndexHandler(self._registry)
        base_query = await index_handler.build_query(read_op.search_index, session)

        # Get storage model
        if isinstance(read_op.search_index, SQLQueryIndex):
            storage_model = index_handler._extract_model_from_query(base_query)
            if storage_model is None:
                return None
            registration = self._registry.get_for_storage_type(storage_model)
        else:
            registration = self._registry.get_for_user_type(read_op.search_index.data_type)

        if not registration:
            return None

        model: type[DeclarativeBase] = registration.storage_type

        # Build UPDATE statement
        update_stmt = update(model)

        # Apply WHERE conditions from ReadOperation
        if base_query.whereclause is not None:
            update_stmt = update_stmt.where(base_query.whereclause)

        # Apply WHERE conditions from FilterOperation
        for filter_op in filter_ops:
            if isinstance(filter_op, FilterOperation):
                conditions = self._predicate_analyzer.extract_conditions(filter_op.predicate, model)
                if conditions:
                    for condition in conditions:
                        update_stmt = update_stmt.where(condition)
                else:
                    # Cannot convert predicate to SQL - cannot optimize
                    return None

        # Extract update values from update_op.data
        update_values = self._extract_update_values(update_op.data, registration)
        if update_values is None:
            return None

        # Apply update values
        update_stmt = update_stmt.values(**update_values)

        # Add RETURNING clause to get updated rows
        update_stmt = update_stmt.returning(model)

        # Create optimized UpdateOperation with SQLQueryIndex
        optimized_index = SQLQueryIndex[Any](query=update_stmt)

        return UpdateOperation(search_index=optimized_index, data=update_op.data)

    async def build_patch_async(
        self,
        read_op: ReadOperation[Any],
        filter_ops: list[Any],
        patch_op: PatchOperation[Any],
        session: Any,
    ) -> PatchOperation[Any] | None:
        """Build optimized UPDATE query from reado | [filtero]* | patcho pattern.

        Args:
            read_op: ReadOperation to extract WHERE conditions from.
            filter_ops: List of FilterOperation to add additional conditions.
            patch_op: PatchOperation with patch data.
            session: SQLAlchemy session for building queries.

        Returns:
            Optimized PatchOperation with SQLQueryIndex, or None if cannot optimize.

        """
        from haolib.pipelines.operations import FilterOperation, PatchOperation  # noqa: PLC0415

        # Check if patch is callable - patcho doesn't support callable
        if callable(patch_op.patch):
            return None

        # Check type compatibility - both should work with same data type
        # We'll verify this by checking if they use the same registry entry

        # Build base query from ReadOperation index
        index_handler = IndexHandler(self._registry)
        base_query = await index_handler.build_query(read_op.search_index, session)

        # Get storage model
        if isinstance(read_op.search_index, SQLQueryIndex):
            storage_model = index_handler._extract_model_from_query(base_query)
            if storage_model is None:
                return None
            registration = self._registry.get_for_storage_type(storage_model)
        else:
            registration = self._registry.get_for_user_type(read_op.search_index.data_type)

        if not registration:
            return None

        model: type[DeclarativeBase] = registration.storage_type

        # Build UPDATE statement
        update_stmt = update(model)

        # Apply WHERE conditions from ReadOperation
        if base_query.whereclause is not None:
            update_stmt = update_stmt.where(base_query.whereclause)

        # Apply WHERE conditions from FilterOperation
        for filter_op in filter_ops:
            if isinstance(filter_op, FilterOperation):
                conditions = self._predicate_analyzer.extract_conditions(filter_op.predicate, model)
                if conditions:
                    for condition in conditions:
                        update_stmt = update_stmt.where(condition)
                else:
                    # Cannot convert predicate to SQL - cannot optimize
                    return None

        # Extract patch values
        patch_values = self._extract_patch_values(patch_op.patch)
        if patch_values is None:
            return None

        # Apply patch values (partial update)
        update_stmt = update_stmt.values(**patch_values)

        # Add RETURNING clause to get updated rows
        update_stmt = update_stmt.returning(model)

        # Create optimized PatchOperation with SQLQueryIndex
        # Note: SQLQueryIndex now supports Update statements
        optimized_index = SQLQueryIndex[Any](query=update_stmt)  # type: ignore[arg-type]

        return PatchOperation(search_index=optimized_index, patch=patch_op.patch)

    async def build_delete_async(
        self,
        read_op: ReadOperation[Any],
        filter_ops: list[Any],
        delete_op: DeleteOperation[Any],  # noqa: ARG002
        session: Any,
    ) -> DeleteOperation[Any] | None:
        """Build optimized DELETE query from reado | [filtero]* | deleteo pattern.

        Args:
            read_op: ReadOperation to extract WHERE conditions from.
            filter_ops: List of FilterOperation to add additional conditions.
            delete_op: DeleteOperation to execute.
            session: SQLAlchemy session for building queries.

        Returns:
            Optimized DeleteOperation with SQLQueryIndex, or None if cannot optimize.

        """
        from haolib.pipelines.operations import DeleteOperation, FilterOperation  # noqa: PLC0415

        # Check type compatibility - both should work with same data type
        # We'll verify this by checking if they use the same registry entry

        # Build base query from ReadOperation index
        index_handler = IndexHandler(self._registry)
        base_query = await index_handler.build_query(read_op.search_index, session)

        # Get storage model
        if isinstance(read_op.search_index, SQLQueryIndex):
            storage_model = index_handler._extract_model_from_query(base_query)
            if storage_model is None:
                return None
            registration = self._registry.get_for_storage_type(storage_model)
        else:
            registration = self._registry.get_for_user_type(read_op.search_index.data_type)

        if not registration:
            return None

        model: type[DeclarativeBase] = registration.storage_type

        # Build DELETE statement
        delete_stmt = delete(model)

        # Apply WHERE conditions from ReadOperation
        if base_query.whereclause is not None:
            delete_stmt = delete_stmt.where(base_query.whereclause)

        # Apply WHERE conditions from FilterOperation
        for filter_op in filter_ops:
            if isinstance(filter_op, FilterOperation):
                conditions = self._predicate_analyzer.extract_conditions(filter_op.predicate, model)
                if conditions:
                    for condition in conditions:
                        delete_stmt = delete_stmt.where(condition)
                else:
                    # Cannot convert predicate to SQL - cannot optimize
                    return None

        # Create optimized DeleteOperation with SQLQueryIndex
        # Note: SQLQueryIndex now supports Delete statements
        optimized_index = SQLQueryIndex[Any](query=delete_stmt)  # type: ignore[arg-type]

        return DeleteOperation(search_index=optimized_index)

    def _extract_update_values(
        self,
        data: Any,
        registration: Any,
    ) -> dict[str, Any] | None:
        """Extract update values from data object.

        Args:
            data: Data object (user type instance).
            registration: Data type registration.

        Returns:
            Dictionary of column names to values, or None if cannot extract.

        """
        if data is None:
            return None

        # Convert to storage model
        storage_obj = registration.to_storage(data)

        # Extract column values
        update_values = {}
        for column in storage_obj.__table__.columns:  # type: ignore[attr-defined]
            value = getattr(storage_obj, column.name)
            update_values[column.name] = value

        return update_values

    def _extract_patch_values(self, patch: Any) -> dict[str, Any] | None:
        """Extract patch values from patch object.

        Args:
            patch: Patch object (dict, Pydantic, or dataclass).

        Returns:
            Dictionary of column names to values, or None if cannot extract.

        """
        if patch is None:
            return None

        if isinstance(patch, dict):
            return patch

        # Try Pydantic model_dump
        if hasattr(patch, "model_dump"):
            return patch.model_dump()

        # Try dataclass asdict
        if hasattr(patch, "__dict__"):
            return patch.__dict__

        return None

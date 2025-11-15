"""Query builder for constructing optimized SQL queries from operations.

Builds optimized SQLAlchemy queries by combining ReadOperation with FilterOperation
predicates converted to SQL conditions.
"""

from typing import Any

from sqlalchemy.orm import DeclarativeBase

from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.indexes.sql import SQLQueryIndex
from haolib.storages.indexes.sqlalchemy import IndexHandler
from haolib.storages.operations.concrete import FilterOperation, ReadOperation
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
        optimized_index = SQLQueryIndex(
            data_type=read_op.search_index.data_type,
            index_name=f"optimized_{read_op.search_index.index_name}",
            query=query,
        )

        return ReadOperation(search_index=optimized_index)

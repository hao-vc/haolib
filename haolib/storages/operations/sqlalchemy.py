"""Operations handler for SQLAlchemy storage.

Handles execution of individual operations.
"""

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import selectinload

from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.events.operations import (
    AfterCreateEvent,
    AfterDeleteEvent,
    AfterFilterEvent,
    AfterMapEvent,
    AfterReadEvent,
    AfterReduceEvent,
    AfterTransformEvent,
    AfterUpdateEvent,
    BeforeCreateEvent,
    BeforeDeleteEvent,
    BeforeFilterEvent,
    BeforeMapEvent,
    BeforeReadEvent,
    BeforeReduceEvent,
    BeforeTransformEvent,
    BeforeUpdateEvent,
)
from haolib.storages.indexes.sql import SQLQueryIndex
from haolib.storages.indexes.sqlalchemy import IndexHandler
from haolib.storages.operations.concrete import (
    CreateOperation,
    DeleteOperation,
    FilterOperation,
    MapOperation,
    ReadOperation,
    ReduceOperation,
    TransformOperation,
    UpdateOperation,
)
from haolib.storages.transactions.sqlalchemy import SQLAlchemyStorageTransaction

if TYPE_CHECKING:
    from haolib.storages.sqlalchemy import SQLAlchemyStorage


class SQLAlchemyOperationsHandler:
    """Handler for executing SQLAlchemy operations."""

    def __init__(
        self,
        registry: DataTypeRegistry,
        relationship_load_depth: int = 2,
        storage: SQLAlchemyStorage | None = None,
    ) -> None:
        """Initialize the operations handler.

        Args:
            registry: Data type registry for converting between user and storage types.
            relationship_load_depth: Maximum depth for loading nested relationships in updates.
            storage: Storage instance for emitting events (optional).

        """
        self._registry = registry
        self._index_handler = IndexHandler(registry)
        self._relationship_load_depth = relationship_load_depth
        self._storage = storage

    async def execute_create[T_Data](
        self,
        operation: CreateOperation[T_Data],
        transaction: SQLAlchemyStorageTransaction,
    ) -> list[T_Data]:
        """Execute create operation.

        Args:
            operation: Create operation to execute.
            transaction: Transaction to use.

        Returns:
            List of created data (converted to user types).

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeCreateEvent(
                component=self._storage,
                operation=operation,
                transaction=transaction,
            )
            await self._storage.events.emit(before_event)

        session = await transaction.get_session()

        # Convert user types to storage models
        storage_models = []
        for item in operation.data:
            registration = self._registry.get_for_user_type(type(item))
            if registration:
                storage_model = registration.to_storage(item)
                storage_models.append(storage_model)
            else:
                # If no registration - assume it's already a storage model
                storage_models.append(item)

        # Save to database
        session.add_all(storage_models)
        await session.flush()

        # Convert back to user types
        result = []
        for model in storage_models:
            registration = self._registry.get_for_storage_type(type(model))
            if registration:
                result.append(registration.from_storage(model))
            else:
                result.append(model)

        # Emit after event
        if self._storage is not None:
            after_event = AfterCreateEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

        return result

    async def execute_read[T_Data](
        self,
        operation: ReadOperation[T_Data],
        transaction: SQLAlchemyStorageTransaction,
    ) -> AsyncIterator[T_Data]:
        """Execute read operation.

        Args:
            operation: Read operation to execute.
            transaction: Transaction to use.

        Yields:
            Data items (converted to user types).

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeReadEvent(
                component=self._storage,
                operation=operation,
                transaction=transaction,
            )
            await self._storage.events.emit(before_event)

        session = await transaction.get_session()

        # Build query from index
        query = await self._index_handler.build_query(operation.search_index, session)

        # Execute query and stream results
        result = await session.stream_scalars(query)

        # Convert results to user types
        async for row in result:
            registration = self._registry.get_for_storage_type(type(row))
            if registration:
                yield registration.from_storage(row)
            else:
                yield row

        # Emit after event (after iteration completes)
        # Note: This is a limitation - we can't know when iteration completes
        # For now, we emit before the iterator is returned
        if self._storage is not None:
            after_event = AfterReadEvent(
                component=self._storage,
                operation=operation,
                result=result,  # The original result stream
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

    async def execute_update[T_Data](  # noqa: PLR0915
        self,
        operation: UpdateOperation[T_Data],
        transaction: SQLAlchemyStorageTransaction,
    ) -> list[T_Data]:
        """Execute update operation.

        Args:
            operation: Update operation to execute.
            transaction: Transaction to use.

        Returns:
            List of updated data (converted to user types).

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeUpdateEvent(
                component=self._storage,
                operation=operation,
                transaction=transaction,
            )
            await self._storage.events.emit(before_event)

        session = await transaction.get_session()

        # Build query from index
        query = await self._index_handler.build_query(operation.search_index, session)

        # Get storage model type from registry
        # For SQLQueryIndex, extract data_type from query
        if isinstance(operation.search_index, SQLQueryIndex):
            data_type = self._index_handler.get_data_type_from_query(query)
        else:
            data_type = operation.search_index.data_type

        registration = self._registry.get_for_user_type(data_type)
        if not registration:
            msg = f"No storage model registered for {data_type}"
            raise ValueError(msg)

        model = registration.storage_type

        # Apply patch
        if isinstance(operation.patch, dict):
            # Use bulk update for dict patch (most efficient)
            update_stmt = update(model)

            # Apply where clause from Select query
            if query.whereclause is not None:
                update_stmt = update_stmt.where(query.whereclause)

            # Apply values from patch dict
            update_stmt = update_stmt.values(**operation.patch)

            # Use returning to get primary keys of updated rows
            pk_columns = list(model.__table__.primary_key.columns)  # type: ignore[attr-defined]
            if pk_columns:
                pk_column = pk_columns[0]
                update_stmt = update_stmt.returning(pk_column)  # type: ignore[assignment]

                # Execute update
                result = await session.execute(update_stmt)
                await session.flush()

                # Get IDs of updated objects
                updated_ids = [row[0] for row in result.all()]

                if not updated_ids:
                    return []

                # Reload objects with relationships using selectinload
                # Get all relationship names from model
                mapper = inspect(model)
                relationship_names = list(mapper.relationships.keys())

                # Build reload query with selectinload for all relationships
                reload_query = select(model).where(pk_column.in_(updated_ids))

                # Apply selectinload for each relationship (first level)
                for rel_name in relationship_names:
                    reload_query = reload_query.options(selectinload(getattr(model, rel_name)))

                # Load nested relationships up to configured depth
                if self._relationship_load_depth > 1:
                    for rel_name in relationship_names:
                        rel_attr = getattr(model, rel_name)
                        try:
                            # Get nested relationships
                            rel_mapper = inspect(rel_attr.property.mapper.class_)
                            nested_rel_names = list(rel_mapper.relationships.keys())
                            for nested_rel_name in nested_rel_names:
                                nested_rel_attr = getattr(rel_mapper.class_, nested_rel_name)
                                reload_query = reload_query.options(
                                    selectinload(rel_attr).selectinload(nested_rel_attr)
                                )
                        except (AttributeError, KeyError):
                            # No nested relationships or error
                            pass

                # Execute reload query
                reload_result = await session.scalars(reload_query)
                updated_models = list(reload_result.all())
            else:
                # No primary key - return all columns and refresh each
                update_stmt = update_stmt.returning(model)  # type: ignore[assignment]
                result = await session.execute(update_stmt)
                await session.flush()
                updated_models = list(result.scalars().all())
                # Refresh each to load relationships
                mapper = inspect(model)
                relationship_names = list(mapper.relationships.keys())
                for updated_model in updated_models:
                    await session.refresh(updated_model, relationship_names)

            # Convert back to user types
            result_list = []
            for updated_model in updated_models:
                if registration:
                    result_list.append(registration.from_storage(updated_model))
                else:
                    result_list.append(updated_model)

            # Emit after event
            if self._storage is not None:
                after_event = AfterUpdateEvent(
                    component=self._storage,
                    operation=operation,
                    result=result_list,
                    transaction=transaction,
                )
                await self._storage.events.emit(after_event)

            return result_list

        # Callable patch - need to load objects to apply function
        # (can't use bulk update for callable patches)
        results = await session.scalars(query)
        models = list(results.all())

        if not models:
            return []

        # Apply callable patch
        for model in models:
            model_registration = self._registry.get_for_storage_type(type(model))
            if model_registration:
                # Convert to user type
                user_obj = model_registration.from_storage(model)
                # Apply patch function
                updated = operation.patch(user_obj)
                # Convert back to storage model
                updated_model = model_registration.to_storage(updated)
                # Update model attributes
                for column in updated_model.__table__.columns:  # type: ignore[attr-defined]
                    setattr(model, column.name, getattr(updated_model, column.name))
            else:
                # No registration - apply directly
                # Type ignore: patch expects T_Data but model might not match exactly
                updated = operation.patch(model)  # type: ignore[arg-type]
                for column in model.__table__.columns:  # type: ignore[attr-defined]
                    setattr(model, column.name, getattr(updated, column.name))

        await session.flush()

        # Convert back to user types
        result_list = []
        for model in models:
            model_registration = self._registry.get_for_storage_type(type(model))
            if model_registration:
                result_list.append(model_registration.from_storage(model))
            else:
                result_list.append(model)

        # Emit after event
        if self._storage is not None:
            after_event = AfterUpdateEvent(
                component=self._storage,
                operation=operation,
                result=result_list,
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

        return result_list

    async def execute_delete[T_Data](
        self,
        operation: DeleteOperation[T_Data],
        transaction: SQLAlchemyStorageTransaction,
    ) -> int:
        """Execute delete operation.

        Args:
            operation: Delete operation to execute.
            transaction: Transaction to use.

        Returns:
            Number of deleted items.

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeDeleteEvent(
                component=self._storage,
                operation=operation,
                transaction=transaction,
            )
            await self._storage.events.emit(before_event)

        session = await transaction.get_session()

        # Build query from index
        query = await self._index_handler.build_query(operation.search_index, session)

        # Get storage model type from registry
        # For SQLQueryIndex, extract data_type from query
        if isinstance(operation.search_index, SQLQueryIndex):
            data_type = self._index_handler.get_data_type_from_query(query)
        else:
            data_type = operation.search_index.data_type

        registration = self._registry.get_for_user_type(data_type)
        if not registration:
            msg = f"No storage model registered for {data_type}"
            raise ValueError(msg)

        model = registration.storage_type

        # Extract where clause from Select query and apply to delete
        delete_stmt = delete(model)

        # Apply where clause from Select query
        if query.whereclause is not None:
            delete_stmt = delete_stmt.where(query.whereclause)

        # Use returning clause to get deleted rows and count them
        # Get primary key column(s) for returning
        pk_columns = list(model.__table__.primary_key.columns)  # type: ignore[attr-defined]
        if pk_columns:
            # Return primary key(s) to count deleted rows
            delete_stmt = delete_stmt.returning(*list(pk_columns))  # type: ignore[assignment]
            result = await session.execute(delete_stmt)
            await session.flush()
            # Count returned rows
            deleted_count = len(list(result.all()))

            # Emit after event
            if self._storage is not None:
                after_event = AfterDeleteEvent(
                    component=self._storage,
                    operation=operation,
                    result=deleted_count,
                    transaction=transaction,
                )
                await self._storage.events.emit(after_event)

            return deleted_count

        # No primary key - fallback to executing without returning
        # and using a separate count query
        # Count before deletion
        count_query = select(func.count()).select_from(query.subquery())
        count_result = await session.execute(count_query)
        count = count_result.scalar() or 0

        # Execute delete
        await session.execute(delete_stmt)
        await session.flush()

        # Emit after event
        if self._storage is not None:
            after_event = AfterDeleteEvent(
                component=self._storage,
                operation=operation,
                result=count,
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

        return count

    async def execute_filter[T_Data](
        self,
        operation: FilterOperation[T_Data],
        transaction: SQLAlchemyStorageTransaction,
        previous_result: Any,
    ) -> list[T_Data]:
        """Execute filter operation.

        Args:
            operation: Filter operation to execute.
            transaction: Transaction to use.
            previous_result: Result from previous operation (iterable or AsyncIterator).

        Returns:
            Filtered list of data.

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeFilterEvent(
                component=self._storage,
                operation=operation,
                input_data=previous_result,
                transaction=transaction,
            )
            await self._storage.events.emit(before_event)

        # Filter is executed in Python (can be optimized by storage)
        if hasattr(previous_result, "__aiter__"):
            # AsyncIterator
            result = [item async for item in previous_result if operation.predicate(item)]
        else:
            # Regular iterable
            result = [item for item in previous_result if operation.predicate(item)]

        # Emit after event
        if self._storage is not None:
            after_event = AfterFilterEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

        return result

    async def execute_map[T_Data, T_Result](
        self,
        operation: MapOperation[T_Data, T_Result],
        transaction: SQLAlchemyStorageTransaction,
        previous_result: Any,
    ) -> list[T_Result]:
        """Execute map operation.

        Args:
            operation: Map operation to execute.
            transaction: Transaction to use.
            previous_result: Result from previous operation (iterable or AsyncIterator).

        Returns:
            Mapped list of data.

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeMapEvent(
                component=self._storage,
                operation=operation,
                input_data=previous_result,
                transaction=transaction,
            )
            await self._storage.events.emit(before_event)

        # Map is executed in Python (can be optimized by storage)
        if hasattr(previous_result, "__aiter__"):
            # AsyncIterator
            result = []
            index = 0
            async for item in previous_result:
                result.append(operation.mapper(item, index))
                index += 1
        else:
            # Regular iterable
            result = [operation.mapper(item, index) for index, item in enumerate(previous_result)]

        # Emit after event
        if self._storage is not None:
            after_event = AfterMapEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

        return result

    async def execute_reduce[T_Data, T_Result](
        self,
        operation: ReduceOperation[T_Data, T_Result],
        transaction: SQLAlchemyStorageTransaction,
        previous_result: Any,
    ) -> T_Result:
        """Execute reduce operation.

        Args:
            operation: Reduce operation to execute.
            transaction: Transaction to use.
            previous_result: Result from previous operation (iterable or AsyncIterator).

        Returns:
            Reduced value.

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeReduceEvent(
                component=self._storage,
                operation=operation,
                input_data=previous_result,
                transaction=transaction,
            )
            await self._storage.events.emit(before_event)

        # Reduce is executed in Python (can be optimized by storage)
        accumulator = operation.initial
        if hasattr(previous_result, "__aiter__"):
            # AsyncIterator
            async for item in previous_result:
                accumulator = operation.reducer(accumulator, item)
        else:
            # Regular iterable
            for item in previous_result:
                accumulator = operation.reducer(accumulator, item)

        # Emit after event
        if self._storage is not None:
            after_event = AfterReduceEvent(
                component=self._storage,
                operation=operation,
                result=accumulator,
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

        return accumulator

    async def execute_transform[T_Data, T_Result](
        self,
        operation: TransformOperation[T_Data, T_Result],
        transaction: SQLAlchemyStorageTransaction,
        previous_result: Any,
    ) -> T_Result:
        """Execute transform operation.

        Args:
            operation: Transform operation to execute.
            transaction: Transaction to use.
            previous_result: Result from previous operation.

        Returns:
            Transformed data.

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeTransformEvent(
                component=self._storage,
                operation=operation,
                input_data=previous_result,
                transaction=transaction,
            )
            await self._storage.events.emit(before_event)

        # Transform is executed in Python
        result = operation.transformer(previous_result)

        # Emit after event
        if self._storage is not None:
            after_event = AfterTransformEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

        return result

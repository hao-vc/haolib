"""Operations handler for SQLAlchemy storage.

Handles execution of individual operations.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Any

from sqlalchemy import delete, func, select, update
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import selectinload

from haolib.storages.data_types.registry import DataTypeRegistry

# Import events lazily to avoid circular import
# Events are imported in methods that use them
from haolib.storages.indexes.sql import SQLQueryIndex
from haolib.storages.indexes.sqlalchemy import IndexHandler

# Import operations lazily to avoid circular import
# Operations are imported in methods that use them
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
            from haolib.storages.events.operations import BeforeCreateEvent  # noqa: PLC0415

            before_event = BeforeCreateEvent(
                component=self._storage,
                operation=operation,
                transaction=transaction,
            )
            await self._storage.events.emit(before_event)

        session = await transaction.get_session()

        # Convert user types to storage models
        storage_models = []
        models_to_create = []
        for item in operation.data:
            registration = self._registry.get_for_user_type(type(item))
            if registration:
                storage_model = registration.to_storage(item)
            else:
                # If no registration - assume it's already a storage model
                storage_model = item

            # Check if model already exists in database (has ID and is in session)
            # This handles pass-through case where create() is called with already-created objects
            model_type = type(storage_model)
            pk_columns = list(model_type.__table__.primary_key.columns)  # type: ignore[attr-defined]
            if pk_columns:
                pk_column = pk_columns[0]
                pk_name = pk_column.name
                pk_value = getattr(storage_model, pk_name, None)

                # If model has ID, check if it's already in database
                if pk_value is not None:
                    # Check if object is already in session (already persisted)
                    if storage_model in session:
                        # Object is already in database - pass through without creating
                        storage_models.append(storage_model)
                        continue
                    # Try to get from database
                    from sqlalchemy import select  # noqa: PLC0415

                    stmt = select(model_type).where(pk_column == pk_value)
                    result = await session.execute(stmt)
                    existing = result.scalar_one_or_none()
                    if existing is not None:
                        # Object already exists - pass through without creating
                        storage_models.append(existing)
                        continue

            # Object doesn't exist or has no ID - create it
            models_to_create.append(storage_model)
            storage_models.append(storage_model)

        # Save new models to database
        if models_to_create:
            session.add_all(models_to_create)
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
            from haolib.storages.events.operations import AfterCreateEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import BeforeReadEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import AfterReadEvent  # noqa: PLC0415

            after_event = AfterReadEvent(
                component=self._storage,
                operation=operation,
                result=result,  # The original result stream
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

    async def execute_patch[T_Data](  # noqa: PLR0915
        self,
        operation: PatchOperation[T_Data],
        transaction: SQLAlchemyStorageTransaction,
        previous_result: Any | None = None,
    ) -> list[T_Data]:
        """Execute patch operation (partial update).

        Can work in two modes:
        1. Search mode: uses search_index to find data, patch to update
        2. Pipeline mode: uses previous_result as data to update

        Args:
            operation: Patch operation to execute.
            transaction: Transaction to use.
            previous_result: Previous operation result (if in pipeline mode).

        Returns:
            List of updated data (converted to user types).

        """
        session = await transaction.get_session()

        # Determine mode: pipeline mode (previous_result) or search mode (search_index)
        if previous_result is not None:
            # Pipeline mode: use previous_result
            # Emit before event for pipeline mode
            if self._storage is not None:
                from haolib.storages.events.operations import BeforePatchEvent  # noqa: PLC0415

                before_event = BeforePatchEvent(
                    component=self._storage,
                    operation=operation,
                    transaction=transaction,
                )
                await self._storage.events.emit(before_event)
            # Pipeline mode: use previous_result
            from collections.abc import AsyncIterator  # noqa: PLC0415

            # Collect items from previous_result
            if isinstance(previous_result, AsyncIterator):
                items = [item async for item in previous_result]
            else:
                items = list(previous_result) if isinstance(previous_result, (list, tuple)) else [previous_result]

            if not items:
                return []

            # Get data type from first item
            first_item = items[0]
            data_type = type(first_item)
            registration = self._registry.get_for_user_type(data_type)
            if not registration:
                msg = f"No storage model registered for {data_type}"
                raise ValueError(msg)

            model = registration.storage_type
            pk_columns = list(model.__table__.primary_key.columns)  # type: ignore[attr-defined]
            if not pk_columns:
                msg = f"Model {model} has no primary key"
                raise ValueError(msg)
            pk_column = pk_columns[0]
            pk_name = pk_column.name

            # Extract IDs from items
            item_ids = []
            for item in items:
                if registration:
                    # Convert to storage model to get ID
                    storage_obj = registration.to_storage(item)
                    item_id = getattr(storage_obj, pk_name)
                else:
                    item_id = getattr(item, pk_name)
                item_ids.append(item_id)

            # Build update query using IDs
            update_stmt = update(model).where(pk_column.in_(item_ids))

            # Apply patch values (patch is required in pipeline mode)
            if operation.patch is None:
                msg = "PatchOperation in pipeline mode requires patch parameter"
                raise ValueError(msg)

            patch_values = operation.patch
            update_stmt = update_stmt.values(**patch_values)
        else:
            # Search mode: use search_index
            if operation.search_index is None:
                msg = "PatchOperation requires either search_index or previous_result"
                raise ValueError(msg)

            if operation.patch is None:
                msg = "PatchOperation requires patch parameter"
                raise ValueError(msg)

            # Build query from index
            query = await self._index_handler.build_query(operation.search_index, session)

            # Check if this is an optimized UPDATE query (from SQLQueryIndex with Update statement)
            from sqlalchemy import Update  # noqa: PLC0415

            if isinstance(query, Update):
                # This is an optimized UPDATE query - execute it directly
                # The query already has WHERE conditions and VALUES from optimization
                # Emit before event for optimized patch
                if self._storage is not None:
                    from haolib.storages.events.operations import BeforePatchEvent  # noqa: PLC0415

                    before_event = BeforePatchEvent(
                        component=self._storage,
                        operation=operation,
                        transaction=transaction,
                    )
                    await self._storage.events.emit(before_event)

                # Execute UPDATE with RETURNING to get updated rows
                results = await session.scalars(query)
                models = list(results.all())

                if not models:
                    # Emit after event even for empty result
                    if self._storage is not None:
                        from haolib.storages.events.operations import AfterPatchEvent  # noqa: PLC0415

                        after_event = AfterPatchEvent(
                            component=self._storage,
                            operation=operation,
                            result=[],
                            transaction=transaction,
                        )
                        await self._storage.events.emit(after_event)
                    return []

                # Convert back to user types
                result_list = []
                for model_obj in models:
                    model_registration = self._registry.get_for_storage_type(type(model_obj))
                    if model_registration:
                        result_list.append(model_registration.from_storage(model_obj))
                    else:
                        result_list.append(model_obj)

                # Emit after event for optimized patch
                if self._storage is not None:
                    from haolib.storages.events.operations import AfterPatchEvent  # noqa: PLC0415

                    after_event = AfterPatchEvent(
                        component=self._storage,
                        operation=operation,
                        result=result_list,
                        transaction=transaction,
                    )
                    await self._storage.events.emit(after_event)

                return result_list

            # Regular search mode: build UPDATE query manually
            # Emit before event for regular search mode
            if self._storage is not None:
                from haolib.storages.events.operations import BeforePatchEvent  # noqa: PLC0415

                before_event = BeforePatchEvent(
                    component=self._storage,
                    operation=operation,
                    transaction=transaction,
                )
                await self._storage.events.emit(before_event)
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

            # Use bulk update for partial update (only specified fields)
            update_stmt = update(model)

            # Apply where clause from Select query
            if query.whereclause is not None:
                update_stmt = update_stmt.where(query.whereclause)

            # Apply values from patch dict (only specified fields)
            update_stmt = update_stmt.values(**operation.patch)

            # Get primary key column for returning
            pk_columns = list(model.__table__.primary_key.columns)  # type: ignore[attr-defined]
            if pk_columns:
                pk_column = pk_columns[0]
            else:
                pk_column = None

        # Use returning to get primary keys of updated rows
        if pk_column is not None:
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
                            reload_query = reload_query.options(selectinload(rel_attr).selectinload(nested_rel_attr))
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
            from haolib.storages.events.operations import AfterPatchEvent  # noqa: PLC0415

            after_event = AfterPatchEvent(
                component=self._storage,
                operation=operation,
                result=result_list,
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

        return result_list

    async def execute_update[T_Data](
        self,
        operation: UpdateOperation[T_Data],
        transaction: SQLAlchemyStorageTransaction,
        previous_result: Any | None = None,
    ) -> list[T_Data]:
        """Execute update operation (full update).

        Can work in two modes:
        1. Search mode: uses search_index to find data, data to update
        2. Pipeline mode: uses previous_result as data to update

        Args:
            operation: Update operation to execute.
            transaction: Transaction to use.
            previous_result: Previous operation result (if in pipeline mode).

        Returns:
            List of updated data (converted to user types).

        """
        session = await transaction.get_session()

        # Determine mode: pipeline mode (previous_result) or search mode (search_index)
        if previous_result is not None:
            # Pipeline mode: use previous_result
            # Emit before event for pipeline mode
            if self._storage is not None:
                from haolib.storages.events.operations import BeforeUpdateEvent  # noqa: PLC0415

                before_event = BeforeUpdateEvent(
                    component=self._storage,
                    operation=operation,
                    transaction=transaction,
                )
                await self._storage.events.emit(before_event)
            # Pipeline mode: use previous_result
            from collections.abc import AsyncIterator  # noqa: PLC0415

            # Collect items from previous_result
            if isinstance(previous_result, AsyncIterator):
                items = [item async for item in previous_result]
            else:
                items = list(previous_result) if isinstance(previous_result, (list, tuple)) else [previous_result]

            if not items:
                return []

            # Get data type from first item
            first_item = items[0]
            data_type = type(first_item)
            registration = self._registry.get_for_user_type(data_type)
            if not registration:
                msg = f"No storage model registered for {data_type}"
                raise ValueError(msg)

            model = registration.storage_type
            pk_columns = list(model.__table__.primary_key.columns)  # type: ignore[attr-defined]
            if not pk_columns:
                msg = f"Model {model} has no primary key"
                raise ValueError(msg)
            pk_column = pk_columns[0]
            pk_name = pk_column.name

            # Extract IDs from items and prepare update data
            item_ids = []
            update_data_map = {}
            for item in items:
                if registration:
                    # Convert to storage model to get ID
                    storage_obj = registration.to_storage(item)
                    item_id = getattr(storage_obj, pk_name)
                    # Use item as update data (or apply transform function)
                    if operation.data:
                        updated_item = operation.data(item) if callable(operation.data) else operation.data
                    else:
                        updated_item = item
                    update_data_map[item_id] = updated_item
                else:
                    item_id = getattr(item, pk_name)
                    if operation.data:
                        updated_item = operation.data(item) if callable(operation.data) else operation.data
                    else:
                        updated_item = item
                    update_data_map[item_id] = updated_item
                item_ids.append(item_id)

            # Load existing models
            query = select(model).where(pk_column.in_(item_ids))
            results = await session.scalars(query)
            models = list(results.all())

            if not models:
                return []

            # Apply full update to each model
            for model_obj in models:
                model_id = getattr(model_obj, pk_name)
                updated_item = update_data_map.get(model_id)
                if updated_item is None:
                    continue

                model_registration = self._registry.get_for_storage_type(type(model_obj))
                if model_registration:
                    # Convert to storage model
                    updated_model = model_registration.to_storage(updated_item)
                    # Update all model attributes (full replacement)
                    for column in updated_model.__table__.columns:  # type: ignore[attr-defined]
                        setattr(model_obj, column.name, getattr(updated_model, column.name))
                else:
                    # No registration - apply directly
                    for column in model_obj.__table__.columns:  # type: ignore[attr-defined]
                        setattr(model_obj, column.name, getattr(updated_item, column.name))
        else:
            # Search mode: use search_index
            if operation.search_index is None:
                msg = "UpdateOperation requires either search_index or previous_result"
                raise ValueError(msg)

            if operation.data is None:
                msg = "UpdateOperation requires data parameter"
                raise ValueError(msg)

            # Build query from index
            query = await self._index_handler.build_query(operation.search_index, session)

            # Check if this is an optimized UPDATE query (from SQLQueryIndex with Update statement)
            from sqlalchemy import Update  # noqa: PLC0415

            if isinstance(query, Update):
                # This is an optimized UPDATE query - execute it directly
                # The query already has WHERE conditions and VALUES from optimization
                # Execute UPDATE with RETURNING to get updated rows
                results = await session.scalars(query)
                models = list(results.all())

                if not models:
                    # Emit after event even for empty result
                    if self._storage is not None:
                        from haolib.storages.events.operations import AfterUpdateEvent  # noqa: PLC0415

                        after_event = AfterUpdateEvent(
                            component=self._storage,
                            operation=operation,
                            result=[],
                            transaction=transaction,
                        )
                        await self._storage.events.emit(after_event)
                    return []

                # Convert back to user types
                result_list = []
                for model_obj in models:
                    model_registration = self._registry.get_for_storage_type(type(model_obj))
                    if model_registration:
                        result_list.append(model_registration.from_storage(model_obj))
                    else:
                        result_list.append(model_obj)

                # Emit after event for optimized update
                if self._storage is not None:
                    from haolib.storages.events.operations import AfterUpdateEvent  # noqa: PLC0415

                    after_event = AfterUpdateEvent(
                        component=self._storage,
                        operation=operation,
                        result=result_list,
                        transaction=transaction,
                    )
                    await self._storage.events.emit(after_event)

                return result_list

            # Regular search mode: build SELECT query and apply update manually
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

            # Load objects to apply full update
            results = await session.scalars(query)
            models = list(results.all())

            if not models:
                return []

            # Apply full update
            for model_obj in models:
                model_registration = self._registry.get_for_storage_type(type(model_obj))
                if model_registration:
                    # Convert to user type
                    user_obj = model_registration.from_storage(model_obj)
                    # Apply data (object or function)
                    updated = operation.data(user_obj) if callable(operation.data) else operation.data
                    # Convert back to storage model
                    updated_model = model_registration.to_storage(updated)
                    # Update all model attributes (full replacement)
                    for column in updated_model.__table__.columns:  # type: ignore[attr-defined]
                        setattr(model_obj, column.name, getattr(updated_model, column.name))
                else:
                    # No registration - apply directly
                    # Type ignore: data expects T_Data but model might not match exactly
                    updated = operation.data(model_obj) if callable(operation.data) else operation.data  # type: ignore[arg-type]
                    for column in model_obj.__table__.columns:  # type: ignore[attr-defined]
                        setattr(model_obj, column.name, getattr(updated, column.name))

        await session.flush()

        # Convert back to user types
        result_list = []
        for model_obj in models:
            model_registration = self._registry.get_for_storage_type(type(model_obj))
            if model_registration:
                result_list.append(model_registration.from_storage(model_obj))
            else:
                result_list.append(model_obj)

        # Emit after event
        if self._storage is not None:
            from haolib.storages.events.operations import AfterUpdateEvent  # noqa: PLC0415

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
        previous_result: Any | None = None,
    ) -> int:
        """Execute delete operation.

        Can work in two modes:
        1. Search mode: uses search_index to find data to delete
        2. Pipeline mode: uses previous_result as data to delete

        Args:
            operation: Delete operation to execute.
            transaction: Transaction to use.
            previous_result: Previous operation result (if in pipeline mode).

        Returns:
            Number of deleted items.

        """
        # Emit before event
        if self._storage is not None:
            from haolib.storages.events.operations import BeforeDeleteEvent  # noqa: PLC0415

            before_event = BeforeDeleteEvent(
                component=self._storage,
                operation=operation,
                transaction=transaction,
            )
            await self._storage.events.emit(before_event)

        session = await transaction.get_session()

        # Determine mode: pipeline mode (previous_result) or search mode (search_index)
        if previous_result is not None:
            # Pipeline mode: use previous_result
            from collections.abc import AsyncIterator  # noqa: PLC0415

            # Collect items from previous_result
            if isinstance(previous_result, AsyncIterator):
                items = [item async for item in previous_result]
            else:
                items = list(previous_result) if isinstance(previous_result, (list, tuple)) else [previous_result]

            if not items:
                return 0

            # Get data type from first item
            first_item = items[0]
            data_type = type(first_item)
            registration = self._registry.get_for_user_type(data_type)
            if not registration:
                msg = f"No storage model registered for {data_type}"
                raise ValueError(msg)

            model = registration.storage_type
            pk_columns = list(model.__table__.primary_key.columns)  # type: ignore[attr-defined]
            if not pk_columns:
                msg = f"Model {model} has no primary key"
                raise ValueError(msg)
            pk_column = pk_columns[0]
            pk_name = pk_column.name

            # Extract IDs from items
            item_ids = []
            for item in items:
                if registration:
                    # Convert to storage model to get ID
                    storage_obj = registration.to_storage(item)
                    item_id = getattr(storage_obj, pk_name)
                else:
                    item_id = getattr(item, pk_name)
                item_ids.append(item_id)

            # Build delete query using IDs
            delete_stmt = delete(model).where(pk_column.in_(item_ids))
        else:
            # Search mode: use search_index
            if operation.search_index is None:
                msg = "DeleteOperation requires either search_index or previous_result"
                raise ValueError(msg)

            # Build query from index
            query = await self._index_handler.build_query(operation.search_index, session)

            # Check if this is an optimized DELETE query (from SQLQueryIndex with Delete statement)
            from sqlalchemy import Delete  # noqa: PLC0415

            if isinstance(query, Delete):
                # This is an optimized DELETE query - execute it directly
                # The query already has WHERE conditions from optimization
                # Note: BeforeDeleteEvent was already emitted at the start of the function
                result = await session.execute(query)
                deleted_count = result.rowcount

                # Emit after event for optimized delete
                if self._storage is not None:
                    from haolib.storages.events.operations import AfterDeleteEvent  # noqa: PLC0415

                    after_event = AfterDeleteEvent(
                        component=self._storage,
                        operation=operation,
                        result=deleted_count,
                        transaction=transaction,
                    )
                    await self._storage.events.emit(after_event)

                return deleted_count

            # Regular search mode: build DELETE query manually
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
                from haolib.storages.events.operations import AfterDeleteEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import AfterDeleteEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import BeforeFilterEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import AfterFilterEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import BeforeMapEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import AfterMapEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import BeforeReduceEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import AfterReduceEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import BeforeTransformEvent  # noqa: PLC0415

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
            from haolib.storages.events.operations import AfterTransformEvent  # noqa: PLC0415

            after_event = AfterTransformEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=transaction,
            )
            await self._storage.events.emit(after_event)

        return result

"""SQLAlchemy storage implementation."""

from collections.abc import AsyncIterator, Sequence
from types import TracebackType
from typing import Any, Self

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from haolib.components.events import EventEmitter
from haolib.components.plugins.helpers import apply_preset
from haolib.components.plugins.registry import PluginRegistry
from haolib.storages.abstract import AbstractStorage
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.operations.base import (
    Operation,
    Pipeline,
    TargetBoundOperation,
    TargetSwitch,
)
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
from haolib.storages.operations.optimizer.sqlalchemy import SQLAlchemyPipelineOptimizer
from haolib.storages.operations.sqlalchemy import SQLAlchemyOperationsHandler
from haolib.storages.plugins.abstract import AbstractStoragePlugin, AbstractStoragePluginPreset
from haolib.storages.transactions.sqlalchemy import SQLAlchemyStorageTransaction


class SQLAlchemyOperationExecutor:
    """Executor for SQLAlchemy operations and pipelines."""

    def __init__(
        self,
        storage: SQLAlchemyStorage,
        registry: DataTypeRegistry,
        relationship_load_depth: int = 2,
    ) -> None:
        """Initialize the executor.

        Args:
            storage: SQLAlchemy storage instance.
            registry: Data type registry.
            relationship_load_depth: Maximum depth for loading nested relationships.

        """
        self._storage = storage
        self._registry = registry
        self._operations_handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=relationship_load_depth,
            storage=storage,
        )
        self._optimizer = SQLAlchemyPipelineOptimizer(registry=registry)

    async def execute[T_Result](
        self,
        operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result],
        transaction: SQLAlchemyStorageTransaction,
    ) -> T_Result:
        """Execute operation or pipeline.

        Analyzes pipeline and optimizes it when possible to execute on storage side.

        Args:
            operation: Operation or pipeline to execute.
            transaction: Transaction to use.

        Returns:
            Result of execution.

        Raises:
            TypeError: If operation type is not supported.

        """
        # Analyze pipeline for optimization
        analysis = self._optimizer.analyze(operation)

        # Execute based on analysis
        if analysis.execution_plan == "storage" and analysis.optimized_operation:
            # Entire pipeline can be executed in SQL
            # Build optimized query if needed
            optimized_op = await self._build_optimized_operation_if_needed(analysis, transaction)
            return await self._execute_operation(optimized_op, transaction, previous_result=None)

        if analysis.execution_plan == "hybrid" and analysis.optimized_operation:
            # Hybrid: part in SQL, part in Python
            # Build optimized query if needed
            optimized_op = await self._build_optimized_operation_if_needed(analysis, transaction)
            # Execute optimized part in SQL
            sql_result = await self._execute_operation(optimized_op, transaction, previous_result=None)
            # Execute remaining operations in Python
            return await self._execute_remaining_operations(analysis.remaining_operations, sql_result, transaction)

        # All in Python - execute normally
        if isinstance(operation, Pipeline):
            return await self._execute_pipeline(operation, transaction)

        return await self._execute_operation(operation, transaction, previous_result=None)

    async def _execute_pipeline(
        self,
        pipeline: Pipeline[Any, Any, Any],
        transaction: SQLAlchemyStorageTransaction,
    ) -> Any:
        """Execute pipeline sequentially.

        Args:
            pipeline: Pipeline to execute.
            transaction: Transaction to use.

        Returns:
            Result of pipeline execution.

        """
        # Handle TargetBoundOperation and TargetSwitch in first operation
        first_op = pipeline.first
        if isinstance(first_op, TargetBoundOperation):
            # Unwrap TargetBoundOperation - execute operation directly
            # first_op.operation is Operation or Pipeline, not TargetSwitch
            first_op = first_op.operation
        elif isinstance(first_op, TargetSwitch):
            msg = "TargetSwitch should not be passed to SQLAlchemy executor directly"
            raise TypeError(msg)

        # Execute first operation
        first_result = await self._execute_operation(first_op, transaction, previous_result=None)

        # Handle TargetBoundOperation and TargetSwitch in second operation
        second_op = pipeline.second
        if isinstance(second_op, TargetBoundOperation):
            # Unwrap TargetBoundOperation - execute operation directly
            # second_op.operation is Operation or Pipeline, not TargetSwitch
            second_op = second_op.operation
        elif isinstance(second_op, TargetSwitch):
            msg = "TargetSwitch should not be passed to SQLAlchemy executor directly"
            raise TypeError(msg)

        # Execute second operation with first result
        return await self._execute_operation(second_op, transaction, previous_result=first_result)

    async def _execute_operation(
        self,
        operation: Operation[Any, Any] | Pipeline[Any, Any, Any],
        transaction: SQLAlchemyStorageTransaction,
        previous_result: Any,
    ) -> Any:
        """Execute single operation.

        Args:
            operation: Operation to execute.
            transaction: Transaction to use.
            previous_result: Result from previous operation (for pipelines).

        Returns:
            Result of operation execution.

        Raises:
            TypeError: If operation type is not supported.

        """
        # If it's a nested pipeline - execute recursively
        if isinstance(operation, Pipeline):
            return await self._execute_pipeline(operation, transaction)

        # Pattern matching by operation type
        match operation:
            case CreateOperation():
                if previous_result is not None:
                    msg = "CreateOperation cannot receive data from previous operation"
                    raise ValueError(msg)
                return await self._operations_handler.execute_create(operation, transaction)

            case ReadOperation():
                if previous_result is not None:
                    msg = "ReadOperation cannot receive data from previous operation"
                    raise ValueError(msg)
                # ReadOperation returns AsyncIterator (not awaitable)
                return self._operations_handler.execute_read(operation, transaction)

            case UpdateOperation():
                if previous_result is not None:
                    msg = "UpdateOperation cannot receive data from previous operation"
                    raise ValueError(msg)
                return await self._operations_handler.execute_update(operation, transaction)

            case DeleteOperation():
                if previous_result is not None:
                    msg = "DeleteOperation cannot receive data from previous operation"
                    raise ValueError(msg)
                return await self._operations_handler.execute_delete(operation, transaction)

            case FilterOperation():
                if previous_result is None:
                    msg = "FilterOperation requires data from previous operation"
                    raise ValueError(msg)
                # If previous_result is AsyncIterator - collect it first
                if isinstance(previous_result, AsyncIterator):
                    previous_result = await self._collect_async_iterator(previous_result)
                return await self._operations_handler.execute_filter(operation, transaction, previous_result)

            case MapOperation():
                if previous_result is None:
                    msg = "MapOperation requires data from previous operation"
                    raise ValueError(msg)
                # If previous_result is AsyncIterator - collect it first
                if isinstance(previous_result, AsyncIterator):
                    previous_result = await self._collect_async_iterator(previous_result)
                return await self._operations_handler.execute_map(operation, transaction, previous_result)

            case ReduceOperation():
                if previous_result is None:
                    msg = "ReduceOperation requires data from previous operation"
                    raise ValueError(msg)
                # If previous_result is AsyncIterator - collect it first
                if isinstance(previous_result, AsyncIterator):
                    previous_result = await self._collect_async_iterator(previous_result)
                return await self._operations_handler.execute_reduce(operation, transaction, previous_result)

            case TransformOperation():
                if previous_result is None:
                    msg = "TransformOperation requires data from previous operation"
                    raise ValueError(msg)
                # If previous_result is AsyncIterator - collect it first
                if isinstance(previous_result, AsyncIterator):
                    previous_result = await self._collect_async_iterator(previous_result)
                return await self._operations_handler.execute_transform(operation, transaction, previous_result)

            case _:
                msg = f"Unsupported operation type: {type(operation)}"
                raise TypeError(msg)

    async def _collect_async_iterator(self, async_iter: AsyncIterator[Any]) -> list[Any]:
        """Collect all items from AsyncIterator.

        Args:
            async_iter: AsyncIterator to collect.

        Returns:
            List of collected items.

        """
        return [item async for item in async_iter]

    async def _execute_remaining_operations(
        self,
        operations: Sequence[Operation],
        previous_result: Any,
        transaction: SQLAlchemyStorageTransaction,
    ) -> Any:
        """Execute remaining operations in Python after SQL execution.

        Args:
            operations: Remaining operations to execute in Python.
            previous_result: Result from SQL execution.
            transaction: Transaction to use.

        Returns:
            Final result after executing all remaining operations.

        """
        result = previous_result

        # If result is AsyncIterator, collect it first
        if isinstance(result, AsyncIterator):
            result = await self._collect_async_iterator(result)

        # Execute remaining operations sequentially
        for op in operations:
            result = await self._execute_operation(op, transaction, previous_result=result)

        return result

    async def _build_optimized_operation_if_needed(
        self,
        analysis: Any,
        transaction: SQLAlchemyStorageTransaction,
    ) -> Operation[Any, Any]:
        """Build optimized operation if needed.

        If the optimized operation needs query building (has filters),
        build it using the optimizer's async method.

        Args:
            analysis: Pipeline analysis result.
            transaction: Transaction to use.

        Returns:
            Optimized operation ready for execution.

        """
        if not analysis.optimized_operation:
            msg = "No optimized operation in analysis"
            raise ValueError(msg)

        # If we have SQL operations with filters, build optimized query
        if analysis.sql_operations and len(analysis.sql_operations) > 1:
            # Check if we have filters
            has_filters = any(isinstance(op, FilterOperation) for op in analysis.sql_operations)
            if has_filters:
                session = await transaction.get_session()
                optimized = await self._optimizer.build_optimized_operation_async(
                    list(analysis.sql_operations), session
                )
                if optimized:
                    return optimized

        return analysis.optimized_operation


class SQLAlchemyStorage(AbstractStorage):
    """SQLAlchemy storage implementation.

    Supports all CRUD operations and ETL pipelines.
    Can optimize pipelines to execute as single SQL queries.

    Storage manages engine lifecycle through async context manager.
    Recommended for APP-scoped components in dependency injection containers.

    Example:
        ```python
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

        engine = create_async_engine("postgresql+asyncpg://...")

        # Option 1: Let storage create session_maker from engine
        async with SQLAlchemyStorage(
            engine=engine,
            data_type_registry=registry
        ) as storage:
            await storage.execute(createo([user1, user2]))
        # Engine automatically disposed on exit

        # Option 2: Provide custom session_maker
        session_maker = async_sessionmaker(engine, expire_on_commit=False)
        async with SQLAlchemyStorage(
            engine=engine,
            session_maker=session_maker,
            data_type_registry=registry
        ) as storage:
            await storage.execute(createo([user1, user2]))
        # Engine automatically disposed on exit

        # Use with operations
        from haolib.storages.dsl import createo, reado, filtero
        from haolib.storages.indexes import index

        await storage.execute(createo([user1, user2]))

        user_index = index(User, age=25)
        async for user in await storage.execute(reado(search_index=user_index)):
            print(user)
        ```

    """

    def __init__(
        self,
        engine: AsyncEngine,
        data_type_registry: DataTypeRegistry,
        session_maker: async_sessionmaker[AsyncSession] | None = None,
        relationship_load_depth: int = 2,
    ) -> None:
        """Initialize SQLAlchemy storage.

        Args:
            engine: SQLAlchemy async engine. Storage will manage engine lifecycle
                (dispose on context exit). Required for proper resource management.
            data_type_registry: Data type registry for type conversions.
            session_maker: Optional SQLAlchemy async session maker. If not provided,
                will be created from engine with default settings (expire_on_commit=False).
            relationship_load_depth: Maximum depth for loading nested relationships
                in update operations (default: 2). Set to 0 to disable nested loading.

        """
        self._engine = engine
        self._session_maker = session_maker or async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        self._data_type_registry = data_type_registry
        self._event_emitter = EventEmitter[SQLAlchemyStorage]()
        self._executor = SQLAlchemyOperationExecutor(
            storage=self,
            registry=data_type_registry,
            relationship_load_depth=relationship_load_depth,
        )

        # Initialize plugins
        self._plugin_registry = PluginRegistry[SQLAlchemyStorage]()

    @property
    def engine(self) -> AsyncEngine:
        """Get the SQLAlchemy engine.

        Returns:
            The SQLAlchemy async engine instance.

        """
        return self._engine

    @property
    def data_type_registry(self) -> DataTypeRegistry:
        """Get the data type registry."""
        return self._data_type_registry

    @property
    def version(self) -> str:
        """Get the version of the storage."""
        return "0.1.0"

    @property
    def events(self) -> EventEmitter[SQLAlchemyStorage]:
        """Get event emitter for subscribing to storage events."""
        return self._event_emitter

    @property
    def plugin_registry(self) -> PluginRegistry[SQLAlchemyStorage]:
        """Get plugin registry for storage plugins."""
        return self._plugin_registry

    def use_plugin(self, plugin: AbstractStoragePlugin[SQLAlchemyStorage]) -> Self:
        """Use a storage plugin."""
        self._plugin_registry.add(plugin, self.version)
        return self

    def use_preset(
        self, preset: AbstractStoragePluginPreset[SQLAlchemyStorage, AbstractStoragePlugin[SQLAlchemyStorage]]
    ) -> Self:
        """Use a storage plugin preset."""
        # mypy can't infer that SQLAlchemyStorage implements AbstractComponent
        apply_preset(self, preset, self._plugin_registry)  # type: ignore[misc]
        return self

    async def __aenter__(self) -> Self:
        """Enter the component context.

        Returns:
            Self for use in async context manager.

        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        """Exit the component context.

        Disposes the engine to ensure proper cleanup of database connections.
        This is essential for APP-scoped components in dependency injection containers.

        Args:
            exc_type: Exception type if exception occurred.
            exc_value: Exception value if exception occurred.
            traceback: Exception traceback if exception occurred.

        """
        await self._engine.dispose()

    async def close(self) -> None:
        """Close storage and dispose engine.

        Can be called manually if not using async context manager.
        After calling this method, storage should not be used.

        """
        await self._engine.dispose()

    def _begin_transaction(self) -> SQLAlchemyStorageTransaction:
        """Begin a new transaction (internal method).

        Returns:
            SQLAlchemyStorageTransaction for internal use.

        """
        session = self._session_maker()
        return SQLAlchemyStorageTransaction(session)

    async def execute[T_Result](
        self,
        operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result],
    ) -> T_Result:
        """Execute operation or pipeline atomically.

        Storage analyzes the operation/pipeline and executes it optimally.
        It may optimize the pipeline to execute on storage side (e.g., single SQL query)
        or execute it in Python code.

        Each operation or pipeline is automatically wrapped in a transaction.
        To execute multiple operations in a single transaction, compose them into a Pipeline.

        Args:
            operation: Operation or pipeline to execute.

        Returns:
            Result of execution.

        Raises:
            RuntimeError: If storage operation fails.
            TypeError: If operation type is not supported.

        Example:
            ```python
            from haolib.storages.dsl import createo, reado, filtero
            from haolib.storages.indexes import index

            # Simple operation (executed atomically in a transaction)
            await storage.execute(createo([user1, user2]))

            # Pipeline (all operations in single transaction)
            user_index = index(User, age=18)
            pipeline = (
                createo([user1, user2])
                | reado(search_index=user_index)
                | filtero(lambda u: u.age >= 18)
            )
            results = await storage.execute(pipeline)
            ```

        """
        # Automatically create transaction for each operation/pipeline
        txn = self._begin_transaction()
        async with txn:
            result = await self._executor.execute(operation, txn)
            # If result is AsyncIterator and we're in a pipeline context,
            # we need to collect it inside the transaction
            # But for standalone ReadOperation, we return AsyncIterator as-is
            # The caller is responsible for consuming it within transaction context
            # For pipelines, ExecutablePipelineExecutor will handle collection
            if isinstance(result, AsyncIterator):
                # Collect async iterator to avoid transaction closure issues
                # This ensures all data is loaded before transaction closes
                return [item async for item in result]  # type: ignore[return-value]
            return result

    async def execute_with_transaction[T_Result](
        self,
        operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result],
        transaction: SQLAlchemyStorageTransaction,
    ) -> T_Result:
        """Execute operation or pipeline with existing transaction.

        This method allows executing operations within an existing transaction,
        enabling multiple operations to share the same transaction context.
        Used internally by ExecutablePipelineExecutor to group operations.

        Args:
            operation: Operation or pipeline to execute.
            transaction: Existing transaction to use.

        Returns:
            Result of execution.

        Raises:
            RuntimeError: If storage operation fails.
            TypeError: If operation type is not supported.

        """
        result = await self._executor.execute(operation, transaction)
        # Handle AsyncIterator same way as execute()
        if isinstance(result, AsyncIterator):
            return [item async for item in result]  # type: ignore[return-value]
        return result

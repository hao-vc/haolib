"""SQLAlchemy storage implementation."""

from collections.abc import AsyncIterator, Callable, Sequence
from types import TracebackType
from typing import Any, Self

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from haolib.components.events import EventEmitter
from haolib.components.plugins.helpers import apply_preset
from haolib.components.plugins.registry import PluginRegistry
from haolib.pipelines.base import (
    Operation,
    Pipeline,
    TargetBoundOperation,
    TargetSwitch,
)
from haolib.pipelines.context import PipelineContext

# Import operations lazily to avoid circular import
# Operations are imported in methods that use them
from haolib.storages.abstract import AbstractStorage
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.fluent.composites import (
    CreateComposite,
    DeleteComposite,
    PatchComposite,
    ReadComposite,
    UpdateComposite,
)
from haolib.storages.fluent.protocols import (
    CreateOperatable,
    DeleteOperatable,
    PatchOperatable,
    ReadOperatable,
    UpdateOperatable,
)
from haolib.storages.indexes.abstract import SearchIndex

# Import operations lazily to avoid circular import
# Operations are imported in methods that use them
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
        previous_result: Any = None,
        pipeline_context: PipelineContext | None = None,
    ) -> T_Result:
        """Execute operation or pipeline.

        Analyzes pipeline and optimizes it when possible to execute on storage side.

        Args:
            operation: Operation or pipeline to execute.
            transaction: Transaction to use.
            previous_result: Optional result from previous operation (for pipeline mode).
            pipeline_context: Optional context about the entire pipeline for global optimization.

        Returns:
            Result of execution.

        Raises:
            TypeError: If operation type is not supported.

        """
        # Analyze pipeline for optimization (with global context)
        analysis = self._optimizer.analyze(operation, pipeline_context=pipeline_context)

        # Execute based on analysis
        if analysis.execution_plan == "storage":
            # Entire pipeline can be executed in SQL
            # Build optimized query if needed
            optimized_op = await self._build_optimized_operation_if_needed(analysis, transaction)
            if optimized_op:
                return await self._execute_operation(optimized_op, transaction, previous_result=previous_result)
            # If optimization failed, fall back to normal execution
            if isinstance(operation, Pipeline):
                return await self._execute_pipeline(operation, transaction)
            return await self._execute_operation(operation, transaction, previous_result=previous_result)

        if analysis.execution_plan == "hybrid" and analysis.optimized_operation:
            # Hybrid: part in SQL, part in Python
            # Build optimized query if needed
            optimized_op = await self._build_optimized_operation_if_needed(analysis, transaction)
            if optimized_op:
                # Execute optimized part in SQL
                sql_result = await self._execute_operation(optimized_op, transaction, previous_result=previous_result)
                # Execute remaining operations in Python
                return await self._execute_remaining_operations(analysis.remaining_operations, sql_result, transaction)
            # If optimization failed, fall back to normal execution
            if isinstance(operation, Pipeline):
                return await self._execute_pipeline(operation, transaction)
            return await self._execute_operation(operation, transaction, previous_result=previous_result)

        # All in Python - execute normally
        if isinstance(operation, Pipeline):
            return await self._execute_pipeline(operation, transaction)

        return await self._execute_operation(operation, transaction, previous_result=previous_result)

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

        # Handle AsyncIterator from ReadOperation - collect it before passing to next operation
        from collections.abc import AsyncIterator  # noqa: PLC0415

        if isinstance(first_result, AsyncIterator):
            # Collect AsyncIterator into list for next operation
            first_result = [item async for item in first_result]

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

        # Import operations lazily to avoid circular import
        from haolib.pipelines.operations import (  # noqa: PLC0415
            CreateOperation,
            DeleteOperation,
            FilterOperation,
            MapOperation,
            PatchOperation,
            ReadOperation,
            ReduceOperation,
            TransformOperation,
            UpdateOperation,
        )

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

            case PatchOperation():
                # If previous_result is AsyncIterator - collect it first
                if isinstance(previous_result, AsyncIterator):
                    previous_result = await self._collect_async_iterator(previous_result)
                return await self._operations_handler.execute_patch(operation, transaction, previous_result)

            case UpdateOperation():
                # If previous_result is AsyncIterator - collect it first
                if isinstance(previous_result, AsyncIterator):
                    previous_result = await self._collect_async_iterator(previous_result)
                return await self._operations_handler.execute_update(operation, transaction, previous_result)

            case DeleteOperation():
                # If previous_result is AsyncIterator - collect it first
                if isinstance(previous_result, AsyncIterator):
                    previous_result = await self._collect_async_iterator(previous_result)
                return await self._operations_handler.execute_delete(operation, transaction, previous_result)

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
    ) -> Operation[Any, Any] | None:
        """Build optimized operation if needed.

        If the optimized operation needs query building (has filters or update/patch/delete),
        build it using the optimizer's async method.

        Args:
            analysis: Pipeline analysis result.
            transaction: Transaction to use.

        Returns:
            Optimized operation ready for execution, or None if cannot optimize.

        """
        # If optimized_operation is None, we need to build it from sql_operations
        if analysis.optimized_operation is None:
            if analysis.sql_operations:
                session = await transaction.get_session()
                optimized = await self._optimizer.build_optimized_operation_async(
                    list(analysis.sql_operations), session
                )
                if optimized:
                    return optimized
            return None

        # If we have SQL operations with filters or update/patch/delete, build optimized query
        if analysis.sql_operations and len(analysis.sql_operations) > 1:
            from haolib.pipelines.operations import (  # noqa: PLC0415
                DeleteOperation,
                FilterOperation,
                PatchOperation,
                UpdateOperation,
            )

            # Check if we have filters or update/patch/delete operations
            has_filters = any(isinstance(op, FilterOperation) for op in analysis.sql_operations)
            has_update = any(
                isinstance(op, (UpdateOperation, PatchOperation, DeleteOperation)) for op in analysis.sql_operations
            )

            if has_filters or has_update:
                session = await transaction.get_session()
                optimized = await self._optimizer.build_optimized_operation_async(
                    list(analysis.sql_operations), session
                )
                if optimized:
                    return optimized

        return analysis.optimized_operation


class SQLAlchemyStorage(
    AbstractStorage,
    ReadOperatable,
    CreateOperatable,
    UpdateOperatable,
    PatchOperatable,
    DeleteOperatable,
):
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
            await storage.create([user1, user2]).returning().execute()
        # Engine automatically disposed on exit

        # Option 2: Provide custom session_maker
        session_maker = async_sessionmaker(engine, expire_on_commit=False)
        async with SQLAlchemyStorage(
            engine=engine,
            session_maker=session_maker,
            data_type_registry=registry
        ) as storage:
            await storage.create([user1, user2]).returning().execute()
        # Engine automatically disposed on exit

        # Use with fluent API
        from haolib.storages.indexes.params import ParamIndex

        await storage.create([user1, user2]).returning().execute()

        user_index = ParamIndex(User, age=25)
        users = await storage.read(user_index).returning().execute()
        for user in users:
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
        operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result] | Any,
        previous_result: Any = None,
        pipeline_context: PipelineContext | None = None,
    ) -> T_Result:
        """Execute operation or pipeline atomically.

        Storage analyzes the operation/pipeline and executes it optimally.
        It may optimize the pipeline to execute on storage side (e.g., single SQL query)
        or execute it in Python code.

        Each operation or pipeline is automatically wrapped in a transaction.
        To execute multiple operations in a single transaction, compose them into a Pipeline.

        Args:
            operation: Operation or pipeline to execute.
            previous_result: Optional result from previous operation (for pipeline mode).
            pipeline_context: Optional context about the entire pipeline for global optimization.

        Returns:
            Result of execution.

        Raises:
            RuntimeError: If storage operation fails.
            TypeError: If operation type is not supported.

        Example:
            ```python
            from haolib.pipelines import filtero
            from haolib.storages.indexes.params import ParamIndex

            # Simple operation (executed atomically in a transaction)
            await storage.create([user1, user2]).returning().execute()

            # Pipeline (all operations in single transaction)
            user_index = ParamIndex(User, age=18)
            pipeline = (
                storage.create([user1, user2]).returning()
                | storage.read(user_index).returning()
                | filtero(lambda u: u.age >= 18)
            )
            results = await pipeline.execute()
            ```

        """
        # Handle BaseComposite - call its execute method
        from haolib.storages.fluent.composites import BaseComposite  # noqa: PLC0415

        if isinstance(operation, BaseComposite):
            return await operation.execute()

        # Automatically create transaction for each operation/pipeline
        txn = self._begin_transaction()
        async with txn:
            result = await self._executor.execute(
                operation, txn, previous_result=previous_result, pipeline_context=pipeline_context
            )
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
        previous_result: Any = None,
        pipeline_context: PipelineContext | None = None,
    ) -> T_Result:
        """Execute operation or pipeline with existing transaction.

        This method allows executing operations within an existing transaction,
        enabling multiple operations to share the same transaction context.
        Used internally by ExecutablePipelineExecutor to group operations.

        Args:
            operation: Operation or pipeline to execute.
            transaction: Existing transaction to use.
            previous_result: Optional result from previous operation (for pipeline mode).
            pipeline_context: Optional context about the entire pipeline for global optimization.

        Returns:
            Result of execution.

        Raises:
            RuntimeError: If storage operation fails.
            TypeError: If operation type is not supported.

        """
        result = await self._executor.execute(
            operation, transaction, previous_result=previous_result, pipeline_context=pipeline_context
        )
        # Handle AsyncIterator same way as execute()
        if isinstance(result, AsyncIterator):
            return [item async for item in result]  # type: ignore[return-value]
        return result

    def read[T_Data](
        self,
        index: SearchIndex[T_Data],
    ) -> ReadComposite[T_Data]:
        """Create read composite.

        Args:
            index: Search index (ParamIndex or SQLQueryIndex).

        Returns:
            ReadComposite for chaining operations.

        Example:
            ```python
            from haolib.storages.indexes.params import ParamIndex

            # Read with returning
            users = await storage.read(ParamIndex(User, age=25)).returning().execute()

            # Read + Update
            await storage.read(ParamIndex(User, age=25)).update({"age": 26}).returning().execute()
            ```

        """
        # Import lazily to avoid circular import
        from haolib.pipelines.operations import ReadOperation  # noqa: PLC0415

        op = ReadOperation(search_index=index)
        return ReadComposite(storage=self, operation=op)

    def create[T_Data](
        self,
        data: list[T_Data] | None = None,
    ) -> CreateComposite[T_Data]:
        """Create create composite.

        Args:
            data: Optional data to create. If None, data will come from previous_result.

        Returns:
            CreateComposite for chaining operations.

        Example:
            ```python
            # Create with explicit data
            await storage.create([user1, user2]).returning().execute()

            # Create from previous_result
            pipeline = (
                storage.read(ParamIndex(User, age=25)).returning()
                | storage.create()
            )
            await pipeline.execute()
            ```

        """
        # Import lazily to avoid circular import
        from haolib.pipelines.operations import CreateOperation  # noqa: PLC0415

        op = CreateOperation(data=data or [])
        return CreateComposite(storage=self, operation=op)

    def update[T_Data](
        self,
        data: T_Data | Callable[[T_Data], T_Data] | None = None,
    ) -> UpdateComposite[T_Data]:
        """Create update composite.

        Args:
            data: Data to update with. Can be object or callable.

        Returns:
            UpdateComposite for chaining operations.

        Example:
            ```python
            # Update with explicit data
            await storage.update(User(age=26)).returning().execute()

            # Update from previous_result
            pipeline = (
                storage.read(ParamIndex(User, age=25)).returning()
                | storage.update(lambda u: User(age=u.age + 1))
            )
            await pipeline.execute()
            ```

        """
        # Import lazily to avoid circular import
        from haolib.pipelines.operations import UpdateOperation  # noqa: PLC0415

        op: UpdateOperation[T_Data] = UpdateOperation(data=data)
        return UpdateComposite(storage=self, operation=op)

    def patch[T_Data](
        self,
        patch: dict[str, Any] | Any | None = None,
    ) -> PatchComposite[T_Data]:
        """Create patch composite.

        Args:
            patch: Patch to apply. Can be dict, Pydantic model, or dataclass.

        Returns:
            PatchComposite for chaining operations.

        Example:
            ```python
            # Patch with explicit data
            await storage.patch({"age": 26}).returning().execute()

            # Patch from previous_result
            pipeline = (
                storage.read(ParamIndex(User, age=25)).returning()
                | storage.patch({"age": 26})
            )
            await pipeline.execute()
            ```

        """
        # Import lazily to avoid circular import
        from haolib.pipelines.operations import PatchOperation  # noqa: PLC0415

        op: PatchOperation[T_Data] = PatchOperation(patch=patch)
        return PatchComposite(storage=self, operation=op)

    def delete[T_Data](self) -> DeleteComposite[T_Data]:
        """Create delete composite.

        Returns:
            DeleteComposite for chaining operations.

        Example:
            ```python
            # Delete from previous_result
            pipeline = (
                storage.read(ParamIndex(User, age=25)).returning()
                | storage.delete()
            )
            await pipeline.execute()
            ```

        """
        # Import lazily to avoid circular import
        from haolib.pipelines.operations import DeleteOperation  # noqa: PLC0415

        op: DeleteOperation[T_Data] = DeleteOperation()
        return DeleteComposite(storage=self, operation=op)

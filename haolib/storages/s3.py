"""S3 storage implementation."""

from collections.abc import AsyncIterator, Callable
from types import TracebackType
from typing import Any, Self

from haolib.components.events import EventEmitter
from haolib.components.plugins.helpers import apply_preset
from haolib.components.plugins.registry import PluginRegistry
from haolib.database.files.s3.clients.abstract import AbstractS3Client
from haolib.pipelines.base import Operation, Pipeline, TargetBoundOperation, TargetSwitch

# Import operations lazily to avoid circular import
# Operations are imported in methods that use them
from haolib.pipelines.context import PipelineContext
from haolib.storages.abstract import AbstractStorage
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.fluent.composites import (
    CreateComposite,
    DeleteComposite,
    ReadComposite,
    UpdateComposite,
)
from haolib.storages.fluent.protocols import (
    CreateOperatable,
    DeleteOperatable,
    ReadOperatable,
    UpdateOperatable,
)
from haolib.storages.indexes.path import PathIndex

# Import operations lazily to avoid circular import
# Operations are imported in methods that use them
from haolib.storages.operations.s3 import S3OperationsHandler
from haolib.storages.plugins.abstract import AbstractStoragePlugin, AbstractStoragePluginPreset
from haolib.storages.targets.abstract import AbstractDataTarget


class S3OperationExecutor:
    """Executor for S3 operations and pipelines."""

    def __init__(
        self,
        handler: S3OperationsHandler,
    ) -> None:
        """Initialize the executor.

        Args:
            handler: S3 operations handler.

        """
        self._handler = handler

    async def execute[T_Result](
        self,
        operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result],
        previous_result: Any = None,
        pipeline_context: PipelineContext | None = None,
    ) -> T_Result:
        """Execute operation or pipeline.

        For S3, all operations execute in Python (no storage-side optimization).

        Args:
            operation: Operation or pipeline to execute.
            previous_result: Optional result from previous operation (for pipeline mode).
            pipeline_context: Optional context about the entire pipeline for global optimization.

        Returns:
            Result of execution.

        Raises:
            TypeError: If operation type is not supported.

        """
        if isinstance(operation, Pipeline):
            return await self._execute_pipeline(
                operation, previous_result=previous_result, pipeline_context=pipeline_context
            )

        return await self._execute_operation(
            operation, previous_result=previous_result, pipeline_context=pipeline_context
        )

    async def _execute_operation[T_Result](
        self,
        operation: Operation[Any, T_Result],
        previous_result: Any,
        pipeline_context: PipelineContext | None = None,
        previous_operation: Operation[Any, Any] | None = None,
    ) -> T_Result:
        """Execute single operation.

        Args:
            operation: Operation to execute.
            previous_result: Previous operation result (for pipeline).
            pipeline_context: Optional context about the entire pipeline for global optimization.

        Returns:
            Operation result.

        """
        # Import operations lazily to avoid circular import
        from haolib.pipelines.operations import (
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

        if isinstance(operation, CreateOperation):
            return await self._handler.execute_create(operation)  # type: ignore[return-value]

        if isinstance(operation, ReadOperation):
            # ReadOperation returns AsyncIterator, not awaitable
            return self._handler.execute_read(operation)  # type: ignore[return-value]

        if isinstance(operation, PatchOperation):
            return await self._handler.execute_patch(operation, previous_result)  # type: ignore[return-value]

        if isinstance(operation, UpdateOperation):
            return await self._handler.execute_update(operation, previous_result, pipeline_context, previous_operation)  # type: ignore[return-value]

        if isinstance(operation, DeleteOperation):
            return await self._handler.execute_delete(operation, previous_result, previous_operation)  # type: ignore[return-value]

        if isinstance(operation, FilterOperation):
            if previous_result is None:
                msg = "FilterOperation requires previous result"
                raise ValueError(msg)
            return await self._handler.execute_filter(operation, previous_result)  # type: ignore[return-value]

        if isinstance(operation, MapOperation):
            if previous_result is None:
                msg = "MapOperation requires previous result"
                raise ValueError(msg)
            return await self._handler.execute_map(operation, previous_result)  # type: ignore[return-value]

        if isinstance(operation, ReduceOperation):
            if previous_result is None:
                msg = "ReduceOperation requires previous result"
                raise ValueError(msg)
            return await self._handler.execute_reduce(operation, previous_result)  # type: ignore[return-value]

        if isinstance(operation, TransformOperation):
            if previous_result is None:
                msg = "TransformOperation requires previous result"
                raise ValueError(msg)
            return await self._handler.execute_transform(operation, previous_result)  # type: ignore[return-value]

        msg = f"Unsupported operation type: {type(operation)}"
        raise TypeError(msg)

    async def _execute_pipeline[T_Result](
        self,
        pipeline: Pipeline[Any, Any, T_Result],
        previous_result: Any = None,
        pipeline_context: PipelineContext | None = None,
        previous_operation: Operation[Any, Any] | None = None,
    ) -> T_Result:
        """Execute pipeline of operations.

        Args:
            pipeline: Pipeline to execute.
            previous_result: Optional result from previous operation (for pipeline mode).
            pipeline_context: Optional context about the entire pipeline for global optimization.
            previous_operation: Previous operation (for passing context to next operation).

        Returns:
            Final pipeline result.

        """
        # Handle TargetBoundOperation and TargetSwitch in first operation
        first_op = pipeline.first
        if isinstance(first_op, TargetBoundOperation):
            # Unwrap TargetBoundOperation - execute operation directly
            # first_op.operation is Operation or Pipeline, not TargetSwitch
            first_op = first_op.operation
        elif isinstance(first_op, TargetSwitch):
            msg = "TargetSwitch should not be passed to S3 executor directly"
            raise TypeError(msg)

        # Execute first operation
        if isinstance(first_op, Pipeline):
            first_result = await self._execute_pipeline(
                first_op,
                previous_result=previous_result,
                pipeline_context=pipeline_context,
                previous_operation=previous_operation,
            )
        else:
            first_result = await self._execute_operation(
                first_op,
                previous_result=previous_result,
                pipeline_context=pipeline_context,
                previous_operation=previous_operation,
            )

        # Handle TargetBoundOperation and TargetSwitch in second operation
        second_op = pipeline.second
        if isinstance(second_op, TargetBoundOperation):
            # Unwrap TargetBoundOperation - execute operation directly
            # second_op.operation is Operation or Pipeline, not TargetSwitch
            second_op = second_op.operation
        elif isinstance(second_op, TargetSwitch):
            msg = "TargetSwitch should not be passed to S3 executor directly"
            raise TypeError(msg)

        # Execute second operation with first result and first_op as previous_operation
        if isinstance(second_op, Pipeline):
            return await self._execute_pipeline(
                second_op,
                previous_result=first_result,
                pipeline_context=pipeline_context,
                previous_operation=first_op if isinstance(first_op, Operation) else None,
            )
        return await self._execute_operation(
            second_op,
            previous_result=first_result,
            pipeline_context=pipeline_context,
            previous_operation=first_op if isinstance(first_op, Operation) else None,
        )


class S3Storage(
    AbstractStorage,
    ReadOperatable,
    CreateOperatable,
    UpdateOperatable,
    DeleteOperatable,
):
    """S3 storage implementation.

    Supports all CRUD operations and ETL pipelines.
    Operations execute in Python (no storage-side optimization).

    Storage uses S3 client for object storage operations.
    Recommended for APP-scoped components in dependency injection containers.

    Example:
        ```python
        from haolib.database.files.s3.clients import Aioboto3S3Client
        from haolib.storages.s3 import S3Storage
        from haolib.storages.indexes.path import PathIndex

        async with Aioboto3S3Client(...) as s3_client:
            storage = S3Storage(
                s3_client=s3_client,
                bucket="my-bucket",
                data_type_registry=registry
            )

            # Create - returns list of tuples (data, path)
            result = await storage.create([user1, user2]).returning().execute()
            for data, path in result:
                print(f"Saved {data} to {path}")
            # Output:
            # Saved User(name='Alice') to User/123e4567-e89b-12d3-a456-426614174000.json
            # Saved User(name='Bob') to User/123e4567-e89b-12d3-a456-426614174001.json

            # Read using path from create result
            _, path = result[0]
            index = PathIndex(data_type=User, path=path)
            users = await storage.read(index).returning().execute()
            for user in users:
                print(user)
            ```

    """

    def __init__(
        self,
        s3_client: AbstractS3Client,
        bucket: str,
        data_type_registry: DataTypeRegistry,
        path_generator: Callable[[type, Any, str | None], str] | None = None,
        serializer: Callable[[Any], bytes] | None = None,
        deserializer: Callable[[bytes, type], Any] | None = None,
        content_type: str | Callable[[type, Any], str] = "application/json",
    ) -> None:
        """Initialize S3 storage.

        Args:
            s3_client: S3 client for operations.
            bucket: S3 bucket name.
            data_type_registry: Data type registry for type conversions.
            path_generator: Function to generate S3 key path from data type, item, and content_type.
                Default: Smart path generator that determines extension from content_type.
            serializer: Function to serialize data to bytes. Default: JSON.
            deserializer: Function to deserialize bytes to data. Default: JSON.
            content_type: Content-Type for S3 objects. Can be a string or a function that
                determines content_type from data_type and item. Default: "application/json".

        """
        self._s3_client = s3_client
        self._bucket = bucket
        self._data_type_registry = data_type_registry
        self._event_emitter = EventEmitter[S3Storage]()
        self._handler = S3OperationsHandler(
            s3_client=s3_client,
            bucket=bucket,
            registry=data_type_registry,
            path_generator=path_generator,
            serializer=serializer,
            deserializer=deserializer,
            content_type=content_type,
            storage=self,
        )
        self._executor = S3OperationExecutor(handler=self._handler)

        # Initialize plugins
        self._plugin_registry = PluginRegistry[S3Storage]()

    @property
    def s3_client(self) -> AbstractS3Client:
        """Get the S3 client.

        Returns:
            The S3 client instance.

        """
        return self._s3_client

    @property
    def bucket(self) -> str:
        """Get the bucket name.

        Returns:
            The S3 bucket name.

        """
        return self._bucket

    @property
    def data_type_registry(self) -> DataTypeRegistry:
        """Get the data type registry."""
        return self._data_type_registry

    @property
    def events(self) -> EventEmitter[S3Storage]:
        """Get the event emitter."""
        return self._event_emitter

    @property
    def version(self) -> str:
        """Get the version of the storage."""
        return "0.1.0"

    @property
    def plugin_registry(self) -> PluginRegistry[S3Storage]:
        """Get the plugin registry."""
        return self._plugin_registry

    def use_plugin(self, plugin: AbstractStoragePlugin[S3Storage]) -> Self:
        """Use a storage plugin."""
        self._plugin_registry.add(plugin, self.version)
        return self

    def use_preset(self, preset: AbstractStoragePluginPreset[S3Storage, AbstractStoragePlugin[S3Storage]]) -> Self:
        """Use a storage plugin preset."""
        # mypy can't infer that S3Storage implements AbstractComponent
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

        Args:
            exc_type: Exception type if exception occurred.
            exc_value: Exception value if exception occurred.
            traceback: Exception traceback if exception occurred.

        """
        # S3 client lifecycle is managed externally

    async def close(self) -> None:
        """Close storage.

        Can be called manually if not using async context manager.
        After calling this method, storage should not be used.

        """
        # S3 client lifecycle is managed externally

    async def execute[T_Result](
        self,
        operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result],
        previous_result: Any = None,
        pipeline_context: PipelineContext | None = None,
    ) -> T_Result:
        """Execute operation or pipeline atomically.

        Storage executes operations in Python (no storage-side optimization).
        Each operation or pipeline is executed atomically (operations execute immediately).

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
            from haolib.storages.indexes.path import PathIndex

            # Simple operation (executed atomically)
            await storage.create([user1, user2]).returning().execute()

            # Pipeline (all operations executed sequentially)
            index = PathIndex(data_type=User, path="User/user1.json")
            pipeline = (
                storage.read(index).returning()
                | filtero(lambda u: u.age >= 18)
            )
            results = await pipeline.execute()
            ```

        """
        # Operations execute immediately (no transaction needed for S3)
        return await self._executor.execute(
            operation, previous_result=previous_result, pipeline_context=pipeline_context
        )

    def read[T_Data](
        self,
        index: PathIndex[T_Data],
    ) -> ReadComposite[T_Data]:
        """Create read composite.

        Args:
            index: Path index for S3.

        Returns:
            ReadComposite for chaining operations.

        Example:
            ```python
            from haolib.storages.indexes.path import PathIndex

            # Read with returning
            users = await storage.read(PathIndex(User, path="users/user1.json")).returning().execute()

            # Read + Update
            await storage.read(PathIndex(User, path="users/user1.json")).update({"age": 26}).returning().execute()
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
                storage.read(PathIndex(User, path="users/user1.json")).returning()
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
            data: Data to update with. Can be dict, object, or callable.

        Returns:
            UpdateComposite for chaining operations.

        Example:
            ```python
            # Update with explicit data
            await storage.update({"age": 26}).returning().execute()

            # Update from previous_result
            pipeline = (
                storage.read(PathIndex(User, path="users/user1.json")).returning()
                | storage.update({"age": 26})
            )
            await pipeline.execute()
            ```

        """
        # Import lazily to avoid circular import
        from haolib.pipelines.operations import UpdateOperation  # noqa: PLC0415

        op = UpdateOperation(data=data)
        return UpdateComposite(storage=self, operation=op)

    def delete[T_Data](self) -> DeleteComposite[T_Data]:
        """Create delete composite.

        Returns:
            DeleteComposite for chaining operations.

        Example:
            ```python
            # Delete from previous_result
            pipeline = (
                storage.read(PathIndex(User, path="users/user1.json")).returning()
                | storage.delete()
            )
            await pipeline.execute()
            ```

        """
        # Import lazily to avoid circular import
        from haolib.pipelines.operations import DeleteOperation  # noqa: PLC0415

        op = DeleteOperation()
        return DeleteComposite(storage=self, operation=op)

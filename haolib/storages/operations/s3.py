"""Operations handler for S3 storage.

Handles execution of individual operations for S3 storage.
"""

import json
import mimetypes
import uuid
from collections.abc import AsyncIterator, Callable
from typing import TYPE_CHECKING, Any

from haolib.database.files.s3.clients.abstract import AbstractS3Client
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.dsl.patch import normalize_patch
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
from haolib.storages.indexes.path import PathIndex
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

if TYPE_CHECKING:
    from haolib.storages.s3 import S3Storage


class S3OperationsHandler:
    """Handler for executing S3 operations."""

    def __init__(
        self,
        s3_client: AbstractS3Client,
        bucket: str,
        registry: DataTypeRegistry,
        path_generator: Callable[[type, Any, str | None], str] | None = None,
        serializer: Callable[[Any], bytes] | None = None,
        deserializer: Callable[[bytes, type], Any] | None = None,
        content_type: str | Callable[[type, Any], str] = "application/json",
        storage: S3Storage | None = None,
    ) -> None:
        """Initialize the operations handler.

        Args:
            s3_client: S3 client for operations.
            bucket: S3 bucket name.
            registry: Data type registry for converting between user and storage types.
            path_generator: Function to generate S3 key path from data type, item, and content_type.
                Default: Smart path generator that determines extension from content_type.
            serializer: Function to serialize data to bytes. Default: JSON.
            deserializer: Function to deserialize bytes to data. Default: JSON.
            content_type: Content-Type for S3 objects. Can be a string or a function that
                determines content_type from data_type and item. Default: "application/json".
            storage: Storage instance for emitting events (optional).

        """
        self._s3_client = s3_client
        self._bucket = bucket
        self._registry = registry
        self._path_generator = path_generator or self._default_path_generator
        self._serializer = serializer or self._default_serializer
        self._deserializer = deserializer or self._default_deserializer
        self._content_type = content_type
        self._storage = storage

    @staticmethod
    def _get_extension_from_content_type(content_type: str) -> str:
        """Get file extension from content type.

        Args:
            content_type: MIME type (e.g., "image/jpeg", "application/json").

        Returns:
            File extension without dot (e.g., "jpg", "json").

        """
        # Use mimetypes to get extension
        extension = mimetypes.guess_extension(content_type, strict=False)
        if extension:
            # Remove leading dot
            return extension[1:]
        # Fallback: extract from content_type
        if "/" in content_type:
            subtype = content_type.split("/")[1]
            # Common mappings
            if subtype == "jpeg":
                return "jpg"
            if subtype in ("x-json", "json"):
                return "json"
            return subtype.split("+")[0]  # Handle things like "application/vnd.api+json"
        return "bin"  # Default for unknown types

    @staticmethod
    def _default_path_generator(data_type: type, item: Any, content_type: str | None = None) -> str:  # noqa: ARG004
        """Generate default path for S3 object.

        Intelligently determines file extension from content_type.
        For bytes data, uses content_type to determine extension.
        For other types, defaults to .json.

        Args:
            data_type: Type of data.
            item: Data item.
            content_type: Content-Type for the object (optional).

        Returns:
            S3 key path.

        """
        # Determine extension
        if content_type:
            extension = S3OperationsHandler._get_extension_from_content_type(content_type)
        elif data_type is bytes:
            # For bytes without content_type, default to binary
            extension = "bin"
        else:
            # For non-bytes, default to JSON
            extension = "json"

        return f"{data_type.__name__}/{uuid.uuid4().hex}.{extension}"

    @staticmethod
    def _default_serializer(data: Any) -> bytes:
        """Serialize data to bytes using JSON.

        Args:
            data: Data to serialize.

        Returns:
            Serialized bytes.

        """
        if isinstance(data, bytes):
            return data
        # Convert objects to dict if they have __dict__
        if hasattr(data, "__dict__") and not isinstance(data, (dict, list, str, int, float, bool, type(None))):
            data_dict = data.__dict__.copy()
            # Filter out SQLAlchemy internal state
            data_dict.pop("_sa_instance_state", None)
            data = data_dict
        return json.dumps(data, default=str).encode("utf-8")

    @staticmethod
    def _default_deserializer(data: bytes, data_type: type) -> Any:
        """Deserialize bytes to data using JSON.

        Args:
            data: Bytes to deserialize.
            data_type: Expected data type.

        Returns:
            Deserialized data.

        """
        if data_type is bytes:
            return data
        return json.loads(data.decode("utf-8"))

    def _get_content_type(self, data_type: type, item: Any) -> str:
        """Get content type for item.

        Args:
            data_type: Type of data.
            item: Data item.

        Returns:
            Content-Type string.

        """
        if callable(self._content_type):
            return self._content_type(data_type, item)
        return self._content_type

    def _generate_path(self, data_type: type, item: Any, content_type: str | None = None) -> str:
        """Generate S3 path for item.

        Args:
            data_type: Type of data.
            item: Data item.
            content_type: Content-Type for the object (optional).

        Returns:
            S3 key path.

        """
        return self._path_generator(data_type, item, content_type)

    def _serialize(self, data: Any) -> bytes:
        """Serialize data to bytes.

        Args:
            data: Data to serialize.

        Returns:
            Serialized bytes.

        """
        return self._serializer(data)

    def _deserialize(self, data: bytes, data_type: type) -> Any:
        """Deserialize bytes to data.

        Args:
            data: Bytes to deserialize.
            data_type: Expected data type.

        Returns:
            Deserialized data.

        """
        return self._deserializer(data, data_type)

    async def execute_create[T_Data](
        self,
        operation: CreateOperation[T_Data],
    ) -> list[T_Data]:
        """Execute create operation.

        Args:
            operation: Create operation to execute.

        Returns:
            List of created data (with paths stored if needed).

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeCreateEvent(
                component=self._storage,
                operation=operation,
                transaction=None,
            )
            await self._storage.events.emit(before_event)

        result = []
        for item in operation.data:
            # Convert user type to storage type if needed
            registration = self._registry.get_for_user_type(type(item))
            storage_item = registration.to_storage(item) if registration else item

            # Get content type
            data_type = type(item)
            content_type = self._get_content_type(data_type, storage_item)

            # Generate path (with content_type for smart extension detection)
            path = self._generate_path(data_type, storage_item, content_type)

            # Serialize
            serialized = self._serialize(storage_item)

            # Upload to S3
            await self._s3_client.put_object(
                bucket=self._bucket,
                key=path,
                body=serialized,
                content_type=content_type,
            )

            # Convert back to user type
            if registration:
                result.append(registration.from_storage(storage_item))
            else:
                result.append(storage_item)

        # Emit after event
        if self._storage is not None:
            after_event = AfterCreateEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=None,
            )
            await self._storage.events.emit(after_event)

        return result

    async def execute_read[T_Data](
        self,
        operation: ReadOperation[T_Data],
    ) -> AsyncIterator[T_Data]:
        """Execute read operation.

        Args:
            operation: Read operation to execute.

        Yields:
            Data items (converted to user types).

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeReadEvent(
                component=self._storage,
                operation=operation,
                transaction=None,
            )
            await self._storage.events.emit(before_event)

        # Validate index type
        if not isinstance(operation.search_index, PathIndex):
            msg = f"S3 storage only supports PathIndex, got {type(operation.search_index)}"
            raise TypeError(msg)

        path_index = operation.search_index
        path = path_index.path
        data_type = path_index.data_type

        # Get object from S3
        response = await self._s3_client.get_object(
            bucket=self._bucket,
            key=path,
        )

        # Extract body from response (S3GetObjectResponse has body field)
        body = response.body

        # Deserialize
        deserialized = self._deserialize(body, data_type)

        # Convert to user type if needed
        # First check if deserialized is already the storage type
        registration = self._registry.get_for_storage_type(type(deserialized))
        if registration:
            item = registration.from_storage(deserialized)
        # Check if deserialized is a dict and we need to convert it to user type
        elif isinstance(deserialized, dict):
            # Try to construct user type from dict
            if hasattr(data_type, "__init__"):
                try:
                    # Check for Pydantic v2 model_validate method
                    if hasattr(data_type, "model_validate") and callable(getattr(data_type, "model_validate", None)):
                        item = data_type.model_validate(deserialized)  # type: ignore[attr-defined]
                    # Check for Pydantic v1 parse_obj method
                    elif hasattr(data_type, "parse_obj") and callable(getattr(data_type, "parse_obj", None)):
                        item = data_type.parse_obj(deserialized)  # type: ignore[attr-defined]
                    else:
                        # Try dict unpacking for dataclasses or __init__ with **kwargs
                        item = data_type(**deserialized)
                except Exception:
                    # If construction fails, check if we have a registration that can help
                    user_registration = self._registry.get_for_user_type(data_type)
                    if user_registration:
                        # We have a registration, but deserialized is a dict
                        # This means we stored the user type directly (not storage type)
                        # Try one more time with the user type
                        try:
                            item = data_type(**deserialized)
                        except Exception:
                            # Last resort: fallback to dict
                            item = deserialized
                    else:
                        # No registration - fallback to dict
                        item = deserialized
            else:
                item = deserialized
        else:
            item = deserialized

        # Yield item (type is T_Data after conversion, but mypy can't infer it)
        yield item  # type: ignore[misc]

        # Emit after event (note: AsyncIterator can't be easily captured, so we emit after yield)
        # This is a limitation - we emit after the first item is yielded
        if self._storage is not None:
            after_event = AfterReadEvent(
                component=self._storage,
                operation=operation,
                result=operation,  # Pass operation as result placeholder
                transaction=None,
            )
            await self._storage.events.emit(after_event)

    async def execute_update[T_Data](  # noqa: PLR0915
        self,
        operation: UpdateOperation[T_Data],
    ) -> list[T_Data]:
        """Execute update operation.

        Args:
            operation: Update operation to execute.

        Returns:
            List of updated data.

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeUpdateEvent(
                component=self._storage,
                operation=operation,
                transaction=None,
            )
            await self._storage.events.emit(before_event)

        # Validate index type
        if not isinstance(operation.search_index, PathIndex):
            msg = f"S3 storage only supports PathIndex, got {type(operation.search_index)}"
            raise TypeError(msg)

        path_index = operation.search_index
        path = path_index.path
        data_type = path_index.data_type

        # Get object from S3
        response = await self._s3_client.get_object(
            bucket=self._bucket,
            key=path,
        )

        # Extract body from response (S3GetObjectResponse has body field)
        body = response.body

        # Deserialize
        deserialized = self._deserialize(body, data_type)

        # Convert to user type if needed
        # First check if deserialized is already the storage type
        registration = self._registry.get_for_storage_type(type(deserialized))
        if registration:
            item = registration.from_storage(deserialized)
        # Check if deserialized is a dict and we need to convert it to user type
        elif isinstance(deserialized, dict):
            # Try to construct user type from dict
            if hasattr(data_type, "__init__"):
                try:
                    # Check for Pydantic v2 model_validate method
                    if hasattr(data_type, "model_validate") and callable(getattr(data_type, "model_validate", None)):
                        item = data_type.model_validate(deserialized)  # type: ignore[attr-defined]
                    # Check for Pydantic v1 parse_obj method
                    elif hasattr(data_type, "parse_obj") and callable(getattr(data_type, "parse_obj", None)):
                        item = data_type.parse_obj(deserialized)  # type: ignore[attr-defined]
                    else:
                        # Try dict unpacking for dataclasses or __init__ with **kwargs
                        item = data_type(**deserialized)
                except Exception:
                    # If construction fails, check if we have a registration that can help
                    user_registration = self._registry.get_for_user_type(data_type)
                    if user_registration:
                        # We have a registration, but deserialized is a dict
                        # This means we stored the user type directly (not storage type)
                        # Try one more time with the user type
                        try:
                            item = data_type(**deserialized)
                        except Exception:
                            # Last resort: fallback to dict
                            item = deserialized
                    else:
                        # No registration - fallback to dict
                        item = deserialized
            else:
                item = deserialized
        else:
            item = deserialized

        # Apply patch
        patch = normalize_patch(operation.patch)
        if isinstance(patch, dict):
            # Dict patch - update fields
            if isinstance(item, dict):
                item.update(patch)
            else:
                # Try to update object attributes
                for key, value in patch.items():
                    if hasattr(item, key):
                        setattr(item, key, value)
        elif callable(patch):
            # Callable patch - transform function
            item = patch(item)

        # Convert back to storage type
        updated_storage = registration.to_storage(item) if registration else item

        # Get content type
        content_type = self._get_content_type(data_type, updated_storage)

        # Serialize and upload
        serialized = self._serialize(updated_storage)
        await self._s3_client.put_object(
            bucket=self._bucket,
            key=path,
            body=serialized,
            content_type=content_type,
        )

        # Convert back to user type
        # Type is list[T_Data] after conversion, but mypy can't infer it
        result: list[T_Data] = [registration.from_storage(updated_storage)] if registration else [updated_storage]  # type: ignore[assignment]

        # Emit after event
        if self._storage is not None:
            after_event = AfterUpdateEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=None,
            )
            await self._storage.events.emit(after_event)

        return result

    async def execute_delete[T_Data](
        self,
        operation: DeleteOperation[T_Data],
    ) -> int:
        """Execute delete operation.

        Args:
            operation: Delete operation to execute.

        Returns:
            Number of deleted items (0 or 1 for S3).

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeDeleteEvent(
                component=self._storage,
                operation=operation,
                transaction=None,
            )
            await self._storage.events.emit(before_event)

        # Validate index type
        if not isinstance(operation.search_index, PathIndex):
            msg = f"S3 storage only supports PathIndex, got {type(operation.search_index)}"
            raise TypeError(msg)

        path_index = operation.search_index
        path = path_index.path

        # Delete object from S3
        await self._s3_client.delete_object(
            bucket=self._bucket,
            key=path,
        )

        deleted_count = 1

        # Emit after event
        if self._storage is not None:
            after_event = AfterDeleteEvent(
                component=self._storage,
                operation=operation,
                result=deleted_count,
                transaction=None,
            )
            await self._storage.events.emit(after_event)

        return deleted_count

    async def execute_filter[T_Data](
        self,
        operation: FilterOperation[T_Data],
        previous_result: list[T_Data] | AsyncIterator[T_Data],
    ) -> list[T_Data]:
        """Execute filter operation.

        Args:
            operation: Filter operation to execute.
            previous_result: Previous operation result to filter.

        Returns:
            Filtered list.

        """
        # Collect async iterator if needed
        if isinstance(previous_result, AsyncIterator):
            items = [item async for item in previous_result]
        else:
            items = list(previous_result)

        # Emit before event
        if self._storage is not None:
            before_event = BeforeFilterEvent(
                component=self._storage,
                operation=operation,
                input_data=items,
                transaction=None,
            )
            await self._storage.events.emit(before_event)

        # Filter
        result = [item for item in items if operation.predicate(item)]

        # Emit after event
        if self._storage is not None:
            after_event = AfterFilterEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=None,
            )
            await self._storage.events.emit(after_event)

        return result

    async def execute_map[T_Data, T_Result](
        self,
        operation: MapOperation[T_Data, T_Result],
        previous_result: list[T_Data] | AsyncIterator[T_Data],
    ) -> list[T_Result]:
        """Execute map operation.

        Args:
            operation: Map operation to execute.
            previous_result: Previous operation result to map.

        Returns:
            Mapped list.

        """
        # Collect async iterator if needed
        if isinstance(previous_result, AsyncIterator):
            items = [item async for item in previous_result]
        else:
            items = list(previous_result)

        # Emit before event
        if self._storage is not None:
            before_event = BeforeMapEvent(
                component=self._storage,
                operation=operation,
                input_data=items,
                transaction=None,
            )
            await self._storage.events.emit(before_event)

        # Map
        result = [operation.mapper(item, idx) for idx, item in enumerate(items)]

        # Emit after event
        if self._storage is not None:
            after_event = AfterMapEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=None,
            )
            await self._storage.events.emit(after_event)

        return result

    async def execute_reduce[T_Data, T_Result](
        self,
        operation: ReduceOperation[T_Data, T_Result],
        previous_result: list[T_Data] | AsyncIterator[T_Data],
    ) -> T_Result:
        """Execute reduce operation.

        Args:
            operation: Reduce operation to execute.
            previous_result: Previous operation result to reduce.

        Returns:
            Reduced value.

        """
        # Collect async iterator if needed
        if isinstance(previous_result, AsyncIterator):
            items = [item async for item in previous_result]
        else:
            items = list(previous_result)

        # Emit before event
        if self._storage is not None:
            before_event = BeforeReduceEvent(
                component=self._storage,
                operation=operation,
                input_data=items,
                transaction=None,
            )
            await self._storage.events.emit(before_event)

        # Reduce
        result = operation.initial
        for item in items:
            result = operation.reducer(result, item)

        # Emit after event
        if self._storage is not None:
            after_event = AfterReduceEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=None,
            )
            await self._storage.events.emit(after_event)

        return result

    async def execute_transform[T_Data, T_Result](
        self,
        operation: TransformOperation[T_Data, T_Result],
        previous_result: T_Data,
    ) -> T_Result:
        """Execute transform operation.

        Args:
            operation: Transform operation to execute.
            previous_result: Previous operation result to transform.

        Returns:
            Transformed result.

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeTransformEvent(
                component=self._storage,
                operation=operation,
                input_data=previous_result,
                transaction=None,
            )
            await self._storage.events.emit(before_event)

        # Transform
        result = operation.transformer(previous_result)

        # Emit after event
        if self._storage is not None:
            after_event = AfterTransformEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=None,
            )
            await self._storage.events.emit(after_event)

        return result

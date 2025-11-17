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

# Import operations lazily to avoid circular import
# Operations are imported in methods that use them
if TYPE_CHECKING:
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

# Import events lazily to avoid circular import
# Events are imported in methods that use them
from haolib.storages.events.operations import (
    AfterDeleteEvent,
    AfterFilterEvent,
    AfterMapEvent,
    AfterPatchEvent,
    AfterReadEvent,
    AfterReduceEvent,
    AfterTransformEvent,
    AfterUpdateEvent,
    BeforeDeleteEvent,
    BeforeFilterEvent,
    BeforeMapEvent,
    BeforePatchEvent,
    BeforeReadEvent,
    BeforeReduceEvent,
    BeforeTransformEvent,
    BeforeUpdateEvent,
)
from haolib.storages.indexes.path import PathIndex

# Import operations lazily to avoid circular import
# Operations are imported in methods that use them

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
    ) -> list[tuple[T_Data, str]]:
        """Execute create operation.

        Args:
            operation: Create operation to execute.

        Returns:
            List of tuples (created_data, s3_path) for each created item.

        Example:
            ```python
            result = await storage.create([user1, user2]).returning().execute()
            for data, path in result:
                print(f"Saved {data} to {path}")
            # Output:
            # Saved User(name='Alice') to User/123e4567-e89b-12d3-a456-426614174000.json
            # Saved User(name='Bob') to User/123e4567-e89b-12d3-a456-426614174001.json
            ```

        """
        # Emit before event
        if self._storage is not None:
            from haolib.storages.events.operations import BeforeCreateEvent  # noqa: PLC0415

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
            user_item = registration.from_storage(storage_item) if registration else storage_item

            # Return tuple (data, path)
            result.append((user_item, path))

        # Emit after event
        # Extract just the data for the event (backward compatibility)
        data_result = [item for item, _ in result]
        if self._storage is not None:
            from haolib.storages.events.operations import AfterCreateEvent  # noqa: PLC0415

            after_event = AfterCreateEvent(
                component=self._storage,
                operation=operation,
                result=data_result,
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

    async def execute_patch[T_Data](  # noqa: PLR0915
        self,
        operation: PatchOperation[T_Data],
        previous_result: Any | None = None,
    ) -> list[T_Data]:
        """Execute patch operation (partial update).

        Can work in two modes:
        1. Search mode: uses search_index to find data, patch to update
        2. Pipeline mode: uses previous_result as data to update

        Args:
            operation: Patch operation to execute.
            previous_result: Previous operation result (if in pipeline mode).

        Returns:
            List of updated data.

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforePatchEvent(
                component=self._storage,
                operation=operation,
                transaction=None,
            )
            await self._storage.events.emit(before_event)

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
                return []

            # Get data type from first item
            first_item = items[0]
            data_type = type(first_item)

            # Apply patch to each item
            if operation.patch is None:
                msg = "PatchOperation in pipeline mode requires patch parameter"
                raise ValueError(msg)

            updated_items = []
            for item in items:
                # Apply patch to item
                if hasattr(item, "model_dump"):
                    # Pydantic model
                    item_dict = item.model_dump()
                    item_dict.update(operation.patch)
                    updated_item = type(item)(**item_dict)
                elif hasattr(item, "__dict__"):
                    # Dataclass or regular class
                    item_dict = item.__dict__.copy()
                    item_dict.update(operation.patch)
                    updated_item = type(item)(**item_dict)
                else:
                    # Fallback: try to update directly
                    updated_item = {**item, **operation.patch} if isinstance(item, dict) else item
                updated_items.append(updated_item)

            # Save updated items back to S3
            # For S3, we need to determine paths from items
            # This is a limitation - we need to know the path for each item
            # For now, we'll require that items have a path attribute or use a default path
            saved_items = []
            for updated_item in updated_items:
                # Try to get path from item
                if hasattr(updated_item, "path"):
                    path = updated_item.path
                elif hasattr(updated_item, "id"):
                    # Use ID as path
                    path = f"{data_type.__name__}/{updated_item.id}"
                else:
                    msg = "Cannot determine S3 path for item in pipeline mode. Item must have 'path' or 'id' attribute."
                    raise ValueError(msg)

                # Save to S3
                content_type = self._get_content_type(data_type, updated_item)
                serialized = self._serialize(updated_item)
                await self._s3_client.put_object(
                    bucket=self._bucket,
                    key=path,
                    body=serialized,
                    content_type=content_type,
                )
                saved_items.append(updated_item)

            # Emit after event
            if self._storage is not None:
                after_event = AfterPatchEvent(
                    component=self._storage,
                    operation=operation,
                    result=saved_items,
                    transaction=None,
                )
                await self._storage.events.emit(after_event)

            return saved_items

        # Search mode: use search_index
        if operation.search_index is None:
            msg = "PatchOperation requires either search_index or previous_result"
            raise ValueError(msg)

        if operation.patch is None:
            msg = "PatchOperation requires patch parameter"
            raise ValueError(msg)

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

        # Apply partial patch (only specified fields)
        if isinstance(item, dict):
            item.update(operation.patch)
        else:
            # Try to update object attributes
            for key, value in operation.patch.items():
                if hasattr(item, key):
                    setattr(item, key, value)

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
            after_event = AfterPatchEvent(
                component=self._storage,
                operation=operation,
                result=result,
                transaction=None,
            )
            await self._storage.events.emit(after_event)

        return result

    async def execute_update[T_Data](
        self,
        operation: UpdateOperation[T_Data],
        previous_result: Any | None = None,
        pipeline_context: Any | None = None,  # noqa: ARG002
        previous_operation: Any | None = None,
    ) -> list[T_Data]:
        """Execute update operation (full update).

        Can work in two modes:
        1. Search mode: uses search_index to find data, data to update
        2. Pipeline mode: uses previous_result as data to update

        Args:
            operation: Update operation to execute.
            previous_result: Previous operation result (if in pipeline mode).
            pipeline_context: Optional context about the entire pipeline for global optimization.
            previous_operation: Previous operation (used to extract path for S3 in pipeline mode).

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
                return []

            # Get data type from first item
            first_item = items[0]
            data_type = type(first_item)

            # Apply update to each item
            updated_items: list[T_Data] = []
            for item in items:
                # Apply data (object or function)
                if operation.data:
                    updated_item: T_Data = operation.data(item) if callable(operation.data) else operation.data  # type: ignore[assignment]
                else:
                    updated_item = item  # type: ignore[assignment]
                updated_items.append(updated_item)

            # Save updated items back to S3
            # In pipeline mode, we need to preserve paths from original items
            # Store original items with their paths before updating
            saved_items: list[T_Data] = []

            # Try to extract path from previous_operation if it was ReadOperation
            read_path: str | None = None
            if previous_operation is not None:
                # Import operations lazily to avoid circular import
                from haolib.pipelines.operations import ReadOperation  # noqa: PLC0415

                if isinstance(previous_operation, ReadOperation) and isinstance(
                    previous_operation.search_index, PathIndex
                ):
                    read_path = previous_operation.search_index.path

            for i, updated_item in enumerate(updated_items):
                original_item = items[i]
                # Try to get path from original item (before update)
                # Path might be stored in a tuple (data, path) from create operation
                # or in the item itself
                path: str | None = None

                # First, try to use path from ReadOperation if available
                if read_path is not None:
                    path = read_path
                # Check if original_item is a tuple (data, path) from create
                elif isinstance(original_item, tuple) and len(original_item) == 2:
                    _, path = original_item
                elif not isinstance(original_item, tuple) and hasattr(original_item, "path"):
                    path = original_item.path  # type: ignore[attr-defined]
                elif not isinstance(original_item, tuple) and hasattr(original_item, "id"):
                    # Use ID as path
                    path = f"{data_type.__name__}/{original_item.id}"  # type: ignore[attr-defined]

                # If still no path, try updated_item
                if path is None:
                    if not isinstance(updated_item, tuple) and hasattr(updated_item, "path"):
                        path = updated_item.path  # type: ignore[attr-defined]
                    elif not isinstance(updated_item, tuple) and hasattr(updated_item, "id"):
                        path = f"{data_type.__name__}/{updated_item.id}"  # type: ignore[attr-defined]

                if path is None:
                    msg = (
                        "Cannot determine S3 path for item in pipeline mode. "
                        "Item must have 'path' or 'id' attribute, or come from a create operation."
                    )
                    raise ValueError(msg)

                # Get content type and serialize
                content_type = self._get_content_type(data_type, updated_item)
                serialized = self._serialize(updated_item)
                await self._s3_client.put_object(
                    bucket=self._bucket,
                    key=path,
                    body=serialized,
                    content_type=content_type,
                )
                saved_items.append(updated_item)

            # Emit after event
            if self._storage is not None:
                after_event = AfterUpdateEvent(
                    component=self._storage,
                    operation=operation,
                    result=saved_items,
                    transaction=None,
                )
                await self._storage.events.emit(after_event)

            return saved_items

        # Search mode: use search_index
        if operation.search_index is None:
            msg = "UpdateOperation requires either search_index or previous_result"
            raise ValueError(msg)

        if operation.data is None:
            msg = "UpdateOperation requires data parameter"
            raise ValueError(msg)

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

        # Apply full update (replace entire object)
        updated_data: T_Data = operation.data(item) if callable(operation.data) else operation.data  # type: ignore[assignment]

        # Convert back to storage type
        updated_storage = registration.to_storage(updated_data) if registration else updated_data

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
        previous_result: Any | None = None,
        previous_operation: Any | None = None,
    ) -> int:
        """Execute delete operation.

        Can work in two modes:
        1. Search mode: uses search_index to find data to delete
        2. Pipeline mode: uses previous_result as data to delete

        Args:
            operation: Delete operation to execute.
            previous_result: Previous operation result (if in pipeline mode).
            previous_operation: Previous operation (used to extract path for S3 in pipeline mode).

        Returns:
            Number of deleted items.

        """
        # Emit before event
        if self._storage is not None:
            before_event = BeforeDeleteEvent(
                component=self._storage,
                operation=operation,
                transaction=None,
            )
            await self._storage.events.emit(before_event)

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

            # Try to extract path from previous_operation if it was ReadOperation
            read_path: str | None = None
            if previous_operation is not None:
                # Import operations lazily to avoid circular import
                from haolib.pipelines.operations import ReadOperation  # noqa: PLC0415

                if isinstance(previous_operation, ReadOperation) and isinstance(
                    previous_operation.search_index, PathIndex
                ):
                    read_path = previous_operation.search_index.path

            # Get paths from items and delete
            deleted_count = 0
            for item in items:
                # First, try to use path from ReadOperation if available
                if read_path is not None:
                    path = read_path
                # Try to get path from item
                elif hasattr(item, "path"):
                    path = item.path  # type: ignore[attr-defined]
                elif hasattr(item, "id"):
                    # Use ID as path
                    data_type = type(item)
                    path = f"{data_type.__name__}/{item.id}"  # type: ignore[attr-defined]
                else:
                    msg = "Cannot determine S3 path for item in pipeline mode. Item must have 'path' or 'id' attribute."
                    raise ValueError(msg)

                # Delete object from S3
                await self._s3_client.delete_object(
                    bucket=self._bucket,
                    key=path,
                )
                deleted_count += 1

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

        # Search mode: use search_index
        if operation.search_index is None:
            msg = "DeleteOperation requires either search_index or previous_result"
            raise ValueError(msg)

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

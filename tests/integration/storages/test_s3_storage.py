"""Integration tests for S3 storage operations.

Tests basic CRUD operations and pipelines for S3Storage.
"""

from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Any

import pytest
import pytest_asyncio

from haolib.database.files.s3.clients.abstract import (
    AbstractS3Client,
    S3BucketAlreadyExistsClientException,
    S3BucketAlreadyOwnedByYouClientException,
    S3NoSuchKeyClientException,
)
from haolib.database.files.s3.clients.pydantic import S3DeleteObjectsDelete, S3DeleteObjectsDeleteObject
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.dsl import createo, deleteo, filtero, mapo, reado, updateo
from haolib.storages.indexes.path import PathIndex
from haolib.storages.s3 import S3Storage
from tests.integration.storages.conftest import User

# Constants for test values
BUCKET_NAME = "test-storage-bucket"
ALICE_AGE = 25
BOB_AGE = 30
ALICE_UPDATED_AGE = 26


@pytest_asyncio.fixture
async def s3_storage(
    s3_client: AbstractS3Client,
    registry: DataTypeRegistry,
) -> AsyncIterator[S3Storage]:
    """Create S3 storage for testing."""
    # Create bucket (ignore if already exists)
    with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
        await s3_client.create_bucket(BUCKET_NAME)

    storage = S3Storage(
        s3_client=s3_client,
        bucket=BUCKET_NAME,
        data_type_registry=registry,
    )

    yield storage

    # Cleanup: delete all objects in bucket
    with suppress(Exception):
        # List all objects
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME)
        if response.contents:
            # Delete objects
            delete_objects = S3DeleteObjectsDelete(
                objects=[S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None]
            )
            await s3_client.delete_objects(bucket=BUCKET_NAME, delete=delete_objects)


class TestS3StorageOperations:
    """Integration tests for S3Storage operations."""

    @pytest.mark.asyncio
    async def test_execute_create(self, s3_storage: S3Storage) -> None:
        """Test create operation."""
        users = [
            User(name="Alice", age=ALICE_AGE, email="alice@example.com"),
            User(name="Bob", age=BOB_AGE, email="bob@example.com"),
        ]

        result = await s3_storage.execute(createo(users))

        assert len(result) == 2
        assert result[0].name == "Alice"
        assert result[0].age == ALICE_AGE
        assert result[0].email == "alice@example.com"
        assert result[1].name == "Bob"
        assert result[1].age == BOB_AGE
        assert result[1].email == "bob@example.com"

    @pytest.mark.asyncio
    async def test_execute_read(self, s3_storage: S3Storage, s3_client: AbstractS3Client) -> None:
        """Test read operation."""
        # First create a user
        user = User(name="Alice", age=ALICE_AGE, email="alice@example.com")
        await s3_storage.execute(createo([user]))

        # Get the path (we need to know it for PathIndex)
        # Since we use default path generator, we need to find the object
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="User/")
        assert response.contents is not None and len(response.contents) == 1
        path = response.contents[0].key
        assert path is not None

        # Read using PathIndex
        index = PathIndex(data_type=User, index_name="alice", path=path)
        read_result = await s3_storage.execute(reado(search_index=index))
        async for read_user in read_result:
            assert read_user.name == "Alice"
            assert read_user.age == ALICE_AGE
            assert read_user.email == "alice@example.com"

    @pytest.mark.asyncio
    async def test_execute_update(self, s3_storage: S3Storage, s3_client: AbstractS3Client) -> None:
        """Test update operation."""
        # First create a user
        user = User(name="Alice", age=ALICE_AGE, email="alice@example.com")
        await s3_storage.execute(createo([user]))

        # Get the path
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="User/")
        assert response.contents is not None and len(response.contents) == 1
        path = response.contents[0].key
        assert path is not None

        # Update using PathIndex
        index = PathIndex(data_type=User, index_name="alice", path=path)
        result = await s3_storage.execute(updateo(search_index=index, patch={"age": ALICE_UPDATED_AGE}))

        assert len(result) == 1
        assert result[0].name == "Alice"
        assert result[0].age == ALICE_UPDATED_AGE
        assert result[0].email == "alice@example.com"

        # Verify in S3
        read_result = await s3_storage.execute(reado(search_index=index))
        async for read_user in read_result:
            assert read_user.age == ALICE_UPDATED_AGE

    @pytest.mark.asyncio
    async def test_execute_delete(self, s3_storage: S3Storage, s3_client: AbstractS3Client) -> None:
        """Test delete operation."""
        # First create a user
        user = User(name="Alice", age=ALICE_AGE, email="alice@example.com")
        await s3_storage.execute(createo([user]))

        # Get the path
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="User/")
        assert response.contents is not None and len(response.contents) == 1
        path = response.contents[0].key
        assert path is not None

        # Delete using PathIndex
        index = PathIndex(data_type=User, index_name="alice", path=path)
        deleted_count = await s3_storage.execute(deleteo(search_index=index))

        assert deleted_count == 1

        # Verify deleted
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="User/")
        assert response.contents is None or len(response.contents) == 0

    @pytest.mark.asyncio
    async def test_execute_pipeline_read_filter(self, s3_storage: S3Storage, s3_client: AbstractS3Client) -> None:
        """Test pipeline with read and filter."""
        # Create users
        users = [
            User(name="Alice", age=ALICE_AGE, email="alice@example.com"),
            User(name="Bob", age=BOB_AGE, email="bob@example.com"),
        ]
        await s3_storage.execute(createo(users))

        # Get paths
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="User/")
        assert response.contents is not None and len(response.contents) == 2

        # Read first user and filter
        path = response.contents[0].key
        assert path is not None
        index = PathIndex(data_type=User, index_name="user", path=path)
        pipeline = reado(search_index=index) | filtero(lambda u: u.age >= 25)

        # Pipeline returns list after filter, not AsyncIterator
        results = await s3_storage.execute(pipeline)

        assert len(results) == 1
        assert results[0].age >= 25

    @pytest.mark.asyncio
    async def test_execute_pipeline_read_map(self, s3_storage: S3Storage, s3_client: AbstractS3Client) -> None:
        """Test pipeline with read and map."""
        # Create user
        user = User(name="Alice", age=ALICE_AGE, email="alice@example.com")
        await s3_storage.execute(createo([user]))

        # Get path
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="User/")
        assert response.contents is not None and len(response.contents) == 1
        path = response.contents[0].key
        assert path is not None

        # Read and map to email
        index = PathIndex(data_type=User, index_name="user", path=path)

        def get_email(user: User, _idx: int) -> str:
            return user.email

        pipeline = reado(search_index=index) | mapo(get_email)

        results = await s3_storage.execute(pipeline)
        assert len(results) == 1
        assert results[0] == "alice@example.com"

    @pytest.mark.asyncio
    async def test_execute_read_nonexistent(self, s3_storage: S3Storage) -> None:
        """Test read operation with non-existent path."""
        index = PathIndex(data_type=User, index_name="nonexistent", path="User/nonexistent.json")

        with pytest.raises(S3NoSuchKeyClientException):
            async for _ in await s3_storage.execute(reado(search_index=index)):
                pass

    @pytest.mark.asyncio
    async def test_execute_update_with_callable_patch(self, s3_storage: S3Storage, s3_client: AbstractS3Client) -> None:
        """Test update operation with callable patch."""
        # First create a user
        user = User(name="Alice", age=ALICE_AGE, email="alice@example.com")
        await s3_storage.execute(createo([user]))

        # Get the path
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="User/")
        assert response.contents is not None and len(response.contents) == 1
        path = response.contents[0].key
        assert path is not None

        # Update using callable patch
        index = PathIndex(data_type=User, index_name="alice", path=path)

        def update_age(user: User) -> User:
            return User(name=user.name, age=ALICE_UPDATED_AGE, email=user.email)

        result = await s3_storage.execute(updateo(search_index=index, patch=update_age))

        assert len(result) == 1
        assert result[0].age == ALICE_UPDATED_AGE

    @pytest.mark.asyncio
    async def test_custom_path_generator(self, s3_client: AbstractS3Client, registry: DataTypeRegistry) -> None:
        """Test custom path generator."""
        # Create bucket (ignore if already exists)
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(BUCKET_NAME)

        def custom_path_generator(data_type: type, item: Any, content_type: str | None = None) -> str:
            if hasattr(item, "id") and item.id is not None:
                return f"{data_type.__name__}/{item.id}.json"
            return f"{data_type.__name__}/custom.json"

        storage = S3Storage(
            s3_client=s3_client,
            bucket=BUCKET_NAME,
            data_type_registry=registry,
            path_generator=custom_path_generator,
        )

        user = User(name="Alice", age=ALICE_AGE, email="alice@example.com")
        await storage.execute(createo([user]))

        # Verify path
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="User/")
        assert response.contents is not None and len(response.contents) == 1
        key = response.contents[0].key
        assert key is not None
        assert key == "User/custom.json"

        # Cleanup
        await s3_client.delete_object(bucket=BUCKET_NAME, key=key)

    @pytest.mark.asyncio
    async def test_execute_create_with_bytes(self, s3_storage: S3Storage) -> None:
        """Test create operation with bytes data."""
        # Create image data (simulated JPEG)
        image_data = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00"

        # Create storage with image content type
        storage = S3Storage(
            s3_client=s3_storage.s3_client,
            bucket=s3_storage.bucket,
            data_type_registry=s3_storage.data_type_registry,
            content_type="image/jpeg",
        )

        result = await storage.execute(createo([image_data]))

        assert len(result) == 1
        assert result[0] == image_data

        # Verify path has .jpg extension
        response = await s3_storage.s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="bytes/")
        assert response.contents is not None and len(response.contents) == 1
        key = response.contents[0].key
        assert key is not None
        assert key.endswith(".jpg")

        # Cleanup
        await s3_storage.s3_client.delete_object(bucket=BUCKET_NAME, key=key)

    @pytest.mark.asyncio
    async def test_execute_create_with_dynamic_content_type(
        self, s3_client: AbstractS3Client, registry: DataTypeRegistry
    ) -> None:
        """Test create operation with dynamic content_type function."""
        # Create bucket (ignore if already exists)
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(BUCKET_NAME)

        def content_type_detector(data_type: type, item: Any) -> str:
            if data_type == bytes:
                # Simple heuristic: check for JPEG magic bytes
                if item.startswith(b"\xff\xd8\xff"):
                    return "image/jpeg"
                if item.startswith(b"\x89PNG"):
                    return "image/png"
                return "application/octet-stream"
            return "application/json"

        storage = S3Storage(
            s3_client=s3_client,
            bucket=BUCKET_NAME,
            data_type_registry=registry,
            content_type=content_type_detector,
        )

        # Create JPEG image
        jpeg_data = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00"
        result = await storage.execute(createo([jpeg_data]))

        assert len(result) == 1
        assert result[0] == jpeg_data

        # Verify path has .jpg extension
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="bytes/")
        assert response.contents is not None and len(response.contents) == 1
        key = response.contents[0].key
        assert key is not None
        assert key.endswith(".jpg")

        # Verify content_type in S3
        obj_response = await s3_client.get_object(bucket=BUCKET_NAME, key=key)
        assert obj_response.content_type == "image/jpeg"

        # Cleanup
        await s3_client.delete_object(bucket=BUCKET_NAME, key=key)

    @pytest.mark.asyncio
    async def test_execute_read_bytes(self, s3_storage: S3Storage, s3_client: AbstractS3Client) -> None:
        """Test read operation with bytes data."""
        # Create image storage
        storage = S3Storage(
            s3_client=s3_storage.s3_client,
            bucket=s3_storage.bucket,
            data_type_registry=s3_storage.data_type_registry,
            content_type="image/jpeg",
        )

        # Create image data
        image_data = b"\xff\xd8\xff\xe0\x00\x10JFIF\x00\x01\x01\x01\x00H\x00H\x00\x00"
        await storage.execute(createo([image_data]))

        # Get the path
        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME, prefix="bytes/")
        assert response.contents is not None and len(response.contents) == 1
        path = response.contents[0].key
        assert path is not None

        # Read using PathIndex
        index = PathIndex(data_type=bytes, index_name="image", path=path)
        read_result = await storage.execute(reado(search_index=index))
        async for read_data in read_result:
            assert read_data == image_data
            assert isinstance(read_data, bytes)

        # Cleanup
        await s3_client.delete_object(bucket=BUCKET_NAME, key=path)

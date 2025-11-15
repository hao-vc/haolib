"""Integration tests for executable pipelines across multiple storages."""

from collections.abc import AsyncIterator
from contextlib import suppress

import pytest
import pytest_asyncio

from haolib.database.files.s3.clients.abstract import (
    AbstractS3Client,
    S3BucketAlreadyExistsClientException,
    S3BucketAlreadyOwnedByYouClientException,
)
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.dsl import createo, reado, reduceo, transformo
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.s3 import S3Storage
from haolib.storages.sqlalchemy import SQLAlchemyStorage
from tests.integration.storages.conftest import User

# Constants for test values
BUCKET_NAME = "test-executable-pipeline-bucket"
ALICE_AGE = 25
BOB_AGE = 30


@pytest_asyncio.fixture
async def sql_storage(sqlalchemy_storage: SQLAlchemyStorage) -> SQLAlchemyStorage:
    """SQL storage for testing."""
    return sqlalchemy_storage


@pytest_asyncio.fixture
async def s3_storage(
    s3_client: AbstractS3Client,
    registry: DataTypeRegistry,
) -> AsyncIterator[S3Storage]:
    """S3 storage for testing."""
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
        from haolib.database.files.s3.clients.pydantic import S3DeleteObjectsDelete, S3DeleteObjectsDeleteObject

        response = await s3_client.list_objects_v2(bucket=BUCKET_NAME)
        if response.contents:
            delete_objects = S3DeleteObjectsDelete(
                objects=[S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None]
            )
            await s3_client.delete_objects(bucket=BUCKET_NAME, delete=delete_objects)


class TestExecutablePipeline:
    """Integration tests for executable pipelines."""

    @pytest.mark.asyncio
    async def test_simple_pipeline_sql_to_s3(
        self,
        sql_storage: SQLAlchemyStorage,
        s3_storage: S3Storage,
    ) -> None:
        """Test simple pipeline from SQL to S3."""
        # Create users in SQL
        users = [
            User(name="Alice", age=ALICE_AGE, email="alice@example.com"),
            User(name="Bob", age=BOB_AGE, email="bob@example.com"),
        ]
        await sql_storage.execute(createo(users))

        # Create pipeline: SQL -> reduce -> transform -> S3
        # New syntax: operation ^ storage for binding, | for composition
        # Both operators have same precedence (10), so they execute left-to-right
        pipeline = (
            reado(search_index=ParamIndex(User)) ^ sql_storage  # Read all users
            | reduceo(lambda acc, u: acc + u.age, 0) ^ sql_storage
            | transformo(lambda total: str(total).encode())
            | createo([lambda data: data]) ^ s3_storage
        )

        # Execute pipeline
        result = await pipeline.execute()

        # Result should be list of created items
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] == str(ALICE_AGE + BOB_AGE).encode()

    @pytest.mark.asyncio
    async def test_pipeline_with_multiple_storage_switches(
        self,
        sql_storage: SQLAlchemyStorage,
        s3_storage: S3Storage,
    ) -> None:
        """Test pipeline with multiple storage switches."""
        # Create users in SQL
        users = [
            User(name="Alice", age=ALICE_AGE, email="alice@example.com"),
            User(name="Bob", age=BOB_AGE, email="bob@example.com"),
        ]
        await sql_storage.execute(createo(users))

        # Create pipeline: SQL -> reduce -> transform -> S3
        # New syntax: operation ^ storage for binding, | for composition
        # Both operators have same precedence (10), so they execute left-to-right
        pipeline = (
            reado(search_index=ParamIndex(User)) ^ sql_storage
            | reduceo(lambda acc, u: acc + u.age, 0) ^ sql_storage
            | transformo(lambda total: str(total).encode())
            | createo([lambda data: data]) ^ s3_storage
        )

        # Execute pipeline
        result = await pipeline.execute()

        # Result should be list of created items (from createo)
        assert isinstance(result, list)
        assert len(result) == 1
        # The result is bytes from createo, so we decode and check
        assert result[0] == str(ALICE_AGE + BOB_AGE).encode()

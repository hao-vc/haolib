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
from haolib.pipelines import filtero, mapo, reduceo, transformo
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
        await sql_storage.create(users).returning().execute()

        # Create pipeline: SQL -> reduce -> transform -> S3
        pipeline = (
            sql_storage.read(ParamIndex(User)).returning()  # Read all users
            | reduceo(lambda acc, u: acc + u.age, 0)
            | transformo(lambda total_list: str(total_list[0]).encode() if isinstance(total_list, list) and len(total_list) == 1 else str(total_list).encode())
            | s3_storage.create()  # Uses previous_result
        )

        # Execute pipeline
        result = await pipeline.execute()

        # Result should be list of tuples (data, path) from S3 create
        assert isinstance(result, list)
        assert len(result) == 1
        data, path = result[0]
        assert data == str(ALICE_AGE + BOB_AGE).encode()
        assert path.startswith("bytes/")

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
        await sql_storage.create(users).returning().execute()

        # Create pipeline: SQL -> reduce -> transform -> S3
        pipeline = (
            sql_storage.read(ParamIndex(User)).returning()
            | reduceo(lambda acc, u: acc + u.age, 0)
            | transformo(lambda total_list: str(total_list[0]).encode() if isinstance(total_list, list) and len(total_list) == 1 else str(total_list).encode())
            | s3_storage.create()  # Uses previous_result
        )

        # Execute pipeline
        result = await pipeline.execute()

        # Result should be list of tuples (data, path) from S3 create
        assert isinstance(result, list)
        assert len(result) == 1
        # The result is tuple (bytes, path) from createo
        data, path = result[0]
        assert data == str(ALICE_AGE + BOB_AGE).encode()
        assert path.startswith("bytes/")

    @pytest.mark.asyncio
    async def test_pipeline_create_with_merged_data(
        self,
        sql_storage: SQLAlchemyStorage,
        s3_storage: S3Storage,
    ) -> None:
        """Test pipeline where createo merges previous_result with additional data."""
        # Create users in SQL
        users = [
            User(name="Alice", age=ALICE_AGE, email="alice@example.com"),
            User(name="Bob", age=BOB_AGE, email="bob@example.com"),
        ]
        await sql_storage.create(users).returning().execute()

        # Create pipeline: SQL -> filter -> create with additional data
        # previous_result (filtered users) should be prepended to [extra_user]
        extra_user = User(name="Charlie", age=40, email="charlie@example.com")
        pipeline = (
            sql_storage.read(ParamIndex(User)).returning()
            | filtero(lambda u: u.age >= 30)
            | s3_storage.create([extra_user])  # previous_result + [extra_user]
        )

        # Execute pipeline
        result = await pipeline.execute()

        # Result should contain both filtered users (Bob) and extra_user
        assert isinstance(result, list)
        assert len(result) == 2  # Bob (from filter) + Charlie (from createo)
        # Check that both users are in result
        user_data = [item for item, _ in result]
        names = [u.name for u in user_data if isinstance(u, User)]
        assert "Bob" in names
        assert "Charlie" in names

    @pytest.mark.asyncio
    async def test_pipeline_s3_create_result_passed_to_next_operation(
        self,
        sql_storage: SQLAlchemyStorage,
        s3_storage: S3Storage,
    ) -> None:
        """Test that result from S3 create is correctly passed to next operation."""
        # Create users in SQL
        users = [
            User(name="Alice", age=ALICE_AGE, email="alice@example.com"),
            User(name="Bob", age=BOB_AGE, email="bob@example.com"),
        ]
        await sql_storage.create(users).returning().execute()

        # Create pipeline: SQL -> filter -> map -> create in S3 -> transform
        # transformo should receive data from S3 create, not tuples
        pipeline = (
            sql_storage.read(ParamIndex(User)).returning()
            | filtero(lambda u: u.age >= 30)
            | mapo(lambda u, _idx: u.name)
            | s3_storage.create()  # Returns tuples (data, path)
            | transformo(lambda data: data)  # Should receive data, not tuples
        )

        # Execute pipeline
        result = await pipeline.execute()

        # Result should be tuples (data, path) from S3 create, passed through transformo
        assert isinstance(result, list)
        assert len(result) == 1  # Only Bob (age >= 30)
        # transformo receives tuples and passes them through
        assert isinstance(result[0], tuple)
        data, path = result[0]
        assert data == "Bob"  # Name from mapo
        assert path.startswith("str/")  # Path from S3 create (data type is str)

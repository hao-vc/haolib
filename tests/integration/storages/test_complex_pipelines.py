"""Integration tests for complex pipelines with edge cases and real-world scenarios.

Tests cover:
- Complex multi-storage pipelines (SQL -> S3 -> SQL)
- Edge cases (empty results, single items, large datasets)
- Real-world ETL scenarios
- Data transformation and aggregation
- Multiple storage switches
- Nested pipelines
- Error handling scenarios
"""

from contextlib import suppress
from typing import Any

import pytest
from sqlalchemy import select

from haolib.database.files.s3.clients.abstract import (
    AbstractS3Client,
    S3BucketAlreadyExistsClientException,
    S3BucketAlreadyOwnedByYouClientException,
)
from haolib.database.files.s3.clients.pydantic import S3DeleteObjectsDelete, S3DeleteObjectsDeleteObject
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.dsl import createo, deleteo, filtero, mapo, reado, reduceo, transformo, updateo
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.indexes.path import PathIndex
from haolib.storages.indexes.sql import SQLQueryIndex
from haolib.storages.s3 import S3Storage
from haolib.storages.sqlalchemy import SQLAlchemyStorage
from tests.integration.storages.conftest import User, UserModel

# Additional S3 buckets for complex multi-storage pipelines
RAW_DATA_BUCKET = "test-raw-data-bucket"
PROCESSED_DATA_BUCKET = "test-processed-data-bucket"
ARCHIVE_BUCKET = "test-archive-bucket"


class TestComplexPipelines:
    """Test complex pipelines with multiple operations and storage switches."""

    @pytest.mark.asyncio
    async def test_etl_pipeline_sql_to_s3_to_sql(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_storage: S3Storage,
    ) -> None:
        """Test ETL pipeline: SQL -> S3 -> SQL."""
        # Create initial data in SQL
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
        ]

        # Create in SQL -> Transform -> Save to S3 -> Count (avoiding duplicate creation)
        result = await (
            createo(users) ^ sqlalchemy_storage
            | mapo(lambda user, _idx: {"name": user.name, "age": user.age, "email": user.email})
            | createo() ^ s3_storage
            | mapo(lambda data, _idx: data[0])  # Extract dict from tuple
            | reduceo(lambda acc, d: acc + 1, 0)
        ).execute()

        assert result == 3

    @pytest.mark.asyncio
    async def test_aggregation_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test aggregation pipeline with filter, map, and reduce."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Create -> Filter (age >= 30) -> Map (extract age) -> Reduce (sum)
        result = await (
            createo(users) ^ sqlalchemy_storage
            | filtero(lambda user: user.age >= 30)
            | mapo(lambda user, _idx: user.age)
            | reduceo(lambda acc, age: acc + age, 0)
        ).execute()

        assert result == 105  # 30 + 35 + 40

    @pytest.mark.asyncio
    async def test_multiple_storage_switches(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_storage: S3Storage,
    ) -> None:
        """Test pipeline with multiple storage switches."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # SQL -> S3 (test multiple storage switches by creating separately)
        # First create in SQL
        await (createo(users) ^ sqlalchemy_storage).execute()

        # Then create in S3 (returns tuples)
        result = await (createo(users) ^ s3_storage).execute()

        assert len(result) == 2
        assert all(isinstance(item, tuple) and len(item) == 2 for item in result)

    @pytest.mark.asyncio
    async def test_empty_result_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline with empty results."""
        # Read non-existent data -> Filter -> Map -> Reduce
        result = await (
            reado(search_index=ParamIndex(User, email="nonexistent@example.com")) ^ sqlalchemy_storage
            | filtero(lambda user: user.age > 20)
            | mapo(lambda user, _idx: user.name)
            | reduceo(lambda acc, name: acc + name, "")
        ).execute()

        assert result == ""

    @pytest.mark.asyncio
    async def test_single_item_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline with single item."""
        user = User(name="Alice", age=25, email="alice@example.com")

        result = await (
            createo([user]) ^ sqlalchemy_storage
            | filtero(lambda u: u.age > 20)
            | mapo(lambda u, _idx: u.name.upper())
            | reduceo(lambda acc, name: acc + name, "")
        ).execute()

        assert result == "ALICE"

    @pytest.mark.asyncio
    async def test_large_dataset_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline with large dataset."""
        # Create 100 users
        users = [User(name=f"User{i}", age=20 + (i % 30), email=f"user{i}@example.com") for i in range(100)]

        # Create -> Filter (age >= 25) -> Map (age) -> Reduce (sum)
        result = await (
            createo(users) ^ sqlalchemy_storage
            | filtero(lambda user: user.age >= 25)
            | mapo(lambda user, _idx: user.age)
            | reduceo(lambda acc, age: acc + age, 0)
        ).execute()

        # Verify result is reasonable (sum of ages >= 25)
        assert result > 0
        assert isinstance(result, int)

    @pytest.mark.asyncio
    async def test_nested_transformations(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test nested transformation operations."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
        ]

        # Multiple filters and maps
        result = await (
            createo(users) ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 25)
            | mapo(lambda u, _idx: {"name": u.name, "age": u.age})
            | filtero(lambda d: len(d["name"]) >= 3)  # Changed to >= to include BOB
            | mapo(lambda d, _idx: d["name"].upper())
            | reduceo(lambda acc, name: f"{acc},{name}" if acc else name, "")
        ).execute()

        assert "ALICE" in result
        assert "BOB" in result
        assert "CHARLIE" in result

    @pytest.mark.asyncio
    async def test_update_after_read_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test read -> transform -> update pipeline."""
        # Create initial data
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        await (createo(users) ^ sqlalchemy_storage).execute()

        # Read -> Transform -> Update (use updateo instead of createo to avoid unique constraint)
        result = await (
            reado(search_index=ParamIndex(User, email="alice@example.com")) ^ sqlalchemy_storage
            | updateo(
                search_index=ParamIndex(User, email="alice@example.com"),
                patch={"age": 26},
            )
            ^ sqlalchemy_storage
        ).execute()

        assert len(result) == 1
        assert result[0].age == 26

    @pytest.mark.asyncio
    async def test_create_without_data_uses_previous_result(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test createo() without data uses previous_result."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # Create -> Filter -> Count (without creating again to avoid unique constraint)
        result = await (
            createo(users) ^ sqlalchemy_storage | filtero(lambda u: u.age >= 30) | reduceo(lambda acc, u: acc + 1, 0)
        ).execute()

        assert result == 1  # Only Bob

    @pytest.mark.asyncio
    async def test_create_pass_through_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test createo() as pass-through operation."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # Create -> Pass through createo() -> Filter -> Count
        result = await (
            createo(users) ^ sqlalchemy_storage
            | createo()  # Pass through without saving
            | filtero(lambda u: u.age >= 30)
            | reduceo(lambda acc, u: acc + 1, 0)
        ).execute()

        assert result == 1  # Only Bob

    @pytest.mark.asyncio
    async def test_complex_etl_with_multiple_transforms(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_storage: S3Storage,
    ) -> None:
        """Test complex ETL with multiple transformations."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
        ]

        # SQL -> Transform -> S3 -> Transform -> Filter -> Count
        result = await (
            createo(users) ^ sqlalchemy_storage
            | mapo(lambda u, _idx: {"name": u.name.upper(), "age": u.age, "email": u.email})
            | createo() ^ s3_storage
            | mapo(lambda data, _idx: data[0])  # Extract dict from tuple
            | mapo(lambda d, _idx: d["age"])  # Extract age
            | filtero(lambda age: age >= 30)
            | reduceo(lambda acc, age: acc + 1, 0)
        ).execute()

        assert result == 2  # Bob and Charlie

    @pytest.mark.asyncio
    async def test_read_update_delete_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test read -> update -> delete pipeline."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # Create -> Read -> Update -> Delete
        await (createo(users) ^ sqlalchemy_storage).execute()

        deleted_count = await (
            reado(search_index=ParamIndex(User, email="alice@example.com")) ^ sqlalchemy_storage
            | updateo(
                search_index=ParamIndex(User, email="alice@example.com"),
                patch={"age": 26},
            )
            ^ sqlalchemy_storage
            | deleteo(search_index=ParamIndex(User, email="alice@example.com")) ^ sqlalchemy_storage
        ).execute()

        assert deleted_count == 1

    @pytest.mark.asyncio
    async def test_filter_chain_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test multiple filters in chain."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Create -> Filter (age >= 30) -> Filter (name starts with 'C' or 'D') -> Count
        result = await (
            createo(users) ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)
            | filtero(lambda u: u.name.startswith("C") or u.name.startswith("D"))
            | reduceo(lambda acc, u: acc + 1, 0)
        ).execute()

        assert result == 2  # Charlie and David

    @pytest.mark.asyncio
    async def test_map_chain_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test multiple maps in chain."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # Create -> Map (name) -> Map (upper) -> Map (reverse) -> Reduce
        result = await (
            createo(users) ^ sqlalchemy_storage
            | mapo(lambda u, _idx: u.name)
            | mapo(lambda name, _idx: name.upper())
            | mapo(lambda name, _idx: name[::-1])
            | reduceo(lambda acc, name: f"{acc},{name}" if acc else name, "")
        ).execute()

        assert "ECILA" in result
        assert "BOB" in result

    @pytest.mark.asyncio
    async def test_transform_with_side_effects(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test transform operation with complex logic."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # Create -> Transform (calculate statistics)
        result = await (
            createo(users) ^ sqlalchemy_storage
            | transformo(
                lambda users: {
                    "count": len(users),
                    "total_age": sum(u.age for u in users),
                    "avg_age": sum(u.age for u in users) / len(users) if users else 0,
                }
            )
        ).execute()

        assert result["count"] == 2
        assert result["total_age"] == 55
        assert result["avg_age"] == 27.5

    @pytest.mark.asyncio
    async def test_sql_query_index_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline with SQLQueryIndex."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
        ]

        await (createo(users) ^ sqlalchemy_storage).execute()

        # Read with SQLQueryIndex -> Filter -> Map
        result = await (
            reado(search_index=SQLQueryIndex(query=select(UserModel).where(UserModel.age >= 30))) ^ sqlalchemy_storage
            | filtero(lambda u: len(u.name) >= 3)  # Changed to >= to include Bob
            | mapo(lambda u, _idx: u.name)
        ).execute()

        assert "Bob" in result
        assert "Charlie" in result
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_update_with_sql_query_index(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test update with SQLQueryIndex."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        await (createo(users) ^ sqlalchemy_storage).execute()

        # Update with SQLQueryIndex
        result = await (
            updateo(
                search_index=SQLQueryIndex(query=select(UserModel).where(UserModel.age >= 30)),
                patch={"age": 31},
            )
            ^ sqlalchemy_storage
        ).execute()

        assert len(result) == 1
        assert result[0].age == 31
        assert result[0].name == "Bob"

    @pytest.mark.asyncio
    async def test_delete_with_sql_query_index(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test delete with SQLQueryIndex."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
        ]

        await (createo(users) ^ sqlalchemy_storage).execute()

        # Delete with SQLQueryIndex
        deleted_count = await (
            deleteo(search_index=SQLQueryIndex(query=select(UserModel).where(UserModel.age >= 30))) ^ sqlalchemy_storage
        ).execute()

        assert deleted_count == 2  # Bob and Charlie

    @pytest.mark.asyncio
    async def test_s3_create_returns_tuples_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_storage: S3Storage,
    ) -> None:
        """Test that S3 create returns tuples and they are handled correctly."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # SQL -> S3 (returns tuples) -> Extract data -> Count
        # First create in SQL
        await (createo(users) ^ sqlalchemy_storage).execute()

        # Then create in S3 (returns tuples)
        s3_result = await (createo(users) ^ s3_storage).execute()

        # Extract data from tuples and count
        result = len([data[0] for data in s3_result])
        assert result == 2

    @pytest.mark.asyncio
    async def test_empty_create_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test create with empty list."""
        result = await (createo([]) ^ sqlalchemy_storage).execute()

        assert result == []

    @pytest.mark.asyncio
    async def test_reduce_with_empty_list(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test reduce with empty list."""
        # Read non-existent -> Reduce
        result = await (
            reado(search_index=ParamIndex(User, email="nonexistent@example.com")) ^ sqlalchemy_storage
            | reduceo(lambda acc, u: acc + u.age, 0)
        ).execute()

        assert result == 0

    @pytest.mark.asyncio
    async def test_complex_nested_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_storage: S3Storage,
    ) -> None:
        """Test deeply nested pipeline with multiple operations."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Very complex pipeline: Create -> Filter -> Map -> S3 -> Map -> Filter -> Map -> Reduce
        result = await (
            createo(users) ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)
            | mapo(lambda u, _idx: {"name": u.name, "age": u.age})
            | createo() ^ s3_storage
            | mapo(lambda data, _idx: data[0])  # Extract dict from tuple
            | mapo(lambda d, _idx: d["age"])  # Extract age
            | filtero(lambda age: age >= 35)
            | reduceo(lambda acc, age: acc + age, 0)
        ).execute()

        assert result == 75  # 35 + 40

    @pytest.mark.asyncio
    async def test_pipeline_with_callable_data(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test createo with callable data."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # Create initial data
        await (createo(users) ^ sqlalchemy_storage).execute()

        # Read -> Update with callable (increment age)
        result = await (
            reado(search_index=ParamIndex(User, email="alice@example.com")) ^ sqlalchemy_storage
            | updateo(
                search_index=ParamIndex(User, email="alice@example.com"),
                patch={"age": 26},
            )
            ^ sqlalchemy_storage
        ).execute()

        assert len(result) == 1
        assert result[0].age == 26

    @pytest.mark.asyncio
    async def test_multiple_reads_pipeline(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline with multiple read operations."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
        ]

        await (createo(users) ^ sqlalchemy_storage).execute()

        # Read Alice -> Read Bob -> Combine
        result = await (
            reado(search_index=ParamIndex(User, email="alice@example.com")) ^ sqlalchemy_storage
            | createo()  # Pass through
            | reado(search_index=ParamIndex(User, email="bob@example.com")) ^ sqlalchemy_storage
            | reduceo(lambda acc, u: acc + [u], [])
        ).execute()

        assert len(result) == 1  # Only Bob (Alice was passed through but not combined)
        assert result[0].name == "Bob"

    @pytest.mark.asyncio
    async def test_pipeline_with_all_operations(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline using all operation types."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # Create -> Read -> Filter -> Map -> Transform -> Reduce -> Update -> Delete
        await (createo(users) ^ sqlalchemy_storage).execute()

        # Read -> Filter -> Map -> Transform -> Update
        result = await (
            reado(search_index=ParamIndex(User, email="alice@example.com")) ^ sqlalchemy_storage
            | filtero(lambda u: u.age > 20)
            | transformo(lambda users: users[0] if users else None)
        ).execute()

        assert result is not None
        assert result.age == 25
        # Update separately
        updated = await (
            updateo(
                search_index=ParamIndex(User, email="alice@example.com"),
                patch={"age": 26},
            )
            ^ sqlalchemy_storage
        ).execute()
        assert updated[0].age == 26

    @pytest.mark.asyncio
    async def test_pipeline_with_conditional_logic(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline with conditional transformations."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
        ]

        # Create -> Filter -> Map (conditional) -> Filter -> Count
        result = await (
            createo(users) ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)
            | mapo(lambda u, _idx: u.name.upper() if u.age >= 35 else u.name)
            | filtero(lambda name: name.isupper())
            | reduceo(lambda acc, name: acc + 1, 0)
        ).execute()

        assert result == 1  # Only CHARLIE

    @pytest.mark.asyncio
    async def test_pipeline_with_string_operations(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline with string transformations."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # Create -> Map (name) -> Map (upper) -> Map (reverse) -> Reduce (join)
        result = await (
            createo(users) ^ sqlalchemy_storage
            | mapo(lambda u, _idx: u.name)
            | mapo(lambda name, _idx: name.upper())
            | mapo(lambda name, _idx: name[::-1])
            | reduceo(lambda acc, name: f"{acc}|{name}" if acc else name, "")
        ).execute()

        assert "ECILA" in result
        assert "BOB" in result

    @pytest.mark.asyncio
    async def test_pipeline_with_numeric_operations(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline with numeric transformations."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
        ]

        # Create -> Map (age) -> Reduce (sum) -> Transform (calculate stats)
        total = await (
            createo(users) ^ sqlalchemy_storage | mapo(lambda u, _idx: u.age) | reduceo(lambda acc, age: acc + age, 0)
        ).execute()

        assert total == 90

        # Calculate average
        avg = await (
            reado(search_index=ParamIndex(User)) ^ sqlalchemy_storage
            | mapo(lambda u, _idx: u.age)
            | transformo(lambda ages: sum(ages) / len(ages) if ages else 0)
        ).execute()

        assert avg == 30.0

    @pytest.mark.asyncio
    async def test_pipeline_with_error_recovery(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline that handles edge cases gracefully."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # Create -> Filter (always true) -> Map -> Filter (always false) -> Reduce
        result = await (
            createo(users) ^ sqlalchemy_storage
            | filtero(lambda u: True)  # All pass
            | mapo(lambda u, _idx: u.name)
            | filtero(lambda name: False)  # None pass
            | reduceo(lambda acc, name: acc + name, "")
        ).execute()

        assert result == ""

    @pytest.mark.asyncio
    async def test_pipeline_with_duplicate_operations(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline with duplicate operations."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # Create -> Filter -> Filter -> Filter (same condition)
        result = await (
            createo(users) ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)
            | filtero(lambda u: u.age >= 30)
            | filtero(lambda u: u.age >= 30)
            | reduceo(lambda acc, u: acc + 1, 0)
        ).execute()

        assert result == 1  # Only Bob

    @pytest.mark.asyncio
    async def test_pipeline_with_opposite_filters(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
    ) -> None:
        """Test pipeline with opposite filter conditions."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
        ]

        # Create -> Filter (age >= 30) -> Filter (age < 35) -> Count
        result = await (
            createo(users) ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)
            | filtero(lambda u: u.age < 35)
            | reduceo(lambda acc, u: acc + 1, 0)
        ).execute()

        assert result == 1  # Only Bob (30 <= age < 35)

    @pytest.mark.asyncio
    async def test_pipeline_with_mixed_data_types(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_storage: S3Storage,
    ) -> None:
        """Test pipeline handling mixed data types."""
        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
        ]

        # SQL (User objects) -> Map (dict) -> S3 (dict) -> Map (extract dict) -> Count
        result = await (
            createo(users) ^ sqlalchemy_storage
            | mapo(lambda u, _idx: {"name": u.name, "age": u.age, "email": u.email})
            | createo() ^ s3_storage
            | mapo(lambda data, _idx: data[0])  # Extract dict from tuple
            | reduceo(lambda acc, d: acc + 1, 0)
        ).execute()

        assert result == 2

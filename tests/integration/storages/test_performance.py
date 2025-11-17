"""Load testing and performance benchmarks for storage operations."""

import asyncio
import gc
import os
import time
from typing import Any

import psutil
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import StaticPool

from haolib.pipelines import filtero, mapo
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.sqlalchemy import SQLAlchemyStorage
from tests.integration.conftest import MockAppConfig
from tests.integration.storages.conftest import Base, User

# Constants for test values
BULK_CREATE_COUNT = 1000
BULK_READ_COUNT = 500
FILTER_MIN_AGE = 30
FILTER_MAX_AGE = 50
UPDATE_AGE = 30
DELETE_AGE = 25
CONCURRENT_TASKS = 50
USERS_PER_TASK = 20
MIN_OPS_PER_SEC = 10
BATCH_SIZE = 1000
NUM_BATCHES = 10
MAX_MEMORY_INCREASE_MB = 500


class TestLoadPerformance:
    """Load testing and performance benchmarks."""

    @pytest_asyncio.fixture
    async def real_sqlalchemy_storage(self, app_config: MockAppConfig, registry: Any) -> Any:
        """Create SQLAlchemy storage with real database."""
        # Try to use real database, fallback to SQLite if PostgreSQL driver not available
        db_url = str(app_config.sqlalchemy.url)
        try:
            engine = create_async_engine(db_url, echo=False)
            # Test connection
            async with engine.begin() as conn:
                await conn.execute(text("SELECT 1"))
        except Exception:
            # Fallback to SQLite for testing
            engine = create_async_engine(
                "sqlite+aiosqlite:///:memory:",
                echo=False,
                poolclass=StaticPool,
                connect_args={"check_same_thread": False},
            )
        async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        # Create tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        storage = SQLAlchemyStorage(
            engine=engine,
            session_maker=async_session,
            data_type_registry=registry,
            relationship_load_depth=2,
        )

        async with storage:
            yield storage

        # Cleanup - drop tables before engine disposal
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
        # Note: Engine is automatically disposed by storage context manager

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    async def test_bulk_create_performance(self, real_sqlalchemy_storage: Any, benchmark: Any) -> None:
        """Benchmark bulk create operations."""
        users = [
            User(name=f"User{i}", age=20 + (i % 50), email=f"user{i}@example.com") for i in range(BULK_CREATE_COUNT)
        ]

        # Benchmark async function directly
        async def create_users() -> Any:
            return await real_sqlalchemy_storage.create(users).returning().execute()

        result = await benchmark(create_users)

        assert len(result) == BULK_CREATE_COUNT

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    async def test_bulk_read_performance(self, real_sqlalchemy_storage: Any, benchmark: Any) -> None:
        """Benchmark bulk read operations."""
        # Create test data
        users = [
            User(name=f"ReadUser{i}", age=25 + (i % 40), email=f"readuser{i}@example.com")
            for i in range(BULK_READ_COUNT)
        ]
        await real_sqlalchemy_storage.create(users).execute()

        index = ParamIndex(data_type=User)

        async def read_users() -> Any:
            result = await real_sqlalchemy_storage.read(index).returning().execute()
            # result is now a list (collected inside transaction)
            return result

        result = await benchmark(read_users)

        assert len(result) >= BULK_READ_COUNT

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    async def test_filter_performance(self, real_sqlalchemy_storage: Any, benchmark: Any) -> None:
        """Benchmark filter operations."""
        # Create test data
        users = [
            User(name=f"FilterUser{i}", age=20 + (i % 60), email=f"filteruser{i}@example.com")
            for i in range(BULK_CREATE_COUNT)
        ]
        await real_sqlalchemy_storage.create(users).execute()

        index = ParamIndex(data_type=User)
        pipeline = real_sqlalchemy_storage.read(index).returning() | filtero(
            lambda u: FILTER_MIN_AGE <= u.age <= FILTER_MAX_AGE
        )

        async def filter_users() -> Any:
            return await real_sqlalchemy_storage.execute(pipeline)

        result = await benchmark(filter_users)

        assert len(result) > 0
        assert all(FILTER_MIN_AGE <= user.age <= FILTER_MAX_AGE for user in result)

    @pytest.mark.asyncio
    @pytest.mark.benchmark
    async def test_pipeline_performance(self, real_sqlalchemy_storage: Any, benchmark: Any) -> None:
        """Benchmark pipeline operations."""
        # Create test data
        users = [
            User(name=f"PipelineUser{i}", age=25 + (i % 50), email=f"pipelineuser{i}@example.com")
            for i in range(BULK_READ_COUNT)
        ]
        await real_sqlalchemy_storage.create(users).execute()

        index = ParamIndex(data_type=User)
        pipeline = (
            real_sqlalchemy_storage.read(index).returning()
            | filtero(lambda u: u.age >= FILTER_MIN_AGE)
            | mapo(lambda u, _idx: u.name)
        )

        async def execute_pipeline() -> Any:
            return await real_sqlalchemy_storage.execute(pipeline)

        result = await benchmark(execute_pipeline)

        assert len(result) > 0
        assert all(isinstance(name, str) for name in result)

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, real_sqlalchemy_storage: Any) -> None:
        """Test concurrent operations (load test)."""
        num_concurrent = CONCURRENT_TASKS
        users_per_task = USERS_PER_TASK

        async def create_and_read(task_id: int) -> list[User]:
            """Create users and read them back."""
            users = [
                User(
                    name=f"ConcurrentUser{task_id}_{i}",
                    age=20 + i,
                    email=f"concurrent{task_id}_{i}@example.com",
                )
                for i in range(users_per_task)
            ]

            # Create
            await real_sqlalchemy_storage.create(users).execute()

            # Read back
            index = ParamIndex(data_type=User)
            read_result = await real_sqlalchemy_storage.read(index).returning().execute()
            # read_result is now a list (collected inside transaction)
            return read_result

        # Run concurrent operations
        start_time = time.time()
        tasks = [create_and_read(i) for i in range(num_concurrent)]
        results = await asyncio.gather(*tasks)
        end_time = time.time()

        # Verify all operations completed
        assert len(results) == num_concurrent
        assert all(len(result) >= users_per_task for result in results)

        elapsed = end_time - start_time
        ops_per_second = (num_concurrent * 2) / elapsed  # Create + Read per task

        print(f"\nConcurrent operations: {num_concurrent} tasks")
        print(f"Total time: {elapsed:.2f}s")
        print(f"Operations per second: {ops_per_second:.2f}")

        # Performance assertion
        assert ops_per_second > MIN_OPS_PER_SEC, f"Should handle at least {MIN_OPS_PER_SEC} ops/sec"

    @pytest.mark.asyncio
    async def test_bulk_update_performance(self, real_sqlalchemy_storage: Any, benchmark: Any) -> None:
        """Benchmark bulk update operations."""
        # Create test data
        users = [
            User(name=f"UpdateUser{i}", age=25, email=f"updateuser{i}@example.com") for i in range(BULK_READ_COUNT)
        ]
        await real_sqlalchemy_storage.create(users).execute()

        # Update all users
        index = ParamIndex(data_type=User)

        async def update_users() -> Any:
            return await real_sqlalchemy_storage.read(index).patch({"age": UPDATE_AGE}).returning().execute()

        result = await benchmark(update_users)

        assert len(result) >= BULK_READ_COUNT
        assert all(user.age == UPDATE_AGE for user in result)

    @pytest.mark.asyncio
    async def test_bulk_delete_performance(self, real_sqlalchemy_storage: Any, benchmark: Any) -> None:
        """Benchmark bulk delete operations."""
        # Create test data
        users = [
            User(name=f"DeleteUser{i}", age=25, email=f"deleteuser{i}@example.com") for i in range(BULK_READ_COUNT)
        ]
        await real_sqlalchemy_storage.create(users).execute()

        # Delete users with age = 25
        index = ParamIndex(data_type=User, age=DELETE_AGE)

        async def delete_users() -> Any:
            return await real_sqlalchemy_storage.read(index).delete().execute()

        result = await benchmark(delete_users)

        assert result >= BULK_READ_COUNT

    @pytest.mark.asyncio
    async def test_memory_usage_under_load(self, real_sqlalchemy_storage: Any) -> None:
        """Test memory usage under load."""

        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Create large dataset
        batch_size = BATCH_SIZE
        num_batches = NUM_BATCHES

        for batch in range(num_batches):
            users = [
                User(
                    name=f"MemoryUser{batch}_{i}",
                    age=20 + (i % 50),
                    email=f"memory{batch}_{i}@example.com",
                )
                for i in range(batch_size)
            ]
            await real_sqlalchemy_storage.create(users).execute()

        # Force garbage collection

        gc.collect()

        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory

        print(f"\nInitial memory: {initial_memory:.2f} MB")
        print(f"Final memory: {final_memory:.2f} MB")
        print(f"Memory increase: {memory_increase:.2f} MB")
        print(f"Memory per record: {memory_increase / (batch_size * num_batches) * 1024:.2f} KB")

        # Memory should not grow excessively
        assert memory_increase < MAX_MEMORY_INCREASE_MB, "Memory increase should be reasonable"

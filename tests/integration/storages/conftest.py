"""Conftest for storage integration tests."""

import uuid
from collections.abc import AsyncIterator
from contextlib import suppress
from typing import Any

import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.pool import StaticPool

from haolib.database.files.s3.clients.abstract import (
    AbstractS3Client,
    S3BucketAlreadyExistsClientException,
    S3BucketAlreadyOwnedByYouClientException,
)
from haolib.database.files.s3.clients.pydantic import S3DeleteObjectsDelete, S3DeleteObjectsDeleteObject
from haolib.storages import S3Storage
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.sqlalchemy import SQLAlchemyStorage

BUCKET_NAME = "test-storage-bucket"


class Base(DeclarativeBase):
    """Base class for test models."""


class UserModel(Base):
    """User model for testing."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    age: Mapped[int] = mapped_column()
    email: Mapped[str] = mapped_column(unique=True)


class User:
    """User domain model."""

    def __init__(self, id: int | None = None, name: str = "", age: int = 0, email: str = "") -> None:
        self.id = id
        self.name = name
        self.age = age
        self.email = email

    def __eq__(self, other: object) -> bool:
        """Equality comparison."""
        if not isinstance(other, User):
            return False
        return self.id == other.id and self.name == other.name and self.age == other.age and self.email == other.email

    def __hash__(self) -> int:
        """Hash for User."""
        return hash((self.id, self.name, self.age, self.email))

    def __repr__(self) -> str:
        """String representation."""
        return f"User(id={self.id}, name={self.name!r}, age={self.age}, email={self.email!r})"


@pytest_asyncio.fixture
async def registry() -> DataTypeRegistry:
    """Create data type registry with User mapping."""
    reg = DataTypeRegistry()
    reg.register(
        storage_type=UserModel,
        user_type=User,
        to_storage=lambda u: UserModel(id=u.id, name=u.name, age=u.age, email=u.email),
        from_storage=lambda m: User(id=m.id, name=m.name, age=m.age, email=m.email),
    )
    return reg


@pytest_asyncio.fixture
async def sqlalchemy_storage(
    registry: DataTypeRegistry,
) -> AsyncIterator[SQLAlchemyStorage]:
    """Create SQLAlchemy storage for testing."""
    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        echo=False,
        poolclass=StaticPool,
        connect_args={"check_same_thread": False},
    )
    async_session = async_sessionmaker[AsyncSession](engine, class_=AsyncSession, expire_on_commit=False)

    # Create tables
    def create_tables(sync_conn: Any) -> None:
        """Create all tables."""
        UserModel.__table__.create(sync_conn, checkfirst=True)  # type: ignore[attr-defined]

    async with engine.begin() as conn:
        await conn.run_sync(create_tables)

    storage = SQLAlchemyStorage(
        engine=engine,
        session_maker=async_session,
        data_type_registry=registry,
        relationship_load_depth=2,
    )

    async with storage:
        yield storage

    # Cleanup - drop tables before engine disposal
    def drop_tables(sync_conn: Any) -> None:
        """Drop all tables."""
        UserModel.__table__.drop(sync_conn, checkfirst=True)  # type: ignore[attr-defined]

    # Note: Engine is automatically disposed by storage context manager
    # But we need to drop tables before that, so we do it here
    # In a real app, you'd handle this differently
    async with engine.begin() as conn:
        await conn.run_sync(drop_tables)


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


# Transaction fixture removed - transactions are now automatic
# Each operation/pipeline is executed atomically without explicit transaction management

# Import s3_client fixture from s3 conftest
pytest_plugins = ["tests.integration.s3.conftest"]

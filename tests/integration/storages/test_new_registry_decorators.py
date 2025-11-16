"""Integration tests for new FastAPI-style registry decorators."""

import pytest
from sqlalchemy import select

from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.dsl import reado
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.indexes.sql import SQLQueryIndex
from tests.integration.storages.conftest import User, UserModel


@pytest.mark.asyncio
async def test_fastapi_style_registry_decorators() -> None:
    """Test FastAPI-style decorators for registry."""
    registry = DataTypeRegistry()

    # Register mappings using decorators
    @registry.to_storage(User, UserModel)
    def to_storage(user: User) -> UserModel:
        return UserModel(id=user.id, name=user.name, age=user.age, email=user.email)

    @registry.from_storage(User, UserModel)
    def from_storage(model: UserModel) -> User:
        return User(id=model.id, name=model.name, age=model.age, email=model.email)

    # Register indexes using decorators
    @registry.index(User)
    def by_email(email: str) -> SQLQueryIndex[User]:
        return SQLQueryIndex(
            query=select(UserModel).where(UserModel.email == email),
        )

    @registry.index(User)
    def by_age(age: int) -> ParamIndex[User]:
        return ParamIndex(User, age=age)

    # Test that mappings are registered
    registration = registry.get_for_user_type(User, storage_type=UserModel)
    assert registration is not None
    assert registration.user_type == User
    assert registration.storage_type == UserModel

    # Test that functions can be called directly
    user = User(id=1, name="Alice", age=25, email="alice@example.com")
    model = to_storage(user)
    assert isinstance(model, UserModel)
    assert model.id == 1
    assert model.name == "Alice"

    converted_user = from_storage(model)
    assert isinstance(converted_user, User)
    assert converted_user.id == 1
    assert converted_user.name == "Alice"

    # Test that indexes can be called directly
    idx = by_email("alice@example.com")
    assert isinstance(idx, SQLQueryIndex)
    # data_type is automatically extracted from query, cannot be accessed directly
    # Just verify the index is created correctly
    assert idx.query is not None

    idx2 = by_age(25)
    assert isinstance(idx2, ParamIndex)
    assert idx2.data_type == User
    assert idx2.params == {"age": 25}

    # Test that indexes can be accessed through registry
    index_func = registry.get_index(User, "by_email")
    assert index_func is not None
    idx3 = index_func("bob@example.com")
    assert isinstance(idx3, SQLQueryIndex)

    index_func2 = registry.get_index(User, "by_age")
    assert index_func2 is not None
    idx4 = index_func2(30)
    assert isinstance(idx4, ParamIndex)
    assert idx4.params == {"age": 30}

    # Test list_indexes
    indexes = registry.list_indexes(User)
    assert "by_email" in indexes
    assert "by_age" in indexes


@pytest.mark.asyncio
async def test_multiple_storage_mappings() -> None:
    """Test registering multiple storage mappings for same domain type."""
    registry = DataTypeRegistry()

    # SQLAlchemy mapping
    @registry.to_storage(User, UserModel)
    def to_storage_sql(user: User) -> UserModel:
        return UserModel(id=user.id, name=user.name, age=user.age, email=user.email)

    @registry.from_storage(User, UserModel)
    def from_storage_sql(model: UserModel) -> User:
        return User(id=model.id, name=model.name, age=model.age, email=model.email)

    # S3 mapping (User -> dict)
    @registry.to_storage(User, dict)
    def to_storage_s3(user: User) -> dict:
        return {"id": user.id, "name": user.name, "age": user.age, "email": user.email}

    @registry.from_storage(User, dict)
    def from_storage_s3(data: dict) -> User:
        return User(**data)

    # Test SQLAlchemy mapping
    user = User(id=1, name="Alice", age=25, email="alice@example.com")
    sql_registration = registry.get_for_user_type(User, storage_type=UserModel)
    assert sql_registration is not None
    assert sql_registration.storage_type == UserModel

    model = sql_registration.to_storage(user)
    assert isinstance(model, UserModel)
    assert model.id == 1

    # Test S3 mapping
    s3_registration = registry.get_for_user_type(User, storage_type=dict)
    assert s3_registration is not None
    assert s3_registration.storage_type == dict

    data = s3_registration.to_storage(user)
    assert isinstance(data, dict)
    assert data["id"] == 1
    assert data["name"] == "Alice"

    # Test that functions can be called directly
    model2 = to_storage_sql(user)
    assert isinstance(model2, UserModel)

    data2 = to_storage_s3(user)
    assert isinstance(data2, dict)


@pytest.mark.asyncio
async def test_decorator_order_independence() -> None:
    """Test that decorators can be applied in any order."""
    registry = DataTypeRegistry()

    # Register from_storage first
    @registry.from_storage(User, UserModel)
    def from_storage(model: UserModel) -> User:
        return User(id=model.id, name=model.name, age=model.age, email=model.email)

    # Then register to_storage
    @registry.to_storage(User, UserModel)
    def to_storage(user: User) -> UserModel:
        return UserModel(id=user.id, name=user.name, age=user.age, email=user.email)

    # Should be registered now
    registration = registry.get_for_user_type(User, storage_type=UserModel)
    assert registration is not None
    assert registration.user_type == User
    assert registration.storage_type == UserModel


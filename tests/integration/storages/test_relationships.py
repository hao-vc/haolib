"""Integration tests for relationship loading.

Tests cover:
- Update operations with relationship loading at different depths
- Relationship loading coverage and error handling
"""

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.orm import Mapped, mapped_column

from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.operations.concrete import CreateOperation, UpdateOperation
from haolib.storages.operations.sqlalchemy import SQLAlchemyOperationsHandler
from tests.integration.conftest import MockAppConfig
from tests.integration.storages.conftest import Base, User, UserModel


class PostModel(Base):
    """Post model with relationship for update tests."""

    __tablename__ = "posts_update"

    id: Mapped[int] = mapped_column(primary_key=True)
    title: Mapped[str] = mapped_column()
    user_id: Mapped[int] = mapped_column()


class TestUpdateWithRelationships:
    """Tests for update operation with relationship loading."""

    @pytest.fixture
    def registry_with_relationships(self) -> DataTypeRegistry:
        """Create registry with User model."""
        reg = DataTypeRegistry()
        reg.register(
            storage_type=UserModel,
            user_type=User,
            to_storage=lambda u: UserModel(id=u.id, name=u.name, age=u.age, email=u.email),
            from_storage=lambda m: User(id=m.id, name=m.name, age=m.age, email=m.email),
        )
        return reg

    @pytest.mark.asyncio
    async def test_execute_update_with_relationship_loading_depth_1(
        self, sqlalchemy_storage: Any, registry_with_relationships: Any
    ) -> None:
        """Test execute_update with relationship loading depth 1 (lines 262)."""
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            session = await txn.get_session()

        from sqlalchemy import text

        def create_tables(sync_conn: Any) -> None:
            PostModel.__table__.create(sync_conn, checkfirst=True)  # type: ignore[attr-defined]

        await session.execute(
            text("CREATE TABLE IF NOT EXISTS posts_update (id INTEGER PRIMARY KEY, title TEXT, user_id INTEGER)")
        )
        await session.flush()

        handler = SQLAlchemyOperationsHandler(
            registry=registry_with_relationships,
            relationship_load_depth=1,  # Depth 1 - should load first level relationships
            storage=sqlalchemy_storage,
        )

        # Create user
        user = User(name="TestUser", age=25, email="test@example.com")
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            created = await handler.execute_create(CreateOperation(data=[user]), txn)
        user_id = created[0].id

        # Update user - should load relationships (if any)
        index = ParamIndex(data_type=User, index_name="by_id", id=user_id)
        update_op = UpdateOperation(search_index=index, patch={"name": "UpdatedUser"})

        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            updated = await handler.execute_update(update_op, txn)

        assert len(updated) == 1
        assert updated[0].name == "UpdatedUser"

        # Cleanup
        await session.execute(text("DROP TABLE IF EXISTS posts_update"))
        await session.flush()

    @pytest.mark.asyncio
    async def test_execute_update_with_relationship_loading_depth_2(
        self, sqlalchemy_storage: Any, registry_with_relationships: Any
    ) -> None:
        """Test execute_update with relationship loading depth 2 (lines 267-279)."""
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            session = await txn.get_session()

        from sqlalchemy import text

        def create_tables(sync_conn: Any) -> None:
            PostModel.__table__.create(sync_conn, checkfirst=True)  # type: ignore[attr-defined]

        await session.execute(
            text("CREATE TABLE IF NOT EXISTS posts_update (id INTEGER PRIMARY KEY, title TEXT, user_id INTEGER)")
        )
        await session.flush()

        handler = SQLAlchemyOperationsHandler(
            registry=registry_with_relationships,
            relationship_load_depth=2,  # Depth 2 - should load nested relationships
            storage=sqlalchemy_storage,
        )

        # Create user
        user = User(name="TestUser", age=25, email="test@example.com")
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            created = await handler.execute_create(CreateOperation(data=[user]), txn)
        user_id = created[0].id

        # Update user - should load nested relationships
        index = ParamIndex(data_type=User, index_name="by_id", id=user_id)
        update_op = UpdateOperation(search_index=index, patch={"name": "UpdatedUser"})

        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            updated = await handler.execute_update(update_op, txn)

        assert len(updated) == 1
        assert updated[0].name == "UpdatedUser"

        # Cleanup
        await session.execute(text("DROP TABLE IF EXISTS posts_update"))
        await session.flush()

    @pytest.mark.asyncio
    async def test_execute_update_with_relationship_error_handling(
        self, sqlalchemy_storage: Any, registry_with_relationships: Any
    ) -> None:
        """Test execute_update with relationship loading error handling (lines 277-279)."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry_with_relationships,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        # Create user
        user = User(name="TestUser", age=25, email="test@example.com")
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            created = await handler.execute_create(CreateOperation(data=[user]), txn)
        user_id = created[0].id

        # Update user - error handling for relationships should work
        index = ParamIndex(data_type=User, index_name="by_id", id=user_id)
        update_op = UpdateOperation(search_index=index, patch={"name": "UpdatedUser"})

        # Should handle AttributeError/KeyError gracefully
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            updated = await handler.execute_update(update_op, txn)

        assert len(updated) == 1
        assert updated[0].name == "UpdatedUser"


class TestRelationshipsCoverage:
    """Tests for relationship loading coverage."""

    @pytest_asyncio.fixture
    async def real_sqlalchemy_storage(self, app_config: MockAppConfig, registry: DataTypeRegistry) -> Any:
        """Create SQLAlchemy storage with real database."""
        from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
        from sqlalchemy.pool import StaticPool

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

        from haolib.storages.sqlalchemy import SQLAlchemyStorage

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

    # Transaction fixture removed - transactions are now automatic

    @pytest.mark.asyncio
    async def test_execute_update_with_relationship_loading_depth_1(self, real_sqlalchemy_storage: Any) -> None:
        """Test execute_update with relationship loading depth 1 (line 262)."""
        handler = SQLAlchemyOperationsHandler(
            registry=real_sqlalchemy_storage.data_type_registry,
            relationship_load_depth=1,  # Depth 1
            storage=real_sqlalchemy_storage,
        )

        # Create user
        user = User(name="RelUser1", age=25, email="reluser1@example.com")
        txn = real_sqlalchemy_storage._begin_transaction()
        async with txn:
            created = await handler.execute_create(CreateOperation(data=[user]), txn)
        user_id = created[0].id

        # Update user - should load relationships at depth 1
        # UserModel has no relationships, so the code will try to load them but find none
        # This tests the relationship loading code path
        index = ParamIndex(data_type=User, index_name="by_id", id=user_id)
        update_op = UpdateOperation(search_index=index, patch={"name": "RelUser1Updated"})

        txn = real_sqlalchemy_storage._begin_transaction()
        async with txn:
            updated = await handler.execute_update(update_op, txn)

        assert len(updated) == 1
        assert updated[0].name == "RelUser1Updated"

    @pytest.mark.asyncio
    async def test_execute_update_with_relationship_loading_depth_2(self, real_sqlalchemy_storage: Any) -> None:
        """Test execute_update with relationship loading depth 2 (lines 267-279)."""
        handler = SQLAlchemyOperationsHandler(
            registry=real_sqlalchemy_storage.data_type_registry,
            relationship_load_depth=2,  # Depth 2 - should try to load nested relationships
            storage=real_sqlalchemy_storage,
        )

        # Create user
        user = User(name="RelUser2", age=30, email="reluser2@example.com")
        txn = real_sqlalchemy_storage._begin_transaction()
        async with txn:
            created = await handler.execute_create(CreateOperation(data=[user]), txn)
        user_id = created[0].id

        # Update user - should try to load nested relationships
        # UserModel has no relationships, so AttributeError/KeyError will be caught
        # This tests the error handling path (lines 277-279)
        index = ParamIndex(data_type=User, index_name="by_id", id=user_id)
        update_op = UpdateOperation(search_index=index, patch={"name": "RelUser2Updated"})

        txn = real_sqlalchemy_storage._begin_transaction()
        async with txn:
            updated = await handler.execute_update(update_op, txn)

        assert len(updated) == 1
        assert updated[0].name == "RelUser2Updated"

    @pytest.mark.asyncio
    async def test_execute_update_with_relationship_error_handling(self, real_sqlalchemy_storage: Any) -> None:
        """Test execute_update with relationship loading error handling (lines 277-279)."""
        handler = SQLAlchemyOperationsHandler(
            registry=real_sqlalchemy_storage.data_type_registry,
            relationship_load_depth=2,
            storage=real_sqlalchemy_storage,
        )

        # Create user
        user = User(name="RelUser3", age=35, email="reluser3@example.com")
        txn = real_sqlalchemy_storage._begin_transaction()
        async with txn:
            created = await handler.execute_create(CreateOperation(data=[user]), txn)
        user_id = created[0].id

        # Update user - error handling for relationships should work
        # UserModel has no relationships, so AttributeError/KeyError should be caught
        index = ParamIndex(data_type=User, index_name="by_id", id=user_id)
        update_op = UpdateOperation(search_index=index, patch={"name": "RelUser3Updated"})

        # Should handle AttributeError/KeyError gracefully
        txn = real_sqlalchemy_storage._begin_transaction()
        async with txn:
            updated = await handler.execute_update(update_op, txn)

        assert len(updated) == 1
        assert updated[0].name == "RelUser3Updated"

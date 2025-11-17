"""Integration tests for SQLAlchemy storage operations.

Tests basic CRUD operations and pipelines for both SQLAlchemyOperationsHandler
and SQLAlchemyStorage.
"""

from typing import Any

import pytest

from haolib.pipelines import filtero, mapo, reduceo, transformo
from haolib.pipelines.base import Pipeline
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
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.operations.sqlalchemy import SQLAlchemyOperationsHandler
from tests.integration.storages.conftest import User, UserModel

# Constants for test values
MIN_USERS_COUNT = 2
SINGLE_USER_COUNT = 1
ALICE_AGE = 25
BOB_AGE = 30
CHARLIE_AGE = 35
DAVID_AGE = 40
EVE_AGE = 28
FRANK_AGE = 32
GRACE_AGE = 27
GRACE_UPDATED_AGE = 28
HENRY_AGE = 29
HENRY_UPDATED_AGE = 30
IVAN_AGE = 31
JACK_AGE = 25
FILTER_MIN_AGE = 30
MARY_AGE = 29
NICK_AGE = 33
RACHEL_AGE = 26
STEVE_AGE = 29
EXPECTED_TOTAL_AGE = 87  # 26 + 29 + 32
KATE_AGE = 28
LEO_AGE = 31
EXPECTED_TOTAL_AGE_2 = 59  # 28 + 31


class TestOperationsHandler:
    """Integration tests for SQLAlchemyOperationsHandler."""

    @pytest.mark.asyncio
    async def test_execute_create(self, sqlalchemy_storage: Any, registry: Any) -> None:
        """Test execute_create operation."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        users = [
            User(name="Alice", age=ALICE_AGE, email="alice@example.com"),
            User(name="Bob", age=BOB_AGE, email="bob@example.com"),
        ]

        operation = CreateOperation(data=users)
        # Create transaction for handler method
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            result = await handler.execute_create(operation, txn)

        assert len(result) == MIN_USERS_COUNT
        assert result[0].name == "Alice"
        assert result[0].age == ALICE_AGE
        assert result[0].email == "alice@example.com"
        assert result[0].id is not None
        assert result[1].name == "Bob"
        assert result[1].age == BOB_AGE
        assert result[1].email == "bob@example.com"
        assert result[1].id is not None

    @pytest.mark.asyncio
    async def test_execute_create_without_registration(self, sqlalchemy_storage: Any, registry: Any) -> None:
        """Test execute_create with unregistered types (assumes storage model)."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        # Create storage models directly
        # Note: Even though we pass UserModel, it gets converted back to User
        # because registry has mapping for UserModel -> User
        user_models = [
            UserModel(name="Charlie", age=CHARLIE_AGE, email="charlie@example.com"),
            UserModel(name="David", age=DAVID_AGE, email="david@example.com"),
        ]

        operation = CreateOperation(data=user_models)
        # Create transaction for handler method
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            result = await handler.execute_create(operation, txn)

        assert len(result) == MIN_USERS_COUNT
        # Result is converted back to User through registry
        assert isinstance(result[0], User)
        assert result[0].name == "Charlie"
        assert result[0].id is not None

    @pytest.mark.asyncio
    async def test_execute_read(self, sqlalchemy_storage: Any, registry: Any) -> None:
        """Test execute_read operation."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        # Create users first
        users = [
            User(name="Eve", age=EVE_AGE, email="eve@example.com"),
            User(name="Frank", age=FRANK_AGE, email="frank@example.com"),
        ]
        # Create transaction for handler methods
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            create_op = CreateOperation(data=users)
            await handler.execute_create(create_op, txn)

        # Read all users
        index = ParamIndex(data_type=User)
        read_op = ReadOperation(search_index=index)

        # Create new transaction for read
        txn2 = sqlalchemy_storage._begin_transaction()
        async with txn2:
            results = [user async for user in handler.execute_read(read_op, txn2)]

        assert len(results) >= MIN_USERS_COUNT
        # Check that we got our users
        emails = {user.email for user in results}
        assert "eve@example.com" in emails
        assert "frank@example.com" in emails

    @pytest.mark.asyncio
    async def test_execute_update_with_dict_patch(self, sqlalchemy_storage: Any, registry: Any) -> None:
        """Test execute_update with dict patch."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        # Create user
        user = User(name="Grace", age=GRACE_AGE, email="grace@example.com")
        create_op = CreateOperation(data=[user])
        # Create transaction for handler methods
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            created = await handler.execute_create(create_op, txn)
        user_id = created[0].id

        # Update user (partial update)
        index = ParamIndex(data_type=User, id=user_id)
        patch_op = PatchOperation(search_index=index, patch={"age": GRACE_UPDATED_AGE, "name": "Grace Updated"})

        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            updated = await handler.execute_patch(patch_op, txn)

        assert len(updated) == SINGLE_USER_COUNT
        assert updated[0].id == user_id
        assert updated[0].name == "Grace Updated"
        assert updated[0].age == GRACE_UPDATED_AGE
        assert updated[0].email == "grace@example.com"

    @pytest.mark.asyncio
    async def test_execute_update_with_callable_data(self, sqlalchemy_storage: Any, registry: Any) -> None:
        """Test execute_update with callable data."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        # Create user
        user = User(name="Henry", age=HENRY_AGE, email="henry@example.com")
        create_op = CreateOperation(data=[user])
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            created = await handler.execute_create(create_op, txn)
        user_id = created[0].id

        # Update user with callable (full update)
        def update_user(u: User) -> User:
            return User(id=u.id, name="Henry Updated", age=u.age + 1, email=u.email)

        index = ParamIndex(data_type=User, id=user_id)
        update_op = UpdateOperation(search_index=index, data=update_user)

        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            updated = await handler.execute_update(update_op, txn)

        assert len(updated) == SINGLE_USER_COUNT
        assert updated[0].id == user_id
        assert updated[0].name == "Henry Updated"
        assert updated[0].age == HENRY_UPDATED_AGE

    @pytest.mark.asyncio
    async def test_execute_delete(self, sqlalchemy_storage: Any, registry: Any) -> None:
        """Test execute_delete operation."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        # Create user
        user = User(name="Ivan", age=IVAN_AGE, email="ivan@example.com")
        create_op = CreateOperation(data=[user])
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            created = await handler.execute_create(create_op, txn)
        user_id = created[0].id

        # Delete user
        index = ParamIndex(data_type=User, id=user_id)
        delete_op = DeleteOperation(search_index=index)

        txn2 = sqlalchemy_storage._begin_transaction()
        async with txn2:
            deleted_count = await handler.execute_delete(delete_op, txn2)

        assert deleted_count == SINGLE_USER_COUNT

        # Verify deleted
        read_index = ParamIndex(data_type=User, id=user_id)
        read_op = ReadOperation(search_index=read_index)
        txn3 = sqlalchemy_storage._begin_transaction()
        async with txn3:
            results = [user async for user in handler.execute_read(read_op, txn3)]
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_execute_filter(self, sqlalchemy_storage: Any, registry: Any) -> None:
        """Test execute_filter operation."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        # Create users
        users = [
            User(name="Jack", age=JACK_AGE, email="jack@example.com"),
            User(name="Kate", age=KATE_AGE, email="kate@example.com"),
            User(name="Leo", age=LEO_AGE, email="leo@example.com"),
        ]
        create_op = CreateOperation(data=users)
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            await handler.execute_create(create_op, txn)

        # Filter users by age >= 30
        index = ParamIndex(data_type=User)
        read_op = ReadOperation(search_index=index)
        txn2 = sqlalchemy_storage._begin_transaction()
        async with txn2:
            read_result = handler.execute_read(read_op, txn2)
            # Collect async iterator inside transaction
            users_list = [user async for user in read_result]
        filter_op: FilterOperation[User] = FilterOperation(predicate=lambda u: u.age >= FILTER_MIN_AGE)
        txn3 = sqlalchemy_storage._begin_transaction()
        async with txn3:
            filtered = await handler.execute_filter(filter_op, txn3, users_list)

        assert len(filtered) >= 1
        assert all(user.age >= FILTER_MIN_AGE for user in filtered)

    @pytest.mark.asyncio
    async def test_execute_map(self, sqlalchemy_storage: Any, registry: Any) -> None:
        """Test execute_map operation."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        # Create users
        users = [
            User(name="Mary", age=MARY_AGE, email="mary@example.com"),
            User(name="Nick", age=NICK_AGE, email="nick@example.com"),
        ]
        create_op = CreateOperation(data=users)
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            await handler.execute_create(create_op, txn)

        # Map users to their names
        index = ParamIndex(data_type=User)
        read_op = ReadOperation(search_index=index)
        txn2 = sqlalchemy_storage._begin_transaction()
        async with txn2:
            read_result = handler.execute_read(read_op, txn2)
            # Collect async iterator inside transaction
            users_list = [user async for user in read_result]
        map_op: MapOperation[User, str] = MapOperation(mapper=lambda user, _idx: user.name)
        txn3 = sqlalchemy_storage._begin_transaction()
        async with txn3:
            mapped = await handler.execute_map(map_op, txn3, users_list)

        assert len(mapped) >= MIN_USERS_COUNT
        assert "Mary" in mapped
        assert "Nick" in mapped
        assert all(isinstance(name, str) for name in mapped)

    @pytest.mark.asyncio
    async def test_execute_reduce(self, sqlalchemy_storage: Any, registry: Any) -> None:
        """Test execute_reduce operation."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        # Create users
        users = [
            User(name="Oscar", age=RACHEL_AGE, email="oscar@example.com"),
            User(name="Paul", age=STEVE_AGE, email="paul@example.com"),
            User(name="Quinn", age=FRANK_AGE, email="quinn@example.com"),
        ]
        create_op = CreateOperation(data=users)
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            await handler.execute_create(create_op, txn)

        # Reduce to sum of ages
        index = ParamIndex(data_type=User)
        read_op = ReadOperation(search_index=index)
        txn2 = sqlalchemy_storage._begin_transaction()
        async with txn2:
            read_result = handler.execute_read(read_op, txn2)
            # Collect async iterator inside transaction
            users_list = [user async for user in read_result]
        reduce_op: ReduceOperation[User, int] = ReduceOperation(
            reducer=lambda acc, user: acc + user.age,
            initial=0,
        )
        txn3 = sqlalchemy_storage._begin_transaction()
        async with txn3:
            total_age = await handler.execute_reduce(reduce_op, txn3, users_list)

        assert total_age >= EXPECTED_TOTAL_AGE

    @pytest.mark.asyncio
    async def test_execute_transform(self, sqlalchemy_storage: Any, registry: Any) -> None:
        """Test execute_transform operation."""
        handler = SQLAlchemyOperationsHandler(
            registry=registry,
            relationship_load_depth=2,
            storage=sqlalchemy_storage,
        )

        # Create users
        users = [
            User(name="Rachel", age=RACHEL_AGE, email="rachel@example.com"),
            User(name="Steve", age=STEVE_AGE, email="steve@example.com"),
        ]
        create_op = CreateOperation(data=users)
        txn = sqlalchemy_storage._begin_transaction()
        async with txn:
            await handler.execute_create(create_op, txn)

        # Transform to list of emails
        index = ParamIndex(data_type=User)
        read_op = ReadOperation(search_index=index)
        txn2 = sqlalchemy_storage._begin_transaction()
        async with txn2:
            read_result = handler.execute_read(read_op, txn2)
            # Collect async iterator inside transaction
            users_list = [user async for user in read_result]

        transform_op: TransformOperation[list[User], list[str]] = TransformOperation(
            transformer=lambda users: [u.email for u in users]
        )
        txn3 = sqlalchemy_storage._begin_transaction()
        async with txn3:
            emails = await handler.execute_transform(transform_op, txn3, users_list)

        assert len(emails) >= MIN_USERS_COUNT
        assert "rachel@example.com" in emails
        assert "steve@example.com" in emails


class TestStorageOperations:
    """Integration tests for SQLAlchemyStorage operations."""

    @pytest.mark.asyncio
    async def test_execute_create_operation(self, sqlalchemy_storage: Any) -> None:
        """Test executing create operation."""
        users = [
            User(name="Alice", age=ALICE_AGE, email="alice@example.com"),
            User(name="Bob", age=BOB_AGE, email="bob@example.com"),
        ]

        result = await sqlalchemy_storage.create(users).returning().execute()

        assert len(result) == MIN_USERS_COUNT
        assert result[0].name == "Alice"
        assert result[0].id is not None
        assert result[1].name == "Bob"
        assert result[1].id is not None

    @pytest.mark.asyncio
    async def test_execute_read_operation(self, sqlalchemy_storage: Any) -> None:
        """Test executing read operation."""
        # Create users first
        users = [
            User(name="Charlie", age=KATE_AGE, email="charlie@example.com"),
            User(name="David", age=FRANK_AGE, email="david@example.com"),
        ]
        await sqlalchemy_storage.create(users).returning().execute()

        # Read all users
        index = ParamIndex(data_type=User)
        results = await sqlalchemy_storage.read(index).returning().execute()

        assert len(results) >= MIN_USERS_COUNT
        emails = {user.email for user in results}
        assert "charlie@example.com" in emails
        assert "david@example.com" in emails

    @pytest.mark.asyncio
    async def test_execute_update_operation(self, sqlalchemy_storage: Any) -> None:
        """Test executing update operation."""
        # Create user
        user = User(name="Eve", age=EVE_AGE, email="eve@example.com")
        created = await sqlalchemy_storage.create([user]).returning().execute()
        user_id = created[0].id

        # Update user (partial update)
        index = ParamIndex(data_type=User, id=user_id)
        updated = (
            await sqlalchemy_storage.read(index)
            .patch({"age": GRACE_UPDATED_AGE, "name": "Eve Updated"})
            .returning()
            .execute()
        )

        assert len(updated) == SINGLE_USER_COUNT
        assert updated[0].name == "Eve Updated"
        assert updated[0].age == GRACE_UPDATED_AGE

    @pytest.mark.asyncio
    async def test_execute_delete_operation(self, sqlalchemy_storage: Any) -> None:
        """Test executing delete operation."""
        # Create user
        user = User(name="Frank", age=IVAN_AGE, email="frank@example.com")
        created = await sqlalchemy_storage.create([user]).returning().execute()
        user_id = created[0].id

        # Delete user
        index = ParamIndex(data_type=User, id=user_id)
        deleted_count = await sqlalchemy_storage.read(index).delete().execute()

        assert deleted_count == SINGLE_USER_COUNT

    @pytest.mark.asyncio
    async def test_execute_pipeline(self, sqlalchemy_storage: Any) -> None:
        """Test executing pipeline of operations."""
        # Create users
        users = [
            User(name="Grace", age=25, email="grace@example.com"),
            User(name="Henry", age=30, email="henry@example.com"),
            User(name="Iris", age=35, email="iris@example.com"),
        ]
        await sqlalchemy_storage.create(users).returning().execute()

        # Pipeline: read -> filter -> map (all in single transaction)
        index = ParamIndex(data_type=User)
        pipeline: Pipeline[Any, Any, Any] = (
            sqlalchemy_storage.read(index).returning()
            | filtero(lambda u: u.age >= FILTER_MIN_AGE)
            | mapo(lambda u, _idx: u.name)
        )

        result = await pipeline.execute()

        assert len(result) >= MIN_USERS_COUNT
        assert "Henry" in result
        assert "Iris" in result

    @pytest.mark.asyncio
    async def test_execute_with_auto_transaction(self, sqlalchemy_storage: Any) -> None:
        """Test executing operation with automatic transaction."""
        users = [
            User(name="Jack", age=JACK_AGE, email="jack@example.com"),
        ]

        # Execute without providing transaction - should create one automatically
        result = await sqlalchemy_storage.create(users).returning().execute()

        assert len(result) == SINGLE_USER_COUNT
        assert result[0].name == "Jack"
        assert result[0].id is not None

    @pytest.mark.asyncio
    async def test_execute_reduce_operation(self, sqlalchemy_storage: Any) -> None:
        """Test executing reduce operation."""
        # Create users
        users = [
            User(name="Kate", age=KATE_AGE, email="kate@example.com"),
            User(name="Leo", age=LEO_AGE, email="leo@example.com"),
        ]
        await sqlalchemy_storage.create(users).returning().execute()

        # Reduce to sum of ages
        index = ParamIndex(data_type=User)
        pipeline = sqlalchemy_storage.read(index).returning() | reduceo(
            reducer=lambda acc, user: acc + user.age,
            initial=0,
        )

        total_age = await pipeline.execute()

        assert total_age >= EXPECTED_TOTAL_AGE_2

    @pytest.mark.asyncio
    async def test_execute_transform_operation(self, sqlalchemy_storage: Any) -> None:
        """Test executing transform operation."""
        # Create users
        users = [
            User(name="Mary", age=MARY_AGE, email="mary@example.com"),
            User(name="Nick", age=NICK_AGE, email="nick@example.com"),
        ]
        await sqlalchemy_storage.create(users).returning().execute()

        # Transform to list of emails
        # Pipeline: read -> transform (transform needs previous result from pipeline)
        index = ParamIndex(data_type=User)
        pipeline = sqlalchemy_storage.read(index).returning() | transformo(
            transformer=lambda users: [u.email for u in users]
        )
        emails = await pipeline.execute()

        assert len(emails) >= MIN_USERS_COUNT
        assert "mary@example.com" in emails
        assert "nick@example.com" in emails

    @pytest.mark.asyncio
    async def test_execute_read_update_pipeline(self, sqlalchemy_storage: Any) -> None:
        """Test executing read | update pipeline (pipeline mode)."""
        # Create user
        users = [
            User(name="PipelineUser", age=25, email="pipeline@example.com"),
        ]
        created = await sqlalchemy_storage.create(users).returning().execute()
        user_id = created[0].id

        # Read and update in pipeline
        index = ParamIndex(data_type=User, id=user_id)
        pipeline = sqlalchemy_storage.read(index).returning() | sqlalchemy_storage.update(
            data=lambda u: User(id=u.id, name=u.name.upper(), age=u.age + 1, email=u.email)
        )

        updated = await pipeline.execute()

        assert len(updated) == 1
        assert updated[0].name == "PIPELINEUSER"
        assert updated[0].age == 26

    @pytest.mark.asyncio
    async def test_execute_read_patch_pipeline(self, sqlalchemy_storage: Any) -> None:
        """Test executing read | patch pipeline (pipeline mode)."""
        # Create user
        users = [
            User(name="PatchUser", age=25, email="patch@example.com"),
        ]
        created = await sqlalchemy_storage.create(users).returning().execute()
        user_id = created[0].id

        # Read and patch in pipeline
        index = ParamIndex(data_type=User, id=user_id)
        pipeline = sqlalchemy_storage.read(index).returning() | sqlalchemy_storage.patch(patch={"age": 30})

        updated = await pipeline.execute()

        assert len(updated) == 1
        assert updated[0].name == "PatchUser"
        assert updated[0].age == 30

    @pytest.mark.asyncio
    async def test_execute_read_delete_pipeline(self, sqlalchemy_storage: Any) -> None:
        """Test executing read | delete pipeline (pipeline mode)."""
        # Create user
        users = [
            User(name="DeleteUser", age=25, email="delete@example.com"),
        ]
        created = await sqlalchemy_storage.create(users).returning().execute()
        user_id = created[0].id

        # Read and delete in pipeline
        index = ParamIndex(data_type=User, id=user_id)
        pipeline = sqlalchemy_storage.read(index).returning() | sqlalchemy_storage.delete()

        deleted_count = await pipeline.execute()

        assert deleted_count == 1

        # Verify user is deleted - read should return empty iterator
        read_index = ParamIndex(data_type=User, id=user_id)
        read_result = await sqlalchemy_storage.read(read_index).returning().execute()
        # reado returns AsyncIterator, collect it to verify it's empty
        if hasattr(read_result, "__aiter__"):
            users_list = [user async for user in read_result]
            assert len(users_list) == 0
        else:
            # If it's already a list, just check it's empty
            assert len(read_result) == 0

    @pytest.mark.asyncio
    async def test_execute_read_filter_update_pipeline(self, sqlalchemy_storage: Any) -> None:
        """Test executing read | filter | update pipeline."""
        # Create users
        users = [
            User(name="FilterUpdate1", age=25, email="filter1@example.com"),
            User(name="FilterUpdate2", age=35, email="filter2@example.com"),
        ]
        await sqlalchemy_storage.create(users).returning().execute()

        # Read, filter by age >= 30, and update
        index = ParamIndex(data_type=User)
        pipeline = (
            sqlalchemy_storage.read(index).returning()
            | filtero(lambda u: u.age >= 30)
            | sqlalchemy_storage.update(data=lambda u: User(id=u.id, name=u.name.upper(), age=u.age, email=u.email))
        )

        updated = await pipeline.execute()

        assert len(updated) == 1
        assert updated[0].name == "FILTERUPDATE2"
        assert updated[0].age == 35

    @pytest.mark.asyncio
    async def test_execute_read_map_update_pipeline(self, sqlalchemy_storage: Any) -> None:
        """Test executing read | map | update pipeline."""
        # Create user
        users = [
            User(name="MapUpdate", age=25, email="map@example.com"),
        ]
        created = await sqlalchemy_storage.create(users).returning().execute()
        user_id = created[0].id

        # Read, map to extract user, and update
        index = ParamIndex(data_type=User, id=user_id)
        pipeline = (
            sqlalchemy_storage.read(index).returning()
            | mapo(lambda u, idx: u)
            | sqlalchemy_storage.update(
                data=lambda u: User(id=u.id, name=u.name + "_updated", age=u.age, email=u.email)
            )
        )

        updated = await pipeline.execute()

        assert len(updated) == 1
        assert updated[0].name == "MapUpdate_updated"

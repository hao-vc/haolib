"""Tests for SQLAlchemyPipelineOptimizer."""

from typing import Any

import pytest
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from haolib.database.models.base.sqlalchemy import SQLAlchemyBaseModel
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
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.operations.optimizer.sqlalchemy import SQLAlchemyPipelineOptimizer


class UserModel(SQLAlchemyBaseModel):
    """Test user model."""

    __tablename__ = "test_users_optimizer"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column()
    age: Mapped[int] = mapped_column()


class User:
    """Test user class."""

    def __init__(self, id: int, name: str, age: int) -> None:
        self.id = id
        self.name = name
        self.age = age


# Define predicate functions at module level
def predicate_age_ge_18_optimizer(u: Any) -> bool:
    """Predicate for age >= 18."""
    return u.age >= 18


def predicate_name_starts_with_a_optimizer(u: Any) -> bool:
    """Predicate that cannot be converted."""
    return u.name.startswith("A")


@pytest.fixture
def registry() -> DataTypeRegistry:
    """Create data type registry."""
    reg = DataTypeRegistry()
    reg.register(
        storage_type=UserModel,
        user_type=User,
        to_storage=lambda u: UserModel(id=u.id, name=u.name, age=u.age),
        from_storage=lambda m: User(id=m.id, name=m.name, age=m.age),
    )
    return reg


@pytest.fixture
def optimizer(registry: DataTypeRegistry) -> SQLAlchemyPipelineOptimizer:
    """Create optimizer."""
    return SQLAlchemyPipelineOptimizer(registry)


class TestSQLAlchemyPipelineOptimizer:
    """Tests for SQLAlchemyPipelineOptimizer."""

    def test_analyze_single_read_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with single ReadOperation."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        analysis = optimizer.analyze(read_op)

        assert analysis.execution_plan == "storage"
        assert analysis.can_execute_on_storage is True
        assert analysis.optimized_operation == read_op
        assert len(analysis.sql_operations) == 1

    def test_analyze_read_with_filter(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with ReadOperation and FilterOperation."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        filter_op = FilterOperation(predicate=predicate_age_ge_18_optimizer)
        pipeline = read_op | filter_op

        analysis = optimizer.analyze(pipeline)

        # FilterOperation can be converted, so should be storage
        assert analysis.execution_plan == "storage"
        assert analysis.can_execute_on_storage is True
        assert analysis.optimized_operation is not None

    def test_analyze_read_with_map(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with ReadOperation and MapOperation."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)
        map_op = MapOperation(mapper=lambda u, idx: u.name)
        pipeline = read_op | map_op

        analysis = optimizer.analyze(pipeline)

        assert analysis.execution_plan == "hybrid"
        assert analysis.can_execute_on_storage is False
        assert analysis.optimized_operation is not None
        assert len(analysis.remaining_operations) == 1

    def test_analyze_create_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with CreateOperation."""
        users = [User(id=1, name="Alice", age=25)]
        create_op = CreateOperation(data=users)

        analysis = optimizer.analyze(create_op)

        assert analysis.execution_plan == "storage"
        assert analysis.can_execute_on_storage is True

    def test_analyze_patch_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with PatchOperation."""
        index = ParamIndex(data_type=User, id=1)
        patch_op = PatchOperation(search_index=index, patch={"name": "Bob"})

        analysis = optimizer.analyze(patch_op)

        assert analysis.execution_plan == "storage"
        assert analysis.can_execute_on_storage is True

    def test_analyze_delete_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with DeleteOperation."""
        index = ParamIndex(data_type=User, id=1)
        delete_op = DeleteOperation(search_index=index)

        analysis = optimizer.analyze(delete_op)

        assert analysis.execution_plan == "storage"
        assert analysis.can_execute_on_storage is True

    def test_analyze_transform_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with TransformOperation."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        def transformer(users: Any) -> Any:
            return [u.name for u in users]

        transform_op = TransformOperation(transformer=transformer)
        pipeline = read_op | transform_op

        analysis = optimizer.analyze(pipeline)

        assert analysis.execution_plan == "hybrid"
        assert analysis.can_execute_on_storage is False
        # ReadOperation can be in SQL, TransformOperation in Python
        assert len(analysis.remaining_operations) == 1

    def test_analyze_reduce_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with ReduceOperation."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)
        reduce_op = ReduceOperation(reducer=lambda acc, age: acc + age, initial=0)
        pipeline = read_op | reduce_op

        analysis = optimizer.analyze(pipeline)

        assert analysis.execution_plan == "hybrid"
        assert analysis.can_execute_on_storage is False

    def test_analyze_all_python(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with all Python operations."""
        map_op = MapOperation(mapper=lambda u, idx: u.name)
        transform_op = TransformOperation(transformer=lambda names: [n.upper() for n in names])
        pipeline = map_op | transform_op

        analysis = optimizer.analyze(pipeline)

        assert analysis.execution_plan == "python"
        assert analysis.can_execute_on_storage is False
        assert len(analysis.remaining_operations) == 2

    def test_analyze_nested_pipeline(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with nested pipeline."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)
        filter_op = FilterOperation(predicate=lambda u: u.age >= 18)
        map_op = MapOperation(mapper=lambda u, idx: u.name)

        inner_pipeline = read_op | filter_op
        outer_pipeline = inner_pipeline | map_op

        analysis = optimizer.analyze(outer_pipeline)

        assert analysis.execution_plan == "hybrid"
        assert analysis.can_execute_on_storage is False

    def test_optimize_with_optimization(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test optimize when optimization is possible."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)
        filter_op = FilterOperation(predicate=lambda u: u.age >= 18)
        pipeline = read_op | filter_op

        result = optimizer.optimize(pipeline)

        assert result is not None

    def test_optimize_without_optimization(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test optimize when optimization is not possible."""
        map_op = MapOperation(mapper=lambda u, idx: u.name)
        transform_op = TransformOperation(transformer=lambda names: [n.upper() for n in names])
        pipeline = map_op | transform_op

        result = optimizer.optimize(pipeline)

        assert result == pipeline

    def test_flatten_pipeline_single_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _flatten_pipeline with single operation."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        operations = optimizer._flatten_pipeline(read_op)

        assert len(operations) == 1
        assert operations[0] == read_op

    def test_flatten_pipeline_simple_pipeline(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _flatten_pipeline with simple pipeline."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)
        filter_op = FilterOperation(predicate=lambda u: u.age >= 18)
        pipeline = read_op | filter_op

        operations = optimizer._flatten_pipeline(pipeline)

        assert len(operations) == 2
        assert operations[0] == read_op
        assert operations[1] == filter_op

    def test_flatten_pipeline_nested_pipeline(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _flatten_pipeline with nested pipeline."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)
        filter_op = FilterOperation(predicate=lambda u: u.age >= 18)
        map_op = MapOperation(mapper=lambda u, idx: u.name)

        inner_pipeline = read_op | filter_op
        outer_pipeline = inner_pipeline | map_op

        operations = optimizer._flatten_pipeline(outer_pipeline)

        assert len(operations) == 3
        assert operations[0] == read_op
        assert operations[1] == filter_op
        assert operations[2] == map_op

    def test_can_execute_in_sql_read_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with ReadOperation."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        result = optimizer._can_execute_in_sql(read_op)

        assert result is True

    def test_can_execute_in_sql_create_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with CreateOperation."""
        users = [User(id=1, name="Alice", age=25)]
        create_op = CreateOperation(data=users)

        result = optimizer._can_execute_in_sql(create_op)

        assert result is True

    def test_can_execute_in_sql_filter_operation_valid(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with valid FilterOperation."""
        filter_op = FilterOperation(predicate=predicate_age_ge_18_optimizer)

        result = optimizer._can_execute_in_sql(filter_op)

        assert result is True

    def test_can_execute_in_sql_filter_operation_invalid(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with invalid FilterOperation."""
        # Filter with method call (cannot be converted)
        filter_op = FilterOperation(predicate=predicate_name_starts_with_a_optimizer)

        result = optimizer._can_execute_in_sql(filter_op)

        assert result is False

    def test_can_execute_in_sql_map_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with MapOperation."""
        map_op = MapOperation(mapper=lambda u, idx: u.name)

        result = optimizer._can_execute_in_sql(map_op)

        assert result is False

    def test_can_execute_in_sql_reduce_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with ReduceOperation."""
        reduce_op = ReduceOperation(reducer=lambda acc, age: acc + age, initial=0)

        result = optimizer._can_execute_in_sql(reduce_op)

        assert result is False

    def test_can_execute_in_sql_transform_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with TransformOperation."""
        transform_op = TransformOperation(transformer=lambda users: [u.name for u in users])

        result = optimizer._can_execute_in_sql(transform_op)

        assert result is False

    def test_build_optimized_operation_empty(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _build_optimized_operation with empty operations."""
        result = optimizer._build_optimized_operation([])

        assert result is None

    def test_build_optimized_operation_no_read(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _build_optimized_operation without ReadOperation."""
        users = [User(id=1, name="Alice", age=25)]
        create_op = CreateOperation(data=users)

        result = optimizer._build_optimized_operation([create_op])

        assert result is None

    def test_build_optimized_operation_single_read(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _build_optimized_operation with single ReadOperation."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        result = optimizer._build_optimized_operation([read_op])

        assert result == read_op

    def test_build_optimized_operation_read_with_filter(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _build_optimized_operation with ReadOperation and FilterOperation."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        filter_op = FilterOperation(predicate=predicate_age_ge_18_optimizer)

        result = optimizer._build_optimized_operation([read_op, filter_op])

        # Should return read_op as placeholder
        assert result == read_op

    @pytest.mark.asyncio
    async def test_build_optimized_operation_async(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test build_optimized_operation_async."""
        from unittest.mock import AsyncMock, MagicMock, patch

        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        filter_op = FilterOperation(predicate=predicate_age_ge_18_optimizer)
        operations = [read_op, filter_op]

        mock_session = AsyncMock()

        # Mock QueryBuilder.build_async
        with patch.object(optimizer._query_builder, "build_async") as mock_build:
            mock_result = ReadOperation(search_index=index)
            mock_build.return_value = mock_result

            result = await optimizer.build_optimized_operation_async(operations, mock_session)

            assert result == mock_result
            mock_build.assert_called_once_with(operations, mock_session)

    def test_analyze_read_with_update_pipeline_mode(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with ReadOperation | UpdateOperation in pipeline mode (no search_index)."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        # UpdateOperation without search_index - uses previous_result (pipeline mode)
        update_op = UpdateOperation(search_index=None, data=None)
        pipeline = read_op | update_op

        analysis = optimizer.analyze(pipeline)

        # Should be python execution plan (cannot optimize pipeline mode)
        # When operation uses previous_result, entire pipeline executes in Python
        assert analysis.execution_plan == "python"
        assert analysis.can_execute_on_storage is False
        assert len(analysis.sql_operations) == 0  # No SQL operations (entire pipeline in Python)
        assert len(analysis.remaining_operations) == 2  # Both read_op and update_op

    def test_analyze_read_with_patch_pipeline_mode(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with ReadOperation | PatchOperation in pipeline mode (no search_index)."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        # PatchOperation without search_index - uses previous_result (pipeline mode)
        patch_op = PatchOperation(search_index=None, patch=None)
        pipeline = read_op | patch_op

        analysis = optimizer.analyze(pipeline)

        # Should be python execution plan (cannot optimize pipeline mode)
        # When operation uses previous_result, entire pipeline executes in Python
        assert analysis.execution_plan == "python"
        assert analysis.can_execute_on_storage is False
        assert len(analysis.sql_operations) == 0  # No SQL operations (entire pipeline in Python)
        assert len(analysis.remaining_operations) == 2  # Both read_op and patch_op

    def test_analyze_read_with_delete_pipeline_mode(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with ReadOperation | DeleteOperation in pipeline mode (no search_index)."""
        index = ParamIndex(data_type=User, age=25)
        read_op = ReadOperation(search_index=index)

        # DeleteOperation without search_index - uses previous_result (pipeline mode)
        # This pattern can now be optimized into a single DELETE query
        delete_op: DeleteOperation[User] = DeleteOperation(search_index=None)
        pipeline = read_op | delete_op

        analysis = optimizer.analyze(pipeline)

        # Should be storage execution plan (can optimize reado | deleteo pattern)
        # The optimizer recognizes this pattern and marks it for optimization
        assert analysis.execution_plan == "storage"
        assert analysis.can_execute_on_storage is True
        assert len(analysis.sql_operations) == 2  # Both read_op and delete_op for optimization

    def test_analyze_update_search_mode(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with UpdateOperation in search mode (has search_index and data)."""
        index = ParamIndex(data_type=User, id=1)
        update_op = UpdateOperation(
            search_index=index,
            data=User(id=1, name="John", age=30),
        )

        analysis = optimizer.analyze(update_op)

        # Should be storage execution plan (can optimize search mode)
        assert analysis.execution_plan == "storage"
        assert analysis.can_execute_on_storage is True
        assert len(analysis.sql_operations) == 1

    def test_analyze_patch_search_mode(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with PatchOperation in search mode (has search_index and patch)."""
        index = ParamIndex(data_type=User, id=1)
        patch_op = PatchOperation(
            search_index=index,
            patch={"age": 30},
        )

        analysis = optimizer.analyze(patch_op)

        # Should be storage execution plan (can optimize search mode)
        assert analysis.execution_plan == "storage"
        assert analysis.can_execute_on_storage is True
        assert len(analysis.sql_operations) == 1

    def test_analyze_delete_search_mode(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test analyze with DeleteOperation in search mode (has search_index)."""
        index = ParamIndex(data_type=User, id=1)
        delete_op = DeleteOperation(search_index=index)

        analysis = optimizer.analyze(delete_op)

        # Should be storage execution plan (can optimize search mode)
        assert analysis.execution_plan == "storage"
        assert analysis.can_execute_on_storage is True
        assert len(analysis.sql_operations) == 1

    def test_can_execute_in_sql_update_with_previous(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with UpdateOperation that receives previous_result."""
        index = ParamIndex(data_type=User, id=1)
        # UpdateOperation without search_index - uses previous_result
        update_op = UpdateOperation(search_index=None, data=None)

        result = optimizer._can_execute_in_sql(update_op, has_previous=True)

        assert result is False

    def test_can_execute_in_sql_update_with_previous_but_has_index(
        self, optimizer: SQLAlchemyPipelineOptimizer
    ) -> None:
        """Test _can_execute_in_sql with UpdateOperation that has search_index even with previous_result."""
        index = ParamIndex(data_type=User, id=1)
        # UpdateOperation with search_index - can still be optimized
        update_op = UpdateOperation(
            search_index=index,
            data=User(id=1, name="John", age=30),
        )

        result = optimizer._can_execute_in_sql(update_op, has_previous=True)

        assert result is True

    def test_can_execute_in_sql_patch_with_previous(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with PatchOperation that receives previous_result."""
        # PatchOperation without search_index - uses previous_result
        patch_op = PatchOperation(search_index=None, patch=None)

        result = optimizer._can_execute_in_sql(patch_op, has_previous=True)

        assert result is False

    def test_can_execute_in_sql_delete_with_previous(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with DeleteOperation that receives previous_result."""
        # DeleteOperation without search_index - uses previous_result
        # Now DeleteOperation can be executed in SQL even in pipeline mode (can be optimized with reado)
        delete_op = DeleteOperation(search_index=None)

        result = optimizer._can_execute_in_sql(delete_op, has_previous=True)

        assert result is True  # Can be optimized with reado pattern

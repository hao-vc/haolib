"""Unit tests for optimizer edge cases."""

import pytest

from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.operations.concrete import MapOperation, ReduceOperation, TransformOperation
from haolib.storages.operations.optimizer.sqlalchemy import SQLAlchemyPipelineOptimizer


class TestOptimizerEdgeCases:
    """Tests for optimizer edge cases."""

    @pytest.fixture
    def optimizer(self) -> SQLAlchemyPipelineOptimizer:
        """Create optimizer."""
        registry = DataTypeRegistry()
        return SQLAlchemyPipelineOptimizer(registry=registry)

    def test_can_execute_in_sql_map_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with MapOperation (line 187)."""

        map_op = MapOperation(mapper=lambda x, idx: x * 2)

        result = optimizer._can_execute_in_sql(map_op)

        assert result is False

    def test_can_execute_in_sql_reduce_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with ReduceOperation (line 187)."""

        reduce_op = ReduceOperation(reducer=lambda acc, x: acc + x, initial=0)

        result = optimizer._can_execute_in_sql(reduce_op)

        assert result is False

    def test_can_execute_in_sql_transform_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with TransformOperation (line 187)."""

        transform_op = TransformOperation(transformer=lambda x: list(x))

        result = optimizer._can_execute_in_sql(transform_op)

        assert result is False

    def test_can_execute_in_sql_unknown_operation(self, optimizer: SQLAlchemyPipelineOptimizer) -> None:
        """Test _can_execute_in_sql with unknown operation (line 191)."""

        class UnknownOperation:
            pass

        result = optimizer._can_execute_in_sql(UnknownOperation())  # type: ignore[arg-type]

        assert result is False

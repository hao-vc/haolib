"""Tests for PipelineContext."""

import pytest

from haolib.pipelines.base import Pipeline
from haolib.pipelines.context import PipelineContext
from haolib.pipelines.operations import CreateOperation, ReadOperation


class TestPipelineContext:
    """Tests for PipelineContext."""

    def test_pipeline_context_creation(self) -> None:
        """Test creating PipelineContext."""
        from haolib.storages.indexes.params import ParamIndex
        from haolib.storages.sqlalchemy import SQLAlchemyStorage

        # Create a simple pipeline
        read_op = ReadOperation(search_index=ParamIndex(int))
        pipeline = Pipeline(first=read_op, second=CreateOperation(data=[1, 2, 3]))

        # Create context
        target_operations = {}
        context = PipelineContext(
            full_pipeline=pipeline,
            current_position=0,
            target_operations=target_operations,
        )

        assert context.full_pipeline == pipeline
        assert context.current_position == 0
        assert context.target_operations == target_operations

    def test_get_operations_for_target(self) -> None:
        """Test getting operations for a target."""
        from haolib.storages.indexes.params import ParamIndex
        from haolib.storages.sqlalchemy import SQLAlchemyStorage

        # Create a simple pipeline
        read_op = ReadOperation(search_index=ParamIndex(int))
        create_op = CreateOperation(data=[1, 2, 3])
        pipeline = Pipeline(first=read_op, second=create_op)

        # Mock target
        target = object()

        # Create context
        target_operations = {target: [read_op, create_op]}
        context = PipelineContext(
            full_pipeline=pipeline,
            current_position=0,
            target_operations=target_operations,
        )

        assert context.get_operations_for_target(target) == [read_op, create_op]
        assert context.get_operations_for_target(object()) == []

    def test_get_future_operations(self) -> None:
        """Test getting future operations for a target."""
        from haolib.storages.indexes.params import ParamIndex

        # Create a simple pipeline
        read_op = ReadOperation(search_index=ParamIndex(int))
        create_op = CreateOperation(data=[1, 2, 3])
        pipeline = Pipeline(first=read_op, second=create_op)

        # Mock target
        target = object()

        # Create context
        target_operations = {target: [read_op, create_op]}
        context = PipelineContext(
            full_pipeline=pipeline,
            current_position=0,
            target_operations=target_operations,
        )

        # At position 0, future operations should include create_op
        future_ops = context.get_future_operations(target)
        assert len(future_ops) == 1
        assert future_ops[0] == create_op

        # At position 1, no future operations
        context_at_pos_1 = PipelineContext(
            full_pipeline=pipeline,
            current_position=1,
            target_operations=target_operations,
        )
        assert len(context_at_pos_1.get_future_operations(target)) == 0

    def test_will_return_to_target(self) -> None:
        """Test checking if pipeline will return to target."""
        from haolib.storages.indexes.params import ParamIndex

        # Create a simple pipeline
        read_op = ReadOperation(search_index=ParamIndex(int))
        create_op = CreateOperation(data=[1, 2, 3])
        pipeline = Pipeline(first=read_op, second=create_op)

        # Mock target
        target = object()

        # Create context
        target_operations = {target: [read_op, create_op]}
        context = PipelineContext(
            full_pipeline=pipeline,
            current_position=0,
            target_operations=target_operations,
        )

        # At position 0, should return to target
        assert context.will_return_to_target(target) is True

        # At position 1, should not return to target
        context_at_pos_1 = PipelineContext(
            full_pipeline=pipeline,
            current_position=1,
            target_operations=target_operations,
        )
        assert context_at_pos_1.will_return_to_target(target) is False

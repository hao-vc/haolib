"""Unit tests for pipeline validator."""

from typing import TYPE_CHECKING, Any

import pytest

from haolib.pipelines import PipelineValidationError, PipelineValidator, filtero, mapo, reduceo, transformo
from haolib.pipelines.operations import CreateOperation, ReadOperation
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.targets.abstract import AbstractDataTarget
from tests.integration.storages.conftest import User

if TYPE_CHECKING:
    from haolib.pipelines.base import Operation, Pipeline


class MockDataTarget:
    """Mock data target for testing."""

    async def execute[T_Result](
        self,
        operation: Operation[Any, T_Result] | Pipeline[Any, Any, T_Result],
    ) -> T_Result:
        """Execute operation (mock implementation)."""
        # This is just for type checking - actual execution is not tested here
        raise NotImplementedError("Mock target should not be executed in validator tests")


# Create a mock target instance that satisfies AbstractDataTarget protocol
mock_storage: AbstractDataTarget = MockDataTarget()


class TestPipelineValidator:
    """Unit tests for PipelineValidator."""

    def test_validate_valid_pipeline(self) -> None:
        """Test validation of valid pipeline."""
        # Valid pipeline: read with target -> filter -> create with target

        # Note: This test uses direct Operation classes since validator tests
        # the validation logic, not the fluent API
        from haolib.pipelines.base import TargetBoundOperation
        from haolib.pipelines.operations import CreateOperation, ReadOperation

        read_op = TargetBoundOperation(operation=ReadOperation(search_index=ParamIndex(User)), target=mock_storage)
        create_op = TargetBoundOperation(operation=CreateOperation(data=[lambda users: users]), target=mock_storage)
        pipeline: Pipeline[Any, Any, Any] = read_op | filtero(lambda u: u.age >= 18) | create_op

        validator = PipelineValidator()
        # Should not raise
        validator.validate(pipeline)

    def test_validate_filter_without_previous_operation(self) -> None:
        """Test validation fails when filter is first operation."""
        # Invalid: filter requires previous result but is first
        pipeline = filtero(lambda u: u.age >= 18)

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0
        assert "requires previous result" in str(exc_info.value)
        assert "first operation" in str(exc_info.value)

    def test_validate_read_without_target(self) -> None:
        """Test validation fails when read operation has no target."""
        # Invalid: read requires target but is not bound
        pipeline = ReadOperation(search_index=ParamIndex(User))

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0
        assert "requires target binding" in str(exc_info.value)

    def test_validate_create_without_target_but_with_previous_result(self) -> None:
        """Test validation passes when create receives previous result."""
        # Valid: create receives previous result from filter
        from haolib.pipelines.base import TargetBoundOperation

        pipeline: Pipeline[Any, Any, Any] = (
            TargetBoundOperation(operation=ReadOperation(search_index=ParamIndex(User)), target=mock_storage)
            | filtero(lambda u: u.age >= 18)
            | CreateOperation(data=[lambda users: users])  # No target, but receives previous result
        )

        validator = PipelineValidator()
        # Should not raise - create receives previous result
        validator.validate(pipeline)

    def test_validate_create_without_target_and_without_previous_result(self) -> None:
        """Test validation fails when create has no target and no previous result."""
        # Invalid: create has no data and no previous result
        pipeline = CreateOperation(data=[])

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0
        assert "has no data and no previous_result" in str(exc_info.value)

    def test_validate_create_without_target_but_with_data(self) -> None:
        """Test validation passes when create has data but no target."""
        # Valid: create has explicit data, doesn't need target or previous_result
        pipeline = CreateOperation(data=[User(name="Alice", age=25, email="alice@example.com")])

        validator = PipelineValidator()
        # Should not raise - create has data
        validator.validate(pipeline)

    def test_validate_map_without_previous_operation(self) -> None:
        """Test validation fails when map is first operation."""
        from haolib.pipelines.base import Pipeline

        # Invalid: map requires previous result but is first
        pipeline: Pipeline[Any, Any, Any] = mapo(lambda u, idx: u.name)

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0

    def test_validate_reduce_without_previous_operation(self) -> None:
        """Test validation fails when reduce is first operation."""
        from haolib.pipelines.base import Pipeline

        # Invalid: reduce requires previous result but is first
        pipeline: Pipeline[Any, Any, Any] = reduceo(lambda acc, u: acc + u.age, 0)

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0

    def test_validate_transform_without_previous_operation(self) -> None:
        """Test validation fails when transform is first operation."""
        from haolib.pipelines.base import Pipeline

        # Invalid: transform requires previous result but is first
        pipeline: Pipeline[Any, Any, Any] = transformo(lambda users: [u.name for u in users])

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0

    def test_validate_complex_valid_pipeline(self) -> None:
        """Test validation of complex valid pipeline."""
        from haolib.pipelines.base import Pipeline, TargetBoundOperation

        # Valid complex pipeline
        pipeline: Pipeline[Any, Any, Any] = (
            TargetBoundOperation(operation=ReadOperation(search_index=ParamIndex(User)), target=mock_storage)
            | filtero(lambda u: u.age >= 18)
            | mapo(lambda u, idx: u.name)
            | reduceo(lambda acc, name: acc + len(name), 0)
            | transformo(lambda total: str(total))
            | TargetBoundOperation(operation=CreateOperation(data=[lambda data: data]), target=mock_storage)
        )

        validator = PipelineValidator()
        # Should not raise
        validator.validate(pipeline)

    def test_validate_pipeline_with_intermediate_operation_missing_target(self) -> None:
        """Test validation fails when intermediate operation needs target but doesn't have it."""
        from haolib.pipelines.base import Pipeline, TargetBoundOperation

        # Invalid: read in middle of pipeline without target
        pipeline: Pipeline[Any, Any, Any] = (
            TargetBoundOperation(operation=ReadOperation(search_index=ParamIndex(User)), target=mock_storage)
            | filtero(lambda u: u.age >= 18)
            | ReadOperation(search_index=ParamIndex(User))  # Missing target
        )

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        # Should fail on the second read operation (index 2)
        assert exc_info.value.operation_index == 2
        assert "requires target binding" in str(exc_info.value)

    def test_validate_pipeline_method(self) -> None:
        """Test that Pipeline.validate() method works."""
        from haolib.pipelines.base import Pipeline, TargetBoundOperation

        # Valid pipeline
        pipeline: Pipeline[Any, Any, Any] = TargetBoundOperation(
            operation=ReadOperation(search_index=ParamIndex(User)), target=mock_storage
        ) | filtero(lambda u: u.age >= 18)

        # Should not raise
        pipeline.validate()

    def test_validate_pipeline_method_raises_error(self) -> None:
        """Test that Pipeline.validate() method raises error for invalid pipeline."""
        from haolib.pipelines.base import Pipeline

        # Invalid pipeline - read without target
        read_op = ReadOperation(search_index=ParamIndex(User))
        # Create a pipeline with read without target (invalid)
        invalid_pipeline = Pipeline(first=read_op, second=read_op)

        with pytest.raises(PipelineValidationError):
            invalid_pipeline.validate()

    def test_validate_map_bound_to_target(self) -> None:
        """Test validation fails when map operation is bound to target."""
        from haolib.pipelines.base import Pipeline, TargetBoundOperation  # Moved import here

        # Invalid: map requires previous result but is bound to target
        pipeline: Pipeline[Any, Any, Any] = (
            TargetBoundOperation(operation=ReadOperation(search_index=ParamIndex(User)), target=mock_storage)
            | mapo(lambda u, _idx: u.name) ^ mock_storage
        )

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 1
        assert "requires previous result and executes in Python" in str(exc_info.value)
        assert "should not be bound" in str(exc_info.value)

    def test_validate_filter_bound_to_target(self) -> None:
        """Test validation fails when filter operation is bound to target."""
        from haolib.pipelines.base import Pipeline, TargetBoundOperation  # Moved import here

        # Invalid: filter requires previous result but is bound to target
        pipeline: Pipeline[Any, Any, Any] = (
            TargetBoundOperation(operation=ReadOperation(search_index=ParamIndex(User)), target=mock_storage)
            | filtero(lambda u: u.age >= 18) ^ mock_storage
        )

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 1
        assert "requires previous result and executes in Python" in str(exc_info.value)

    def test_validate_reduce_bound_to_target(self) -> None:
        """Test validation fails when reduce operation is bound to target."""
        from haolib.pipelines.base import Pipeline, TargetBoundOperation  # Moved import here

        # Invalid: reduce requires previous result but is bound to target
        pipeline: Pipeline[Any, Any, Any] = (
            TargetBoundOperation(operation=ReadOperation(search_index=ParamIndex(User)), target=mock_storage)
            | reduceo(lambda acc, u: acc + u.age, 0) ^ mock_storage
        )

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 1
        assert "requires previous result and executes in Python" in str(exc_info.value)

    def test_validate_transform_bound_to_target(self) -> None:
        """Test validation fails when transform operation is bound to target."""
        from haolib.pipelines.base import Pipeline, TargetBoundOperation  # Moved import here

        # Invalid: transform requires previous result but is bound to target
        pipeline: Pipeline[Any, Any, Any] = (
            TargetBoundOperation(operation=ReadOperation(search_index=ParamIndex(User)), target=mock_storage)
            | transformo(lambda users: [u.name for u in users]) ^ mock_storage
        )

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 1
        assert "requires previous result and executes in Python" in str(exc_info.value)

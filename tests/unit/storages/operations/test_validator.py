"""Unit tests for pipeline validator."""

from typing import TYPE_CHECKING, Any

import pytest

from haolib.storages.dsl import createo, filtero, mapo, reado, reduceo, transformo
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.operations.validator import PipelineValidationError, PipelineValidator
from haolib.storages.targets.abstract import AbstractDataTarget
from tests.integration.storages.conftest import User

if TYPE_CHECKING:
    from haolib.storages.operations.base import Operation, Pipeline


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

        pipeline: Pipeline[Any, Any, Any] = (
            reado(search_index=ParamIndex(User)) ^ mock_storage
            | filtero(lambda u: u.age >= 18)
            | createo([lambda users: users]) ^ mock_storage
        )

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
        pipeline = reado(search_index=ParamIndex(User))

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0
        assert "requires target binding" in str(exc_info.value)

    def test_validate_create_without_target_but_with_previous_result(self) -> None:
        """Test validation passes when create receives previous result."""
        # Valid: create receives previous result from filter
        pipeline: Pipeline[Any, Any, Any] = (
            reado(search_index=ParamIndex(User)) ^ mock_storage
            | filtero(lambda u: u.age >= 18)
            | createo([lambda users: users])  # No target, but receives previous result
        )

        validator = PipelineValidator()
        # Should not raise - create receives previous result
        validator.validate(pipeline)

    def test_validate_create_without_target_and_without_previous_result(self) -> None:
        """Test validation fails when create has no target and no previous result."""
        # Invalid: create requires target but is not bound and doesn't receive previous result
        pipeline = createo([User(name="Alice", age=25, email="alice@example.com")])

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0
        assert "requires target binding" in str(exc_info.value)

    def test_validate_map_without_previous_operation(self) -> None:
        """Test validation fails when map is first operation."""
        from haolib.storages.operations.base import Pipeline

        # Invalid: map requires previous result but is first
        pipeline: Pipeline[Any, Any, Any] = mapo(lambda u, idx: u.name)

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0

    def test_validate_reduce_without_previous_operation(self) -> None:
        """Test validation fails when reduce is first operation."""
        from haolib.storages.operations.base import Pipeline

        # Invalid: reduce requires previous result but is first
        pipeline: Pipeline[Any, Any, Any] = reduceo(lambda acc, u: acc + u.age, 0)

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0

    def test_validate_transform_without_previous_operation(self) -> None:
        """Test validation fails when transform is first operation."""
        from haolib.storages.operations.base import Pipeline

        # Invalid: transform requires previous result but is first
        pipeline: Pipeline[Any, Any, Any] = transformo(lambda users: [u.name for u in users])

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        assert exc_info.value.operation_index == 0

    def test_validate_complex_valid_pipeline(self) -> None:
        """Test validation of complex valid pipeline."""
        from haolib.storages.operations.base import Pipeline

        # Valid complex pipeline
        pipeline: Pipeline[Any, Any, Any] = (
            reado(search_index=ParamIndex(User)) ^ mock_storage
            | filtero(lambda u: u.age >= 18)
            | mapo(lambda u, idx: u.name)
            | reduceo(lambda acc, name: acc + len(name), 0)
            | transformo(lambda total: str(total))
            | createo([lambda data: data]) ^ mock_storage
        )

        validator = PipelineValidator()
        # Should not raise
        validator.validate(pipeline)

    def test_validate_pipeline_with_intermediate_operation_missing_target(self) -> None:
        """Test validation fails when intermediate operation needs target but doesn't have it."""
        from haolib.storages.operations.base import Pipeline

        # Invalid: read in middle of pipeline without target
        pipeline: Pipeline[Any, Any, Any] = (
            reado(search_index=ParamIndex(User)) ^ mock_storage
            | filtero(lambda u: u.age >= 18)
            | reado(search_index=ParamIndex(User))  # Missing target
        )

        validator = PipelineValidator()
        with pytest.raises(PipelineValidationError) as exc_info:
            validator.validate(pipeline)

        # Should fail on the second read operation (index 2)
        assert exc_info.value.operation_index == 2
        assert "requires target binding" in str(exc_info.value)

    def test_validate_pipeline_method(self) -> None:
        """Test that Pipeline.validate() method works."""
        from haolib.storages.operations.base import Pipeline

        # Valid pipeline
        pipeline: Pipeline[Any, Any, Any] = reado(search_index=ParamIndex(User)) ^ mock_storage | filtero(
            lambda u: u.age >= 18
        )

        # Should not raise
        pipeline.validate()

    def test_validate_pipeline_method_raises_error(self) -> None:
        """Test that Pipeline.validate() method raises error for invalid pipeline."""
        from haolib.storages.operations.base import Pipeline

        # Invalid pipeline - read without target
        read_op = reado(search_index=ParamIndex(User))
        # Create a pipeline with read without target (invalid)
        invalid_pipeline = Pipeline(first=read_op, second=read_op)

        with pytest.raises(PipelineValidationError):
            invalid_pipeline.validate()

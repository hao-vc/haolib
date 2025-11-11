"""Test FastStream entrypoint."""

from collections.abc import Callable
from typing import Any

import pytest
from dishka import AsyncContainer
from faststream import FastStream

from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.faststream import (
    FastStreamEntrypoint,
    FastStreamEntrypointComponent,
)
from tests.integration.entrypoints.conftest import (
    run_entrypoint_briefly,
)


class TestFastStreamEntrypointValidation:
    """Test FastStream entrypoint validation."""

    def test_validate_succeeds_with_valid_app(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that validation succeeds with a valid FastStream app."""
        faststream_entrypoint.validate()

    def test_validate_fails_without_broker(self) -> None:
        """Test that validation fails when broker is not configured."""
        app = FastStream()  # App without broker
        entrypoint = FastStreamEntrypoint(app=app)

        with pytest.raises(EntrypointInconsistencyError, match="FastStream broker is not configured"):
            entrypoint.validate()


class TestFastStreamEntrypointLifecycle:
    """Test FastStream entrypoint lifecycle methods."""

    @pytest.mark.asyncio
    async def test_startup_validates_config(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that startup validates configuration."""
        await faststream_entrypoint.startup()
        await faststream_entrypoint.shutdown()

    @pytest.mark.asyncio
    async def test_startup_is_idempotent(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that startup can be called multiple times safely."""
        await faststream_entrypoint.startup()
        await faststream_entrypoint.startup()  # Should not raise
        await faststream_entrypoint.shutdown()

    @pytest.mark.asyncio
    async def test_shutdown_is_idempotent(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that shutdown can be called multiple times safely."""
        await faststream_entrypoint.startup()
        await faststream_entrypoint.shutdown()
        await faststream_entrypoint.shutdown()  # Should not raise

    @pytest.mark.asyncio
    async def test_run_with_startup(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that run works after startup."""
        await run_entrypoint_briefly(faststream_entrypoint)


class TestFastStreamEntrypointBuilder:
    """Test FastStream entrypoint builder methods."""

    def test_setup_dishka_returns_self(
        self, faststream_entrypoint: FastStreamEntrypoint, mock_container: AsyncContainer
    ) -> None:
        """Test that setup_dishka returns self for chaining."""
        result = faststream_entrypoint.setup_dishka(mock_container)
        assert result is faststream_entrypoint

    def test_setup_exception_handlers_returns_self(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that setup_exception_handlers returns self for chaining."""
        handlers: dict[type[Exception], Callable[..., Any]] = {ValueError: lambda _: None}
        result = faststream_entrypoint.setup_exception_handlers(handlers)
        assert result is faststream_entrypoint

    def test_setup_exception_handlers_fails_without_broker(self) -> None:
        """Test that setup_exception_handlers fails without broker."""
        app = FastStream()  # App without broker
        entrypoint = FastStreamEntrypoint(app=app)

        with pytest.raises(EntrypointInconsistencyError, match="FastStream broker is not set"):
            entrypoint.setup_exception_handlers({ValueError: lambda _: None})

    def test_get_app_returns_faststream_app(self, faststream_entrypoint: FastStreamEntrypoint) -> None:
        """Test that get_app returns the FastStream app."""
        app = faststream_entrypoint.get_app()
        assert isinstance(app, FastStream)

    def test_builder_pattern_chaining(
        self, faststream_entrypoint: FastStreamEntrypoint, mock_container: AsyncContainer
    ) -> None:
        """Test that builder methods can be chained."""
        result = faststream_entrypoint.setup_dishka(mock_container).setup_exception_handlers(
            {ValueError: lambda _: None}
        )
        assert result is faststream_entrypoint


class TestFastStreamEntrypointComponent:
    """Test FastStream entrypoint component."""

    def test_validate_succeeds_with_valid_broker(self, faststream_component: FastStreamEntrypointComponent) -> None:
        """Test that component validation succeeds with valid broker."""
        faststream_component.validate()

    def test_setup_dishka_returns_self(
        self, faststream_component: FastStreamEntrypointComponent, mock_container: AsyncContainer
    ) -> None:
        """Test that setup_dishka returns self for chaining."""
        result = faststream_component.setup_dishka(mock_container)
        assert result is faststream_component

    def test_setup_exception_handlers_returns_self(self, faststream_component: FastStreamEntrypointComponent) -> None:
        """Test that setup_exception_handlers returns self for chaining."""
        handlers: dict[type[Exception], Callable[..., Any]] = {ValueError: lambda _: None}
        result = faststream_component.setup_exception_handlers(handlers)
        assert result is faststream_component

    def test_get_broker_returns_broker(self, faststream_component: FastStreamEntrypointComponent) -> None:
        """Test that get_broker returns the broker."""
        broker = faststream_component.get_broker()
        assert broker is not None

    def test_builder_pattern_chaining(
        self, faststream_component: FastStreamEntrypointComponent, mock_container: AsyncContainer
    ) -> None:
        """Test that builder methods can be chained."""
        result = faststream_component.setup_dishka(mock_container).setup_exception_handlers(
            {ValueError: lambda _: None}
        )
        assert result is faststream_component

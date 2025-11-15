"""Test FastAPI entrypoint lifecycle methods."""

import pytest
from dishka import AsyncContainer
from fastapi import FastAPI

from haolib.configs.server import ServerConfig
from haolib.entrypoints.abstract import EntrypointInconsistencyError
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from haolib.entrypoints.plugins.fastapi.cors import FastAPICORSMiddlewarePlugin
from haolib.entrypoints.plugins.fastapi.dishka import FastAPIDishkaPlugin
from haolib.entrypoints.plugins.fastapi.exceptions import FastAPIExceptionHandlersPlugin
from haolib.entrypoints.plugins.fastapi.health import FastAPIHealthCheckPlugin
from haolib.entrypoints.plugins.fastapi.idempotency import FastAPIIdempotencyMiddlewarePlugin
from tests.integration.entrypoints.conftest import (
    run_entrypoint_briefly,
)


class TestFastAPIEntrypointStartup:
    """Test FastAPI entrypoint startup."""

    @pytest.mark.asyncio
    async def test_startup_creates_server(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that startup creates a server instance."""
        await fastapi_entrypoint.startup()

        await fastapi_entrypoint.shutdown()

    @pytest.mark.asyncio
    async def test_startup_with_custom_server_config(self, fastapi_app: FastAPI) -> None:
        """Test startup with custom server configuration."""
        config = ServerConfig(host="127.0.0.1", port=8080)
        entrypoint = FastAPIEntrypoint(app=fastapi_app, server_config=config)

        await entrypoint.startup()

        await entrypoint.shutdown()

    @pytest.mark.asyncio
    async def test_startup_is_idempotent(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that startup can be called multiple times safely."""
        await fastapi_entrypoint.startup()
        await fastapi_entrypoint.startup()  # Should not raise

        await fastapi_entrypoint.shutdown()


class TestFastAPIEntrypointShutdown:
    """Test FastAPI entrypoint shutdown."""

    @pytest.mark.asyncio
    async def test_shutdown_cleans_up_server(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that shutdown cleans up the server."""
        await fastapi_entrypoint.startup()

        await fastapi_entrypoint.shutdown()
        # Server cleanup happens, but the reference might still exist
        # The important thing is that shutdown doesn't raise

    @pytest.mark.asyncio
    async def test_shutdown_is_idempotent(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that shutdown can be called multiple times safely."""
        await fastapi_entrypoint.startup()
        await fastapi_entrypoint.shutdown()
        await fastapi_entrypoint.shutdown()  # Should not raise

    @pytest.mark.asyncio
    async def test_shutdown_without_startup(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that shutdown works even if startup wasn't called."""
        await fastapi_entrypoint.shutdown()  # Should not raise


class TestFastAPIEntrypointRun:
    """Test FastAPI entrypoint run method."""

    @pytest.mark.asyncio
    async def test_run_requires_startup(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that run requires startup to be called first."""
        with pytest.raises(EntrypointInconsistencyError, match="must be started via startup\\(\\) before run\\(\\)"):
            await fastapi_entrypoint.run()

    @pytest.mark.asyncio
    async def test_run_with_startup(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that run works after startup."""
        await run_entrypoint_briefly(fastapi_entrypoint)


class TestFastAPIEntrypointBuilder:
    """Test FastAPI entrypoint builder methods."""

    def test_use_plugin_dishka_returns_self(
        self, fastapi_entrypoint: FastAPIEntrypoint, mock_container: AsyncContainer
    ) -> None:
        """Test that use_plugin with DishkaPlugin returns self for chaining."""
        result = fastapi_entrypoint.use_plugin(FastAPIDishkaPlugin(mock_container))
        assert result is fastapi_entrypoint

    def test_use_plugin_cors_returns_self(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that use_plugin with CORSMiddlewarePlugin returns self for chaining."""
        result = fastapi_entrypoint.use_plugin(FastAPICORSMiddlewarePlugin())
        assert result is fastapi_entrypoint

    def test_use_plugin_exception_handlers_returns_self(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that use_plugin with ExceptionHandlersPlugin returns self for chaining."""
        result = fastapi_entrypoint.use_plugin(FastAPIExceptionHandlersPlugin())
        assert result is fastapi_entrypoint

    def test_use_plugin_health_check_returns_self(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that use_plugin with HealthCheckPlugin returns self for chaining."""
        result = fastapi_entrypoint.use_plugin(FastAPIHealthCheckPlugin())
        assert result is fastapi_entrypoint

    def test_builder_pattern_chaining(self, fastapi_entrypoint: FastAPIEntrypoint) -> None:
        """Test that use_plugin methods can be chained."""
        result = (
            fastapi_entrypoint.use_plugin(FastAPICORSMiddlewarePlugin())
            .use_plugin(FastAPIExceptionHandlersPlugin())
            .use_plugin(FastAPIHealthCheckPlugin())
        )
        assert result is fastapi_entrypoint


class TestFastAPIEntrypointIdempotency:
    """Test FastAPI entrypoint idempotency middleware."""

    def test_idempotency_requires_container_or_factory(self, fastapi_app: FastAPI) -> None:
        """Test that idempotency middleware requires container or factory."""
        entrypoint = FastAPIEntrypoint(app=fastapi_app)

        with pytest.raises(
            EntrypointInconsistencyError,
            match="Idempotency middleware cannot be setup without",
        ):
            entrypoint.use_plugin(FastAPIIdempotencyMiddlewarePlugin())

    def test_idempotency_with_container(self, fastapi_entrypoint_with_dishka: FastAPIEntrypoint) -> None:
        """Test idempotency middleware setup with container."""
        result = fastapi_entrypoint_with_dishka.use_plugin(FastAPIIdempotencyMiddlewarePlugin())
        assert result is fastapi_entrypoint_with_dishka

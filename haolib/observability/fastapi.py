"""FastAPI observability."""

import logging

import sentry_sdk
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

from haolib.configs.observability import ObservabilityConfig
from haolib.observability.setup import setup_observability


def configure_uvicorn_logging() -> None:
    """Configure uvicorn loggers to propagate to root logger."""

    uvicorn_loggers = ["uvicorn", "uvicorn.error", "uvicorn.access"]

    for logger_name in uvicorn_loggers:
        logger = logging.getLogger(logger_name)

        for handler in logger.handlers[:]:
            logger.removeHandler(handler)

        logger.propagate = True


async def observe_exception(exc: Exception) -> None:
    """Internal exception handler."""

    span = trace.get_current_span()
    span.record_exception(exc)
    span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
    sentry_sdk.capture_exception(exc)


def setup_observability_for_fastapi(app: FastAPI, config: ObservabilityConfig) -> None:
    """Setup observability for FastAPI."""

    configure_uvicorn_logging()

    FastAPIInstrumentor.instrument_app(app)

    setup_observability(config)

"""Observability utilities."""

import sentry_sdk
from opentelemetry import trace


async def observe_exception(exc: Exception) -> None:
    """Observe exception."""

    span = trace.get_current_span()
    span.record_exception(exc)
    span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
    sentry_sdk.capture_exception(exc)

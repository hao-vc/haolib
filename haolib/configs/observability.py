"""Observability config."""

import os

from pydantic import Field
from pydantic_settings import BaseSettings


def get_enable_otel_tracer() -> bool:
    """Get if otel tracer is enabled."""
    return "OTEL_EXPORTER_OTLP_ENDPOINT" in os.environ


class ObservabilityConfig(BaseSettings):
    """Observability configuration.

    This config is used to configure the observability.

    Attributes:
        service_namespace (str): The namespace of the service.
            Defaults to "Humanlessly Autonomously Orchestrated Service".
        enable_otel_tracer (bool): Whether to enable the otel tracer.
            Defaults to the value of the "OTEL_EXPORTER_OTLP_ENDPOINT" environment variable.
        enable_console_tracer (bool): Whether to enable the console tracer. Defaults to False.
        enable_otel_metrics (bool): Whether to enable the otel metrics.
            Defaults to the value of the "OTEL_EXPORTER_OTLP_ENDPOINT" environment variable.
        enable_console_metrics (bool): Whether to enable the console metrics. Defaults to False.
        enable_otel_logs (bool): Whether to enable the otel logs.
            Defaults to the value of the "OTEL_EXPORTER_OTLP_ENDPOINT" environment variable.
        enable_console_logs (bool): Whether to enable the console logs. Defaults to False.
        suppress_httpx_logs (bool): Whether to suppress the httpx logs. Defaults to True.

    """

    service_namespace: str = "Humanlessly Autonomously Orchestrated Service"

    enable_otel_tracer: bool = Field(
        default_factory=get_enable_otel_tracer, description="Whether to enable the otel tracer."
    )
    enable_console_tracer: bool = Field(default=False, description="Whether to enable the console tracer.")

    enable_otel_metrics: bool = Field(
        default_factory=get_enable_otel_tracer, description="Whether to enable the otel metrics."
    )
    enable_console_metrics: bool = Field(default=False, description="Whether to enable the console metrics.")

    enable_otel_logs: bool = Field(
        default_factory=get_enable_otel_tracer, description="Whether to enable the otel logs."
    )
    enable_console_logs: bool = Field(default=False, description="Whether to enable the console logs.")

    suppress_httpx_logs: bool = Field(default=True, description="Whether to suppress the httpx logs.")

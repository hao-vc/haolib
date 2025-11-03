"""Observability setup for the application."""

import logging
import uuid
from typing import Self

from fastapi import FastAPI
from opentelemetry import _logs as logs
from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, ConsoleLogExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    ConsoleMetricExporter,
    PeriodicExportingMetricReader,
)
from opentelemetry.sdk.resources import SERVICE_INSTANCE_ID, SERVICE_NAME, SERVICE_NAMESPACE, Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from sentry_sdk.integrations.opentelemetry import SentrySpanProcessor

from haolib.configs.observability import ObservabilityConfig
from haolib.observability.logfmt import HAOLogfmtFormatter

logger = logging.getLogger(__name__)


class ObservabilitySetupper:
    """Observability setupper."""

    def __init__(
        self,
        config: ObservabilityConfig | None = None,
        service_name: str | None = None,
    ) -> None:
        """Initialize the observability setupper.

        Args:
            config: The observability config.
            If None, the default observability config will be used.
            See `haolib.configs.observability.ObservabilityConfig` for more details.
            service_name: The name of the service to create the resource with.
            The resource itself can be overwritten with the `ObservabilitySetupper.with_resource` method.
            If None, the service name will be set to the name of the module.

        """
        self._config = config or ObservabilityConfig()
        self._resource = Resource.create(
            attributes={SERVICE_INSTANCE_ID: str(uuid.uuid7())}  # type: ignore[attr-defined]
            | ({SERVICE_NAMESPACE: self._config.service_namespace} if self._config.service_namespace else {})
            | ({SERVICE_NAME: service_name} if service_name else {})
        )

        self._logger_provider: LoggerProvider | None = None
        self._tracer_provider: TracerProvider | None = None
        self._meter_provider: MeterProvider | None = None

    def instrument_httpx(self) -> Self:
        """Instrument httpx."""
        HTTPXClientInstrumentor().instrument()

        logger.info("httpx has been instrumented")

        if self._config.suppress_httpx_logs:
            logging.getLogger("httpx").setLevel(logging.WARNING)
            logging.getLogger("httpcore").setLevel(logging.WARNING)
            logger.info("httpx logs have been suppressed")

        return self

    def instrument_fastapi(self, app: FastAPI) -> Self:
        """Instrument FastAPI."""
        uvicorn_loggers = ["uvicorn", "uvicorn.error", "uvicorn.access"]

        for logger_name in uvicorn_loggers:
            logger = logging.getLogger(logger_name)

            for handler in logger.handlers[:]:
                logger.removeHandler(handler)

            logger.propagate = True

        FastAPIInstrumentor.instrument_app(app)

        logger.info("FastAPI has been instrumented")

        return self

    def with_resource(self, resource: Resource) -> Self:
        """Set the resource for the observability.

        Args:
            resource: The resource to use for the observability.
            If None, the default resource will be used.
            See `opentelemetry.sdk.resources.Resource` for more details.

        """
        self._resource = resource
        return self

    def get_resource(self) -> Resource:
        """Get the resource for the observability."""
        return self._resource

    def setup_logging(self, level: int = logging.INFO, formatter: logging.Formatter | None = None) -> Self:
        """Setup logging.

        This method will setup the logging for the observability.
        It will add a console handler to the root logger and set the level of
        the root logger to the level passed as an argument.

        Args:
            level: The level to set for the root logger.
                Defaults to `logging.INFO`.
            formatter: The formatter to use for the console handler.
                If None, the default formatter will be used, which is `HAOLogfmtFormatter`.
                See `haolib.observability.logfmt.HAOLogfmtFormatter` for more details.


        """

        LoggingInstrumentor().instrument()

        root_logger = logging.getLogger()
        root_logger.setLevel(level)

        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter or HAOLogfmtFormatter())
        root_logger.addHandler(console_handler)

        logger_provider = LoggerProvider(resource=self._resource)

        if self._config.enable_console_logs:
            logger_provider.add_log_record_processor(BatchLogRecordProcessor(ConsoleLogExporter()))
            logger.info("Enabled console logs exporter")

        if self._config.enable_otel_logs:
            logger_provider.add_log_record_processor(BatchLogRecordProcessor(OTLPLogExporter()))
            logger.info("Enabled opentelemetry logs exporter")

        logs.set_logger_provider(logger_provider)

        self._logger_provider = logger_provider

        otel_handler = LoggingHandler(logger_provider=logger_provider)
        logging.getLogger().addHandler(otel_handler)

        logger.info("Logging has been setup")

        return self

    def get_logger_provider(self) -> LoggerProvider | None:
        """Get the logger provider for the observability.

        Returns:
            LoggerProvider | None: The logger provider for the observability.
            If None, the logger provider has not been setup.

        """
        return self._logger_provider

    def setup_tracing(self) -> Self:
        """Setup tracing.

        This method will setup the tracing for the observability.

        See `haolib.configs.observability.ObservabilityConfig` for more details.
        """
        tracer_provider = TracerProvider(resource=self._resource)
        tracer_provider.add_span_processor(SentrySpanProcessor())

        if self._config.enable_console_tracer:
            tracer_provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
            logger.info("Enabled console span exporter")

        if self._config.enable_otel_tracer:
            tracer_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
            logger.info("Enabled opentelemetry span exporter")

        trace.set_tracer_provider(tracer_provider)

        self._tracer_provider = tracer_provider

        logger.info("Tracing has been setup")

        return self

    def get_tracer_provider(self) -> TracerProvider | None:
        """Get the tracer provider for the observability.

        Returns:
            TracerProvider | None: The tracer provider for the observability.
            If None, the tracer provider has not been setup.

        """
        return self._tracer_provider

    def setup_metrics(self) -> Self:
        """Setup metrics.

        This method will setup the metrics for the observability.
        See `haolib.configs.observability.ObservabilityConfig` for more details.
        """
        metric_readers = []

        if self._config.enable_console_metrics:
            metric_readers.append(PeriodicExportingMetricReader(ConsoleMetricExporter()))
            logger.info("Enabled console metrics exporter")

        if self._config.enable_otel_metrics:
            metric_readers.append(PeriodicExportingMetricReader(OTLPMetricExporter()))
            logger.info("Enabled opentelemetry metrics exporter")

        meter_provider = MeterProvider(resource=self._resource, metric_readers=metric_readers)
        metrics.set_meter_provider(meter_provider)

        logger.info("Metrics have been setup")

        self._meter_provider = meter_provider

        return self

    def get_meter_provider(self) -> MeterProvider | None:
        """Get the meter provider for the observability.

        Returns:
            MeterProvider | None: The meter provider for the observability.
            If None, the meter provider has not been setup.

        """
        return self._meter_provider

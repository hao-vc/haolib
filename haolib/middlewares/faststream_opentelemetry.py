"""Queues router."""

from faststream.kafka.opentelemetry import KafkaTelemetryMiddleware
from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider


def get_kafka_telemetry_middleware(service_name: str | None = None) -> KafkaTelemetryMiddleware:
    """Get the Kafka telemetry middleware.

    Args:
        service_name: The name of the service to pass to the resource attributes. Defaults to "faststream".

    Returns:
        The Kafka telemetry middleware.

    """
    resource = Resource.create(attributes={"service.name": service_name or "faststream"})
    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)

    return KafkaTelemetryMiddleware(tracer_provider=tracer_provider)

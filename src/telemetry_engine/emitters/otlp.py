"""Turn generated trace data into real OpenTelemetry spans.

The only module that touches the OTel SDK. Everything upstream of it is plain
data, which is what keeps the workload logic testable.

One thing here is worth understanding before changing it: spans are emitted with
*explicit* start and end timestamps. A generated trace claims a 4-second LLM
call, and this module backdates the span rather than sleeping for 4 seconds.
Without that, producing 5k spans/s of realistic multi-second traces would need
tens of thousands of concurrent in-flight spans, and the generator -- not the
pipeline -- would be the bottleneck under measurement.
"""

from __future__ import annotations

import time
from dataclasses import dataclass

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import SpanKind, Status, StatusCode

from telemetry_engine.common.logging import get_logger
from telemetry_engine.emitters.workload import SpanData, StatusClass, TraceData

log = get_logger(__name__)

_SPAN_KINDS = {
    "server": SpanKind.SERVER,
    "client": SpanKind.CLIENT,
    "internal": SpanKind.INTERNAL,
}

_MS_TO_NS = 1_000_000


@dataclass(frozen=True)
class ExporterConfig:
    """How the emitter talks to the collector."""

    endpoint: str = "http://localhost:4317"
    service_name: str = "mock-inference-endpoint"
    service_namespace: str = "llm-telemetry"
    environment: str = "local"

    # BatchSpanProcessor sizing. The default queue (2048) is far too small for
    # the 5k spans/s target: it fills in under half a second and the SDK starts
    # dropping before the collector ever sees the data, which would make the
    # generator look like a pipeline problem.
    max_queue_size: int = 32_768
    max_export_batch_size: int = 1_024
    schedule_delay_ms: int = 1_000
    export_timeout_ms: int = 30_000


def build_tracer_provider(config: ExporterConfig) -> TracerProvider:
    """Construct an isolated TracerProvider wired to the collector.

    Deliberately *not* installed as the global provider: the load generator runs
    several of these in separate worker processes, and tests build throwaway
    ones. Callers hold their own reference.
    """
    resource = Resource.create(
        {
            "service.name": config.service_name,
            "service.namespace": config.service_namespace,
            "deployment.environment": config.environment,
        }
    )
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(
            OTLPSpanExporter(endpoint=config.endpoint, insecure=True),
            max_queue_size=config.max_queue_size,
            max_export_batch_size=config.max_export_batch_size,
            schedule_delay_millis=config.schedule_delay_ms,
            export_timeout_millis=config.export_timeout_ms,
        )
    )
    return provider


def _status(span_data: SpanData) -> Status:
    if span_data.status is StatusClass.OK:
        return Status(StatusCode.OK)
    return Status(StatusCode.ERROR, description=span_data.status.value)


def _emit_span(
    tracer: trace.Tracer,
    span_data: SpanData,
    *,
    trace_start_ns: int,
    parent_context: trace.Context | None,
) -> int:
    """Emit one span and its children. Returns the number of spans emitted."""
    start_ns = trace_start_ns + int(span_data.start_offset_ms * _MS_TO_NS)
    end_ns = start_ns + int(span_data.duration_ms * _MS_TO_NS)

    span = tracer.start_span(
        span_data.name,
        context=parent_context,
        kind=_SPAN_KINDS[span_data.kind],
        attributes=span_data.attributes,  # type: ignore[arg-type]
        start_time=start_ns,
    )
    span.set_status(_status(span_data))

    emitted = 1
    child_context = trace.set_span_in_context(span)
    for child in span_data.children:
        emitted += _emit_span(
            tracer, child, trace_start_ns=trace_start_ns, parent_context=child_context
        )

    span.end(end_time=end_ns)
    return emitted


def emit_trace(
    tracer: trace.Tracer,
    trace_data: TraceData,
    *,
    trace_start_ns: int | None = None,
) -> int:
    """Emit a whole generated trace. Returns the span count.

    `trace_start_ns` defaults to backdating the trace so that it *ends* now --
    a trace whose spans claim to run into the future would be rejected or look
    absurd on a dashboard.
    """
    if trace_start_ns is None:
        total_ms = trace_data.root.duration_ms
        trace_start_ns = time.time_ns() - int(total_ms * _MS_TO_NS)

    return _emit_span(tracer, trace_data.root, trace_start_ns=trace_start_ns, parent_context=None)

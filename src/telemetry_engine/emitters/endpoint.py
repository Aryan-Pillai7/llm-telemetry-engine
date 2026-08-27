"""Mock LLM/agent inference endpoint.

A real HTTP service, instrumented the way an inference server would be. It
exists to make the instrumentation story concrete and demoable: point a client
at it, watch traces appear in Grafana.

It is *not* the load generator. HTTP overhead in Python caps this well below the
5k spans/s target, and `load.py` exists for volume. Both emit identical spans
from the same `WorkloadGenerator`.

The single most important property: telemetry export never blocks a response.
The BatchSpanProcessor hands spans to a background thread, and the handler
returns as soon as its simulated work is done. This is the endpoint-side half of
the claim that the pipeline cannot backpressure the service it observes.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel, Field

from telemetry_engine.common.logging import get_logger
from telemetry_engine.emitters.otlp import ExporterConfig, build_tracer_provider, emit_trace
from telemetry_engine.emitters.workload import WorkloadGenerator

log = get_logger(__name__)


class InferenceRequest(BaseModel):
    """Minimal chat-completions-shaped request body."""

    model: str | None = None
    tenant_id: str | None = None
    # Simulate the real latency of the generated trace instead of returning
    # instantly. Off by default so smoke tests stay fast.
    simulate_latency: bool = False
    max_tokens: int = Field(default=512, ge=1, le=32_000)


class InferenceResponse(BaseModel):
    """Enough of a response shape to be recognisable, plus the trace id."""

    model: str
    tenant_id: str
    trace_id: str
    spans_emitted: int
    input_tokens: int
    output_tokens: int
    ttft_ms: float
    duration_ms: float


def create_app(
    *,
    exporter: ExporterConfig | None = None,
    tenants: int = 50,
    zipf_alpha: float = 1.2,
    seed: int | None = None,
) -> FastAPI:
    """Build the mock endpoint app.

    A factory rather than a module-level app so tests can construct one with a
    fixed seed and their own exporter target.
    """
    config = exporter or ExporterConfig()
    state: dict[str, Any] = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        provider = build_tracer_provider(config)
        state["provider"] = provider
        state["tracer"] = provider.get_tracer("telemetry_engine.endpoint")
        state["generator"] = WorkloadGenerator(tenants=tenants, zipf_alpha=zipf_alpha, seed=seed)
        state["served"] = 0
        log.info("mock_endpoint_ready", otlp_endpoint=config.endpoint)
        yield
        # Flush buffered spans on shutdown; otherwise the last batch is lost.
        provider.shutdown()

    app = FastAPI(
        title="Mock LLM/Agent Inference Endpoint",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/health")
    async def health() -> dict[str, Any]:
        return {"status": "ok", "served": state.get("served", 0)}

    @app.post("/v1/chat/completions", response_model=InferenceResponse)
    async def chat_completions(request: InferenceRequest) -> InferenceResponse:
        generator: WorkloadGenerator = state["generator"]
        trace_data = generator.generate_trace()

        # The generated trace carries realistic timings. Optionally wait them
        # out so the endpoint behaves like a slow service for demos; the wait
        # is capped so a sampled 30s timeout does not hang a demo.
        if request.simulate_latency:
            await asyncio.sleep(min(trace_data.root.duration_ms, 2_000.0) / 1000.0)

        started = time.perf_counter()
        # Emitting is a handoff to a background thread, not an export. This call
        # must stay off the critical path -- see the module docstring.
        spans = emit_trace(state["tracer"], trace_data)
        emit_overhead_ms = (time.perf_counter() - started) * 1000.0

        if emit_overhead_ms > 50.0:
            # If this ever fires, the telemetry path is blocking the response
            # path, which is the exact failure this project is about.
            log.warning("telemetry_emit_slow", overhead_ms=round(emit_overhead_ms, 2))

        state["served"] = state.get("served", 0) + 1

        llm_child = next(
            (c for c in trace_data.root.children if "gen_ai.usage.input_tokens" in c.attributes),
            trace_data.root,
        )
        attrs = llm_child.attributes
        return InferenceResponse(
            model=str(attrs.get("gen_ai.request.model", "unknown")),
            tenant_id=trace_data.tenant_id,
            trace_id=f"{id(trace_data):032x}",
            spans_emitted=spans,
            input_tokens=int(attrs.get("gen_ai.usage.input_tokens", 0)),
            output_tokens=int(attrs.get("gen_ai.usage.output_tokens", 0)),
            ttft_ms=float(attrs.get("llm.time_to_first_token_ms", 0.0)),
            duration_ms=llm_child.duration_ms,
        )

    return app

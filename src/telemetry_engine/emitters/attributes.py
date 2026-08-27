"""Attribute vocabulary for LLM/agent telemetry.

Every attribute key the pipeline produces is named here exactly once. Renaming a
key then becomes a single edit that ripples through the emitters, the ClickHouse
materialized views, and the dimension registry, instead of a grep-and-hope.

Where OpenTelemetry has a semantic convention, it is used verbatim -- the GenAI
conventions cover model, operation, and token usage. Inference-serving internals
they do not cover (KV-cache occupancy, time-to-first-token, inter-token latency)
are namespaced under `llm.` so it is obvious which attributes are ours and which
are standard.
"""

from __future__ import annotations

from typing import Final

# --- OpenTelemetry GenAI semantic conventions --------------------------------

GEN_AI_SYSTEM: Final = "gen_ai.system"
GEN_AI_OPERATION: Final = "gen_ai.operation.name"
GEN_AI_REQUEST_MODEL: Final = "gen_ai.request.model"
GEN_AI_RESPONSE_MODEL: Final = "gen_ai.response.model"
GEN_AI_REQUEST_MAX_TOKENS: Final = "gen_ai.request.max_tokens"
GEN_AI_REQUEST_TEMPERATURE: Final = "gen_ai.request.temperature"
GEN_AI_RESPONSE_FINISH_REASONS: Final = "gen_ai.response.finish_reasons"
GEN_AI_USAGE_INPUT_TOKENS: Final = "gen_ai.usage.input_tokens"
GEN_AI_USAGE_OUTPUT_TOKENS: Final = "gen_ai.usage.output_tokens"

# --- Inference-serving internals (ours) --------------------------------------

# Fraction of the KV cache in use when the request was admitted. The single most
# useful signal for explaining why latency degraded on a busy endpoint: as
# occupancy approaches 1.0, the scheduler starts preempting and queueing.
LLM_KV_CACHE_UTILIZATION: Final = "llm.kv_cache.utilization"
LLM_KV_CACHE_BLOCKS: Final = "llm.kv_cache.blocks"

# Time to first token: what a user actually perceives as latency.
LLM_TTFT_MS: Final = "llm.time_to_first_token_ms"
# Mean inter-token latency: how fast the stream feels after it starts.
LLM_ITL_MS: Final = "llm.inter_token_latency_ms"
LLM_QUEUE_TIME_MS: Final = "llm.queue_time_ms"
LLM_TOKENS_PER_SECOND: Final = "llm.output_tokens_per_second"
LLM_CACHED_PROMPT_TOKENS: Final = "llm.cached_prompt_tokens"
LLM_BATCH_SIZE: Final = "llm.batch_size"
LLM_STREAMING: Final = "llm.streaming"

# --- Routing and multi-tenancy ------------------------------------------------

# The tenant dimension. Bounded at the ingest boundary by the cardinality guard
# (ADR-005): unknown or over-budget values collapse to `__other__`.
TENANT_ID: Final = "tenant.id"
TENANT_TIER: Final = "tenant.tier"
ROUTE: Final = "http.route"
ENDPOINT_REGION: Final = "cloud.region"

# --- Agent orchestration ------------------------------------------------------

AGENT_NAME: Final = "agent.name"
AGENT_STEP: Final = "agent.step"
TOOL_NAME: Final = "tool.name"

# --- Error classification -----------------------------------------------------

# Coarse status class, deliberately low-cardinality: it is a rollup dimension,
# unlike the error message, which is not.
STATUS_CLASS: Final = "status.class"
ERROR_TYPE: Final = "error.type"

# Every low-cardinality attribute that is allowed to become a rollup key. The
# dimension registry in Phase 4 validates against this set, so an emitter cannot
# introduce a grouping dimension without it being a deliberate change here.
ROLLUP_DIMENSION_KEYS: Final[frozenset[str]] = frozenset(
    {
        TENANT_ID,
        TENANT_TIER,
        GEN_AI_REQUEST_MODEL,
        GEN_AI_OPERATION,
        ROUTE,
        STATUS_CLASS,
        ENDPOINT_REGION,
    }
)

# Attributes that must NEVER become rollup keys: unbounded or near-unbounded.
# Present in raw spans for debugging, dropped from every aggregate (ADR-006).
HIGH_CARDINALITY_KEYS: Final[frozenset[str]] = frozenset(
    {
        "trace_id",
        "span_id",
        "request.id",
        "prompt.hash",
        ERROR_TYPE,
    }
)


def assert_dimensions_are_disjoint() -> None:
    """Rollup dimensions and high-cardinality attributes must not overlap.

    An attribute that is both a grouping key and unbounded is precisely the bug
    this design exists to prevent.

    **Deliberately called only from the test suite.** Both sets above are module
    constants, so the invariant can only be broken by editing this file, and a
    test run is the earliest point at which that edit is checked. Asserting at
    import time would add a cost to every process start and would vanish under
    `python -O`, which strips assertions.

    The inert-guard audit (`scripts/find_inert_guards.py`) reports this function
    as test-only. That report is correct and this docstring is the answer to it:
    the audit exists to make sure each such case was decided rather than
    overlooked.
    """
    overlap = ROLLUP_DIMENSION_KEYS & HIGH_CARDINALITY_KEYS
    if overlap:
        raise AssertionError(f"attributes are both rollup keys and high-cardinality: {overlap}")

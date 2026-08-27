-- Cardinality allowlist: the registry, materialized where the views can read it.
--
-- `schemas/clickhouse/dimensions.yaml` is the source of truth;
-- `telemetry-engine dimensions apply` writes it here. The rollup materialized
-- views consult the dictionary below on every row and rewrite anything absent
-- from it to `__other__`.
--
-- Putting enforcement in ClickHouse rather than in a Python consumer is forced
-- by ADR-002: ClickHouse reads Redpanda directly, so there is no application
-- process on the ingest path. The upside is that the rollup tables become
-- structurally incapable of exceeding their budget -- there is no path into
-- them that bypasses the check.

CREATE TABLE IF NOT EXISTS telemetry.dim_allowlist
(
    dimension   LowCardinality(String),
    value       String,
    updated_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (dimension, value);

-- Dictionary over the table above, so the lookup is an in-memory hash probe
-- rather than a join per row.
--
-- LIFETIME governs automatic refresh. The sync command issues an explicit
-- SYSTEM RELOAD DICTIONARY as well, because otherwise a freshly applied
-- allowlist appears to do nothing until the lifetime expires.
CREATE DICTIONARY IF NOT EXISTS telemetry.dim_allowlist_dict
(
    dimension String,
    value     String
)
PRIMARY KEY dimension, value
SOURCE(CLICKHOUSE(DB 'telemetry' TABLE 'dim_allowlist'))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 60 MAX 120);

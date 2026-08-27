-- Database bootstrap.
--
-- Migrations are applied in filename order by `telemetry-engine migrate`, and
-- each file must be idempotent: the runner records what it applied, but a
-- half-applied file has to be safe to re-run.

CREATE DATABASE IF NOT EXISTS telemetry;

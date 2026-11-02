-- Minimal logical model for the Databricks analytics adapter, not a ClickHouse schema replica.
-- UTC timestamps; team_id is required on every read. No source events are copied automatically.
CREATE SCHEMA IF NOT EXISTS workspace.posthog;
CREATE TABLE IF NOT EXISTS workspace.posthog.events (
    uuid STRING NOT NULL, team_id BIGINT NOT NULL, event STRING NOT NULL,
    timestamp TIMESTAMP NOT NULL, distinct_id STRING NOT NULL,
    properties STRING COMMENT 'JSON object', person_id STRING,
    person_properties STRING COMMENT 'JSON object',
    created_at TIMESTAMP, ingested_at TIMESTAMP
) USING DELTA CLUSTER BY (team_id, timestamp, event);
CREATE TABLE IF NOT EXISTS workspace.posthog.persons (
    team_id BIGINT NOT NULL, id STRING NOT NULL, properties STRING COMMENT 'JSON object',
    is_identified BOOLEAN, created_at TIMESTAMP, version BIGINT, is_deleted BOOLEAN
) USING DELTA CLUSTER BY (team_id, id);
CREATE TABLE IF NOT EXISTS workspace.posthog.person_distinct_ids (
    team_id BIGINT NOT NULL, distinct_id STRING NOT NULL, person_id STRING NOT NULL,
    version BIGINT, is_deleted BOOLEAN
) USING DELTA CLUSTER BY (team_id, distinct_id);
CREATE TABLE IF NOT EXISTS workspace.posthog.groups (
    team_id BIGINT NOT NULL, group_type_index INT NOT NULL, group_key STRING NOT NULL,
    group_properties STRING COMMENT 'JSON object', created_at TIMESTAMP, version BIGINT
) USING DELTA CLUSTER BY (team_id, group_type_index, group_key);
CREATE TABLE IF NOT EXISTS workspace.posthog.cohort_members (
    team_id BIGINT NOT NULL, cohort_id BIGINT NOT NULL, person_id STRING NOT NULL,
    updated_at TIMESTAMP
) USING DELTA CLUSTER BY (team_id, cohort_id, person_id);

-- Minimal logical model for the Databricks analytics adapter, not a ClickHouse schema replica.
-- UTC timestamps; team_id is required on every read. No source events are copied automatically.
CREATE SCHEMA IF NOT EXISTS workspace.posthog;
CREATE TABLE IF NOT EXISTS workspace.posthog.events (
    uuid STRING NOT NULL, team_id BIGINT NOT NULL, event STRING NOT NULL,
    timestamp TIMESTAMP NOT NULL, distinct_id STRING NOT NULL,
    properties STRING COMMENT 'JSON object', person_id STRING,
    person_properties STRING COMMENT 'JSON object',
    created_at TIMESTAMP, ingested_at TIMESTAMP,
    elements_chain STRING,
    person_created_at TIMESTAMP,
    group0_properties STRING,
    group1_properties STRING,
    group2_properties STRING,
    group3_properties STRING,
    group4_properties STRING,
    group0_created_at TIMESTAMP,
    group1_created_at TIMESTAMP,
    group2_created_at TIMESTAMP,
    group3_created_at TIMESTAMP,
    group4_created_at TIMESTAMP,
    person_mode STRING,
    historical_migration BOOLEAN,
    temporary_properties STRING,
    properties_null_keys ARRAY<STRING>,
    temporary_properties_null_keys ARRAY<STRING>,
    person_properties_null_keys ARRAY<STRING>,
    inserted_at TIMESTAMP,
    captured_at TIMESTAMP,
    consumer_breadcrumbs ARRAY<STRING>,
    total_event_size BIGINT,
    _timestamp TIMESTAMP,
    _offset DECIMAL(20,0),
    _partition DECIMAL(20,0)
) USING DELTA CLUSTER BY (team_id, timestamp, event);
CREATE TABLE IF NOT EXISTS workspace.posthog.persons (
    team_id BIGINT NOT NULL, id STRING NOT NULL, properties STRING COMMENT 'JSON object',
    is_identified BOOLEAN, created_at TIMESTAMP, version BIGINT, is_deleted BOOLEAN, last_seen_at TIMESTAMP
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

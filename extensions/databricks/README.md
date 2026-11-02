# Databricks product analytics

This fork routes six insight types for explicitly enabled teams to a Databricks SQL Warehouse. Other teams retain the existing execution path. PostgreSQL, Redis, ClickHouse and the other PostHog services are still required. Unsupported options fail instead of silently querying ClickHouse.

## Supported analyses

| Insight | Implemented calculation | Constraints |
| --- | --- | --- |
| Trends | Total events, unique users, unique sessions, numeric property sum/average/min/max/median/percentiles; cumulative lines and aggregate numbers | No formulas, smoothing, breakdowns, heatmaps, or period comparison |
| Funnels | Ordered event steps, distinct converting users, configurable conversion window, average/median step conversion time and total median | 2 to 10 event steps; no strict/unordered mode, optional steps, exclusions, alternate visualizations, or breakdowns |
| Retention | Recurring and first occurrence matching filters, calendar hour/day/week/month cohort matrix | Count aggregation; no rolling 24-hour windows, custom brackets, first-ever occurrence, or cumulative retention |
| Paths | Pageview URLs, screens and custom events; 30-minute gap sessions; positional transition frequencies and average transition time | No anchors, path cleaning, custom expressions, or funnel-linked paths; this is PathsQuery, not the experimental PathsV2Query |
| Stickiness | Distinct active periods per user, frequency distribution and cumulative distribution | One-period buckets; no custom stickiness criteria or period comparison |
| Lifecycle | New, returning, resurrecting and negative dormant counts | One event series with populated person_created_at; personless events are excluded |

Event and person property filters support nested AND/OR, exact/not-exact, set/not-set, case-insensitive contains, regex and numeric comparisons. Query values and JSON paths are bound parameters. Actions, group/warehouse series, cohort filters, sampling and test-account filtering are not supported. User drilldowns fail explicitly on enabled teams. These are aggregate implementations, not full PostHog feature parity.

All six support calendar hour/day/week/month windows up to 366 elapsed days, zero-filled output where applicable, and the project's timezone and Sunday/Monday week boundary. The events table is read directly; the other model tables are not queried by these adapters. Runtime queries do not write or copy events. Each statement has a 50-second synchronous wait with cancellation and a 50,000-row result ceiling. Multi-chunk results are fetched fully; failed or truncated results are rejected. Backend/source-specific cache keys separate Databricks results from ClickHouse results.

## Setup

Apply `schema.sql` for a new deployment. For existing prototype tables, apply `clustering.sql` and then `003_event_model.sql` once. The latter adds the missing event/person model columns without replacing tables. It is a one-time additive migration; rerunning it attempts to add existing columns.

Configure the web and query worker processes:

```sh
DATABRICKS_ANALYTICS_TEAM_IDS='[1]'
DATABRICKS_ANALYTICS_WAREHOUSE_ID='<warehouse-id>'
DATABRICKS_ANALYTICS_EVENTS_TABLE='workspace.posthog.events'
DATABRICKS_HOST='https://<workspace-host>'
```

Use Databricks SDK unified authentication for credentials. For local development, configure a CLI profile and set `DATABRICKS_CONFIG_PROFILE`; for deployment, supply a service principal through the secret store. Never commit credentials. Grant the querying identity SELECT on the events table and warehouse access. Keep schema creation credentials separate from runtime credentials.

Use a real PostHog team ID in the allowlist and in each event row. The team predicate comes from the authenticated runner's team, not from request SQL. Every query binds its team, time range and timezone. Table identifiers come only from administrator configuration.

To analyze an existing table without copying events, set the events table setting to a compatible three-part Unity Catalog table or view. Expose the schema in `schema.sql`, including uuid, person_id, distinct_id, properties, person_properties and person_created_at. Timestamps represent UTC instants and JSON properties use serialized JSON strings. Identity-based analyses use person_id, falling back to distinct_id when it is absent. Populate canonical merged person IDs and person_created_at upstream. Deduplicate event IDs upstream. The schema does not implement ingestion, ReplacingMergeTree replacement semantics, user identity merging, cohort calculation or deletion propagation.

## ClickHouse schema and physical layout

The reference is PostHog commit `07507c09ac6a222e65616ff48256b1be82677b7d`, particularly `posthog/models/event/sql.py`, `posthog/models/person/sql.py`, and `posthog/models/group/sql.py`. The Delta events schema now includes the declared base event/person/group fields and native-JSON ingestion metadata columns. It is not an exact ClickHouse table replica: UUIDs and enums are strings, JSON is serialized text, unsigned integer fields use compatible signed/decimal types, and ClickHouse materialized columns, codecs, skip indexes, TTLs, replicas and Kafka tables are not reproduced. The persons and cohort table names/layouts are adapter model tables rather than identical ClickHouse names.

| Table | Reference ClickHouse order | Delta liquid clustering |
| --- | --- | --- |
| Legacy events | team_id, toDate(timestamp), event, cityHash64(distinct_id), cityHash64(uuid) | team_id, timestamp, event |
| JSON events | team_id, toDate(timestamp), event, cityHash64(distinct_id), timestamp, uuid | team_id, timestamp, event |
| Persons | team_id, id | team_id, id |
| Person distinct IDs | team_id, distinct_id | team_id, distinct_id |
| Groups | team_id, group_type_index, group_key | team_id, group_type_index, group_key |
| Static cohorts | team_id, cohort_id, person_id, id | team_id, cohort_id, person_id |

ClickHouse's ordered sparse primary index and Delta liquid clustering have different semantics. Liquid clustering does not preserve the ClickHouse lexicographic key, monthly partitions, SAMPLE BY or replacement identity. Copying the column list does not recreate that behavior. The event keys retain the tenant/time/event predicates; user hash and UUID suffixes are not copied. These are initial choices pending measurements with representative data. Key order does not define a leading-key hierarchy as it does in ClickHouse.

After loading data, run `OPTIMIZE workspace.posthog.events` or use predictive optimization. Changing keys does not rewrite existing files immediately. Avoid combining liquid clustering with partitions or ZORDER. For an existing source exposed through a view, optimize the underlying source table with its owner. Compare scanned bytes and latency before adding a user key or removing a constant team key.

Reference: [Databricks liquid clustering](https://docs.databricks.com/aws/en/tables/clustering).

## Validation status

The isolated adapter suite covers all six response formats, empty buckets, retention dates, cumulative counts, property binding, tenant isolation, multi-chunk transport, failed/truncated responses, unsupported requests, drilldown rejection and cache separation. The suite runs without Postgres or ClickHouse because the external query boundary is mocked.

Manual SQL Warehouse verification uses the read-only synthetic event stream in `smoke_fixture.sql`, including another tenant. Expected results include a signup-to-buy funnel with 2 users and 88,500-second median conversion, retention counts `[2, 1, 1]`, a signup-to-buy path with 600,000-millisecond transition time, stickiness `[1, 2, 0]`, and lifecycle new/returning/resurrecting/dormant vectors `[1,0,0]`, `[0,1,0]`, `[1,0,2]`, `[0,-1,-1]`. Unique-user, numeric property and property-filter queries were also exercised. Empty managed-table queries exercise the deployed schema without populating it.

Full PostHog API/browser verification and comparison against the original ClickHouse runners remain required before deployment. The local adapter checks bypass the unrelated Celery package bootstrap; they do not prove the full Django application starts. No throughput or real-data equivalence claim is made from the small fixture.

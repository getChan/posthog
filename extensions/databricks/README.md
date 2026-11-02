# Databricks Trends prototype

This fork routes daily total event count Trends queries for explicitly enabled teams to a Databricks SQL Warehouse. Other teams retain the existing execution path. PostgreSQL, Redis, ClickHouse and the other PostHog services are still required. Unsupported Trends options fail instead of silently querying ClickHouse.

Apply `schema.sql` with the Databricks Statement Execution API or SQL editor. The five managed Delta tables form a minimal logical model, not a complete replica of PostHog's ClickHouse schema. Only `events` is queried by this prototype. These tables do not implement ingestion, user identity merging, cohort calculation or deletion propagation. JSON properties are stored as strings.

Configure the web and query worker processes:

```sh
DATABRICKS_ANALYTICS_TEAM_IDS='[1]'
DATABRICKS_ANALYTICS_WAREHOUSE_ID='<warehouse-id>'
DATABRICKS_ANALYTICS_EVENTS_TABLE='workspace.posthog.events'
DATABRICKS_HOST='https://<workspace-host>'
```

Use Databricks SDK unified authentication for credentials. For local development, configure a CLI profile and set `DATABRICKS_CONFIG_PROFILE`; for deployment, supply a service principal through the secret store. Never commit credentials. Grant the querying identity SELECT on the events table and warehouse access. Keep schema creation credentials separate from runtime credentials.

Use a real PostHog team ID in the allowlist and in each event row. Every query binds team_id, event name, time range and timezone. Table identifiers come only from administrator configuration. Source timestamps represent UTC instants. Event IDs must be deduplicated upstream. The adapter supports daily total counts for up to 366 elapsed days, fills empty days, and cancels statements that exceed a 50-second wait. Filters, comparisons, formulas and alternate displays are unsupported.

To analyze an existing table without copying events, configure the events table setting to a compatible three-part Unity Catalog table or view. That table must expose at least team_id, event and timestamp. Do not populate the newly created events table merely to use the adapter.

Validation: adapter unit tests cover bound tenant/event/timezone parameters, missing days, and rejection of unsupported filters. Live SQL execution has been checked against an empty managed events table. Full PostHog API and browser verification remains required before deployment. Flush existing Trends caches when changing the backend configuration.

## Table layout

New tables use liquid clustering. Run `clustering.sql` once to enable the same keys on tables created with an earlier schema. The events keys are `(team_id, timestamp, event)`, matching the adapter predicates. Identity tables use team and lookup identifiers; cohort membership uses team, cohort and person. These are initial choices, pending measurements with representative data.

After loading data, run `OPTIMIZE workspace.posthog.events` or use predictive optimization. Changing keys does not rewrite existing files immediately. Avoid combining liquid clustering with partitions or ZORDER. When analyzing an existing source through a view, optimize the underlying source table separately with its owner. A single-team deployment may benefit from fewer keys; compare scanned bytes and latency before retaining or removing team_id. Revisit person identifiers as event keys when funnels and retention queries are implemented.

Reference: [Databricks liquid clustering](https://docs.databricks.com/aws/en/tables/clustering).

from datetime import UTC, datetime

from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from databricks.sdk.service.sql import ResultData, StatementResponse

from posthog.schema import (
    EventsNode,
    FunnelsQuery,
    LifecycleQuery,
    PathsFilter,
    PathsQuery,
    RetentionEntity,
    RetentionFilter,
    RetentionQuery,
    StickinessFilter,
    StickinessQuery,
    TrendsFilter,
    TrendsQuery,
)

from posthog.hogql.errors import QueryError

from products.product_analytics.backend.hogql_queries.databricks_insights import DatabricksInsights
from products.product_analytics.backend.hogql_queries.databricks_sql import AnalyticsSQL, AnalyticsWindow


@override_settings(
    DATABRICKS_ANALYTICS_EVENTS_TABLE="workspace.posthog.events",
    DATABRICKS_ANALYTICS_WAREHOUSE_ID="test",
    DATABRICKS_ANALYTICS_TEAM_IDS={7},
)
class TestDatabricksInsights(SimpleTestCase):
    def window(self, **overrides):
        return AnalyticsWindow(
            **{
                "team_id": 7,
                "start": datetime(2026, 1, 1, tzinfo=UTC),
                "end": datetime(2026, 1, 3, 23, 59, 59, tzinfo=UTC),
                "timezone": "UTC",
                **overrides,
            }
        )

    def calculate(self, query, rows):
        with patch.object(AnalyticsSQL, "execute", return_value=rows):
            return DatabricksInsights(query, self.window()).calculate()

    def test_trends_default_options_and_empty_days(self):
        query = TrendsQuery(series=[EventsNode(event="signup")], trendsFilter=TrendsFilter())
        response = self.calculate(query, [["2026-01-02 00:00:00", "3"]])
        self.assertEqual(response.results[0]["data"], [0, 3, 0])
        self.assertEqual(response.results[0]["count"], 3)
        self.assertFalse(response.hasMore)

    def test_cumulative_trends_count_is_the_final_total(self):
        query = TrendsQuery(
            series=[EventsNode(event="buy")], trendsFilter=TrendsFilter(display="ActionsLineGraphCumulative")
        )
        response = self.calculate(query, [["2026-01-01 00:00:00", "2"], ["2026-01-02 00:00:00", "3"]])
        self.assertEqual(response.results[0]["data"], [2, 5, 5])
        self.assertEqual(response.results[0]["count"], 5)

    def test_funnel_step_counts_and_conversion_seconds(self):
        query = FunnelsQuery(series=[EventsNode(event="signup"), EventsNode(event="buy")])
        response = self.calculate(query, [["4", "2", "60", "45", "45"]])
        self.assertEqual([step["count"] for step in response.results], [4, 2])
        self.assertEqual(response.results[1]["average_conversion_time"], 60)
        self.assertEqual(response.total_median_conversion_time, 45)

    def test_retention_cell_mapping_and_timezone_aware_dates(self):
        query = RetentionQuery(
            retentionFilter=RetentionFilter(totalIntervals=3, targetEntity=RetentionEntity(id="signup", type="events"))
        )
        response = self.calculate(query, [["2026-01-01 00:00:00", "0", "4"], ["2026-01-01 00:00:00", "2", "2"]])
        self.assertEqual([value.count for value in response.results[0].values], [4, 0, 2])
        self.assertEqual(response.results[0].date.utcoffset().total_seconds(), 0)

    def test_stickiness_cumulative_counts_users_once(self):
        query = StickinessQuery(
            series=[EventsNode(event="buy")], stickinessFilter=StickinessFilter(computedAs="cumulative")
        )
        response = self.calculate(query, [["1", "2"], ["3", "1"]])
        self.assertEqual(response.results[0]["data"], [3, 1, 1])
        self.assertEqual(response.results[0]["count"], 3)

    def test_lifecycle_fills_all_statuses_and_preserves_negative_dormancy(self):
        query = LifecycleQuery(series=[EventsNode(event="buy")])
        response = self.calculate(query, [["2026-01-02 00:00:00", "dormant", "-2"]])
        self.assertEqual([item["status"] for item in response.results], ["new", "returning", "resurrecting", "dormant"])
        self.assertEqual(response.results[-1]["data"], [0, -2, 0])
        self.assertEqual(response.results[0]["data"], [0, 0, 0])

    def test_paths_preserves_transition_frequency_and_milliseconds(self):
        query = PathsQuery(pathsFilter=PathsFilter(includeEventTypes=["custom_event"]))
        response = self.calculate(query, [["1_signup", "2_buy", "5", "60000"]])
        self.assertEqual(response.results[0].value, 5)
        self.assertEqual(response.results[0].average_conversion_time, 60000)

    def test_unsupported_options_never_reach_warehouse(self):
        for query in [
            TrendsQuery(series=[EventsNode()], filterTestAccounts=True),
            TrendsQuery(series=[EventsNode()], dateRange={"date_from": "all"}),
            TrendsQuery(series=[EventsNode()], trendsFilter={"formula": "A / 2"}),
            FunnelsQuery(series=[EventsNode(), EventsNode()], funnelsFilter={"funnelOrderType": "unordered"}),
            RetentionQuery(retentionFilter={"timeWindowMode": "24_hour_windows"}),
            PathsQuery(pathsFilter={"startPoint": "signup"}),
        ]:
            with self.subTest(kind=query.kind), patch.object(AnalyticsSQL, "execute") as execute:
                with self.assertRaises(QueryError):
                    DatabricksInsights(query, self.window()).calculate()
                execute.assert_not_called()

    def test_month_and_sunday_week_axes(self):
        self.assertEqual(self.window(interval="month").axis(), [datetime(2026, 1, 1, tzinfo=UTC)])
        self.assertEqual(self.window(interval="week", week_start_day=0).axis(), [datetime(2025, 12, 28, tzinfo=UTC)])

    def test_property_values_are_bound_and_nested_boolean_filters_preserved(self):
        sql = AnalyticsSQL(self.window())
        malicious = "x' OR 1=1 --"
        clause = sql.filters(
            {
                "type": "OR",
                "values": [
                    {"type": "event", "key": "$current_url", "value": malicious, "operator": "exact"},
                    {"type": "person", "key": "plan", "operator": "is_not_set"},
                ],
            }
        )
        self.assertNotIn(malicious, clause)
        self.assertIn(" OR ", clause)
        self.assertIn(malicious, [parameter.value for parameter in sql.parameters])
        self.assertIn("person_properties", clause)

    @patch("products.product_analytics.backend.hogql_queries.databricks_sql.WorkspaceClient")
    def test_transport_binds_tenant_and_reads_multiple_chunks(self, client):
        api = client.return_value.statement_execution
        api.execute_statement.return_value = StatementResponse.from_dict(
            {
                "statement_id": "query",
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["a"]], "next_chunk_index": 1},
            }
        )
        api.get_statement_result_chunk_n.return_value = ResultData.from_dict({"data_array": [["b"]]})
        sql = AnalyticsSQL(self.window())
        self.assertEqual(sql.execute(sql.source()), [["a"], ["b"]])
        call = api.execute_statement.call_args.kwargs
        self.assertIn("team_id = :p0", call["statement"])
        self.assertEqual(call["parameters"][0].value, "7")
        self.assertEqual(call["row_limit"], 50000)
        api.get_statement_result_chunk_n.assert_called_once_with("query", 1)

    @patch("products.product_analytics.backend.hogql_queries.databricks_sql.WorkspaceClient")
    def test_failed_and_truncated_queries_never_return_partial_counts(self, client):
        api = client.return_value.statement_execution
        for payload in [
            {"status": {"state": "FAILED"}},
            {"status": {"state": "SUCCEEDED"}, "manifest": {"truncated": True}},
        ]:
            api.execute_statement.return_value = StatementResponse.from_dict(payload)
            with self.assertRaises(QueryError):
                AnalyticsSQL(self.window()).execute("SELECT 1")

    def test_drilldowns_do_not_fall_back_to_clickhouse(self):
        with self.assertRaises(QueryError):
            AnalyticsSQL.require_actor_backend(7)
        AnalyticsSQL.require_actor_backend(8)

    def test_cache_separates_source_and_backend(self):
        original = AnalyticsSQL.cache_variant(7)
        self.assertEqual(AnalyticsSQL.cache_variant(8), "")
        with override_settings(DATABRICKS_ANALYTICS_EVENTS_TABLE="workspace.other.events"):
            self.assertNotEqual(original, AnalyticsSQL.cache_variant(7))

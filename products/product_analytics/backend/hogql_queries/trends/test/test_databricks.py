from datetime import UTC, datetime
from types import SimpleNamespace

from unittest.mock import patch

from django.test import SimpleTestCase, override_settings

from databricks.sdk.service.sql import StatementResponse

from posthog.schema import TrendsQuery

from posthog.hogql.errors import QueryError

from products.product_analytics.backend.hogql_queries.trends.databricks import DatabricksTrends


@override_settings(
    DATABRICKS_ANALYTICS_EVENTS_TABLE="workspace.posthog.events", DATABRICKS_ANALYTICS_WAREHOUSE_ID="test"
)
class TestDatabricksTrends(SimpleTestCase):
    def runner(self):
        return SimpleNamespace(
            query=TrendsQuery(series=[{"kind": "EventsNode", "event": "signup"}]),
            team=SimpleNamespace(pk=7, timezone="Asia/Seoul"),
            query_date_range=SimpleNamespace(
                date_from=lambda: datetime(2026, 1, 1, tzinfo=UTC),
                date_to=lambda: datetime(2026, 1, 3, tzinfo=UTC),
            ),
            series=[None],
            build_series_response=lambda response, *_: response.results,
        )

    @patch("products.product_analytics.backend.hogql_queries.trends.databricks.WorkspaceClient")
    def test_daily_counts_bind_tenant_and_fill_missing_dates(self, client):
        execute = client.return_value.statement_execution.execute_statement
        execute.return_value = StatementResponse.from_dict(
            {
                "status": {"state": "SUCCEEDED"},
                "result": {"data_array": [["2026-01-02", "3"]]},
            }
        )
        results, more = DatabricksTrends.calculate(self.runner())
        self.assertEqual(results[0][1], [0, 3, 0])
        self.assertFalse(more)
        parameters = {p.name: p.value for p in execute.call_args.kwargs["parameters"]}
        self.assertEqual(parameters["team"], "7")
        self.assertEqual(parameters["event"], "signup")
        self.assertEqual(parameters["tz"], "Asia/Seoul")

    @patch("products.product_analytics.backend.hogql_queries.trends.databricks.WorkspaceClient")
    def test_rejects_filters_before_querying(self, client):
        runner = self.runner()
        runner.query.filterTestAccounts = True
        with self.assertRaises(QueryError):
            DatabricksTrends.calculate(runner)
        client.assert_not_called()

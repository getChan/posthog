import re
from datetime import datetime, timedelta
from typing import TYPE_CHECKING

from django.conf import settings

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementParameterListItem

from posthog.schema import EventsNode, HogQLQueryResponse

from posthog.hogql.errors import QueryError

if TYPE_CHECKING:
    from products.product_analytics.backend.hogql_queries.trends.trends_query_runner import TrendsQueryRunner


class DatabricksTrends:
    @staticmethod
    def enabled(team_id: int) -> bool:
        return team_id in settings.DATABRICKS_ANALYTICS_TEAM_IDS

    @staticmethod
    def calculate(runner: "TrendsQueryRunner") -> tuple[list[dict], bool]:
        query = runner.query
        unsupported = (
            query.interval not in (None, "day")
            or query.properties
            or query.filterTestAccounts
            or query.breakdownFilter
            or query.compareFilter
            or query.samplingFactor
            or query.aggregation_group_type_index is not None
            or query.marketingAnalyticsCostFallback
            or query.calendarHeatmapFilter
            or query.conversionGoal
            or (query.dateRange and query.dateRange.daysOfWeek)
            or query.modifiers
            or (query.trendsFilter and query.trendsFilter.model_dump(exclude_none=True, exclude_defaults=True))
        )
        if unsupported:
            raise QueryError(
                "Databricks supports daily event counts only. Remove filters, comparisons, and display options."
            )
        for series in query.series:
            if (
                not isinstance(series, EventsNode)
                or series.properties
                or series.fixedProperties
                or series.math_multiplier is not None
                or series.math not in (None, "total")
            ):
                raise QueryError(
                    "Databricks supports event series with total counts only. Select an event and total count."
                )
        table = settings.DATABRICKS_ANALYTICS_EVENTS_TABLE
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", table):
            raise QueryError("Configure a three-part Databricks events table name.")
        dates = runner.query_date_range
        if (dates.date_to() - dates.date_from()).days > 366:
            raise QueryError("Databricks daily counts support up to 366 days. Select a shorter date range.")
        client = WorkspaceClient()
        results: list[dict] = []
        for index, series in enumerate(query.series):
            statement = f"""
                SELECT CAST(from_utc_timestamp(timestamp, :tz) AS DATE) AS day, count(*) AS total
                FROM {".".join("`" + part + "`" for part in table.split("."))}
                WHERE team_id = :team AND timestamp >= :start AND timestamp <= :end
                    AND (:event IS NULL OR event = :event)
                GROUP BY 1 ORDER BY 1
            """
            parameters = [
                StatementParameterListItem(name="team", value=str(runner.team.pk), type="BIGINT"),
                StatementParameterListItem(name="start", value=dates.date_from().isoformat(), type="TIMESTAMP"),
                StatementParameterListItem(name="end", value=dates.date_to().isoformat(), type="TIMESTAMP"),
                StatementParameterListItem(name="tz", value=runner.team.timezone),
                StatementParameterListItem(name="event", value=series.event),
            ]
            response = client.statement_execution.execute_statement(
                warehouse_id=settings.DATABRICKS_ANALYTICS_WAREHOUSE_ID,
                statement=statement,
                parameters=parameters,
                wait_timeout="50s",
                on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
            )
            if response.status is None or response.status.state is None or response.status.state.value != "SUCCEEDED":
                raise QueryError("Databricks could not complete the query. Check the warehouse and retry.")
            if response.manifest and response.manifest.truncated:
                raise QueryError("Databricks returned too many results. Select a shorter date range.")
            counts = {row[0]: int(row[1]) for row in (response.result.data_array if response.result else []) or []}
            day = dates.date_from().date()
            days = []
            values = []
            while day <= dates.date_to().date():
                days.append(datetime.combine(day, datetime.min.time()))
                values.append(counts.get(day.isoformat(), 0))
                day += timedelta(days=1)
            result = HogQLQueryResponse(results=[[days, values]], columns=["date", "total"], types=[])
            results.extend(runner.build_series_response(result, runner.series[index], len(query.series)))
        return results, False

import re
import json
import hashlib
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from django.conf import settings

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import ExecuteStatementRequestOnWaitTimeout, StatementParameterListItem

from posthog.schema import EventsNode

from posthog.hogql.errors import QueryError

from posthog.dataclasses import frozen


@frozen
class AnalyticsWindow:
    team_id: int
    start: datetime
    end: datetime
    timezone: str
    interval: str = "day"
    week_start_day: int = 1

    def __post_init__(self) -> None:
        if self.start > self.end or (self.end - self.start).days > 366:
            raise QueryError("Select an ordered date range of up to 366 days for Databricks analytics.")
        if self.interval not in {"hour", "day", "week", "month"}:
            raise QueryError("Select hour, day, week, or month for Databricks analytics.")
        ZoneInfo(self.timezone)

    def align(self, value: datetime) -> datetime:
        value = value.astimezone(ZoneInfo(self.timezone))
        value = value.replace(minute=0, second=0, microsecond=0)
        if self.interval != "hour":
            value = value.replace(hour=0)
        if self.interval == "week":
            weekday = 6 if self.week_start_day == 0 else 0
            value -= timedelta(days=(value.weekday() - weekday) % 7)
        if self.interval == "month":
            value = value.replace(day=1)
        return value

    def advance(self, value: datetime, count: int = 1) -> datetime:
        if self.interval == "month":
            month = value.year * 12 + value.month - 1 + count
            return value.replace(year=month // 12, month=month % 12 + 1)
        return value + timedelta(**{{"hour": "hours", "day": "days", "week": "weeks"}[self.interval]: count})

    def axis(self) -> list[datetime]:
        values = []
        value = self.align(self.start)
        while value <= self.end.astimezone(ZoneInfo(self.timezone)):
            values.append(value)
            value = self.advance(value)
        if len(values) > 9000:
            raise QueryError("Select a shorter date range for Databricks analytics.")
        return values


class AnalyticsSQL:
    def __init__(self, window: AnalyticsWindow) -> None:
        self.window = window
        self.parameters: list[StatementParameterListItem] = []
        self.team = self.bind(window.team_id, "BIGINT")
        self.start = self.bind(window.start.isoformat(), "TIMESTAMP")
        self.end = self.bind(window.end.isoformat(), "TIMESTAMP")
        self.tz = self.bind(window.timezone)

    @staticmethod
    def enabled(team_id: int) -> bool:
        return team_id in settings.DATABRICKS_ANALYTICS_TEAM_IDS

    @classmethod
    def require_actor_backend(cls, team_id: int) -> None:
        if cls.enabled(team_id):
            raise QueryError("User drilldowns are not available for Databricks insights yet. Use the aggregate chart.")

    @classmethod
    def cache_variant(cls, team_id: int) -> str:
        if not cls.enabled(team_id):
            return ""
        identity = f"v2:{settings.DATABRICKS_ANALYTICS_WAREHOUSE_ID}:{settings.DATABRICKS_ANALYTICS_EVENTS_TABLE}"
        return ":databricks:" + hashlib.sha256(identity.encode()).hexdigest()[:16]

    @staticmethod
    def table() -> str:
        table = settings.DATABRICKS_ANALYTICS_EVENTS_TABLE
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", table):
            raise QueryError("Configure a three-part Databricks events table name.")
        return ".".join(f"`{part}`" for part in table.split("."))

    def bind(self, value: object, sql_type: str = "STRING") -> str:
        name = f"p{len(self.parameters)}"
        self.parameters.append(
            StatementParameterListItem(name=name, value=None if value is None else str(value), type=sql_type)
        )
        return f":{name}"

    def bucket(self, expression: str = "timestamp") -> str:
        local = f"from_utc_timestamp({expression}, {self.tz})"
        if self.window.interval == "week" and self.window.week_start_day == 0:
            return f"date_trunc('week', {local} + INTERVAL 1 DAY) - INTERVAL 1 DAY"
        return f"date_trunc('{self.window.interval}', {local})"

    @staticmethod
    def actor() -> str:
        return "coalesce(nullif(person_id, ''), nullif(distinct_id, ''))"

    def property(self, key: str, kind: str = "event") -> str:
        if kind not in {"event", "person"}:
            raise QueryError(
                "Databricks supports event and person property filters. Select one of these property types."
            )
        column = "properties" if kind == "event" else "person_properties"
        path = "$['" + key.replace("\\", "\\\\").replace("'", "\\'") + "']"
        return f"get_json_object({column}, {self.bind(path)})"

    def filters(self, filters: object) -> str:
        if not filters:
            return "true"
        if isinstance(filters, list):
            return "(" + " AND ".join(self.filters(item) for item in filters) + ")"
        item = filters.model_dump(exclude_none=True) if hasattr(filters, "model_dump") else filters
        if not isinstance(item, dict):
            raise QueryError("Unsupported Databricks property filter. Select an event or person property filter.")
        if "values" in item:
            join = item.get("type", "AND")
            if join not in {"AND", "OR"}:
                raise QueryError("Use AND or OR to combine Databricks property filters.")
            values = item["values"]
            return "(" + f" {join} ".join(self.filters(value) for value in values) + ")" if values else "true"
        if not isinstance(item.get("key"), str):
            raise QueryError("Select a property name for the Databricks filter.")
        field = self.property(item["key"], item.get("type", "event"))
        operator = item.get("operator", "exact")
        if operator in {"is_set", "is_not_set"}:
            return f"{field} IS {'NOT ' if operator == 'is_set' else ''}NULL"
        value = item.get("value")
        values = value if isinstance(value, list) else [value]
        if operator in {"exact", "is_not"}:
            positive = (
                " OR ".join(
                    f"{field} IS NULL"
                    if val is None
                    else f"{field} = {self.bind(json.dumps(val) if isinstance(val, bool) else val)}"
                    for val in values
                )
                or "false"
            )
            return f"{'NOT ' if operator == 'is_not' else ''}(coalesce(({positive}), false))"
        if len(values) != 1:
            raise QueryError("Use a single value for this Databricks property operator.")
        if operator in {"gt", "gte", "lt", "lte"}:
            op = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[operator]
            return f"try_cast({field} AS DOUBLE) {op} try_cast({self.bind(value)} AS DOUBLE)"
        if operator in {"icontains", "not_icontains"}:
            match = f"contains(lower({field}), lower({self.bind(value)}))"
            return f"{'NOT ' if operator == 'not_icontains' else ''}coalesce({match}, false)"
        if operator in {"regex", "not_regex"}:
            match = f"{field} RLIKE {self.bind(value)}"
            return f"{'NOT ' if operator == 'not_regex' else ''}coalesce({match}, false)"
        raise QueryError(
            "Unsupported Databricks property operator. Select exact, contains, regex, set, or numeric comparison."
        )

    def event(self, series: object) -> str:
        if not isinstance(series, EventsNode):
            raise QueryError(
                "Databricks analytics requires event series. Select events instead of actions or warehouse series."
            )
        if (
            series.optionalInFunnel
            or series.math_multiplier is not None
            or series.math_hogql
            or series.math_group_type_index is not None
        ):
            raise QueryError("Remove optional steps, multipliers, and HogQL expressions for Databricks analytics.")
        event = "true" if series.event is None else f"event = {self.bind(series.event)}"
        return f"({event} AND {self.filters(series.properties)} AND {self.filters(series.fixedProperties)})"

    def source(
        self,
        properties: object = None,
        *,
        history: bool = False,
        end: datetime | None = None,
        start: datetime | None = None,
    ) -> str:
        upper = self.bind(end.isoformat(), "TIMESTAMP") if end else self.end
        lower = self.bind(start.isoformat(), "TIMESTAMP") if start else self.start
        time = f"timestamp <= {upper}" if history else f"timestamp >= {lower} AND timestamp <= {upper}"
        return f"SELECT *, {self.actor()} AS actor_id FROM {self.table()} WHERE team_id = {self.team} AND {time} AND {self.filters(properties)}"

    def execute(self, statement: str) -> list[list[Any]]:
        client = WorkspaceClient()
        response = client.statement_execution.execute_statement(
            warehouse_id=settings.DATABRICKS_ANALYTICS_WAREHOUSE_ID,
            statement=statement,
            parameters=self.parameters,
            wait_timeout="50s",
            on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CANCEL,
            row_limit=50000,
        )
        if response.status is None or response.status.state is None or response.status.state.value != "SUCCEEDED":
            raise QueryError("Databricks could not complete the query. Check the warehouse and retry.")
        if response.manifest and response.manifest.truncated:
            raise QueryError("Databricks returned too many results. Select a shorter date range.")
        result = response.result
        rows = list(result.data_array or []) if result else []
        while result and result.next_chunk_index is not None:
            if not response.statement_id:
                raise QueryError("Databricks returned an incomplete result. Retry the query.")
            result = client.statement_execution.get_statement_result_chunk_n(
                response.statement_id, result.next_chunk_index
            )
            rows.extend(result.data_array or [])
            if len(rows) > 50000:
                raise QueryError("Databricks returned too many results. Select a shorter date range.")
        return rows

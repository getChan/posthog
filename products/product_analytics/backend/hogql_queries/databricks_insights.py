import json
from collections import Counter
from datetime import datetime
from typing import Any, Generic, TypeVar, overload
from zoneinfo import ZoneInfo

from posthog.schema import (
    EventsNode,
    FunnelsQuery,
    FunnelsQueryResponse,
    LifecycleQuery,
    LifecycleQueryResponse,
    PathsQuery,
    PathsQueryResponse,
    ResolvedDateRangeResponse,
    RetentionQuery,
    RetentionQueryResponse,
    StickinessQuery,
    StickinessQueryResponse,
    TrendsQuery,
    TrendsQueryResponse,
)

from posthog.hogql.errors import QueryError

from products.product_analytics.backend.hogql_queries.databricks_sql import AnalyticsSQL, AnalyticsWindow

InsightQuery = TrendsQuery | FunnelsQuery | RetentionQuery | PathsQuery | StickinessQuery | LifecycleQuery
InsightResponse = (
    TrendsQueryResponse
    | FunnelsQueryResponse
    | RetentionQueryResponse
    | PathsQueryResponse
    | StickinessQueryResponse
    | LifecycleQueryResponse
)


QueryT = TypeVar("QueryT", bound=InsightQuery)


class DatabricksInsights(Generic[QueryT]):
    def __init__(self, query: QueryT, window: AnalyticsWindow) -> None:
        self.query = query
        self.window = window
        self.sql = AnalyticsSQL(window)

    @staticmethod
    def validate_query(query: InsightQuery) -> None:
        if (
            query.filterTestAccounts
            or query.samplingFactor
            or getattr(query, "aggregation_group_type_index", None) is not None
        ):
            raise QueryError("Remove test-account filtering, sampling, and group aggregation for Databricks analytics.")
        if query.modifiers and query.modifiers.model_dump(exclude_none=True, exclude_defaults=True):
            raise QueryError("Remove custom query modifiers for Databricks analytics.")
        if getattr(query, "customAggregationTarget", None):
            raise QueryError("Use person aggregation for Databricks analytics.")
        if query.dateRange and query.dateRange.date_from == "all":
            raise QueryError("Select an explicit date range of up to 366 days for Databricks analytics.")
        if getattr(query, "breakdownFilter", None):
            raise QueryError("Remove breakdowns for this Databricks analysis.")
        if getattr(query, "compareFilter", None) and query.compareFilter.compare:
            raise QueryError("Remove period comparison for this Databricks analysis.")
        if query.dateRange and query.dateRange.daysOfWeek:
            raise QueryError("Remove day-of-week filtering for Databricks analytics.")

    @overload
    def calculate(self: "DatabricksInsights[TrendsQuery]") -> TrendsQueryResponse: ...

    @overload
    def calculate(self: "DatabricksInsights[FunnelsQuery]") -> FunnelsQueryResponse: ...

    @overload
    def calculate(self: "DatabricksInsights[RetentionQuery]") -> RetentionQueryResponse: ...

    @overload
    def calculate(self: "DatabricksInsights[PathsQuery]") -> PathsQueryResponse: ...

    @overload
    def calculate(self: "DatabricksInsights[StickinessQuery]") -> StickinessQueryResponse: ...

    @overload
    def calculate(self: "DatabricksInsights[LifecycleQuery]") -> LifecycleQueryResponse: ...

    def calculate(self) -> InsightResponse:
        self.validate_query(self.query)
        match self.query:
            case TrendsQuery():
                return self.trends(self.query)
            case FunnelsQuery():
                return self.funnels(self.query)
            case RetentionQuery():
                return self.retention(self.query)
            case PathsQuery():
                return self.paths(self.query)
            case StickinessQuery():
                return self.stickiness(self.query)
            case LifecycleQuery():
                return self.lifecycle(self.query)
        raise QueryError("Select a supported Databricks insight type.")

    def resolved_range(self) -> ResolvedDateRangeResponse:
        return ResolvedDateRangeResponse(date_from=self.window.start, date_to=self.window.end)

    def chart(self, series: EventsNode, values: list[int | float], *, order: int = 0) -> dict[str, Any]:
        axis = self.window.axis()
        return {
            "label": series.custom_name or series.event or "All events",
            "data": values,
            "count": sum(values),
            "days": [d.strftime("%Y-%m-%d %H:%M:%S" if self.window.interval == "hour" else "%Y-%m-%d") for d in axis],
            "labels": [d.strftime("%d %b %Y %H:%M" if self.window.interval == "hour" else "%d %b %Y") for d in axis],
            "order": order,
        }

    def row_date(self, value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=ZoneInfo(self.window.timezone))

    def trends(self, query: TrendsQuery) -> TrendsQueryResponse:
        if query.marketingAnalyticsCostFallback or query.calendarHeatmapFilter or query.conversionGoal:
            raise QueryError("Remove marketing, heatmap, and conversion-goal options for Databricks Trends.")
        options = query.trendsFilter
        if options and (
            options.formula or options.formulas or options.formulaNodes or (options.smoothingIntervals or 1) > 1
        ):
            raise QueryError("Remove formulas and smoothing for Databricks Trends.")
        display = options.display if options else None
        if display not in (
            None,
            "ActionsLineGraph",
            "ActionsLineGraphCumulative",
            "ActionsBar",
            "ActionsTable",
            "BoldNumber",
            "ActionsPie",
            "ActionsBarValue",
        ):
            raise QueryError("Select a line, bar, table, pie, or number display for Databricks Trends.")
        results = []
        for index, series in enumerate(query.series):
            predicate = self.sql.event(series)
            math = series.math or "total"
            if math == "total":
                aggregate = "count(*)"
            elif math == "dau":
                aggregate = "count(DISTINCT actor_id)"
            elif math == "unique_session":
                aggregate = f"count(DISTINCT {self.sql.property('$session_id')})"
            elif math in {"sum", "avg", "min", "max", "median", "p90", "p95", "p99"} and series.math_property:
                field = f"try_cast({self.sql.property(series.math_property, series.math_property_type or 'event')} AS DOUBLE)"
                if math in {"median", "p90", "p95", "p99"}:
                    fraction = {"median": 0.5, "p90": 0.9, "p95": 0.95, "p99": 0.99}[math]
                    aggregate = f"percentile_cont({fraction}) WITHIN GROUP (ORDER BY {field})"
                else:
                    aggregate = f"{math}({field})"
            else:
                raise QueryError(
                    "Select total, unique users, sessions, or a supported numeric property aggregation for Databricks Trends."
                )
            source = self.sql.source(query.properties)
            rows = self.sql.execute(
                f"WITH events AS ({source}) SELECT {self.sql.bucket()} AS period, {aggregate} AS total FROM events WHERE {predicate} GROUP BY 1 ORDER BY 1"
            )
            counts = {self.row_date(row[0]): float(row[1] or 0) for row in rows}
            values = [counts.get(day, 0) for day in self.window.axis()]
            if display == "ActionsLineGraphCumulative":
                values = [sum(values[: i + 1]) for i in range(len(values))]
            item = self.chart(series, values, order=index)
            if display == "ActionsLineGraphCumulative":
                item["count"] = values[-1] if values else 0
            if display in {"BoldNumber", "ActionsPie", "ActionsBarValue"}:
                total_rows = self.sql.execute(
                    f"WITH events AS ({source}) SELECT {aggregate} FROM events WHERE {predicate}"
                )
                item["aggregated_value"] = float(total_rows[0][0] or 0)
                item["data"] = []
                item["count"] = 0
            results.append(item)
        return TrendsQueryResponse(results=results, hasMore=False, resolved_date_range=self.resolved_range())

    def stickiness(self, query: StickinessQuery) -> StickinessQueryResponse:
        if query.intervalCount not in (None, 1) or (
            query.stickinessFilter and query.stickinessFilter.stickinessCriteria
        ):
            raise QueryError("Use one-period intervals without custom criteria for Databricks Stickiness.")
        periods = len(self.window.axis())
        results = []
        for index, series in enumerate(query.series):
            event = self.sql.event(series)
            source = self.sql.source(query.properties)
            rows = self.sql.execute(f"""
                WITH events AS ({source}), activity AS (
                    SELECT actor_id, count(DISTINCT {self.sql.bucket()}) AS frequency
                    FROM events WHERE actor_id IS NOT NULL AND {event} GROUP BY actor_id
                ) SELECT frequency, count(*) FROM activity GROUP BY frequency ORDER BY frequency
            """)
            counts = {int(row[0]): int(row[1]) for row in rows}
            data = [counts.get(i, 0) for i in range(1, periods + 1)]
            count = sum(data)
            cumulative = query.stickinessFilter and query.stickinessFilter.computedAs == "cumulative"
            if cumulative:
                data = [sum(data[i:]) for i in range(len(data))]
            results.append(
                {
                    "label": series.custom_name or series.event or "All events",
                    "order": index,
                    "data": data,
                    "count": count,
                    "days": list(range(1, periods + 1)),
                    "labels": [
                        f"{i} {self.window.interval}{'' if i == 1 else 's'}{' or more' if cumulative else ''}"
                        for i in range(1, periods + 1)
                    ],
                }
            )
        return StickinessQueryResponse(results=results, resolved_date_range=self.resolved_range())

    def funnels(self, query: FunnelsQuery) -> FunnelsQueryResponse:
        options = query.funnelsFilter
        if options and (
            options.funnelOrderType not in (None, "ordered")
            or options.funnelVizType not in (None, "steps")
            or options.exclusions
            or options.customAggregationTarget
            or options.hideIncompleteConversionWindowPeriods
            or options.funnelStepReference not in (None, "total")
            or options.funnelAggregateByHogQL
            or options.funnelFromStep is not None
            or options.funnelToStep is not None
        ):
            raise QueryError(
                "Select ordered conversion steps without exclusions or custom aggregation for Databricks Funnels."
            )
        if not 2 <= len(query.series) <= 10:
            raise QueryError("Select between 2 and 10 event steps for Databricks Funnels.")
        matches = [self.sql.event(series) for series in query.series]
        amount = options.funnelWindowInterval if options and options.funnelWindowInterval is not None else 14
        unit = options.funnelWindowIntervalUnit if options else "day"
        seconds = amount * {"second": 1, "minute": 60, "hour": 3600, "day": 86400, "week": 604800}.get(unit or "day", 0)
        if seconds <= 0 or seconds > 366 * 86400:
            raise QueryError("Select a positive funnel conversion window of up to 366 days.")
        source = self.sql.source(query.properties)
        ctes = [
            f"events AS ({source})",
            f"step0 AS (SELECT actor_id, timestamp AS t0, uuid AS u0 FROM events WHERE actor_id IS NOT NULL AND timestamp <= {self.sql.end} AND {matches[0]})",
        ]
        for index in range(1, len(matches)):
            prev = index - 1
            current_properties = Counter(
                json.dumps(p.model_dump(), sort_keys=True, default=str) for p in query.series[index].properties or []
            )
            previous_properties = Counter(
                json.dumps(p.model_dump(), sort_keys=True, default=str) for p in query.series[prev].properties or []
            )
            duplicate = query.series[index].event == query.series[prev].event and not (
                current_properties - previous_properties
            )
            ordering = ">" if duplicate else ">="
            prior = ", ".join(f"p.t{i}, p.u{i}" for i in range(index))
            grouping = ", ".join(f"p.t{i}, p.u{i}" for i in range(index))
            ctes.append(f"""hit{index} AS (
                SELECT p.actor_id, {prior}, min(named_struct('ts', e.timestamp, 'id', e.uuid)) AS hit
                FROM step{prev} p LEFT JOIN events e ON e.actor_id = p.actor_id
                    AND e.timestamp {ordering} p.t{prev}
                    AND e.timestamp <= p.t0 + INTERVAL {int(seconds)} SECOND
                    AND {matches[index]}
                GROUP BY p.actor_id, {grouping}
            )""")
            ctes.append(
                f"step{index} AS (SELECT actor_id, {', '.join(f't{i}, u{i}' for i in range(index))}, hit.ts AS t{index}, hit.id AS u{index} FROM hit{index})"
            )
        last = len(matches) - 1
        depth = " + ".join(f"CASE WHEN t{i} IS NOT NULL THEN 1 ELSE 0 END" for i in range(len(matches)))
        ctes.append(
            f"ranked AS (SELECT *, row_number() OVER (PARTITION BY actor_id ORDER BY ({depth}) DESC, t0, u0) AS attempt FROM step{last})"
        )
        aggregates = []
        for index in range(len(matches)):
            aggregates.append(f"count(t{index})")
            if index:
                duration = f"timestampdiff(MICROSECOND, t{index - 1}, t{index}) / 1000000.0"
                aggregates.extend([f"avg({duration})", f"percentile_cont(0.5) WITHIN GROUP (ORDER BY {duration})"])
        aggregates.append(
            f"percentile_cont(0.5) WITHIN GROUP (ORDER BY timestampdiff(MICROSECOND, t0, t{last}) / 1000000.0)"
        )
        rows = self.sql.execute(
            "WITH " + ", ".join(ctes) + " SELECT " + ", ".join(aggregates) + " FROM ranked WHERE attempt = 1"
        )
        row = rows[0]
        results = []
        position = 0
        for index, series in enumerate(query.series):
            count = int(row[position] or 0)
            position += 1
            avg = median = None
            if index:
                avg = float(row[position]) if row[position] is not None else None
                median = float(row[position + 1]) if row[position + 1] is not None else None
                position += 2
            results.append(
                {
                    "name": series.event or "All events",
                    "custom_name": series.custom_name,
                    "order": index,
                    "people": [],
                    "count": count,
                    "average_conversion_time": avg,
                    "median_conversion_time": median,
                }
            )
        return FunnelsQueryResponse(
            results=results,
            total_median_conversion_time=float(row[-1]) if row[-1] is not None else None,
            resolved_date_range=self.resolved_range(),
        )

    def retention(self, query: RetentionQuery) -> RetentionQueryResponse:
        options = query.retentionFilter
        if (
            options.retentionCustomBrackets
            or options.timeWindowMode == "24_hour_windows"
            or options.aggregationType not in (None, "count")
            or options.minimumOccurrences not in (None, 1)
            or options.cumulative
            or options.customAggregationTarget
            or options.retentionReference not in (None, "total")
        ):
            raise QueryError(
                "Select calendar-period count retention without brackets or cumulative options for Databricks Retention."
            )
        total = options.totalIntervals or 8
        if not 1 <= total <= 366:
            raise QueryError("Select between 1 and 366 retention intervals.")
        entities = []
        for entity in [options.targetEntity, options.returningEntity]:
            if entity and entity.type not in (None, "events"):
                raise QueryError("Select events as both Databricks retention entities.")
            entities.append(
                EventsNode(
                    event=(entity.id or entity.name) if entity else "$pageview",
                    properties=entity.properties if entity else None,
                )
            )
        start_match, return_match = [self.sql.event(entity) for entity in entities]
        mode = options.retentionType or "retention_recurring"
        if mode not in {"retention_recurring", "retention_first_time", "retention_first_ever_occurrence"}:
            raise QueryError("Select recurring or first-time Databricks retention.")
        source = self.sql.source(query.properties, history=mode != "retention_recurring")
        cohort = f"SELECT DISTINCT actor_id, {self.sql.bucket()} AS cohort FROM events WHERE {start_match}"
        if mode != "retention_recurring":
            cohort = (
                f"SELECT actor_id, min({self.sql.bucket()}) AS cohort FROM events WHERE {start_match} GROUP BY actor_id"
            )
        if mode == "retention_first_ever_occurrence":
            raise QueryError(
                "Use first occurrence matching filters for Databricks Retention. First-ever occurrence is not supported yet."
            )
        rows = self.sql.execute(f"""
            WITH events AS ({source}), starts AS ({cohort}), cohorts AS (
                SELECT * FROM starts WHERE cohort >= {self.sql.bucket(self.sql.start)}
            ), returns AS (
                SELECT DISTINCT actor_id, {self.sql.bucket()} AS period FROM events WHERE {return_match}
            ), cells AS (
                SELECT c.cohort, 0 AS offset, count(DISTINCT c.actor_id) AS users FROM cohorts c GROUP BY c.cohort
                UNION ALL
                SELECT c.cohort, timestampdiff({self.window.interval.upper()}, c.cohort, r.period) AS offset, count(DISTINCT c.actor_id)
                FROM cohorts c JOIN returns r ON c.actor_id = r.actor_id AND r.period > c.cohort
                WHERE timestampdiff({self.window.interval.upper()}, c.cohort, r.period) < {total}
                GROUP BY c.cohort, timestampdiff({self.window.interval.upper()}, c.cohort, r.period)
            ) SELECT cohort, offset, sum(users) FROM cells GROUP BY cohort, offset ORDER BY cohort, offset
        """)
        cells = {(self.row_date(row[0]), int(row[1])): int(row[2]) for row in rows}
        results = []
        for index, day in enumerate(self.window.axis()):
            results.append(
                {
                    "date": day,
                    "label": f"{self.window.interval.title()} {index + (options.cohortLabelStartIndex or 0)}",
                    "breakdown_value": None,
                    "values": [
                        {"count": cells.get((day, offset), 0), "label": f"{self.window.interval.title()} {offset}"}
                        for offset in range(total)
                    ],
                }
            )
        return RetentionQueryResponse(results=results, resolved_date_range=self.resolved_range())

    def lifecycle(self, query: LifecycleQuery) -> LifecycleQueryResponse:
        if len(query.series) != 1:
            raise QueryError("Select one event series for Databricks Lifecycle.")
        series = query.series[0]
        match = self.sql.event(series)
        source = self.sql.source(query.properties, start=self.window.advance(self.window.align(self.window.start), -1))
        bucket = self.sql.bucket()
        previous = f"period - INTERVAL 1 {self.window.interval.upper()}"
        following = f"period + INTERVAL 1 {self.window.interval.upper()}"
        rows = self.sql.execute(f"""
            WITH events AS ({source}), active AS (
                SELECT actor_id, {bucket} AS period, min({self.sql.bucket("person_created_at")}) AS born
                FROM events WHERE actor_id IS NOT NULL AND person_created_at IS NOT NULL
                    AND coalesce(get_json_object(properties, '$.$process_person_profile'), 'true') <> 'false'
                    AND {match} GROUP BY actor_id, {bucket}
            ), history AS (
                SELECT *, lag(period) OVER (PARTITION BY actor_id ORDER BY period) AS previous,
                    lead(period) OVER (PARTITION BY actor_id ORDER BY period) AS following FROM active
            ), statuses AS (
                SELECT period, CASE WHEN period = born THEN 'new' WHEN previous = {previous} THEN 'returning' ELSE 'resurrecting' END AS status, 1 AS users FROM history
                UNION ALL
                SELECT {following}, 'dormant', -1 FROM history WHERE following IS NULL OR following <> {following}
            ) SELECT period, status, sum(users) FROM statuses
            WHERE period >= {self.sql.bucket(self.sql.start)} AND period <= {self.sql.bucket(self.sql.end)} GROUP BY period, status ORDER BY period
        """)
        counts = {(self.row_date(row[0]), row[1]): int(row[2]) for row in rows}
        results = []
        for status in ["new", "returning", "resurrecting", "dormant"]:
            item = self.chart(series, [counts.get((day, status), 0) for day in self.window.axis()])
            item.update(label=f"{series.custom_name or series.event or 'All events'} - {status}", status=status)
            results.append(item)
        return LifecycleQueryResponse(results=results, resolved_date_range=self.resolved_range())

    def paths(self, query: PathsQuery) -> PathsQueryResponse:
        options = query.pathsFilter
        if (
            query.funnelPathsFilter
            or options.startPoint
            or options.endPoint
            or options.pathStartKey
            or options.pathEndKey
            or options.pathDropoffKey
            or options.pathGroupings
            or options.localPathCleaningFilters
            or options.pathsHogQLExpression
            or options.pathReplacements
        ):
            raise QueryError("Remove anchors, cleaning, and funnel options for Databricks Paths.")
        step_limit = options.stepLimit or 5
        edge_limit = options.edgeLimit or 50
        if not 2 <= step_limit <= 50 or not 1 <= edge_limit <= 1000:
            raise QueryError("Select 2 to 50 path steps and 1 to 1000 edges for Databricks Paths.")
        types = options.includeEventTypes or ["$pageview"]
        allowed = {"$pageview", "$screen", "custom_event"}
        if set(types) - allowed:
            raise QueryError("Select pageviews, screens, or custom events for Databricks Paths.")
        url = self.sql.property("$current_url")
        if options.stripQueryString:
            url = f"split({url}, '[?]', 2)[0]"
        url = f"regexp_replace({url}, '(.)/$', '$1')"
        screen = self.sql.property("$screen_name")
        label = f"CASE WHEN event = '$pageview' THEN {url} WHEN event = '$screen' THEN {screen} ELSE event END"
        conditions = []
        if "$pageview" in types:
            conditions.append("event = '$pageview'")
        if "$screen" in types:
            conditions.append("event = '$screen'")
        if "custom_event" in types:
            conditions.append("NOT startswith(event, '$')")
        exclude = (
            "true"
            if not options.excludeEvents
            else "(" + label + ") NOT IN (" + ", ".join(self.sql.bind(event) for event in options.excludeEvents) + ")"
        )
        source = self.sql.source(query.properties)
        minimum = options.minEdgeWeight or 0
        maximum = options.maxEdgeWeight if options.maxEdgeWeight is not None else 9223372036854775807
        rows = self.sql.execute(f"""
            WITH events AS ({source}), selected AS (
                SELECT *, {label} AS label, lag(timestamp) OVER (PARTITION BY actor_id ORDER BY timestamp, uuid) AS previous
                FROM events WHERE actor_id IS NOT NULL AND ({" OR ".join(conditions)}) AND {exclude}
            ), sessions AS (
                SELECT *, sum(CASE WHEN previous IS NULL OR timestamp > previous + INTERVAL 30 MINUTE THEN 1 ELSE 0 END)
                    OVER (PARTITION BY actor_id ORDER BY timestamp, uuid ROWS UNBOUNDED PRECEDING) AS session FROM selected
                WHERE label IS NOT NULL AND label <> ''
            ), positions AS (
                SELECT *, row_number() OVER (PARTITION BY actor_id, session ORDER BY timestamp, uuid) AS position FROM sessions
            ), transitions AS (
                SELECT *, lead(label) OVER (PARTITION BY actor_id, session ORDER BY position) AS target,
                    lead(timestamp) OVER (PARTITION BY actor_id, session ORDER BY position) AS target_time FROM positions
                WHERE position <= {step_limit}
            ) SELECT concat(position, '_', label), concat(position + 1, '_', target), count(*) AS weight,
                avg(timestampdiff(MICROSECOND, timestamp, target_time) / 1000.0)
            FROM transitions WHERE target IS NOT NULL GROUP BY position, label, target
            HAVING count(*) >= {int(minimum)} AND count(*) <= {int(maximum)} ORDER BY weight DESC, 1, 2 LIMIT {edge_limit}
        """)
        results = [
            {"source": row[0], "target": row[1], "value": int(row[2]), "average_conversion_time": float(row[3] or 0)}
            for row in rows
        ]
        return PathsQueryResponse(results=results, resolved_date_range=self.resolved_range())

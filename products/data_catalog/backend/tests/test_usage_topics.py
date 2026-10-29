import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

from posthog.test.base import BaseTest, ClickhouseTestMixin, _create_event, flush_persons_and_events

from django.test import SimpleTestCase

from asgiref.sync import async_to_sync
from parameterized import parameterized

from products.data_catalog.backend.facade.contracts import TopicCoverage
from products.data_catalog.backend.facade.enums import FetchOutcome, UsageSnapshotStatus
from products.data_catalog.backend.logic.usage_snapshots import TopicDefinition, previous_topic_list
from products.data_catalog.backend.logic.usage_topics import (
    OTHER_TOPIC,
    TOPIC_LIST_MODEL,
    NewTopicCandidate,
    ReadSession,
    SessionKind,
    SessionReading,
    SessionText,
    build_topics,
    count_session_kinds,
    fetch_topic_sessions,
    group_into_topics,
    new_topics_to_add,
    read_session,
    session_coverage,
    session_notes,
)
from products.data_catalog.backend.models import UsageSnapshot


def _session(session_id: str, *tools: str, person: str = "p1", notes: tuple[str, ...] = ("q",)) -> SessionText:
    return SessionText(session_id=session_id, person=person, notes=notes, tools=frozenset(tools))


def _read(session_id: str, question: str, *tools: str, person: str = "p1") -> ReadSession:
    return ReadSession(
        session=_session(session_id, *tools, person=person),
        reading=SessionReading(kind=SessionKind.ANALYSIS, question=question),
    )


class _FakeStream:
    def __init__(self, text: str) -> None:
        self.text = text

    async def __aenter__(self) -> "_FakeStream":
        return self

    async def __aexit__(self, *_: Any) -> bool:
        return False

    async def get_final_message(self) -> SimpleNamespace:
        return SimpleNamespace(content=[SimpleNamespace(type="text", text=self.text)])


class _FakeClient:
    def __init__(self, respond: Any) -> None:
        self.respond = respond
        self.models: list[str] = []
        self.messages = SimpleNamespace(stream=self._stream)

    def _stream(self, *, model: str, max_tokens: int, messages: list[dict[str, str]]) -> _FakeStream:
        self.models.append(model)
        return _FakeStream(self.respond(messages[0]["content"]))


def _no_heartbeat() -> None:
    return None


class TestTopicCoverage(SimpleTestCase):
    @parameterized.expand(
        [
            ("ran_a_metric", ("data-catalog-metric-run", "execute-sql"), {FetchOutcome.SILENT}, "canonical"),
            ("read_then_adapted", ("execute-sql",), {FetchOutcome.ADAPTED, FetchOutcome.SILENT}, "adapted"),
            ("re_derived_without_reading", ("execute-sql",), {FetchOutcome.MISSED}, "silent"),
            ("sql_matching_no_metric", ("execute-sql",), set(), "raw_sql_only"),
            ("query_tool_without_sql", ("query-trends",), set(), "no_sql"),
        ]
    )
    def test_session_coverage(self, _name: str, tools: tuple[str, ...], outcomes: set, expected: str) -> None:
        assert session_coverage(_session("s", *tools), outcomes) == expected

    def test_topics_rank_by_people_keep_other_last_and_drop_empty_topics(self) -> None:
        definitions = [
            TopicDefinition(title="Revenue", description="Money."),
            TopicDefinition(title="Flags", description=""),
        ]
        members = {
            OTHER_TOPIC: [
                _read("o1", "odd", person="p1"),
                _read("o2", "odd", person="p2"),
                _read("o3", "odd", person="p3"),
            ],
            "Revenue": [
                _read("a", "mrr", "execute-sql", person="p1"),
                _read("b", "arr", "data-catalog-metric-run", person="p2"),
            ],
            "Job": [_read("c", "run", person="p1"), _read("d", "run", person="p1"), _read("e", "run", person="p1")],
            "Flags": [],
        }

        topics = build_topics(definitions, members, {"a": {FetchOutcome.SILENT}})

        assert [(topic.title, topic.people, topic.session_count) for topic in topics] == [
            ("Revenue", 2, 2),
            ("Job", 1, 3),
            (OTHER_TOPIC, 3, 3),
        ]
        assert topics[0].description == "Money."
        assert topics[0].coverage == TopicCoverage(canonical=1, adapted=0, silent=1, raw_sql_only=0, no_sql=0)
        assert topics[0].sample_questions == ["mrr", "arr"]

    def test_session_kinds_count_every_read_including_failed_ones(self) -> None:
        readings = [
            SessionReading(kind=SessionKind.SUPPORT, question="q"),
            SessionReading(kind=SessionKind.LOOKUP, question="q"),
            SessionReading(kind=SessionKind.UNREAD, question=""),
        ]

        counts = count_session_kinds(readings)

        assert (counts.support, counts.lookup, counts.unread, counts.analysis) == (1, 1, 1, 0)

    def test_session_notes_collapse_whitespace_drop_repeats_and_cap(self) -> None:
        data_intents = [f"data {number}" for number in range(20)]

        notes = session_notes(["load  a\nskill", "data 0", ""], data_intents)

        assert notes[:2] == ("load a skill", "data 0")
        assert len(notes) == 12
        assert len(set(notes)) == 12


class TestNewTopicThreshold(SimpleTestCase):
    def _candidate(self, title: str, sessions: int, people: int) -> NewTopicCandidate:
        return NewTopicCandidate(
            definition=TopicDefinition(title=title, description=""),
            members=tuple(_read(f"{title}{n}", "q", person=f"p{n % people}") for n in range(sessions)),
        )

    @parameterized.expand(
        [
            ("enough_sessions_and_people", 3, 2, 100, set(), True),
            ("too_few_sessions", 2, 2, 100, set(), False),
            ("one_person_cannot_add_a_topic", 9, 1, 100, set(), False),
            ("the_bar_rises_with_the_team_volume", 5, 3, 1000, set(), False),
            ("the_title_already_exists", 9, 3, 100, {"New"}, False),
            ("the_list_is_full", 9, 3, 100, {str(n) for n in range(30)}, False),
        ]
    )
    def test_new_topic_needs_evidence(
        self, _name: str, sessions: int, people: int, total: int, existing: set[str], expected: bool
    ) -> None:
        added = new_topics_to_add(
            [self._candidate("New", sessions, people)], topic_sessions=total, existing_titles=existing
        )

        assert bool(added) is expected


class TestReadSession(SimpleTestCase):
    @parameterized.expand(
        [
            (
                "reads_kind_and_question",
                '{"kind": "analysis", "question": "How is MRR?"}',
                SessionKind.ANALYSIS,
                "How is MRR?",
            ),
            ("takes_json_inside_other_text", 'Here: {"kind": "lookup", "question": "x"}', SessionKind.LOOKUP, "x"),
            (
                "topic_kind_without_a_question_is_unclear",
                '{"kind": "support", "question": ""}',
                SessionKind.UNCLEAR,
                "",
            ),
            ("not_json_stays_counted_as_unread", "I cannot help", SessionKind.UNREAD, ""),
            ("unknown_kind_stays_counted_as_unread", '{"kind": "chat", "question": "x"}', SessionKind.UNREAD, ""),
        ]
    )
    def test_read_session(self, _name: str, reply: str, kind: SessionKind, question: str) -> None:
        reading = async_to_sync(read_session)(_FakeClient(lambda _prompt: reply), _session("s"))  # type: ignore[arg-type]

        assert reading == SessionReading(kind=kind, question=question)


class TestGroupIntoTopics(SimpleTestCase):
    def _respond(self, numbers: dict[str, int], new_topics: list[dict[str, Any]] | None = None) -> Any:
        def respond(prompt: str) -> str:
            if prompt.startswith("Below are numbered questions"):
                return json.dumps({"topics": [{"name": "Revenue", "description": "Money."}]})
            if prompt.startswith("These questions fit none"):
                return json.dumps({"topics": new_topics or []})
            question = prompt.split("<question>")[1].split("</question>")[0]
            return str(numbers[question])

        return respond

    def test_first_scan_names_a_list_and_a_later_scan_keeps_it(self) -> None:
        sessions = [_read("a", "mrr", person="p1"), _read("b", "arr", person="p2")]
        first = _FakeClient(self._respond({"mrr": 1, "arr": 1}))

        definitions, members = async_to_sync(group_into_topics)(first, sessions, [], _no_heartbeat)  # type: ignore[arg-type]

        assert [definition.title for definition in definitions] == ["Revenue"]
        assert [member.session.session_id for member in members["Revenue"]] == ["a", "b"]
        assert TOPIC_LIST_MODEL in first.models

        later = _FakeClient(self._respond({"mrr": 1, "arr": 1}))
        carried, _ = async_to_sync(group_into_topics)(later, sessions, definitions, _no_heartbeat)  # type: ignore[arg-type]

        assert carried == definitions
        assert TOPIC_LIST_MODEL not in later.models

    def test_new_subject_joins_a_carried_list_only_with_enough_evidence(self) -> None:
        previous = [TopicDefinition(title="Revenue", description="Money.")]
        sessions = [
            _read("a", "mrr", person="p1"),
            _read("b", "replay 1", person="p1"),
            _read("c", "replay 2", person="p2"),
            _read("d", "replay 3", person="p3"),
            _read("e", "one person 1", person="p4"),
            _read("f", "one person 2", person="p4"),
            _read("g", "one person 3", person="p4"),
        ]
        numbers = dict.fromkeys(("replay 1", "replay 2", "replay 3", "one person 1", "one person 2", "one person 3"), 0)
        client = _FakeClient(
            self._respond(
                {"mrr": 1, **numbers},
                [
                    {"name": "Session replay", "description": "Replay.", "numbers": [1, 2, 3]},
                    {"name": "A solo project", "description": "Solo.", "numbers": [4, 5, 6]},
                ],
            )
        )

        definitions, members = async_to_sync(group_into_topics)(client, sessions, previous, _no_heartbeat)  # type: ignore[arg-type]

        assert [definition.title for definition in definitions] == ["Revenue", "Session replay"]
        assert [member.session.session_id for member in members["Session replay"]] == ["b", "c", "d"]
        assert [member.session.session_id for member in members[OTHER_TOPIC]] == ["e", "f", "g"]


class TestPreviousTopicList(BaseTest):
    def _snapshot(self, status: UsageSnapshotStatus, computed_day: int, titles: list[str] | None) -> UsageSnapshot:
        start = datetime(2026, 9, 1, tzinfo=UTC)
        topics = None if titles is None else [{"title": title, "description": f"{title} desc"} for title in titles]
        return UsageSnapshot.objects.for_team(self.team.id).create(
            team_id=self.team.id,
            status=status.value,
            window_start=start,
            window_end=start + timedelta(days=28),
            computed_at=datetime(2026, 10, computed_day, tzinfo=UTC),
            payload={"topics": topics},
        )

    def test_reads_the_newest_finished_scan_with_topics_other_than_this_one(self) -> None:
        self._snapshot(UsageSnapshotStatus.DONE, 1, ["Oldest"])
        self._snapshot(UsageSnapshotStatus.DONE, 2, ["Revenue"])
        self._snapshot(UsageSnapshotStatus.DONE, 3, None)
        self._snapshot(UsageSnapshotStatus.ERROR, 4, ["Failed scan"])
        current = self._snapshot(UsageSnapshotStatus.DONE, 5, ["This scan"])

        assert previous_topic_list(self.team.id, current.id) == [
            TopicDefinition(title="Revenue", description="Revenue desc")
        ]


class TestTopicSessionsQuery(ClickhouseTestMixin, BaseTest):
    def _call(self, session: str, tool: str, intent: str, source: str, at: datetime) -> None:
        _create_event(
            team=self.team,
            event="$mcp_tool_call",
            distinct_id="d1",
            timestamp=at,
            properties={
                "$session_id": session,
                "$mcp_tool_name": tool,
                "$mcp_intent": intent,
                "$mcp_project_id": str(self.team.id),
                "source": source,
            },
        )

    def test_keeps_people_sessions_that_queried_data_with_opening_and_data_tool_intents(self) -> None:
        end = datetime(2026, 10, 20, 12, 0, tzinfo=UTC)
        at = end - timedelta(hours=3)
        self._call("person", "metric-list", "goal of the session", "mcp", at)
        self._call("person", "skill-get", "load a guide", "mcp", at + timedelta(minutes=1))
        self._call("person", "read-data-schema", "check a schema", "mcp", at + timedelta(minutes=2))
        self._call("person", "execute-sql", "the question", "mcp", at + timedelta(minutes=3))
        self._call("person", "execute-sql", "", "mcp", at + timedelta(minutes=4))
        self._call("person", "skill-get", "load the final checklist", "mcp", at + timedelta(minutes=5))
        self._call("scout", "execute-sql", "scheduled check", "self_driving", at)
        self._call("wizard", "execute-sql", "set up the project", "wizard", at)
        self._call("silent", "execute-sql", "", "mcp", at)
        self._call("tickets", "conversations-tickets-retrieve", "read the ticket", "mcp", at)
        self._call("trends", "query-trends", "weekly signups", "mcp", at)
        flush_persons_and_events()

        sessions = fetch_topic_sessions(
            team=self.team, telemetry_team=self.team, window_start=end - timedelta(days=1), window_end=end
        )

        assert sorted(sessions, key=lambda session: session.session_id) == [
            _session(
                "person",
                "metric-list",
                "skill-get",
                "read-data-schema",
                "execute-sql",
                person="d1",
                notes=("goal of the session", "load a guide", "check a schema", "the question"),
            ),
            _session("trends", "query-trends", person="d1", notes=("weekly signups",)),
        ]

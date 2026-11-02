import json
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from posthog.test.base import BaseTest
from unittest.mock import MagicMock, patch

from django.test import override_settings
from django.utils import timezone

import httpx
from parameterized import parameterized

from posthog.models.project_secret_api_key import ProjectSecretAPIKey
from posthog.models.utils import hash_key_value

from products.tasks.backend.logic.services.compute_settlement import (
    ComputeSettlementDeploymentError,
    SettlementOutcome,
    TeamComputeDelta,
    reconcile_compute_settlement,
    settle_overdue_sandbox_compute,
    settle_sandbox_session,
    settle_task_run_compute,
)
from products.tasks.backend.models import SandboxSession, Task, TaskClientProvenance, TaskRun

MINT_KEY = "phs_" + "c" * 35
GATEWAY_URL = "http://gateway.internal"
HTTPX_POST = "posthog.llm.gateway_internal_client.httpx.post"


def _response(status: int, body: dict[str, Any] | None = None) -> httpx.Response:
    return httpx.Response(status, json=body or {}, request=httpx.Request("POST", GATEWAY_URL))


def _ok(*, amount: str = "1.500000", entry_id: str = "entry-1", duplicate: bool = False) -> httpx.Response:
    return _response(200, {"team_id": 1, "entry_id": entry_id, "amount_usd": amount, "duplicate": duplicate})


@override_settings(
    GATEWAY_COMPUTE_DEBIT_ENABLED=True,
    SANDBOX_AI_GATEWAY_MINT_KEY=MINT_KEY,
    AI_GATEWAY_INTERNAL_URL=GATEWAY_URL,
    AI_GATEWAY_INTERNAL_TOKEN="operator-token",
)
class TestComputeSettlement(BaseTest):
    def setUp(self) -> None:
        super().setUp()
        self.now = timezone.now()
        ProjectSecretAPIKey.objects.create(
            team=self.team, label="mint", secure_value=hash_key_value(MINT_KEY), scopes=["llm_gateway:read"]
        )

    def _session(self, *, with_actor: bool = True, **overrides: Any) -> SandboxSession:
        task = Task.objects.create(
            team=self.team,
            title="t",
            description="",
            origin_product=Task.OriginProduct.USER_CREATED,
            client_provenance=TaskClientProvenance.POSTHOG_DESKTOP,
            created_by=self.user if with_actor else None,
        )
        run = TaskRun.objects.create(task=task, team=self.team, environment=TaskRun.Environment.CLOUD)
        created_at = overrides.pop("created_at", self.now - timedelta(hours=2))
        defaults: dict[str, Any] = {
            "team": self.team,
            "task_run": run,
            "sandbox_id": f"sb-{SandboxSession.objects.unscoped().count()}",
            "origin_product": Task.OriginProduct.USER_CREATED,
            "client_provenance": TaskClientProvenance.POSTHOG_DESKTOP,
            "cpu_cores": 4.0,
            "memory_gb": 16.0,
            "ttl_seconds": 6 * 60 * 60,
            "created_at": created_at,
            "ttl_expires_at": created_at + timedelta(hours=6),
            "user_attributed_at": self.now - timedelta(hours=2),
            "ended_at": self.now - timedelta(hours=1),
            "ended_reason": SandboxSession.EndedReason.CLEANUP,
        }
        defaults.update(overrides)
        return SandboxSession.objects.unscoped().create(**defaults)

    def _reload(self, session: SandboxSession) -> SandboxSession:
        return SandboxSession.objects.unscoped().get(id=session.id)

    @parameterized.expand([(False, SettlementOutcome.SETTLED), (True, SettlementOutcome.DUPLICATE)])
    def test_success_stores_the_gateway_amount_and_entry(self, duplicate: bool, expected: SettlementOutcome) -> None:
        session = self._session()
        with patch(HTTPX_POST, return_value=_ok(duplicate=duplicate)) as post:
            assert settle_sandbox_session(session) == expected

        assert post.call_args.args[0] == f"{GATEWAY_URL}/internal/teams/{self.team.id}/reported-spend"
        assert post.call_args.kwargs["headers"]["X-Internal-Actor"] == "tasks-compute"
        settled = self._reload(session)
        assert settled.compute_settled_at is not None
        assert settled.compute_settled_usd == Decimal("1.500000")
        assert settled.compute_ledger_entry_id == "entry-1"

    def test_body_shape_is_clamped_and_byte_identical_across_retries(self) -> None:
        session = self._session(
            with_actor=False,
            created_at=self.now - timedelta(hours=7),
            user_attributed_at=self.now - timedelta(hours=6, minutes=30),
            ttl_expires_at=self.now - timedelta(hours=6),
            ended_at=self.now - timedelta(hours=1),
        )
        with patch(HTTPX_POST, side_effect=[_response(503), _response(503)]) as post:
            settle_sandbox_session(session)
            settle_sandbox_session(self._reload(session))

        first, second = (json.dumps(call.kwargs["json"]) for call in post.call_args_list)
        assert first == second
        body = post.call_args_list[0].kwargs["json"]
        assert "user" not in body
        assert body["reference"] == f"compute:{session.id}"
        assert body["obo"] == str(self.team.id)
        assert body["run"] == str(session.task_run_id)
        assert body["billable"] is True
        assert body["charge_kind"] == "compute"
        assert body["product"] == "posthog_code"
        assert body["amount_usd"] == "0.770400"
        detail = body["detail"]
        assert detail["window_end"] == session.ttl_expires_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        assert detail["attributed_seconds"] == 1800.0
        assert detail["resource"] == "modal"
        assert [card["version"] for card in detail["rate_cards"]] == ["v1"]
        assert detail["rate_cards"][0]["usd"] == "0.770400"
        assert len(json.dumps(detail).encode()) <= 2048

    def test_body_pins_the_acting_user(self) -> None:
        session = self._session()
        with patch(HTTPX_POST, return_value=_ok()) as post:
            settle_sandbox_session(session)

        assert post.call_args.kwargs["json"]["user"] == self.user.distinct_id

    @parameterized.expand(
        [
            ("conflict", _response(409, {"error": "reference reused"}), SettlementOutcome.CONFLICT, 0, True),
            ("validation", _response(400, {"error": "bad"}), SettlementOutcome.CONFLICT, 0, True),
            ("unavailable", _response(503), SettlementOutcome.RETRY, 1, False),
            ("rate_limited", _response(429), SettlementOutcome.RETRY, 1, False),
            ("transport", httpx.ConnectError("refused"), SettlementOutcome.RETRY, 1, False),
        ]
    )
    def test_failure_statuses(
        self, _name: str, result: Any, expected: SettlementOutcome, attempts: int, conflicted: bool
    ) -> None:
        session = self._session()
        mock = MagicMock(side_effect=result) if isinstance(result, Exception) else MagicMock(return_value=result)
        with patch(HTTPX_POST, mock):
            assert settle_sandbox_session(session) == expected
            assert settle_task_run_compute(session.task_run_id, self.team.id) == (
                [] if conflicted else [SettlementOutcome.RETRY]
            )

        row = self._reload(session)
        assert row.compute_settled_at is None
        assert (row.compute_settle_conflict_at is not None) is conflicted
        assert row.compute_settle_attempts == (0 if conflicted else attempts + 1)
        assert mock.call_count == (1 if conflicted else 2)
        if conflicted:
            assert row.compute_settle_error.startswith(f"HTTP {result.status_code}")

    def test_retry_cap_excludes_the_row(self) -> None:
        session = self._session(compute_settle_attempts=19)
        with patch(HTTPX_POST, return_value=_response(503)):
            assert settle_sandbox_session(session) == SettlementOutcome.ATTEMPTS_EXHAUSTED

        row = self._reload(session)
        assert row.compute_settle_error == "attempts_exhausted"
        assert row.compute_settle_conflict_at is not None
        assert row.compute_settled_at is None

    def test_deployment_error_raises_and_leaves_the_row_untouched(self) -> None:
        session = self._session()
        with patch(HTTPX_POST, return_value=_response(403)), pytest.raises(ComputeSettlementDeploymentError):
            settle_sandbox_session(session)

        row = self._reload(session)
        assert row.compute_settled_at is None
        assert row.compute_settle_conflict_at is None
        assert row.compute_settle_attempts == 0
        assert row.compute_settle_error == ""

    def test_flag_off_is_a_no_op(self) -> None:
        session = self._session()
        with override_settings(GATEWAY_COMPUTE_DEBIT_ENABLED=False), patch(HTTPX_POST) as post:
            assert settle_sandbox_session(session) == SettlementOutcome.DISABLED
            assert settle_task_run_compute(session.task_run_id, self.team.id) == []
            assert settle_overdue_sandbox_compute(self.now) == []

        post.assert_not_called()
        assert self._reload(session).compute_settled_at is None

    @parameterized.expand([("unattributed", True), ("zero_amount", False)])
    def test_settles_locally_without_a_call(self, _name: str, unattributed: bool) -> None:
        ended_at = self.now - timedelta(hours=1)
        session = self._session(user_attributed_at=None if unattributed else ended_at, ended_at=ended_at)
        with patch(HTTPX_POST) as post:
            assert settle_sandbox_session(session) == SettlementOutcome.SETTLED_LOCALLY

        post.assert_not_called()
        row = self._reload(session)
        assert row.compute_settled_at is not None
        assert row.compute_settled_usd == Decimal(0)

    def test_sweep_selects_overdue_rows_only(self) -> None:
        straggler = self._session(ended_at=self.now - timedelta(minutes=30))
        fresh = self._session(ended_at=self.now - timedelta(minutes=2))
        expired_open = self._session(
            ended_at=None,
            ended_reason=None,
            created_at=self.now - timedelta(hours=8),
            user_attributed_at=self.now - timedelta(hours=7, minutes=30),
        )
        hogland_open = self._session(
            ended_at=None,
            ended_reason=None,
            sandbox_backend="hogland",
            created_at=self.now - timedelta(hours=8),
            user_attributed_at=self.now - timedelta(hours=7, minutes=30),
        )

        with patch(HTTPX_POST, return_value=_ok()) as post:
            outcomes = settle_overdue_sandbox_compute(self.now)
            again = settle_overdue_sandbox_compute(self.now)

        assert outcomes == [SettlementOutcome.SETTLED, SettlementOutcome.SETTLED]
        assert again == []
        assert post.call_count == 2
        assert self._reload(straggler).compute_settled_at is not None
        assert self._reload(fresh).compute_settled_at is None
        closed = self._reload(expired_open)
        assert closed.ended_reason == SandboxSession.EndedReason.TTL_SWEEP
        assert closed.compute_settled_at is not None
        untouched = self._reload(hogland_open)
        assert untouched.ended_at is None
        assert untouched.compute_settled_at is None

    def test_sweep_stops_on_a_deployment_error(self) -> None:
        self._session(ended_at=self.now - timedelta(minutes=30))
        self._session(ended_at=self.now - timedelta(minutes=40))
        with patch(HTTPX_POST, return_value=_response(401)) as post:
            assert settle_overdue_sandbox_compute(self.now) == []

        assert post.call_count == 1

    def test_reconciliation_compares_per_customer_team(self) -> None:
        self._session(
            compute_settled_at=self.now - timedelta(minutes=5),
            compute_settled_usd=Decimal("1.540800"),
            compute_ledger_entry_id="entry-1",
        )
        rows = [
            {"keys": {"obo": str(self.team.id)}, "compute_usd": "1.540800", "billable_compute_usd": "1.540800"},
            {"keys": {"obo": "999"}, "compute_usd": "2.000000", "billable_compute_usd": "0"},
        ]
        report = httpx.Response(200, json={"rows": rows}, request=httpx.Request("GET", GATEWAY_URL))
        with patch("posthog.llm.gateway_internal_client.httpx.get", return_value=report) as get:
            result = reconcile_compute_settlement(self.now - timedelta(days=1), self.now, self.team.id)

        assert get.call_args.kwargs["params"]["group_by"] == "obo,charge_kind"
        assert result.writer_parity == (
            TeamComputeDelta(
                team_id=str(self.team.id), local_usd=Decimal("1.540800"), gateway_usd=Decimal("1.540800"), local_rows=1
            ),
            TeamComputeDelta(team_id="999", local_usd=Decimal(0), gateway_usd=Decimal("2.000000"), local_rows=0),
        )
        assert result.pricing_parity[0].local_usd == Decimal("1.540800")

"""Unit tests for the shared Python Temporal cronjob harness."""

import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import cast
from unittest.mock import MagicMock

import pytest
from temporalio import workflow
from temporalio.client import Client, ScheduleAlreadyRunningError, ScheduleOverlapPolicy
from temporalio.service import RPCError, RPCStatusCode

from app.adapters.temporal import cronjob as cronjob_module
from app.adapters.temporal.cronjob import CronjobSpec, connect, ensure_schedule, run_cronjob
from app.adapters.temporal.interceptor import RunMetricsInterceptor
from app.adapters.temporal.metrics import CronjobMetrics


@workflow.defn(name="MyCronjobTick")
class _Workflow:
    @workflow.run
    async def run(self, key: str) -> None: ...


def _spec() -> CronjobSpec:
    return CronjobSpec(
        name="my-cronjob",
        interval=timedelta(hours=6),
        workflow=_Workflow,
        activities=[],
        workflow_args=["all"],
    )


class _RecordingHandle:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.updated = False

    async def update(self, updater):
        if self.error is not None:
            raise self.error
        self.updated = True


class _RecordingClient:
    def __init__(self, error: Exception | None = None, handle_error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple] = []
        self.handle = _RecordingHandle(handle_error)
        self.handle_requests: list[str] = []

    async def create_schedule(self, schedule_id, schedule):
        self.calls.append((schedule_id, schedule))
        if self.error is not None:
            raise self.error

    def get_schedule_handle(self, schedule_id):
        self.handle_requests.append(schedule_id)
        return self.handle


async def test_ensure_schedule_uses_the_cronjob_name_as_schedule_id_and_task_queue():
    client = _RecordingClient()
    await ensure_schedule(cast(Client, client), _spec())
    schedule_id, schedule = client.calls[0]
    assert schedule_id == "my-cronjob"
    assert schedule.action.task_queue == "my-cronjob"
    assert schedule.action.id == "scheduled-my-cronjob"


async def test_ensure_schedule_passes_workflow_args_through():
    client = _RecordingClient()
    await ensure_schedule(cast(Client, client), _spec())
    _, schedule = client.calls[0]
    assert schedule.action.args == ["all"]


async def test_ensure_schedule_sets_the_configured_interval():
    client = _RecordingClient()
    await ensure_schedule(cast(Client, client), _spec())
    _, schedule = client.calls[0]
    assert schedule.spec.intervals[0].every == timedelta(hours=6)


async def test_ensure_schedule_skips_overlapping_runs():
    client = _RecordingClient()
    await ensure_schedule(cast(Client, client), _spec())
    _, schedule = client.calls[0]
    assert schedule.policy.overlap is ScheduleOverlapPolicy.SKIP


async def test_ensure_schedule_tolerates_a_schedule_left_by_a_previous_run():
    # The SDK raises this typed error, not an RPCError carrying "AlreadyExists".
    # Matching on the string instead crash-loops the pod on every restart after
    # the first, which is how this was originally shipped.
    client = _RecordingClient(error=ScheduleAlreadyRunningError())
    await ensure_schedule(cast(Client, client), _spec())  # must not raise


async def test_ensure_schedule_reconciles_an_existing_schedule():
    # Mirrors the Go harness's reconcileScheduleSpec: a changed interval env
    # var reaches the existing schedule on redeploy, no manual deletion.
    client = _RecordingClient(error=ScheduleAlreadyRunningError())
    await ensure_schedule(cast(Client, client), _spec())
    assert client.handle_requests == ["my-cronjob"]
    assert client.handle.updated is True


async def test_a_failed_reconcile_does_not_crash_loop_the_worker():
    # The existing schedule has a valid spec; a transient reconcile failure
    # must log and serve it, never take the pod down.
    client = _RecordingClient(
        error=ScheduleAlreadyRunningError(),
        handle_error=RPCError("transient", RPCStatusCode.UNAVAILABLE, b""),
    )
    await ensure_schedule(cast(Client, client), _spec())  # must not raise


async def test_ensure_schedule_propagates_any_other_rpc_failure():
    client = _RecordingClient(error=RPCError("connection refused", RPCStatusCode.UNAVAILABLE, b""))
    with pytest.raises(RPCError):
        await ensure_schedule(cast(Client, client), _spec())


async def test_connect_defaults_match_the_go_runner(monkeypatch):
    captured = {}

    async def _fake_connect(host_port, namespace):
        captured["host_port"] = host_port
        captured["namespace"] = namespace
        return "client"

    monkeypatch.setattr(os, "environ", {})
    monkeypatch.setattr("app.adapters.temporal.cronjob.Client.connect", _fake_connect)
    assert await connect() == "client"
    assert captured == {"host_port": "localhost:7233", "namespace": "sentinel"}


async def test_connect_reads_the_shared_temporal_env_vars(monkeypatch):
    captured = {}

    async def _fake_connect(host_port, namespace):
        captured["host_port"] = host_port
        captured["namespace"] = namespace
        return "client"

    monkeypatch.setattr(os, "environ", {"TEMPORAL_HOST_PORT": "temporal:7233", "TEMPORAL_NAMESPACE": "vector"})
    monkeypatch.setattr("app.adapters.temporal.cronjob.Client.connect", _fake_connect)
    await connect()
    assert captured == {"host_port": "temporal:7233", "namespace": "vector"}


def test_build_worker_installs_the_run_metrics_interceptor(monkeypatch):
    # The interceptor is this harness's single recording site (see
    # interceptor.py) -- nothing else wires cronjob.runs.total /
    # cronjob.run.duration_seconds, so a Worker built without it is silently
    # invisible to the alerts.
    captured: dict = {}

    class _FakeWorker:
        def __init__(self, *args, **kwargs) -> None:
            captured.update(kwargs)

    monkeypatch.setattr(cronjob_module, "Worker", _FakeWorker)
    metrics = MagicMock(spec=CronjobMetrics)

    with ThreadPoolExecutor(max_workers=1) as executor:
        cronjob_module._build_worker(MagicMock(), _spec(), executor, metrics)

    interceptors = captured["interceptors"]
    assert len(interceptors) == 1
    assert isinstance(interceptors[0], RunMetricsInterceptor)
    assert interceptors[0]._metrics is metrics


async def test_run_cronjob_shuts_down_metrics_when_connect_fails(monkeypatch):
    # A connect()/ensure_schedule() failure happens before the worker's own
    # try/finally. Without shutting the provider down here too, an
    # in-process retry finds the global MeterProvider already taken (see
    # metrics.py's set_meter_provider note) and the failed attempt's
    # periodic reader keeps running.
    shutdown = MagicMock()
    monkeypatch.setattr(cronjob_module, "init_metrics_provider", lambda _name: shutdown)

    async def _failing_connect():
        raise RPCError("connection refused", RPCStatusCode.UNAVAILABLE, b"")

    monkeypatch.setattr(cronjob_module, "connect", _failing_connect)

    with pytest.raises(RPCError):
        await run_cronjob(_spec())

    shutdown.assert_called_once()

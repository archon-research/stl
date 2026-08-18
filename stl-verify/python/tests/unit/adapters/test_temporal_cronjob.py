"""Unit tests for the shared Python Temporal cronjob harness."""

import os
from datetime import timedelta
from typing import cast

import pytest
from temporalio import workflow
from temporalio.client import Client, ScheduleAlreadyRunningError, ScheduleOverlapPolicy
from temporalio.service import RPCError, RPCStatusCode

from app.adapters.temporal.cronjob import CronjobSpec, connect, ensure_schedule


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


class _RecordingClient:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls: list[tuple] = []

    async def create_schedule(self, schedule_id, schedule):
        self.calls.append((schedule_id, schedule))
        if self.error is not None:
            raise self.error


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

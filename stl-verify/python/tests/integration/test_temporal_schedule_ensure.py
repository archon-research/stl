"""Schedule registration is idempotent against a real Temporal server.

The unit tests can only assert that the exception we chose to catch is caught.
Which exception the SDK actually raises on a second create is a fact about
Temporal, so it has to be checked against Temporal: catching the wrong one
crash-loops the pod on every restart after the first.
"""

import uuid
from datetime import timedelta

import pytest
from temporalio import workflow
from temporalio.testing import WorkflowEnvironment

from app.adapters.temporal.cronjob import CronjobSpec, ensure_schedule


@workflow.defn(name="ScheduleEnsureProbe")
class _Probe:
    @workflow.run
    async def run(self, market_key: str) -> None: ...


@pytest.fixture()
async def env():
    async with await WorkflowEnvironment.start_local() as environment:
        yield environment


def _spec() -> CronjobSpec:
    return CronjobSpec(
        name=f"probe-{uuid.uuid4()}",
        interval=timedelta(hours=24),
        workflow=_Probe,
        activities=[],
        workflow_args=["all"],
    )


async def test_first_ensure_registers_the_schedule(env):
    spec = _spec()
    await ensure_schedule(env.client, spec)
    described = await env.client.get_schedule_handle(spec.name).describe()
    assert described.schedule.spec.intervals[0].every == timedelta(hours=24)


async def test_ensure_is_idempotent_across_a_restart(env):
    spec = _spec()
    await ensure_schedule(env.client, spec)
    await ensure_schedule(env.client, spec)  # must not raise

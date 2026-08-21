"""Schedule registration is idempotent against a real Temporal server.

The unit tests can only assert that the exception we chose to catch is caught.
Which exception the SDK actually raises on a second create is a fact about
Temporal, so it has to be checked against Temporal: catching the wrong one
crash-loops the pod on every restart after the first.
"""

import dataclasses
import uuid
from datetime import timedelta

import pytest
from temporalio import workflow

from app.adapters.temporal.cronjob import CronjobSpec, ensure_schedule

# The dev server lives in a module-scoped fixture (conftest.temporal_env), so
# every test must share its event loop.
pytestmark = pytest.mark.asyncio(loop_scope="module")


@workflow.defn(name="ScheduleEnsureProbe")
class _Probe:
    @workflow.run
    async def run(self, market_key: str) -> None: ...


def _spec() -> CronjobSpec:
    return CronjobSpec(
        name=f"probe-{uuid.uuid4()}",
        interval=timedelta(hours=24),
        workflow=_Probe,
        activities=[],
        workflow_args=["all"],
    )


async def test_first_ensure_registers_the_schedule(temporal_env):
    spec = _spec()
    await ensure_schedule(temporal_env.client, spec)
    described = await temporal_env.client.get_schedule_handle(spec.name).describe()
    assert described.schedule.spec.intervals[0].every == timedelta(hours=24)


async def test_ensure_is_idempotent_across_a_restart(temporal_env):
    spec = _spec()
    await ensure_schedule(temporal_env.client, spec)
    await ensure_schedule(temporal_env.client, spec)  # must not raise


async def test_ensure_reconciles_a_changed_interval_on_restart(temporal_env):
    # Same semantics as the Go harness's reconcileScheduleSpec: a changed
    # interval env var reaches the existing schedule on redeploy, without a
    # manual deletion in the Temporal UI.
    spec = _spec()
    await ensure_schedule(temporal_env.client, spec)
    await ensure_schedule(temporal_env.client, dataclasses.replace(spec, interval=timedelta(hours=6)))
    described = await temporal_env.client.get_schedule_handle(spec.name).describe()
    assert described.schedule.spec.intervals[0].every == timedelta(hours=6)

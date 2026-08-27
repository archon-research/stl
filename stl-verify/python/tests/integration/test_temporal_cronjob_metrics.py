"""RunMetricsInterceptor wired into a real Worker, against a real Temporal
dev server.

The unit tests (tests/unit/adapters/test_temporal_interceptor.py) exercise
the interceptor's own logic against a hand-written stand-in for "the next
interceptor in the chain". This checks the other half: that a Worker
actually invokes it around every activity execution, the same way
run_cronjob (app/adapters/temporal/cronjob.py) wires it in production --
mirroring the Go harness's TestRunMetricsInterceptor_RecordsActivityOutcome
(internal/adapters/outbound/temporal/interceptor_test.go), which uses the Go
SDK's in-process test environment instead: the Python SDK's testing module
offers no activity-level equivalent, so this exercises the real thing.

Both a sync (ThreadPoolExecutor-backed) and an async probe activity are
covered for success: production activities are sync (cli/cronjobs/
core_model_runner/activity.py, run via the ThreadPoolExecutor cronjob.py
builds), a different SDK dispatch path from async. Cancellation is covered
for the production shape only -- a sync activity cancelled by worker
shutdown, which is how a deploy landing mid-tick reaches it: the SDK raises
temporalio.exceptions.CancelledError into the activity's own thread, and the
interceptor must classify that "canceled", not "error", because that split
is what keeps VectorCronjobRunFailing quiet on deploys.
"""

import asyncio
import threading
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader, NumberDataPoint
from temporalio import activity, workflow
from temporalio.client import WorkflowFailureError
from temporalio.common import RetryPolicy
from temporalio.exceptions import ApplicationError
from temporalio.worker import Worker

from app.adapters.temporal.interceptor import RunMetricsInterceptor
from app.adapters.temporal.metrics import CronjobMetrics

pytestmark = pytest.mark.asyncio(loop_scope="module")

_SEEDED_COUNTS = {"success": 0, "error": 0, "canceled": 0}

# Set by _blocking_sync_activity once it is running, so the cancellation test
# shuts the worker down mid-activity rather than before the activity starts.
_SYNC_ACTIVITY_STARTED = threading.Event()


# Every probe shares one activity name -- the fixed workflow below always
# calls "MetricsProbeActivity" by name, so each test registers exactly one of
# these functions under it to choose the outcome.
@activity.defn(name="MetricsProbeActivity")
async def _succeeding_async_activity() -> None:
    return None


@activity.defn(name="MetricsProbeActivity")
async def _failing_async_activity() -> None:
    raise ApplicationError("boom", non_retryable=True)


@activity.defn(name="MetricsProbeActivity")
def _succeeding_sync_activity() -> None:
    return None


@activity.defn(name="MetricsProbeActivity")
def _blocking_sync_activity() -> None:
    _SYNC_ACTIVITY_STARTED.set()
    # Never returns on its own: only the CancelledError the SDK raises into
    # this thread on worker shutdown ends it. Short sleeps so the injected
    # exception lands promptly (it is delivered between bytecodes).
    while True:
        time.sleep(0.05)


@workflow.defn(name="MetricsProbeWorkflow")
class _ProbeWorkflow:
    @workflow.run
    async def run(self) -> None:
        await workflow.execute_activity(
            "MetricsProbeActivity",
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


def _counts_by_status(reader: InMemoryMetricReader) -> dict[str, int]:
    data = reader.get_metrics_data()
    assert data is not None
    out: dict[str, int] = dict(_SEEDED_COUNTS)
    for resource_metrics in data.resource_metrics:
        for scope_metrics in resource_metrics.scope_metrics:
            for metric in scope_metrics.metrics:
                if metric.name != "cronjob.runs.total":
                    continue
                for point in metric.data.data_points:
                    assert isinstance(point, NumberDataPoint)
                    status = point.attributes["status"] if point.attributes else None
                    assert isinstance(status, str)
                    out[status] = int(point.value)
    return out


@pytest.fixture
def probe() -> Iterator[tuple[InMemoryMetricReader, CronjobMetrics]]:
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    yield reader, CronjobMetrics(meter_provider=provider)
    provider.shutdown()


def _probe_worker(temporal_env, activity_fn, run_metrics: CronjobMetrics, executor: ThreadPoolExecutor | None):
    return Worker(
        temporal_env.client,
        task_queue=f"metrics-probe-{uuid.uuid4()}",
        workflows=[_ProbeWorkflow],
        activities=[activity_fn],
        activity_executor=executor,
        interceptors=[RunMetricsInterceptor(run_metrics)],
    )


async def _run_one_workflow(
    temporal_env, activity_fn, run_metrics: CronjobMetrics, *, executor: ThreadPoolExecutor | None = None
) -> None:
    worker = _probe_worker(temporal_env, activity_fn, run_metrics, executor)
    async with worker:
        await temporal_env.client.execute_workflow(
            _ProbeWorkflow.run,
            id=f"metrics-probe-{uuid.uuid4()}",
            task_queue=worker.task_queue,
        )


async def test_a_successful_async_activity_records_status_success(temporal_env, probe):
    reader, run_metrics = probe

    await _run_one_workflow(temporal_env, _succeeding_async_activity, run_metrics)

    assert _counts_by_status(reader) == {**_SEEDED_COUNTS, "success": 1}


async def test_a_successful_sync_activity_records_status_success(temporal_env, probe):
    # Production shape: run_core_model_activity is sync, run through the
    # ThreadPoolExecutor cronjob.py builds -- a different SDK dispatch path
    # than the async probe above.
    reader, run_metrics = probe

    with ThreadPoolExecutor(max_workers=1) as executor:
        await _run_one_workflow(temporal_env, _succeeding_sync_activity, run_metrics, executor=executor)

    assert _counts_by_status(reader) == {**_SEEDED_COUNTS, "success": 1}


async def test_a_failing_activity_records_status_error(temporal_env, probe):
    reader, run_metrics = probe

    with pytest.raises(WorkflowFailureError):
        await _run_one_workflow(temporal_env, _failing_async_activity, run_metrics)

    assert _counts_by_status(reader) == {**_SEEDED_COUNTS, "error": 1}


async def test_a_sync_activity_cancelled_by_worker_shutdown_records_status_canceled(temporal_env, probe):
    # The deploy-mid-tick shape: the Worker's default graceful_shutdown_timeout
    # is 0, so leaving `async with worker` cancels the running activity at
    # once and waits for it to finish -- the record must land before the
    # worker is gone, and land on "canceled".
    reader, run_metrics = probe
    _SYNC_ACTIVITY_STARTED.clear()

    with ThreadPoolExecutor(max_workers=1) as executor:
        worker = _probe_worker(temporal_env, _blocking_sync_activity, run_metrics, executor)
        async with worker:
            handle = await temporal_env.client.start_workflow(
                _ProbeWorkflow.run,
                id=f"metrics-probe-{uuid.uuid4()}",
                task_queue=worker.task_queue,
            )
            assert await asyncio.to_thread(_SYNC_ACTIVITY_STARTED.wait, 10), "probe activity never started"

    assert _counts_by_status(reader) == {**_SEEDED_COUNTS, "canceled": 1}
    # The server-side workflow is still running with no worker; end it so it
    # does not outlive the test.
    await handle.cancel()

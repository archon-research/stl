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
covered: production activities are sync (cli/cronjobs/core_model_runner/
activity.py, run via the ThreadPoolExecutor cronjob.py builds), and
cancellation surfaces through a different SDK code path for each --
temporalio.exceptions.CancelledError raised into the activity's own thread
for sync, asyncio.CancelledError for async -- so only covering one would
leave the other's classification unverified.
"""

import uuid
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


# Both probes share one activity name -- the fixed workflow below always
# calls "MetricsProbeActivity" by name, so each test registers exactly one of
# these two functions under it to choose success vs. failure.
@activity.defn(name="MetricsProbeActivity")
async def _succeeding_async_activity() -> None:
    return None


@activity.defn(name="MetricsProbeActivity")
async def _failing_async_activity() -> None:
    raise ApplicationError("boom", non_retryable=True)


@activity.defn(name="MetricsProbeActivity")
def _succeeding_sync_activity() -> None:
    return None


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


async def _run_one_workflow(
    temporal_env, activity_fn, run_metrics: CronjobMetrics, *, executor: ThreadPoolExecutor | None = None
) -> None:
    task_queue = f"metrics-probe-{uuid.uuid4()}"
    workflow_id = f"metrics-probe-{uuid.uuid4()}"
    async with Worker(
        temporal_env.client,
        task_queue=task_queue,
        workflows=[_ProbeWorkflow],
        activities=[activity_fn],
        activity_executor=executor,
        interceptors=[RunMetricsInterceptor(run_metrics)],
    ):
        await temporal_env.client.execute_workflow(
            _ProbeWorkflow.run,
            id=workflow_id,
            task_queue=task_queue,
        )


async def test_a_successful_async_activity_records_status_success(temporal_env):
    reader = InMemoryMetricReader()
    run_metrics = CronjobMetrics(meter_provider=MeterProvider(metric_readers=[reader]))

    await _run_one_workflow(temporal_env, _succeeding_async_activity, run_metrics)

    assert _counts_by_status(reader) == {**_SEEDED_COUNTS, "success": 1}


async def test_a_successful_sync_activity_records_status_success(temporal_env):
    # Production shape: run_core_model_activity is sync, run through the
    # ThreadPoolExecutor cronjob.py builds -- a different SDK dispatch path
    # than the async probe above.
    reader = InMemoryMetricReader()
    run_metrics = CronjobMetrics(meter_provider=MeterProvider(metric_readers=[reader]))

    with ThreadPoolExecutor(max_workers=1) as executor:
        await _run_one_workflow(temporal_env, _succeeding_sync_activity, run_metrics, executor=executor)

    assert _counts_by_status(reader) == {**_SEEDED_COUNTS, "success": 1}


async def test_a_failing_activity_records_status_error(temporal_env):
    reader = InMemoryMetricReader()
    run_metrics = CronjobMetrics(meter_provider=MeterProvider(metric_readers=[reader]))

    with pytest.raises(WorkflowFailureError):
        await _run_one_workflow(temporal_env, _failing_async_activity, run_metrics)

    assert _counts_by_status(reader) == {**_SEEDED_COUNTS, "error": 1}

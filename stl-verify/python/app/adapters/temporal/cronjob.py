"""Client connect, schedule ensure, and worker run for Python Temporal cronjobs.

The Go side of the service does the same three things in
`internal/adapters/outbound/temporal.RunCronjob`. Keep the two in step: the
cronjob name is simultaneously the task queue and the schedule id, and a
restart must never fail because the schedule survived from a previous run.
"""

import asyncio
import logging
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from temporalio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from temporalio.worker import Worker

from app.adapters.temporal.interceptor import RunMetricsInterceptor
from app.adapters.temporal.metrics import CronjobMetrics, init_metrics_provider

logger = logging.getLogger(__name__)

TEMPORAL_HOST_PORT_ENV = "TEMPORAL_HOST_PORT"
TEMPORAL_NAMESPACE_ENV = "TEMPORAL_NAMESPACE"

# Same defaults as the Go runner (internal/adapters/outbound/temporal/temporal.go).
_DEFAULT_HOST_PORT = "localhost:7233"
_DEFAULT_NAMESPACE = "sentinel"


@dataclass(frozen=True)
class CronjobSpec:
    """One cronjob's Temporal wiring.

    `name` is used verbatim as the task queue and the schedule id, so it must
    match the k8s Deployment name to keep the three discoverable together.
    """

    name: str
    interval: timedelta
    workflow: Any
    activities: list[Any]
    workflow_args: list[Any] = field(default_factory=list)


async def connect() -> Client:
    host_port = os.getenv(TEMPORAL_HOST_PORT_ENV, _DEFAULT_HOST_PORT)
    namespace = os.getenv(TEMPORAL_NAMESPACE_ENV, _DEFAULT_NAMESPACE)
    logger.info("connecting to temporal host_port=%s namespace=%s", host_port, namespace)
    return await Client.connect(host_port, namespace=namespace)


async def ensure_schedule(client: Client, spec: CronjobSpec) -> None:
    """Create the schedule, or reconcile the interval into an existing one.

    Temporal owns the schedule, so a changed interval env var only reaches it
    through the reconcile on worker startup — the same semantics as the Go
    harness's reconcileScheduleSpec.
    """
    try:
        await client.create_schedule(
            spec.name,
            Schedule(
                action=ScheduleActionStartWorkflow(
                    spec.workflow.run,
                    args=spec.workflow_args,
                    id=f"scheduled-{spec.name}",
                    task_queue=spec.name,
                ),
                spec=ScheduleSpec(intervals=[ScheduleIntervalSpec(every=spec.interval)]),
                # A tick can outrun its own interval, and two concurrent runs would
                # race on the same append-only results table.
                policy=SchedulePolicy(overlap=ScheduleOverlapPolicy.SKIP),
            ),
        )
        logger.info("schedule created name=%s interval=%s", spec.name, spec.interval)
    except ScheduleAlreadyRunningError:
        # The normal path on every restart after the first. Note this is a
        # typed SDK error, not the RPCError/"AlreadyExists" string match the
        # Go runner uses -- matching on the string here silently crash-loops
        # the pod.
        await _reconcile_schedule(client, spec)


async def _reconcile_schedule(client: Client, spec: CronjobSpec) -> None:
    """Update the existing schedule's spec so a changed interval takes effect
    on redeploy without a manual deletion. The existing schedule already has a
    valid spec, so a failed reconcile (e.g. a transient Temporal error) must
    not crash-loop the worker: log it and serve the existing schedule; the
    next successful startup reconciles again."""

    def _update(update_input: ScheduleUpdateInput) -> ScheduleUpdate:
        schedule = update_input.description.schedule
        schedule.spec = ScheduleSpec(intervals=[ScheduleIntervalSpec(every=spec.interval)])
        return ScheduleUpdate(schedule=schedule)

    try:
        await client.get_schedule_handle(spec.name).update(_update)
        logger.info("schedule reconciled name=%s interval=%s", spec.name, spec.interval)
    except Exception:
        logger.warning(
            "schedule reconcile failed; starting with the existing schedule name=%s", spec.name, exc_info=True
        )


def _build_worker(
    client: Client, spec: CronjobSpec, executor: ThreadPoolExecutor, run_metrics: CronjobMetrics
) -> Worker:
    # One activity slot, hardcoded: every cronjob tick is one CPU-bound pass,
    # and overlap-SKIP already guarantees a schedule never queues a second
    # one. RunMetricsInterceptor is this harness's single recording site
    # (see interceptor.py) -- every activity registered here gets metered
    # for free, so an individual cronjob never wires its own.
    return Worker(
        client,
        task_queue=spec.name,
        workflows=[spec.workflow],
        activities=spec.activities,
        activity_executor=executor,
        max_concurrent_activities=1,
        interceptors=[RunMetricsInterceptor(run_metrics)],
    )


async def run_cronjob(spec: CronjobSpec) -> None:
    """Connect, ensure the schedule, then serve the task queue until stopped.

    Activities run on a thread pool because these ticks are CPU-bound rather
    than IO-bound; leaving them on the event loop would stall heartbeats and
    schedule polling for the duration of a run.

    SIGTERM/SIGINT stop the worker gracefully (the Go runner's
    signal.NotifyContext equivalent): the worker stops polling and tells the
    server, instead of dying mid-poll and leaving the tick to surface as a
    timeout hours later.
    """
    # Must run before CronjobMetrics(): instrument creation binds to
    # whichever MeterProvider is global at that moment (see
    # init_metrics_provider's docstring).
    shutdown_metrics = init_metrics_provider(spec.name)
    run_metrics = CronjobMetrics()
    client = await connect()
    await ensure_schedule(client, spec)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            worker = _build_worker(client, spec, executor, run_metrics)
            async with worker:
                logger.info("worker running task_queue=%s", spec.name)
                await stop.wait()
            logger.info("worker stopped task_queue=%s", spec.name)
    finally:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.remove_signal_handler(sig)
        shutdown_metrics()

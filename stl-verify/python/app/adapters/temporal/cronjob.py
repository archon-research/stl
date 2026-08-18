"""Client connect, schedule ensure, and worker run for Python Temporal cronjobs.

The Go side of the service does the same three things in
`internal/adapters/outbound/temporal.RunCronjob`. Keep the two in step: the
cronjob name is simultaneously the task queue and the schedule id, and a
restart must never fail because the schedule survived from a previous run.
"""

import logging
import os
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
)
from temporalio.worker import Worker

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
    """Create the schedule, tolerating one that already exists.

    Temporal owns the schedule, so an interval change in the environment does
    not propagate: the schedule must be deleted in the UI or CLI and the worker
    restarted. This is the documented repo behaviour, not an oversight.
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
        logger.info("schedule already exists, leaving it untouched name=%s", spec.name)


async def run_cronjob(spec: CronjobSpec, *, max_concurrent_activities: int = 1) -> None:
    """Connect, ensure the schedule, then serve the task queue until cancelled.

    Activities run on a thread pool because these ticks are CPU-bound rather
    than IO-bound; leaving them on the event loop would stall heartbeats and
    schedule polling for the duration of a run.
    """
    client = await connect()
    await ensure_schedule(client, spec)
    with ThreadPoolExecutor(max_workers=max_concurrent_activities) as executor:
        worker = Worker(
            client,
            task_queue=spec.name,
            workflows=[spec.workflow],
            activities=spec.activities,
            activity_executor=executor,
            max_concurrent_activities=max_concurrent_activities,
        )
        logger.info("worker running task_queue=%s", spec.name)
        await worker.run()

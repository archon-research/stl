"""Activity-level interceptor that records cronjob.runs.total /
cronjob.run.duration_seconds for every activity execution.

Mirrors the Go harness's runMetricsInterceptor
(internal/adapters/outbound/temporal/interceptor.go): wrapping at the
activity level, rather than adding a call inside each cronjob's own activity
body, is what makes this a single recording site shared by every Python
cronjob (core-model-runner and anything registered after it) instead of
per-cronjob boilerplate.

Like the Go interceptor, a run interrupted by activity cancellation (worker
shutdown during a deploy rollout, or a schedule cancel) is classified
"canceled" rather than "error", so a deploy landing mid-run does not trip
VectorCronjobRunFailing. The SDK raises temporalio.exceptions.CancelledError
into the activity's own thread for a sync activity on a ThreadPoolExecutor
executor (the harness's shape -- see cronjob.py) and asyncio.CancelledError
for an async one; both propagate through execute_activity like any other
exception, so classifying them here is enough -- no separate ctx.Err() check
the way the Go side needs one.
"""

import asyncio
import logging
import time
from typing import Any

from temporalio.exceptions import CancelledError as ActivityCancelledError
from temporalio.worker import (
    ActivityInboundInterceptor,
    ExecuteActivityInput,
    Interceptor,
)

from app.adapters.temporal.metrics import CronjobMetrics

logger = logging.getLogger(__name__)


class RunMetricsInterceptor(Interceptor):
    def __init__(self, run_metrics: CronjobMetrics) -> None:
        self._metrics = run_metrics

    def intercept_activity(self, next: ActivityInboundInterceptor) -> ActivityInboundInterceptor:
        return _ActivityRunRecorder(next, self._metrics)


class _ActivityRunRecorder(ActivityInboundInterceptor):
    def __init__(self, next: ActivityInboundInterceptor, run_metrics: CronjobMetrics) -> None:
        super().__init__(next)
        self._metrics = run_metrics

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        start = time.monotonic()
        status = "success"
        try:
            return await self.next.execute_activity(input)
        except (asyncio.CancelledError, ActivityCancelledError):
            status = "canceled"
            raise
        except BaseException:
            status = "error"
            raise
        finally:
            # A broken recording must never override the activity's own
            # result/exception -- Python's finally semantics replace a
            # pending return or propagating exception with whatever this
            # block raises, so a metrics bug would otherwise masquerade as
            # the activity's outcome.
            try:
                self._metrics.record_run(time.monotonic() - start, status)
            except Exception:
                logger.warning("failed to record cronjob run metrics", exc_info=True)

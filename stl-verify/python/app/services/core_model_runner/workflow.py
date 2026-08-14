"""Temporal workflow and activity wrapping one CORE model run.

The workflow body stays trivial on purpose: Temporal re-imports this module
inside a sandbox, so the model stack (pandas, arch, sqlalchemy) is imported
through `imports_passed_through` rather than being re-executed there.
"""

import asyncio
from datetime import timedelta

from temporalio import activity, workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from app.services.core_model_runner.service import run_markets
    from cli.cronjobs.core_model_runner.config import RunnerConfig

# Matches the activeDeadlineSeconds of the k8s CronJob this replaced: a full
# "all markets" pass at the default N_MC is hours of Monte Carlo, not minutes.
TICK_TIMEOUT = timedelta(hours=4)


@activity.defn(name="run_core_model")
def run_core_model_activity(market_key: str) -> None:
    """Run every configured market. Sync on purpose -- see the executor note.

    The harness runs activities on a thread pool, so this blocking call does
    not stall the worker's event loop. `asyncio.run` is safe here because the
    thread has no loop of its own.
    """
    configs = RunnerConfig.resolve(market_key)
    asyncio.run(run_markets(configs))


@workflow.defn(name="CoreModelRunnerTick")
class CoreModelRunnerWorkflow:
    @workflow.run
    async def run(self, market_key: str) -> None:
        await workflow.execute_activity(
            run_core_model_activity,
            market_key,
            start_to_close_timeout=TICK_TIMEOUT,
            # No retries. The inputs are static until the next scheduled window,
            # so a retry would recompute the same answer -- and because
            # core_model_results is append-only, a mid-run retry would duplicate
            # rows for the markets that already succeeded.
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

"""Temporal workflow for one CORE model tick.

Deliberately imports nothing from the model stack. Temporal re-imports the
workflow module inside a sandbox, and numpy cannot be loaded twice in one
process, so pulling the service in here — even via `imports_passed_through` —
fails worker startup. The activity is referenced by name instead and lives in
`activity.py`, which the sandbox never touches.
"""

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

ACTIVITY_NAME = "run_core_model"

# Matches the activeDeadlineSeconds of the k8s CronJob this replaced: a full
# "all markets" pass at the default N_MC is hours of Monte Carlo, not minutes.
TICK_TIMEOUT = timedelta(hours=4)


@workflow.defn(name="CoreModelRunnerTick")
class CoreModelRunnerWorkflow:
    @workflow.run
    async def run(self, market_key: str) -> None:
        await workflow.execute_activity(
            ACTIVITY_NAME,
            market_key,
            start_to_close_timeout=TICK_TIMEOUT,
            # No retries. The inputs are static until the next scheduled window,
            # so a retry would recompute the same answer -- and because
            # core_model_results is append-only, a mid-run retry would duplicate
            # rows for the markets that already succeeded.
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

"""The Temporal activity that runs one CORE model tick.

Kept out of `workflow.py` on purpose: activities are not sandboxed, so this is
the only side of the pair that may import the model stack.
"""

import asyncio

from temporalio import activity

from app.services.core_model_runner.config import RunnerConfig
from app.services.core_model_runner.service import run_markets
from app.services.core_model_runner.workflow import ACTIVITY_NAME


@activity.defn(name=ACTIVITY_NAME)
def run_core_model_activity(market_key: str) -> None:
    """Run every market the key resolves to.

    Sync on purpose. The harness runs activities on a thread pool, so this
    blocking call — hours of GARCH fitting and Monte Carlo — does not stall the
    worker's event loop. `asyncio.run` is safe here because the pool thread has
    no loop of its own.
    """
    configs = RunnerConfig.resolve(market_key)
    asyncio.run(run_markets(configs))

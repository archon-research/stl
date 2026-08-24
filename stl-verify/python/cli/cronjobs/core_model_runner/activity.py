"""The Temporal activity that runs one CORE model tick.

Lives in the entry-point layer with the rest of the wiring: it composes the
engine, writer and data reader, so `app/services` stays on ports alone. Kept
out of `workflow.py` on purpose: activities are not sandboxed, so this is the
only side of the pair that may import the model stack.
"""

import asyncio
import os

from temporalio import activity

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader
from app.adapters.postgres.core_model_results_writer import PostgresCoreModelResultsWriter
from app.adapters.postgres.engine import create_db_engine
from app.config import async_database_url
from app.services.core_model_runner.config import RunnerConfig
from app.services.core_model_runner.service import run_markets
from app.services.core_model_runner.workflow import ACTIVITY_NAME


async def run_tick(market_key: str) -> None:
    """Resolve, wire, run, dispose — shared by the activity and the CLI --once.

    The engine is created per tick, inside the tick's own event loop: the sync
    activity runs `asyncio.run` on a pool thread, and asyncpg connections are
    bound to the loop that created them, so a process-lifetime engine would
    break on the second tick.
    """
    configs = RunnerConfig.resolve(market_key)
    # os.environ, not Settings: a missing ExternalSecret must fail loudly, where
    # Settings would silently fall back to .env.default's localhost URL.
    engine = create_db_engine(async_database_url(os.environ["DATABASE_URL"]))
    try:
        await run_markets(
            configs,
            PostgresCoreModelResultsWriter(engine),
            lambda cfg: ParquetCoreModelDataReader(cfg.inputs_dir),
        )
    finally:
        await engine.dispose()


@activity.defn(name=ACTIVITY_NAME)
def run_core_model_activity(market_key: str) -> None:
    """Run every market the key resolves to.

    Sync on purpose. The harness runs activities on a thread pool, so this
    blocking call — hours of GARCH fitting and Monte Carlo — does not stall the
    worker's event loop. `asyncio.run` is safe here because the pool thread has
    no loop of its own.
    """
    asyncio.run(run_tick(market_key))

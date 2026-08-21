"""Compute CRR for one or more markets and append the results to Postgres.

This is the body of one cronjob tick, and also what the one-shot CLI path runs.
The Temporal entry point in `cli/cronjobs/core_model_runner/` owns scheduling
and nothing else.
"""

import logging

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader
from app.adapters.postgres.core_model_results_writer import PostgresCoreModelResultsWriter
from app.adapters.postgres.engine import create_worker_db_engine
from app.ports.core_model_results_writer import CoreModelResultsWriter
from app.risk_engine.core_model.runner import CoreModelConfig, CoreModelPipelineResult, run
from app.services.core_model_runner.config import RunnerConfig

logger = logging.getLogger(__name__)


class CoreModelRunnerService:
    def __init__(self, results_writer: CoreModelResultsWriter) -> None:
        self._results_writer = results_writer

    async def run_market(self, cfg: RunnerConfig) -> CoreModelPipelineResult:
        data_reader = ParquetCoreModelDataReader(cfg.inputs_dir)
        config = CoreModelConfig(market_key=cfg.market_key, params=cfg.params)
        result = await run(config, data_reader, cfg.inputs_dir)
        logger.info("pipeline complete market_key=%s crr_el_pct=%s", result.market_key, result.crr_el_pct)
        await self._results_writer.insert(result)
        logger.info("result written to core_model_results market_key=%s", result.market_key)
        return result


async def run_markets(configs: list[RunnerConfig]) -> None:
    """Run every config against one engine, then fail if any market failed.

    Deliberately the tick's composition root: both entry points (the Temporal
    activity and the CLI --once path) call this and stay wiring-free, so the
    adapter imports above are this function's to make, and swapping data
    sources (parquet -> live tables) is a one-place edit here.

    A failing market does not abort its siblings: the markets are independent,
    and one broken input set (Galaxy's missing order books, say) should not
    withhold every other market's result. The failures are still raised at the
    end so the tick is not recorded as a success.
    """
    if not configs:
        raise ValueError("no market configs to run")

    engine = create_worker_db_engine(configs[0].database_url)
    service = CoreModelRunnerService(PostgresCoreModelResultsWriter(engine))
    failed: list[str] = []
    try:
        for cfg in configs:
            # N_MC is logged so a mistyped override (which falls back to the
            # per-market config silently) is visible in the run output.
            logger.info(
                "running market_key=%s protocol=%s n_mc=%s",
                cfg.market_key,
                cfg.params["PROTOCOL"],
                cfg.params["N_MC"],
            )
            try:
                await service.run_market(cfg)
            except Exception:
                logger.exception("failed market_key=%s -- continuing", cfg.market_key)
                failed.append(cfg.market_key)
    finally:
        await engine.dispose()

    if failed:
        raise RuntimeError(f"one or more markets failed: {failed}")

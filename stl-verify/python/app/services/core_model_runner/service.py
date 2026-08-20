"""Compute CRR for one or more markets and append the results to Postgres.

This is the body of one cronjob tick, and also what the one-shot CLI path runs.
The Temporal entry point in `cli/cronjobs/core_model_runner/` owns scheduling
and nothing else.
"""

import json
import logging

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader
from app.risk_engine.core_model.runner import CoreModelConfig, CoreModelPipelineResult, run
from app.services.core_model_runner.config import RunnerConfig

logger = logging.getLogger(__name__)

_INSERT_RESULT = text("""
    INSERT INTO core_model_results
        (market_key, crr_el_pct, crr_es_pct, crr_var_pct, hhi,
         protocol, forecast_step, n_mc, copula_type, computed_at, params)
    VALUES
        (:market_key, :crr_el_pct, :crr_es_pct, :crr_var_pct, :hhi,
         :protocol, :forecast_step, :n_mc, :copula_type, :computed_at, :params)
""")


def async_db_url(database_url: str) -> str:
    url = make_url(database_url).set(drivername="postgresql+asyncpg")
    query = dict(url.query)
    query.pop("sslmode", None)
    return url.set(query=query).render_as_string(hide_password=False)


class CoreModelRunnerService:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def run_market(self, cfg: RunnerConfig) -> CoreModelPipelineResult:
        data_reader = ParquetCoreModelDataReader(cfg.inputs_dir)
        config = CoreModelConfig(market_key=cfg.market_key, params=cfg.params)
        result = await run(config, data_reader, cfg.inputs_dir)
        logger.info("pipeline complete market_key=%s crr_el_pct=%s", result.market_key, result.crr_el_pct)
        await self._persist(result)
        logger.info("result written to core_model_results market_key=%s", result.market_key)
        return result

    async def _persist(self, result: CoreModelPipelineResult) -> None:
        async with self._engine.begin() as conn:
            await conn.execute(
                _INSERT_RESULT,
                {
                    "market_key": result.market_key,
                    # Decimals go to the NUMERIC columns as-is: a float()
                    # round-trip would re-introduce the binary artifacts the
                    # runner already stripped (0.110351 -> 0.11035100000000000464...).
                    "crr_el_pct": result.crr_el_pct,
                    "crr_es_pct": result.crr_es_pct,
                    "crr_var_pct": result.crr_var_pct,
                    "hhi": result.hhi,
                    "protocol": result.protocol,
                    "forecast_step": result.forecast_step,
                    "n_mc": result.n_mc,
                    "copula_type": result.copula_type,
                    "computed_at": result.computed_at,
                    "params": json.dumps(result.params),
                },
            )


async def run_markets(configs: list[RunnerConfig]) -> None:
    """Run every config against one engine, then fail if any market failed.

    A failing market does not abort its siblings: the markets are independent,
    and one broken input set (Galaxy's missing order books, say) should not
    withhold every other market's result. The failures are still raised at the
    end so the tick is not recorded as a success.
    """
    if not configs:
        raise ValueError("no market configs to run")

    engine = create_async_engine(async_db_url(configs[0].database_url), pool_pre_ping=True)
    service = CoreModelRunnerService(engine)
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

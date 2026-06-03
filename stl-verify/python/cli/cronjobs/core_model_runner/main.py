"""CORE model runner -- compute CRR for one market or all markets and write to DB.

Usage (single market):
    DATABASE_URL=postgresql://... \\
    CORE_MODEL_MARKET_KEY=sparklend_usdt \\
    uv run python -m cli.cronjobs.core_model_runner.main

Usage (all markets):
    DATABASE_URL=postgresql://... \\
    CORE_MODEL_MARKET_KEY=all \\
    uv run python -m cli.cronjobs.core_model_runner.main

Override any param for all markets in an "all" run (e.g. quick test):
    DATABASE_URL=... CORE_MODEL_MARKET_KEY=all CORE_MODEL_N_MC=100 uv run python -m ...

Params inherit: default_params.json -> market_configs.json[key] -> env vars.
"""

import asyncio
import json
import logging

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from app.adapters.parquet.core_model_data_reader import ParquetCoreModelDataReader
from app.risk_engine.core_model.runner import CoreModelConfig, CoreModelPipelineResult, run
from cli.cronjobs.core_model_runner.config import RunnerConfig

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _async_db_url(database_url: str) -> str:
    url = make_url(database_url)
    url = url.set(drivername="postgresql+asyncpg")
    query = dict(url.query)
    query.pop("sslmode", None)
    return url.set(query=query).render_as_string(hide_password=False)


async def _write_result(engine, result: CoreModelPipelineResult) -> None:
    query = text("""
        INSERT INTO core_model_results
            (market_key, crr_el_pct, crr_es_pct, crr_var_pct, hhi,
             protocol, forecast_step, n_mc, copula_type, computed_at, params)
        VALUES
            (:market_key, :crr_el_pct, :crr_es_pct, :crr_var_pct, :hhi,
             :protocol, :forecast_step, :n_mc, :copula_type, :computed_at, :params)
    """)
    async with engine.begin() as conn:
        await conn.execute(
            query,
            {
                "market_key": result.market_key,
                "crr_el_pct": float(result.crr_el_pct),
                "crr_es_pct": float(result.crr_es_pct),
                "crr_var_pct": float(result.crr_var_pct),
                "hhi": float(result.hhi) if result.hhi is not None else None,
                "protocol": result.protocol,
                "forecast_step": result.forecast_step,
                "n_mc": result.n_mc,
                "copula_type": result.copula_type,
                "computed_at": result.computed_at,
                "params": json.dumps(result.params),
            },
        )


async def _run_one(cfg: RunnerConfig, engine) -> None:
    data_reader = ParquetCoreModelDataReader(cfg.inputs_dir)
    config = CoreModelConfig(market_key=cfg.market_key, params=cfg.params)
    result = await run(config, data_reader, cfg.inputs_dir)
    logger.info("pipeline complete market_key=%s crr_el_pct=%s", result.market_key, result.crr_el_pct)
    await _write_result(engine, result)
    logger.info("result written to core_model_results market_key=%s", result.market_key)


async def main() -> None:
    import os

    market_key = os.environ.get("CORE_MODEL_MARKET_KEY", "")

    if market_key == "all":
        configs = RunnerConfig.all_from_env()
        logger.info("starting core-model-runner for all %d markets", len(configs))
        engine = create_async_engine(_async_db_url(configs[0].database_url), pool_pre_ping=True)
        failed: list[str] = []
        try:
            for cfg in configs:
                logger.info("running market_key=%s protocol=%s", cfg.market_key, cfg.params["PROTOCOL"])
                try:
                    await _run_one(cfg, engine)
                except Exception:
                    logger.exception("failed market_key=%s -- continuing", cfg.market_key)
                    failed.append(cfg.market_key)
        finally:
            await engine.dispose()
        if failed:
            raise RuntimeError(f"one or more markets failed: {failed}")
    else:
        cfg = RunnerConfig.from_env()
        logger.info("starting core-model-runner market_key=%s protocol=%s", cfg.market_key, cfg.params["PROTOCOL"])
        engine = create_async_engine(_async_db_url(cfg.database_url), pool_pre_ping=True)
        try:
            await _run_one(cfg, engine)
        finally:
            await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

"""CORE model runner -- compute CRR for one protocol market and write to DB.

Usage:
    DATABASE_URL=postgresql://... \\
    CORE_MODEL_MARKET_KEY=sparklend_usdc \\
    CORE_MODEL_PROTOCOL=SPARKLEND \\
    CORE_MODEL_LOAN_TOKEN=USDC \\
    python -m cli.cronjobs.core_model_runner.main

All CORE model params default to inputs/default_params.json values.
Override any param via env var -- see config.py for the full mapping.
"""

import asyncio
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
             protocol, forecast_step, n_mc, copula_type, computed_at)
        VALUES
            (:market_key, :crr_el_pct, :crr_es_pct, :crr_var_pct, :hhi,
             :protocol, :forecast_step, :n_mc, :copula_type, :computed_at)
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
            },
        )


async def main() -> None:
    cfg = RunnerConfig.from_env()
    logger.info("starting core-model-runner market_key=%s protocol=%s", cfg.market_key, cfg.params["PROTOCOL"])

    engine = create_async_engine(_async_db_url(cfg.database_url), pool_pre_ping=True)
    data_reader = ParquetCoreModelDataReader(cfg.inputs_dir)
    config = CoreModelConfig(market_key=cfg.market_key, params=cfg.params)

    try:
        result = await run(config, data_reader, cfg.inputs_dir)
        logger.info(
            "pipeline complete market_key=%s crr_el_pct=%s",
            result.market_key,
            result.crr_el_pct,
        )
        await _write_result(engine, result)
        logger.info("result written to core_model_results market_key=%s", result.market_key)
    finally:
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())

"""Compute CRR for one or more markets and append the results through a port.

This is the body of one cronjob tick. It depends on ports alone: the entry
point (cli/cronjobs/core_model_runner) owns the wiring — engine, writer and
data-reader factory — and passes it in, so swapping data sources
(parquet -> live tables) is an entry-point edit, not a service edit.
"""

import logging
from collections.abc import Callable
from pathlib import Path

from app.ports.core_model_data_reader import CoreModelDataReader
from app.ports.core_model_results_writer import CoreModelResultsWriter
from app.risk_engine.core_model.config import INPUTS_DIR
from app.risk_engine.core_model.runner import CoreModelConfig, CoreModelPipelineResult, run
from app.services.core_model_runner.config import RunnerConfig

logger = logging.getLogger(__name__)

# The non-reader input files (protocol_defense.json) ship at a fixed path
# inside the image; this is a packaging constant, not per-market config.
_INPUTS = Path(INPUTS_DIR)


async def _run_market(
    cfg: RunnerConfig,
    writer: CoreModelResultsWriter,
    data_reader: CoreModelDataReader,
) -> CoreModelPipelineResult:
    config = CoreModelConfig(market_key=cfg.market_key, params=cfg.params)
    result = await run(config, data_reader, _INPUTS)
    logger.info("pipeline complete market_key=%s crr_el_pct=%s", result.market_key, result.crr_el_pct)
    await writer.insert(result)
    logger.info("result written to core_model_results market_key=%s", result.market_key)
    return result


async def run_markets(
    configs: list[RunnerConfig],
    writer: CoreModelResultsWriter,
    make_data_reader: Callable[[RunnerConfig], CoreModelDataReader],
) -> None:
    """Run every config against the injected writer, then fail if any market failed.

    A failing market does not abort its siblings: the markets are independent,
    and one broken input set (Galaxy's missing order books, say) should not
    withhold every other market's result. The failures are still raised at the
    end so the tick is not recorded as a success.
    """
    if not configs:
        raise ValueError("no market configs to run")

    failed: list[str] = []
    for cfg in configs:
        # N_MC is logged so a mistyped override (which falls back to the
        # per-market config silently) is visible in the run output.
        logger.info(
            "running market_key=%s protocol=%s n_mc=%s orderbook_source=%s price_source=%s position_source=%s",
            cfg.market_key,
            cfg.params["PROTOCOL"],
            cfg.params["N_MC"],
            cfg.orderbook_source,
            cfg.price_source,
            cfg.position_source,
        )
        try:
            await _run_market(cfg, writer, make_data_reader(cfg))
        except Exception:
            logger.exception("failed market_key=%s -- continuing", cfg.market_key)
            failed.append(cfg.market_key)

    if failed:
        raise RuntimeError(f"one or more markets failed: {failed}")

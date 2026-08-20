"""CORE model runner entry point. Scheduling only -- the tick body is a service.

Default mode is a Temporal worker: it registers the schedule and then serves
the task queue, matching every other cronjob in the repo.

    TEMPORAL_HOST_PORT=localhost:7233 DATABASE_URL=postgresql://... \\
    CORE_MODEL_MARKET_KEY=all \\
    uv run python -m cli.cronjobs.core_model_runner.main

`--once` runs a single pass in-process and exits, with no Temporal involved.
That is what `make run-core-model` uses, and what a hand-triggered run for one
market looks like:

    DATABASE_URL=postgresql://... CORE_MODEL_MARKET_KEY=sparklend_usdt \\
    uv run python -m cli.cronjobs.core_model_runner.main --once

Params inherit: default_params.json -> market_configs.json[key] -> env vars.
Changing CORE_MODEL_RUN_INTERVAL_HOURS does not move an existing schedule --
delete it in Temporal and restart the worker (see CONTRIBUTING.md).
"""

import argparse
import asyncio
import logging
import os
from datetime import timedelta

from app.adapters.temporal import CronjobSpec, run_cronjob
from app.services.core_model_runner.activity import run_core_model_activity
from app.services.core_model_runner.config import RunnerConfig
from app.services.core_model_runner.service import run_markets
from app.services.core_model_runner.workflow import CoreModelRunnerWorkflow

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

NAME = "core-model-runner"
_DEFAULT_INTERVAL_HOURS = "24"


def _market_key() -> str:
    return os.environ["CORE_MODEL_MARKET_KEY"]


def _interval() -> timedelta:
    return timedelta(hours=float(os.getenv("CORE_MODEL_RUN_INTERVAL_HOURS", _DEFAULT_INTERVAL_HOURS)))


async def run_once() -> None:
    configs = RunnerConfig.resolve(_market_key())
    logger.info("one-shot run for %d market(s)", len(configs))
    await run_markets(configs)


async def run_worker() -> None:
    market_key = _market_key()
    logger.info("starting %s worker market_key=%s interval=%s", NAME, market_key, _interval())
    await run_cronjob(
        CronjobSpec(
            name=NAME,
            interval=_interval(),
            workflow=CoreModelRunnerWorkflow,
            activities=[run_core_model_activity],
            workflow_args=[market_key],
        )
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog=f"python -m cli.cronjobs.{NAME.replace('-', '_')}.main")
    parser.add_argument(
        "--once",
        action="store_true",
        help="run a single pass in-process and exit, without connecting to Temporal",
    )
    args = parser.parse_args(argv)
    asyncio.run(run_once() if args.once else run_worker())


if __name__ == "__main__":
    main()

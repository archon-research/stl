"""CORE model runner entry point. Scheduling and wiring -- the tick body is a service.

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
CORE_MODEL_RUN_INTERVAL_HOURS is reconciled into the existing Temporal
schedule on worker startup, so changing it needs only a redeploy.
"""

import argparse
import asyncio
import logging
import os
from datetime import timedelta

from app.adapters.temporal import CronjobSpec, run_cronjob
from app.logging import setup_logging
from app.services.core_model_runner.workflow import CoreModelRunnerWorkflow
from cli.cronjobs.core_model_runner.activity import run_core_model_activity, run_tick

logger = logging.getLogger(__name__)

NAME = "core-model-runner"
_DEFAULT_INTERVAL_HOURS = "24"


def _configure_logging() -> None:
    # One config for both trees this process logs under (harness/service/model
    # log under app.*, this entry point under cli.*), so every line in the pod
    # has the repo's shape and LOG_LEVEL/LOG_FORMAT work like they do for the API.
    setup_logging(
        os.getenv("LOG_LEVEL", "INFO"),
        os.getenv("LOG_FORMAT", "json"),
        logger_names=("app", "cli"),
    )


def _market_key() -> str:
    return os.environ["CORE_MODEL_MARKET_KEY"]


def _interval() -> timedelta:
    return timedelta(hours=float(os.getenv("CORE_MODEL_RUN_INTERVAL_HOURS", _DEFAULT_INTERVAL_HOURS)))


async def run_once() -> None:
    market_key = _market_key()
    logger.info("one-shot run market_key=%s", market_key)
    await run_tick(market_key)


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
    _configure_logging()
    asyncio.run(run_once() if args.once else run_worker())


if __name__ == "__main__":
    main()

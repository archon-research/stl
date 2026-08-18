"""The core-model tick runs end to end against a real Temporal server.

The unit tests assert how the workflow is wired. This one actually starts a
Temporal dev server, runs a worker, and drives the workflow, so a mistake in
the sandbox imports or the activity's threading shows up here rather than on
first deploy.
"""

import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from app.services.core_model_runner.workflow import CoreModelRunnerWorkflow


@pytest.fixture()
async def env():
    async with await WorkflowEnvironment.start_local() as environment:
        yield environment


async def _run(env, stub_activity, market_key: str = "all") -> None:
    task_queue = f"core-model-test-{uuid.uuid4()}"
    # The real activity is sync so the CPU-bound tick stays off the event loop;
    # Temporal then requires an executor, exactly as the harness provides.
    with ThreadPoolExecutor(max_workers=1) as executor:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[CoreModelRunnerWorkflow],
            activities=[stub_activity],
            activity_executor=executor,
        ):
            await env.client.execute_workflow(
                CoreModelRunnerWorkflow.run,
                market_key,
                id=f"wf-{uuid.uuid4()}",
                task_queue=task_queue,
            )


async def test_workflow_passes_the_market_key_to_the_activity(env):
    seen: list[str] = []

    @activity.defn(name="run_core_model")
    def _stub(market_key: str) -> None:
        seen.append(market_key)

    await _run(env, _stub, market_key="sparklend_usdt")
    assert seen == ["sparklend_usdt"]


async def test_a_failing_tick_is_not_retried(env):
    attempts: list[int] = []

    @activity.defn(name="run_core_model")
    def _stub(market_key: str) -> None:
        attempts.append(1)
        raise RuntimeError("market blew up")

    with pytest.raises(WorkflowFailureError):
        await _run(env, _stub)

    assert len(attempts) == 1

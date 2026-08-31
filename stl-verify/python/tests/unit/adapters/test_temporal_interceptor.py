"""Unit tests for RunMetricsInterceptor, the harness's single site that
records cronjob.runs.total / cronjob.run.duration_seconds for every activity
execution. Mirrors internal/adapters/outbound/temporal/interceptor.go's test
coverage: success/error/canceled each land on their own status, and the
interceptor still re-raises so Temporal sees the activity's real outcome.
"""

import asyncio
from typing import cast
from unittest.mock import MagicMock

import pytest
from temporalio.exceptions import CancelledError as ActivityCancelledError
from temporalio.worker import ActivityInboundInterceptor, ExecuteActivityInput

from app.adapters.temporal.interceptor import RunMetricsInterceptor


class _FakeNext:
    """Stands in for the next ActivityInboundInterceptor in the chain."""

    def __init__(self, result=None, error: BaseException | None = None) -> None:
        self._result = result
        self._error = error
        self.calls: list[ExecuteActivityInput] = []

    async def execute_activity(self, input: ExecuteActivityInput):
        self.calls.append(input)
        if self._error is not None:
            raise self._error
        return self._result


def _input() -> ExecuteActivityInput:
    return ExecuteActivityInput(fn=lambda: None, args=[], executor=None, headers={})


def _recorder(metrics, fake_next: _FakeNext) -> ActivityInboundInterceptor:
    return RunMetricsInterceptor(metrics).intercept_activity(cast(ActivityInboundInterceptor, fake_next))


async def test_records_status_success_and_returns_the_activitys_result():
    metrics = MagicMock()
    recorder = _recorder(metrics, _FakeNext(result="ok"))

    result = await recorder.execute_activity(_input())

    assert result == "ok"
    (duration, status), _ = metrics.record_run.call_args
    assert duration >= 0
    assert status == "success"


@pytest.mark.parametrize(
    ("error", "want_status"),
    [
        (ValueError("upstream failed"), "error"),
        (RuntimeError("boom"), "error"),
        (asyncio.CancelledError(), "canceled"),
        (ActivityCancelledError(), "canceled"),
    ],
)
async def test_records_status_by_the_activitys_exception_type(error, want_status):
    # asyncio.CancelledError: how an async activity's task-cancel surfaces.
    # temporalio.exceptions.CancelledError: how the SDK cancels a sync
    # activity running on the harness's ThreadPoolExecutor (see cronjob.py) --
    # both must land on "canceled", not "error", or a deploy landing mid-tick
    # trips VectorCronjobRunFailing the way it used to on the Go side before
    # that split existed.
    metrics = MagicMock()
    recorder = _recorder(metrics, _FakeNext(error=error))

    with pytest.raises(type(error)):
        await recorder.execute_activity(_input())

    (duration, status), _ = metrics.record_run.call_args
    assert duration >= 0
    assert status == want_status


async def test_reraises_the_activitys_exact_exception_object():
    metrics = MagicMock()
    boom = ValueError("upstream failed")
    recorder = _recorder(metrics, _FakeNext(error=boom))

    with pytest.raises(ValueError) as exc_info:
        await recorder.execute_activity(_input())
    assert exc_info.value is boom


async def test_delegates_the_call_to_the_next_interceptor():
    metrics = MagicMock()
    fake_next = _FakeNext(result=None)
    recorder = _recorder(metrics, fake_next)

    activity_input = _input()
    await recorder.execute_activity(activity_input)

    assert fake_next.calls == [activity_input]


async def test_a_broken_recording_does_not_hide_a_successful_result():
    # record_run raising must not replace the activity's own return value --
    # Python's `finally` semantics would otherwise let a metrics bug
    # masquerade as the activity's outcome.
    metrics = MagicMock()
    metrics.record_run.side_effect = RuntimeError("metrics backend exploded")
    recorder = _recorder(metrics, _FakeNext(result="ok"))

    result = await recorder.execute_activity(_input())

    assert result == "ok"


async def test_a_broken_recording_does_not_hide_the_activitys_real_exception():
    metrics = MagicMock()
    metrics.record_run.side_effect = RuntimeError("metrics backend exploded")
    boom = ValueError("upstream failed")
    recorder = _recorder(metrics, _FakeNext(error=boom))

    with pytest.raises(ValueError, match="upstream failed"):
        await recorder.execute_activity(_input())

"""The one 422 body for the whole API surface, and the handlers that emit it.

A rejection is machine-readable first: history is never truncated, so this body
is the only signal a caller gets that a request was refused, and a client has to
be able to re-tile a window or drop to a fitting frequency without parsing prose.
One model rather than one per rejection, so the conformance harness has a single
contract to assert against every route.

Both a domain ``TimeSeriesQueryError`` and FastAPI's own ``RequestValidationError``
answer with it, so an unparseable timestamp and an oversized window are the same
shape to a client.
"""

from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.domain.time_series import (
    MaxPointsExceededError,
    TimeSeriesFrequency,
    TimeSeriesQueryError,
)
from app.logging import get_logger

logger = get_logger(__name__)

# Every rejection FastAPI raises before a route is reached — an unparseable
# timestamp, an out-of-range limit, an unknown enum value — under one code, since
# the per-field detail is in the message and a client's branch is the same either
# way: fix the request.
INVALID_REQUEST_CODE = "invalid_request"


class ApiRejectionError(Exception):
    """A caller-fixable rejection raised by a route, outside the time-series policy.

    The counterpart to ``TimeSeriesQueryError`` for the rest of the surface: a bad
    parameter combination, a malformed identifier, a value out of range. Raised
    rather than returned so a route rejects where it notices, and answered with the
    same body — ``HTTPException(422, detail=...)`` would put a bare string on the
    wire under a schema that promises this model.
    """

    def __init__(self, message: str, *, error_code: str = INVALID_REQUEST_CODE) -> None:
        super().__init__(message)
        self.error_code = error_code


class ApiErrorResponse(BaseModel):
    """The body of every ``422`` on the surface.

    The suggestion fields are populated on a max-points rejection and absent
    otherwise, so a client can branch on ``error_code`` and read only what that
    code promises.
    """

    error_code: str = Field(description="Stable, machine-readable rejection code.", examples=["max_points_exceeded"])
    message: str = Field(description="Human-readable explanation. Never the only signal.")
    point_count: int | None = Field(
        default=None, description="Observations the request would return. Max-points rejections only."
    )
    max_points: int | None = Field(
        default=None, description="Ceiling the request exceeded. Max-points rejections only."
    )
    suggested_from_timestamp: datetime | None = Field(
        default=None,
        description=(
            "Lower bound of a narrower window to retry. Scaled by the requested window's "
            "average density, so it is exact only for evenly spaced observations: a series "
            "clustered in this span is rejected again, with a further-narrowed suggestion. "
            "Max-points rejections only, and absent once the scaled span rounds below a second."
        ),
    )
    suggested_to_timestamp: datetime | None = Field(
        default=None,
        description=(
            "Upper bound of the narrower window to retry — the requested upper bound. "
            "Max-points rejections only, and absent with `suggested_from_timestamp`."
        ),
    )
    suggested_frequency: TimeSeriesFrequency | None = Field(
        default=None,
        description=(
            "A frequency that fits the requested window as asked, with "
            "`aggregation_method=end-period`; unlike the window suggestion it needs no "
            "second round trip. Max-points rejections only."
        ),
    )


# Declared on every route through ``FastAPI(responses=...)``, which replaces
# FastAPI's default `HTTPValidationError` 422 so the schema matches what the
# handlers below actually return.
API_ERROR_RESPONSES: dict[int | str, dict] = {
    422: {"model": ApiErrorResponse, "description": "Request rejected; branch on `error_code`."}
}


def error_response(error: ApiErrorResponse) -> JSONResponse:
    """Serialize an error body, omitting the fields its code does not promise."""
    return JSONResponse(status_code=422, content=error.model_dump(mode="json", exclude_none=True))


def time_series_error(exc: TimeSeriesQueryError) -> ApiErrorResponse:
    """The body for a domain rejection, carrying suggestions where the code has them."""
    if isinstance(exc, MaxPointsExceededError):
        return ApiErrorResponse(
            error_code=exc.error_code,
            message=str(exc),
            point_count=exc.point_count,
            max_points=exc.max_points,
            suggested_from_timestamp=exc.suggested_from_timestamp,
            suggested_to_timestamp=exc.suggested_to_timestamp,
            suggested_frequency=exc.suggested_frequency,
        )
    return ApiErrorResponse(error_code=exc.error_code, message=str(exc))


def _validation_message(exc: RequestValidationError) -> str:
    """Name the offending parameters and why, without echoing what was sent."""
    return "; ".join(
        f"{'.'.join(str(part) for part in error.get('loc', ()))}: {error.get('msg', '')}" for error in exc.errors()
    )


def _log_validation_inputs(request: Request, exc: RequestValidationError) -> None:
    """Shape of what was sent, for diagnostics; the value itself never leaves the log."""
    for error in exc.errors():
        if "input" not in error:
            continue
        try:
            raw = error["input"]
            logger.debug(
                "Validation error input",
                extra={
                    "path": request.url.path,
                    "method": request.method,
                    "input_type": type(raw).__name__,
                    "input_len": len(str(raw)),
                },
            )
        except Exception:  # noqa: BLE001 - best-effort diagnostic logging
            pass


def register_error_handlers(application: FastAPI) -> None:
    """Wire every rejection path onto the shared body."""

    @application.exception_handler(ApiRejectionError)
    async def api_rejection_handler(request: Request, exc: ApiRejectionError) -> JSONResponse:
        return error_response(ApiErrorResponse(error_code=exc.error_code, message=str(exc)))

    @application.exception_handler(TimeSeriesQueryError)
    async def time_series_query_error_handler(request: Request, exc: TimeSeriesQueryError) -> JSONResponse:
        logger.warning(
            "Time-series query rejected",
            extra={"path": request.url.path, "method": request.method, "error_code": exc.error_code},
        )
        return error_response(time_series_error(exc))

    @application.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        errors = exc.errors()
        logger.warning(
            "Request validation failed",
            extra={
                "path": request.url.path,
                "method": request.method,
                "validation_error_count": len(errors),
            },
        )
        _log_validation_inputs(request, exc)
        return error_response(ApiErrorResponse(error_code=INVALID_REQUEST_CODE, message=_validation_message(exc)))

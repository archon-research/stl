"""The one 422 body for the whole API surface, and the handlers that emit it.

The body is an RFC 9457 problem detail: ``type`` identifies the rejection, ``title``
labels it, ``detail`` describes the occurrence, and everything specific to one
``type`` rides alongside as an RFC extension member. A rejection is machine-readable
first: history is never truncated, so this body is the only signal a caller gets that
a request was refused, and a client has to be able to re-tile a window or drop to a
fitting frequency without parsing prose. One model rather than one per rejection, so
the conformance harness has a single contract to assert against every route.

Both a domain ``TimeSeriesQueryError`` and FastAPI's own ``RequestValidationError``
answer with it, so an unparseable timestamp and an oversized window are the same
shape to a client.

``type`` carries a bare slug rather than the URI RFC 9457 asks for: the values are
the client's branch, and a URI would commit the surface to resolvable documentation
at every one of them.
"""

from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.domain.time_series import (
    AggregationMethod,
    MaxPointsExceededError,
    TimeSeriesFrequency,
    TimeSeriesQueryError,
)
from app.logging import get_logger

logger = get_logger(__name__)

# Every rejection FastAPI raises before a route is reached — an unparseable
# timestamp, an out-of-range limit, an unknown enum value — under one type, since
# the per-field detail is in ``detail`` and a client's branch is the same either
# way: fix the request.
INVALID_REQUEST_TYPE = "invalid_request"
INVALID_REQUEST_TITLE = "Invalid request"

# The status every body on this surface reports. RFC 9457 makes the member advisory
# and requires it to agree with the response's own code when present.
REJECTION_STATUS = 422


class ApiRejectionError(Exception):
    """A caller-fixable rejection raised by a route, outside the time-series policy.

    The counterpart to ``TimeSeriesQueryError`` for the rest of the surface: a bad
    parameter combination, a malformed identifier, a value out of range. Raised
    rather than returned so a route rejects where it notices, and answered with the
    same body — ``HTTPException(422, detail=...)`` would put a bare string on the
    wire under a schema that promises this model.
    """

    def __init__(
        self,
        message: str,
        *,
        error_type: str = INVALID_REQUEST_TYPE,
        title: str = INVALID_REQUEST_TITLE,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.title = title


class NarrowerWindow(BaseModel):
    """A window to retry the same request over, keyed like the query parameters.

    Scaled by the requested window's average density, so it is exact only for evenly
    spaced observations: a series clustered in this span is rejected again, with a
    further-narrowed suggestion.
    """

    from_timestamp: datetime = Field(description="Lower bound to retry with (UTC).")
    to_timestamp: datetime = Field(description="Upper bound to retry with — the requested one (UTC).")


class ResampledRetry(BaseModel):
    """A frequency to retry the same window on, keyed like the query parameters.

    Fits the window as asked, so unlike ``narrower_window`` it needs no second round
    trip and returns the whole span the caller requested.
    """

    frequency: TimeSeriesFrequency = Field(description="Grid to resample onto.")
    aggregation_method: AggregationMethod = Field(
        description="Method to cut on that grid. A frequency without one is itself a rejection."
    )


class RejectionSuggestions(BaseModel):
    """The ways out of a max-points rejection, each a complete set of query parameters.

    Grouped and keyed to match the request so a client merges one of them into the
    parameters it sent, with no key to rename or trim. The two are alternatives, not
    a set: taking both narrows a window that the frequency alone would have served
    in full.
    """

    narrower_window: NarrowerWindow | None = Field(
        default=None, description="Absent once the scaled span rounds below a second."
    )
    resampled: ResampledRetry | None = Field(default=None, description="Always present on a max-points rejection.")


class ApiErrorResponse(BaseModel):
    """The body of every ``422`` on the surface, as an RFC 9457 problem detail.

    The extension members below are populated on a max-points rejection and absent
    otherwise, so a client can branch on ``type`` and read only what that type
    promises.
    """

    type: str = Field(description="Stable, machine-readable rejection identifier.", examples=["max_points_exceeded"])
    title: str = Field(
        description="Short static label for the `type`. Same across every occurrence of one type.",
        examples=["Too many points"],
    )
    status: int = Field(default=REJECTION_STATUS, description="HTTP status of the response carrying this body.")
    detail: str = Field(description="Human-readable explanation of this occurrence. Never the only signal.")
    point_count: int | None = Field(
        default=None, description="Observations the request would return. Max-points rejections only."
    )
    max_points: int | None = Field(
        default=None, description="Ceiling the request exceeded. Max-points rejections only."
    )
    suggestions: RejectionSuggestions | None = Field(
        default=None, description="Ways out of the rejection. Max-points rejections only."
    )


# Declared on every route through ``FastAPI(responses=...)``, which replaces
# FastAPI's default `HTTPValidationError` 422 so the schema matches what the
# handlers below actually return.
API_ERROR_RESPONSES: dict[int | str, dict] = {
    422: {"model": ApiErrorResponse, "description": "Request rejected; branch on `type`."}
}


def error_response(error: ApiErrorResponse) -> JSONResponse:
    """Serialize a problem detail, omitting the extension members its type does not promise."""
    return JSONResponse(status_code=422, content=error.model_dump(mode="json", exclude_none=True))


def _suggestions(exc: MaxPointsExceededError) -> RejectionSuggestions:
    """Group a rejection's ways out into request-shaped parameter sets."""
    window = (
        NarrowerWindow(from_timestamp=exc.suggested_from_timestamp, to_timestamp=exc.suggested_to_timestamp)
        if exc.suggested_from_timestamp is not None and exc.suggested_to_timestamp is not None
        else None
    )
    return RejectionSuggestions(
        narrower_window=window,
        resampled=ResampledRetry(frequency=exc.suggested_frequency, aggregation_method=AggregationMethod.END_PERIOD),
    )


def time_series_error(exc: TimeSeriesQueryError) -> ApiErrorResponse:
    """The body for a domain rejection, carrying suggestions where the type has them."""
    if isinstance(exc, MaxPointsExceededError):
        return ApiErrorResponse(
            type=exc.error_type,
            title=exc.title,
            detail=str(exc),
            point_count=exc.point_count,
            max_points=exc.max_points,
            suggestions=_suggestions(exc),
        )
    return ApiErrorResponse(type=exc.error_type, title=exc.title, detail=str(exc))


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
        return error_response(ApiErrorResponse(type=exc.error_type, title=exc.title, detail=str(exc)))

    @application.exception_handler(TimeSeriesQueryError)
    async def time_series_query_error_handler(request: Request, exc: TimeSeriesQueryError) -> JSONResponse:
        logger.warning(
            "Time-series query rejected",
            extra={"path": request.url.path, "method": request.method, "error_type": exc.error_type},
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
        return error_response(
            ApiErrorResponse(type=INVALID_REQUEST_TYPE, title=INVALID_REQUEST_TITLE, detail=_validation_message(exc))
        )

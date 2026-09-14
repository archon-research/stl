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

from collections.abc import Mapping
from datetime import datetime
from enum import StrEnum
from typing import Any, ClassVar

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.domain.time_series import (
    AggregationMethod,
    FrequencyTooFineError,
    FrequencyWithoutAggregationMethodError,
    InvalidTimeRangeError,
    MaxPointsExceededError,
    OutOfRangeTimestampError,
    TimeSeriesFrequency,
    TimeSeriesQueryError,
    WindowTooLargeError,
)
from app.logging import get_logger

logger = get_logger(__name__)


class RejectionType(StrEnum):
    """Every ``type`` the surface can put on a 422, closed so a client can be exhaustive.

    An enum rather than a free string: this is the member a caller branches on, and
    publishing the closed set is what lets a generated TS client fail to compile when
    a new rejection appears rather than fall through its `switch`. ``INVALID_REQUEST``
    covers every rejection FastAPI raises before a route is reached — the branch is the
    same either way, and which parameter failed is in ``errors``.
    """

    INVALID_REQUEST = "invalid_request"
    INVALID_TIME_RANGE = InvalidTimeRangeError.error_type
    TIMESTAMP_OUT_OF_RANGE = OutOfRangeTimestampError.error_type
    WINDOW_TOO_LARGE = WindowTooLargeError.error_type
    FREQUENCY_TOO_FINE = FrequencyTooFineError.error_type
    FREQUENCY_REQUIRES_AGGREGATION_METHOD = FrequencyWithoutAggregationMethodError.error_type
    MAX_POINTS_EXCEEDED = MaxPointsExceededError.error_type


# Fail at import time (not at the first rejection) if a domain rejection has no member.
_unpublished_types = {error.error_type for error in TimeSeriesQueryError.__subclasses__()} - set(RejectionType)
if _unpublished_types:
    raise RuntimeError(f"TimeSeriesQueryError subclasses missing a RejectionType member: {_unpublished_types}")

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

    error_type: ClassVar[RejectionType] = RejectionType.INVALID_REQUEST
    title: ClassVar[str] = INVALID_REQUEST_TITLE


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
    resampled: ResampledRetry = Field(description="Grid that fits the window as asked.")


class FieldError(BaseModel):
    """One parameter's rejection, so a client branches per field instead of on prose."""

    field: str = Field(description="Dotted path to the parameter, as `location.name`.", examples=["query.to_timestamp"])
    code: str = Field(description="Machine-readable reason code for this field.", examples=["datetime_parsing"])
    message: str = Field(description="Human-readable reason. The submitted value is redacted out of it.")


class ApiErrorResponse(BaseModel):
    """The body of every ``422`` on the surface, as an RFC 9457 problem detail.

    The extension members below are populated on a max-points rejection and absent
    otherwise, so a client can branch on ``type`` and read only what that type
    promises.
    """

    type: RejectionType = Field(description="Stable, machine-readable rejection identifier.")
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
    errors: list[FieldError] | None = Field(
        default=None, description="The parameters that failed, one entry each. `invalid_request` only."
    )


# Declared on every route through ``FastAPI(responses=...)``, replacing FastAPI's
# default `HTTPValidationError` so the schema matches what the handlers return.
API_ERROR_RESPONSES: dict[int | str, dict] = {
    422: {"model": ApiErrorResponse, "description": "Request rejected; branch on `type`."}
}


def error_response(error: ApiErrorResponse) -> JSONResponse:
    """Serialize a problem detail, omitting the extension members its type does not promise."""
    return JSONResponse(status_code=error.status, content=error.model_dump(mode="json", exclude_none=True))


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
            type=RejectionType(exc.error_type),
            title=exc.title,
            detail=str(exc),
            point_count=exc.point_count,
            max_points=exc.max_points,
            suggestions=_suggestions(exc),
        )
    return ApiErrorResponse(type=RejectionType(exc.error_type), title=exc.title, detail=str(exc))


# Validators interpolate the rejected value into their own message, so the value is
# cut back out here — one funnel, holding for validators not yet written.
_REDACTED = "<redacted>"


def _redact(message: str, error: Mapping[str, Any]) -> str:
    """Replace the submitted value wherever a validator spliced it into its message."""
    raw = str(error.get("input", ""))
    return message.replace(raw, _REDACTED) if raw else message


def _field_errors(exc: RequestValidationError) -> list[FieldError]:
    """Name each offending parameter and why, without echoing what was sent."""
    return [
        FieldError(
            field=".".join(str(part) for part in error.get("loc", ())),
            code=str(error.get("type", "value_error")),
            message=_redact(str(error.get("msg", "")), error),
        )
        for error in exc.errors()
    ]


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
        field_errors = _field_errors(exc)
        return error_response(
            ApiErrorResponse(
                type=RejectionType.INVALID_REQUEST,
                title=INVALID_REQUEST_TITLE,
                detail="; ".join(f"{error.field}: {error.message}" for error in field_errors),
                errors=field_errors,
            )
        )

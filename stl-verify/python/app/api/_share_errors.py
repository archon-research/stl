from fastapi import HTTPException

from app.domain.exceptions import AllocationShareError, MissingShareError, StaleShareError


def share_error_503(exc: AllocationShareError) -> HTTPException:
    """Translate an AllocationShareError subtype into a 503 with a distinct code.

    Shared by every endpoint that surfaces an allocation-share lookup failure
    (the ``/v1/risk/*`` endpoints and the prime risk-capital endpoint) so the
    error contract stays identical across them.
    """
    if isinstance(exc, StaleShareError):
        code = "share_data_stale"
    elif isinstance(exc, MissingShareError):
        code = "share_data_missing"
    else:
        code = "share_data_unavailable"
    return HTTPException(status_code=503, detail={"code": code, "message": str(exc)})

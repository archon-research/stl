from fastapi import HTTPException

from app.domain.exceptions import AllocationShareError


def share_error_503(exc: AllocationShareError) -> HTTPException:
    """Translate an AllocationShareError into a 503 carrying its ``code``.

    Used by the ``/v1/risk/*`` endpoints. The prime risk-capital endpoint no
    longer 503s on a share failure — it degrades that allocation to unpriced and
    reports the same ``exc.code`` as its ``unpriced_reason`` — but both read the
    code off the exception so the contract stays identical.
    """
    return HTTPException(status_code=503, detail={"code": exc.code, "message": str(exc)})

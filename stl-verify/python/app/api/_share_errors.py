from fastapi import HTTPException

from app.domain.exceptions import AllocationUnpricedError


def share_error_503(exc: AllocationUnpricedError) -> HTTPException:
    """Translate an unpriced-allocation error into a 503 carrying its ``code``.

    Used by the ``/v1/risk/*`` endpoints for both share-data and price-data gaps.
    The prime risk-capital endpoint no longer 503s on these — it degrades that
    allocation to unpriced and reports the same ``exc.code`` as its
    ``unpriced_reason`` — but both read the code off the exception so the contract
    stays identical.
    """
    return HTTPException(status_code=503, detail={"code": exc.code, "message": str(exc)})

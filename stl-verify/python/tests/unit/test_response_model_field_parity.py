"""Pins each splat-constructed response model against the entity it projects.

Several endpoints build their response as ``Response(**entity.__dict__)``.
Pydantic's default is ``extra="ignore"``, so a field added to the entity and not
to the response model is silently dropped from the API: the request still
succeeds, the schema still validates, and nothing fails. That is how a figure the
service computed and intended to publish goes missing.

``extra="forbid"`` would catch it too, but at the cost of emitting
``additionalProperties: false`` into the published schema — a promise never to add
a field, which this API does regularly. Asserting the field sets here keeps the
failure in CI and out of the contract.
"""

import dataclasses

import pytest

from app.api.v1 import allocations, exposure, prime_debts, prime_risk_capital, protocol_events, total_capital
from app.domain.entities.prime_debt import PrimeDebtSnapshot
from app.domain.entities.prime_risk_capital import AllocationRiskCapital, ChainRiskCapital
from app.domain.entities.protocol_event import ProtocolEvent
from app.domain.entities.time_series_bucket import (
    AllocationActivityBucket,
    ExposureBucket,
    PrimeDebtBucket,
    ProtocolEventBucket,
    TotalCapitalBucket,
)

# Every (response model, entity) pair whose construction splats the entity.
# Grep for `__dict__` under app/api/ when adding an endpoint.
_SPLAT_CONSTRUCTED_PAIRS = [
    (prime_risk_capital.AllocationRiskCapitalResponse, AllocationRiskCapital),
    (prime_risk_capital.ChainRiskCapitalResponse, ChainRiskCapital),
    (exposure.ExposureBucketResponse, ExposureBucket),
    (total_capital.TotalCapitalBucketResponse, TotalCapitalBucket),
    (prime_debts.PrimeDebtBucketResponse, PrimeDebtBucket),
    (prime_debts.PrimeDebtSnapshotResponse, PrimeDebtSnapshot),
    (protocol_events.ProtocolEventBucketResponse, ProtocolEventBucket),
    (protocol_events.ProtocolEventResponse, ProtocolEvent),
    (allocations.AllocationActivityBucketResponse, AllocationActivityBucket),
]


@pytest.mark.parametrize(
    ("model", "entity"),
    _SPLAT_CONSTRUCTED_PAIRS,
    ids=[model.__name__ for model, _ in _SPLAT_CONSTRUCTED_PAIRS],
)
def test_response_model_publishes_every_field_of_the_entity_it_projects(model, entity):
    entity_fields = {field.name for field in dataclasses.fields(entity)}

    assert set(model.model_fields) == entity_fields, (
        f"{model.__name__} and {entity.__name__} have drifted: "
        f"dropped from the API {sorted(entity_fields - set(model.model_fields))}, "
        f"absent from the entity {sorted(set(model.model_fields) - entity_fields)}"
    )

package allocation_tracker

import (
	"context"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/archon-research/stl/stl-verify/internal/services/allocation_tracker"

// Failure reasons for the underlying-value counter. Bounded set: it is a
// metric label.
const (
	reasonConvertFailed        = "convert_failed"
	reasonMissingAssetAddress  = "missing_asset_address"
	reasonAssetMetadataMissing = "asset_metadata_missing"
)

// Telemetry provides OpenTelemetry metrics for the allocation tracker.
type Telemetry struct {
	underlyingValueFailures metric.Int64Counter

	// chainAttr is fixed at construction: one indexer process serves one chain.
	chainAttr attribute.KeyValue
}

// NewTelemetry creates a Telemetry using the global meter provider. chain is
// the chain name (e.g. "mainnet") attached as the `chain` label.
func NewTelemetry(chain string) (*Telemetry, error) {
	return NewTelemetryWithProvider(otel.GetMeterProvider(), chain)
}

// NewTelemetryWithProvider creates a Telemetry with a custom meter provider.
func NewTelemetryWithProvider(mp metric.MeterProvider, chain string) (*Telemetry, error) {
	meter := mp.Meter(instrumentationName)

	t := &Telemetry{chainAttr: attribute.String("chain", chain)}

	var err error
	if t.underlyingValueFailures, err = meter.Int64Counter(
		"allocation.underlying_value.failures.total",
		metric.WithDescription("Positions persisted with NULL underlying_value for a token type that should produce one, by reason"),
	); err != nil {
		return nil, fmt.Errorf("creating underlyingValueFailures counter: %w", err)
	}

	return t, nil
}

// RecordUnderlyingValueFailure counts one position written with a NULL
// underlying valuation for a token type that should carry one. This is the
// silent-data-quality-hole signal: the write itself succeeds, so no error
// path fires. Nil-safe so unit tests may pass a nil Telemetry. The token
// label is the contract address hex (symbols are not unique); cardinality is
// bounded by the axis-synome entry registry.
func (t *Telemetry) RecordUnderlyingValueFailure(ctx context.Context, tokenType string, token common.Address, reason string) {
	if t == nil {
		return
	}
	t.underlyingValueFailures.Add(ctx, 1, metric.WithAttributes(
		t.chainAttr,
		attribute.String("token_type", tokenType),
		attribute.String("token", token.Hex()),
		attribute.String("reason", reason),
	))
}

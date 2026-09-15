package dextelemetry

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// NFTTransferRecorder emits only the two posm transfer counters, for a writer that
// is not a block-processing indexer: a replay records the rows it queued and landed
// and nothing else.
//
// The instrument names are NewTelemetry's, so a replay's rows land on the same
// series the live indexer's do and the table's growth rules see one number.
//
// Neither counter is seeded here. The seeds NewTelemetry places exist to make an
// absent series distinguishable from a zero one for a running indexer, and a
// hand-started replay has no such alert: seeding from here would put a permanent
// zero series on this worker's service_name in every query of those metrics.
type NFTTransferRecorder struct {
	chainAttr attribute.KeyValue
	attempted metric.Int64Counter
	written   metric.Int64Counter
}

func NewNFTTransferRecorder(prefix string, chainID int64) (*NFTTransferRecorder, error) {
	if prefix == "" {
		return nil, fmt.Errorf("dextelemetry.NewNFTTransferRecorder: prefix must be non-empty")
	}
	chainName, err := entity.ChainName(chainID)
	if err != nil {
		return nil, fmt.Errorf("dextelemetry.NewNFTTransferRecorder: %w", err)
	}
	meter := otel.Meter(prefix + "-dex-worker")

	attempted, err := meter.Int64Counter(prefix+nftTransferRowsAttemptedSuffix,
		metric.WithDescription(nftTransferRowsAttemptedDesc))
	if err != nil {
		return nil, fmt.Errorf("creating %s%s counter: %w", prefix, nftTransferRowsAttemptedSuffix, err)
	}
	written, err := meter.Int64Counter(prefix+nftTransferRowsWrittenSuffix,
		metric.WithDescription(nftTransferRowsWrittenDesc))
	if err != nil {
		return nil, fmt.Errorf("creating %s%s counter: %w", prefix, nftTransferRowsWrittenSuffix, err)
	}

	return &NFTTransferRecorder{
		chainAttr: attribute.String("chain", chainName),
		attempted: attempted,
		written:   written,
	}, nil
}

// RecordNFTTransferRows carries Telemetry.RecordNFTTransferRows' contract: nil-safe,
// and a non-positive count is a no-op rather than a zero datapoint.
func (r *NFTTransferRecorder) RecordNFTTransferRows(ctx context.Context, attempted, written int) {
	if r == nil {
		return
	}
	if attempted > 0 {
		r.attempted.Add(ctx, int64(attempted), metric.WithAttributes(r.chainAttr))
	}
	if written > 0 {
		r.written.Add(ctx, int64(written), metric.WithAttributes(r.chainAttr))
	}
}

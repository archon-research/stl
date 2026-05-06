package telemetry

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// S3BackupMetrics implements outbound.S3BackupRecorder using OpenTelemetry.
// The metric names live under the meterName chosen by the caller — the watcher
// uses "stl-verify/watcher" so panels are namespaced per service.
type S3BackupMetrics struct {
	putDuration   metric.Float64Histogram
	putBytes      metric.Int64Histogram
	putErrors     metric.Int64Counter
	groupDuration metric.Float64Histogram
}

// NewS3BackupMetrics constructs the histograms and counters for the watcher's
// inline S3 backup path. meterName should typically be the service name.
func NewS3BackupMetrics(meterName string) (*S3BackupMetrics, error) {
	meter := otel.Meter(meterName)

	putDuration, err := meter.Float64Histogram(
		"watcher_s3_put_duration_seconds",
		metric.WithDescription("Wall-clock duration of a single S3 PUT for a block data artifact"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("watcher_s3_put_duration_seconds: %w", err)
	}

	putBytes, err := meter.Int64Histogram(
		"watcher_s3_put_bytes",
		metric.WithDescription("Size in bytes of objects written to S3 by the watcher"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("watcher_s3_put_bytes: %w", err)
	}

	putErrors, err := meter.Int64Counter(
		"watcher_s3_put_errors_total",
		metric.WithDescription("Terminal S3 PUT failures bucketed by data_type and error_class"),
	)
	if err != nil {
		return nil, fmt.Errorf("watcher_s3_put_errors_total: %w", err)
	}

	groupDuration, err := meter.Float64Histogram(
		"watcher_s3_backup_group_duration_seconds",
		metric.WithDescription("Wall-clock duration of the parallel S3 PUT group (errgroup.Wait) — the actual cost the watcher pays per block"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return nil, fmt.Errorf("watcher_s3_backup_group_duration_seconds: %w", err)
	}

	return &S3BackupMetrics{
		putDuration:   putDuration,
		putBytes:      putBytes,
		putErrors:     putErrors,
		groupDuration: groupDuration,
	}, nil
}

// RecordPutDuration implements outbound.S3BackupRecorder.
func (m *S3BackupMetrics) RecordPutDuration(ctx context.Context, dataType, outcome string, duration time.Duration) {
	m.putDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(
			attribute.String("data_type", dataType),
			attribute.String("outcome", outcome),
		),
	)
}

// RecordPutBytes implements outbound.S3BackupRecorder.
func (m *S3BackupMetrics) RecordPutBytes(ctx context.Context, dataType string, bytes int64) {
	m.putBytes.Record(ctx, bytes,
		metric.WithAttributes(attribute.String("data_type", dataType)),
	)
}

// RecordPutError implements outbound.S3BackupRecorder.
func (m *S3BackupMetrics) RecordPutError(ctx context.Context, dataType, errorClass string) {
	m.putErrors.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("data_type", dataType),
			attribute.String("error_class", errorClass),
		),
	)
}

// RecordGroupDuration implements outbound.S3BackupRecorder.
func (m *S3BackupMetrics) RecordGroupDuration(ctx context.Context, outcome string, duration time.Duration) {
	m.groupDuration.Record(ctx, duration.Seconds(),
		metric.WithAttributes(attribute.String("outcome", outcome)),
	)
}

// span.go provides small span helpers shared by service telemetry wrappers.
package telemetry

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// SetSpanError records an error on a span and sets its status. It is a no-op
// when err is nil.
func SetSpanError(span trace.Span, err error, description string) {
	if err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, description)
}

// NoopSpan returns a non-recording span for nil-telemetry code paths, so
// callers can unconditionally call span methods.
func NoopSpan() trace.Span {
	return trace.SpanFromContext(context.Background())
}

// StatusAttr renders an error as the conventional success/error status
// metric attribute.
func StatusAttr(err error) attribute.KeyValue {
	if err != nil {
		return attribute.String("status", "error")
	}
	return attribute.String("status", "success")
}

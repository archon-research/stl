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

// SuccessStatusAttr and ErrorStatusAttr are the canonical terminal-status
// attribute values. Every status-labelled series must derive from these
// (directly or via StatusAttr) so seeded series (SeedStatusCounter) and
// recorded series can never drift into parallel series.
func SuccessStatusAttr() attribute.KeyValue {
	return attribute.String("status", "success")
}

func ErrorStatusAttr() attribute.KeyValue {
	return attribute.String("status", "error")
}

// StatusAttr returns the terminal-status attribute derived from err.
func StatusAttr(err error) attribute.KeyValue {
	if err != nil {
		return ErrorStatusAttr()
	}
	return SuccessStatusAttr()
}

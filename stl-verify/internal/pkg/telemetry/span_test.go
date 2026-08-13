package telemetry

import (
	"context"
	"errors"
	"testing"

	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestStatusAttr(t *testing.T) {
	if got := StatusAttr(nil).Value.AsString(); got != "success" {
		t.Errorf("StatusAttr(nil) = %q, want success", got)
	}
	if got := StatusAttr(errors.New("x")).Value.AsString(); got != "error" {
		t.Errorf("StatusAttr(err) = %q, want error", got)
	}
}

func TestNoopSpan(t *testing.T) {
	span := NoopSpan()
	if span == nil {
		t.Fatal("NoopSpan returned nil")
	}
	if span.IsRecording() {
		t.Error("NoopSpan must not record")
	}
	// Must be safe to use.
	span.RecordError(errors.New("ignored"))
	span.End()
}

func TestSetSpanError(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	_, span := tp.Tracer("test").Start(context.Background(), "with-error")
	SetSpanError(span, errors.New("boom"), "operation failed")
	span.End()

	_, clean := tp.Tracer("test").Start(context.Background(), "clean")
	SetSpanError(clean, nil, "ignored for nil error")
	clean.End()

	spans := exporter.GetSpans()
	if len(spans) != 2 {
		t.Fatalf("len(spans) = %d, want 2", len(spans))
	}
	for _, s := range spans {
		switch s.Name {
		case "with-error":
			if s.Status.Code != otelcodes.Error || s.Status.Description != "operation failed" {
				t.Errorf("with-error status = %+v, want error/operation failed", s.Status)
			}
			if len(s.Events) == 0 {
				t.Error("with-error span has no recorded error event")
			}
		case "clean":
			if s.Status.Code == otelcodes.Error {
				t.Error("clean span must not have error status")
			}
			if len(s.Events) != 0 {
				t.Errorf("clean span has %d events, want 0", len(s.Events))
			}
		}
	}
}

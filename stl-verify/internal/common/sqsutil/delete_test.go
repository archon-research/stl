package sqsutil

import (
	"context"
	"errors"
	"maps"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// The backup worker settles its own messages instead of going through
// ProcessMessages, so op="delete" has to be reachable from outside this package
// or VectorSQSDeleteFailed is structurally blind to the one worker that touches
// every block of every chain.
func TestDeleteMessage_CountsTheSettleOutcome(t *testing.T) {
	refusal := errors.New("AccessDenied: DeleteMessage")
	tests := []struct {
		name      string
		deleteErr error
		wantErr   bool
		want      map[settleKey]int64
	}{
		{
			name: "a settled delete",
			want: map[settleKey]int64{{op: "delete", status: "ok"}: 1},
		},
		{
			name:      "a refused delete",
			deleteErr: refusal,
			wantErr:   true,
			want:      map[settleKey]int64{{op: "delete", status: "failed"}: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := installManualMeterProvider(t)
			consumer := &mockConsumer{deleteErrFor: map[string]error{"h1": tt.deleteErr}}
			if tt.deleteErr == nil {
				consumer.deleteErrFor = nil
			}

			err := DeleteMessage(context.Background(), consumer, testutil.DiscardLogger(), 1, makeMsg("1", "h1", blockEvent(100)))

			if gotErr := err != nil; gotErr != tt.wantErr {
				t.Fatalf("DeleteMessage error = %v, want error: %v", err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, refusal) {
				t.Errorf("error = %v, want it to wrap the refusal", err)
			}
			if got := collectSettleCounter(t, reader); !maps.Equal(got, tt.want) {
				t.Errorf("settles = %v, want %v", got, tt.want)
			}
		})
	}
}

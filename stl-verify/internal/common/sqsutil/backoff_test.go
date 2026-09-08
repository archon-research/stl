package sqsutil

import (
	"testing"
	"time"
)

func TestFailureBackoff_Delay(t *testing.T) {
	schedule := FailureBackoff{Base: 15 * time.Second, Cap: 60 * time.Second}
	tests := []struct {
		name         string
		receiveCount int
		want         time.Duration
	}{
		{"an unreported count is a first receive", 0, 15 * time.Second},
		{"the first receive waits the base", 1, 15 * time.Second},
		{"the second receive waits double", 2, 30 * time.Second},
		{"the third receive doubles again", 3, 60 * time.Second},
		{"the cap holds from then on", 4, 60 * time.Second},
		{"a huge count stays at the cap", 1000, 60 * time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := schedule.Delay(tt.receiveCount); got != tt.want {
				t.Errorf("Delay(%d) = %s, want %s", tt.receiveCount, got, tt.want)
			}
		})
	}
}

func TestFailureBackoff_Validate(t *testing.T) {
	tests := []struct {
		name    string
		backoff FailureBackoff
		wantErr bool
	}{
		{"the default schedule", FailureBackoff{Base: DefaultFailureBackoffBase, Cap: DefaultFailureBackoffCap}, false},
		{"a zero base hands the message straight back", FailureBackoff{Base: 0, Cap: 60 * time.Second}, true},
		{"a sub-second base rounds to zero on the wire", FailureBackoff{Base: 400 * time.Millisecond, Cap: 60 * time.Second}, true},
		{"a negative base hands it straight back too", FailureBackoff{Base: -time.Second, Cap: 60 * time.Second}, true},
		{"a cap under the base", FailureBackoff{Base: 15 * time.Second, Cap: 10 * time.Second}, true},
		{"a cap equal to the base is a flat delay", FailureBackoff{Base: 15 * time.Second, Cap: 15 * time.Second}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.backoff.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

package archiving

import "testing"

// Metrics must never break the archiving path, so both disabled shapes of the
// counter stay callable: a build that failed, and no counter at all.
func TestWriteCounter_RecordNoOpsWhenTheCounterIsDisabled(t *testing.T) {
	disabled := []struct {
		name    string
		counter *WriteCounter
	}{
		{name: "counter that failed to build", counter: &WriteCounter{}},
		{name: "no counter at all", counter: nil},
	}
	for _, tt := range disabled {
		t.Run(tt.name, func(t *testing.T) {
			tt.counter.Record(WriteStatusLost, 1)
		})
	}
}

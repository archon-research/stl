package temporal

import (
	"context"
	"testing"

	"go.temporal.io/sdk/testsuite"
)

type sweepPoint struct {
	Scope string `json:"scope"`
	Block int64  `json:"block"`
}

// TestActivityProgress_LoadProgressReportsAbsenceOnAFirstAttempt: an activity's
// first attempt has no heartbeat history. Absence must be reported as such, not
// as a zero-valued record that a caller could mistake for real progress.
func TestActivityProgress_LoadProgressReportsAbsenceOnAFirstAttempt(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	progress := NewActivityProgress[sweepPoint]()

	var found bool
	loadProgress := func(ctx context.Context) error {
		var err error
		_, found, err = progress.LoadProgress(ctx)
		return err
	}
	env.RegisterActivity(loadProgress)
	if _, err := env.ExecuteActivity(loadProgress); err != nil {
		t.Fatalf("ExecuteActivity: %v", err)
	}

	if found {
		t.Error("LoadProgress reported a record on an attempt with no heartbeat history")
	}
}

// TestActivityProgress_LoadProgressReturnsAnEarlierAttemptsRecord: the resume
// path reads what the previous attempt heartbeated, which is what makes a
// killed run pick up where it stopped.
func TestActivityProgress_LoadProgressReturnsAnEarlierAttemptsRecord(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	recorded := sweepPoint{Scope: "chain-1", Block: 23_400_000}
	env.SetHeartbeatDetails(recorded)
	progress := NewActivityProgress[sweepPoint]()

	var loaded sweepPoint
	var found bool
	loadProgress := func(ctx context.Context) error {
		var err error
		loaded, found, err = progress.LoadProgress(ctx)
		return err
	}
	env.RegisterActivity(loadProgress)
	if _, err := env.ExecuteActivity(loadProgress); err != nil {
		t.Fatalf("ExecuteActivity: %v", err)
	}

	if !found {
		t.Fatal("LoadProgress found no record although the attempt has heartbeat details")
	}
	if loaded != recorded {
		t.Errorf("loaded %+v, wanted the recorded %+v", loaded, recorded)
	}
}

// TestActivityProgress_LivenessBeatAfterAResumeCarriesTheLoadedRecord: a resumed
// attempt records nothing of its own until its first unit of work lands. Temporal
// keeps only the LAST heartbeat's details, so a bare liveness ping in that window
// erases the very record the attempt is resuming from — and the attempt after it
// re-sweeps a range this one already covered.
func TestActivityProgress_LivenessBeatAfterAResumeCarriesTheLoadedRecord(t *testing.T) {
	env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
	recorded := sweepPoint{Scope: "chain-1", Block: 23_400_000}
	env.SetHeartbeatDetails(recorded)

	progress := NewActivityProgress[sweepPoint]()
	var sent [][]any
	progress.record = func(_ context.Context, details ...any) { sent = append(sent, details) }

	loadThenBeat := func(ctx context.Context) error {
		if _, _, err := progress.LoadProgress(ctx); err != nil {
			return err
		}
		progress.Beat(ctx)
		return nil
	}
	env.RegisterActivity(loadThenBeat)
	if _, err := env.ExecuteActivity(loadThenBeat); err != nil {
		t.Fatalf("ExecuteActivity: %v", err)
	}

	if len(sent) != 1 {
		t.Fatalf("heartbeats sent = %d, want 1 (the liveness beat)", len(sent))
	}
	if len(sent[0]) != 1 || sent[0][0] != recorded {
		t.Errorf("the beat after a resume carried %v, want the loaded %+v", sent[0], recorded)
	}
}

// TestActivityProgress_ResetSilencesTheBeatUntilSomethingIsRecorded: Reset runs
// at the top of every execution, ahead of the runner's own LoadProgress. A beat
// landing in that window would send bare details and wipe the server's record of
// the PREVIOUS attempt — the resume point this attempt has not read yet.
func TestActivityProgress_ResetSilencesTheBeatUntilSomethingIsRecorded(t *testing.T) {
	ctx := context.Background()
	progress := NewActivityProgress[sweepPoint]()
	var sent [][]any
	progress.record = func(_ context.Context, details ...any) { sent = append(sent, details) }

	if err := progress.SaveProgress(ctx, sweepPoint{Scope: "chain-1", Block: 23_400_000}); err != nil {
		t.Fatalf("SaveProgress: %v", err)
	}
	progress.Reset()
	progress.Beat(ctx)

	if len(sent) != 1 {
		t.Fatalf("heartbeats sent = %d, want 1 (the progress record alone; the beat after a reset must send nothing)", len(sent))
	}
}

// TestActivityProgress_BeatsResumeOnceTheAttemptRecordsItsOwnProgress: the
// silence after a Reset lasts only until this attempt has something of its own
// to re-send — after that the liveness beat must carry it again.
func TestActivityProgress_BeatsResumeOnceTheAttemptRecordsItsOwnProgress(t *testing.T) {
	ctx := context.Background()
	recorded := sweepPoint{Scope: "chain-1", Block: 23_500_000}

	progress := NewActivityProgress[sweepPoint]()
	progress.Reset()
	var sent [][]any
	progress.record = func(_ context.Context, details ...any) { sent = append(sent, details) }

	if err := progress.SaveProgress(ctx, recorded); err != nil {
		t.Fatalf("SaveProgress: %v", err)
	}
	progress.Beat(ctx)

	if len(sent) != 2 {
		t.Fatalf("heartbeats sent = %d, want 2 (the progress record and the liveness beat)", len(sent))
	}
	if len(sent[1]) != 1 || sent[1][0] != recorded {
		t.Errorf("the beat after the first record carried %v, want the recorded %+v", sent[1], recorded)
	}
}

// TestActivityProgress_LivenessBeatCarriesTheLastRecordedProgress: Temporal
// keeps only the LAST heartbeat's details, so once progress is recorded no
// heartbeat may go out empty. A bare liveness ping would erase the resume point
// and silently send the next attempt back to the start of a multi-hour sweep.
func TestActivityProgress_LivenessBeatCarriesTheLastRecordedProgress(t *testing.T) {
	ctx := context.Background()
	recorded := sweepPoint{Scope: "chain-1", Block: 23_400_000}

	progress := NewActivityProgress[sweepPoint]()
	var sent [][]any
	progress.record = func(_ context.Context, details ...any) { sent = append(sent, details) }

	if err := progress.SaveProgress(ctx, recorded); err != nil {
		t.Fatalf("SaveProgress: %v", err)
	}
	progress.Beat(ctx)

	if len(sent) != 2 {
		t.Fatalf("heartbeats sent = %d, want 2 (the progress record and the liveness beat)", len(sent))
	}
	for i, details := range sent {
		if len(details) != 1 || details[0] != recorded {
			t.Errorf("heartbeat %d carried %v, want the recorded %+v", i, details, recorded)
		}
	}
}

// TestActivityProgress_BeatBeforeAnyRecordSendsNothing: an empty store cannot
// tell "this attempt has genuinely made no progress" from "this attempt has not
// read the previous one's resume point yet", so it sends no heartbeat at all
// rather than a bare one that would erase whatever the server holds.
func TestActivityProgress_BeatBeforeAnyRecordSendsNothing(t *testing.T) {
	progress := NewActivityProgress[sweepPoint]()
	var sent [][]any
	progress.record = func(_ context.Context, details ...any) { sent = append(sent, details) }

	progress.Beat(context.Background())

	if len(sent) != 0 {
		t.Errorf("heartbeats sent = %d (%v), want none before the store holds a record", len(sent), sent)
	}
}

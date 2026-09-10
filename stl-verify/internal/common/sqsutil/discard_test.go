package sqsutil

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/rpcerr"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	orphanedBlockNumber = int64(25944523)
	orphanedBlockHash   = "0x70559c33e0f4f4d6e9f0d1c1a5f3b2c1d0e9f8a7b6c5d4e3f2a1b0c9d8e7f6a5"
	canonicalBlockHash  = "0x5174fd08b1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e8"
)

func orphanedBlockMessage() outbound.SQSMessage {
	return makeMsg("m1", "h1", outbound.BlockEvent{
		ChainID:     1,
		BlockNumber: orphanedBlockNumber,
		Version:     0,
		BlockHash:   orphanedBlockHash,
	})
}

func blockUnavailableHandler() BlockEventHandler {
	return func(context.Context, outbound.BlockEvent) error {
		return fmt.Errorf("reading vault state: %w",
			rpcerr.TagBlockUnavailableAtHash(testutil.RPCError{Code: -32001, Msg: "block not found: hash " + orphanedBlockHash}))
	}
}

func supersessionLookup(superseded bool, err error) SupersededBlockLookup {
	return func(context.Context, int64, string) (bool, error) { return superseded, err }
}

func discardConfig(consumer *mockConsumer, lookup SupersededBlockLookup) (Config, *testutil.SlogRecorder) {
	cfg, recorder := recordingConfig(consumer)
	cfg.SupersededBlock = lookup
	return cfg, recorder
}

// blockRecord is the watcher's canonical row at one height, or the absence of one.
type blockRecord struct {
	state *outbound.BlockState
	err   error
}

func (b blockRecord) GetBlockByNumber(context.Context, int64) (*outbound.BlockState, error) {
	return b.state, b.err
}

func canonicalRow(hash string, published bool) blockRecord {
	return blockRecord{state: &outbound.BlockState{
		Number:         orphanedBlockNumber,
		Hash:           hash,
		BlockPublished: published,
	}}
}

func TestProcessMessagesDiscardsAConfirmedSupersededBlock(t *testing.T) {
	consumer := &mockConsumer{batches: [][]outbound.SQSMessage{{orphanedBlockMessage()}}}
	cfg, _ := discardConfig(consumer, supersessionLookup(true, nil))

	if _, err := ProcessMessages(context.Background(), cfg, blockUnavailableHandler()); err != nil {
		t.Fatalf("a discarded message is settled, not a batch failure: %v", err)
	}
	if got := consumer.deleted(); len(got) != 1 || got[0] != "h1" {
		t.Fatalf("deleted handles = %v, want the superseded block's message deleted", got)
	}
}

func TestProcessMessagesLogsAConfirmedSupersededDiscard(t *testing.T) {
	consumer := &mockConsumer{batches: [][]outbound.SQSMessage{{orphanedBlockMessage()}}}
	cfg, recorder := discardConfig(consumer, supersessionLookup(true, nil))

	if _, err := ProcessMessages(context.Background(), cfg, blockUnavailableHandler()); err != nil {
		t.Fatalf("ProcessMessages: %v", err)
	}

	if recorder.CountWarn("discarding") != 1 {
		t.Fatalf("want exactly one WARN announcing the discard, got %v", recorder.MessagesAt(slog.LevelWarn))
	}
	for _, want := range []string{orphanedBlockHash, "m1", "25944523"} {
		if !recorder.ContainsAttr(want) {
			t.Errorf("discard WARN must carry %q so the decision is triageable", want)
		}
	}
}

func TestProcessMessagesCountsAConfirmedSupersededDiscard(t *testing.T) {
	reader := installManualMeterProvider(t)
	consumer := &mockConsumer{batches: [][]outbound.SQSMessage{{orphanedBlockMessage()}}}
	cfg, _ := discardConfig(consumer, supersessionLookup(true, nil))

	if _, err := ProcessMessages(context.Background(), cfg, blockUnavailableHandler()); err != nil {
		t.Fatalf("ProcessMessages: %v", err)
	}

	counts := collectDiscardCounter(t, reader)
	if counts[discardKey{chain: "mainnet", reason: "non_canonical_block"}] != 1 {
		t.Fatalf("sqs.message.discards.total = %v, want one non_canonical_block discard on mainnet", counts)
	}
}

func TestProcessMessagesCountsAForeignChainDiscard(t *testing.T) {
	reader := installManualMeterProvider(t)
	foreign := makeMsg("m1", "h1", outbound.BlockEvent{ChainID: 8453, BlockNumber: orphanedBlockNumber})
	consumer := &mockConsumer{batches: [][]outbound.SQSMessage{{foreign}}}
	cfg, _ := recordingConfig(consumer)

	if _, err := ProcessMessages(context.Background(), cfg, noopHandler); err != nil {
		t.Fatalf("ProcessMessages: %v", err)
	}

	counts := collectDiscardCounter(t, reader)
	if counts[discardKey{chain: "mainnet", reason: "foreign_chain"}] != 1 {
		t.Fatalf("sqs.message.discards.total = %v, want one foreign_chain discard on mainnet", counts)
	}
}

func TestProcessMessagesKeepsAMessageThatIsNotSuperseded(t *testing.T) {
	consumer := &mockConsumer{batches: [][]outbound.SQSMessage{{orphanedBlockMessage()}}}
	cfg, _ := discardConfig(consumer, supersessionLookup(false, nil))

	if _, err := ProcessMessages(context.Background(), cfg, blockUnavailableHandler()); err == nil {
		t.Fatal("a block nothing has superseded is a recoverable read failure and must stay a batch error")
	}
	if got := consumer.deleted(); len(got) != 0 {
		t.Fatalf("deleted handles = %v, want none: no successor was published for this height", got)
	}
}

func TestProcessMessagesKeepsAMessageWhenTheSupersessionCheckFails(t *testing.T) {
	consumer := &mockConsumer{batches: [][]outbound.SQSMessage{{orphanedBlockMessage()}}}
	cfg, recorder := discardConfig(consumer, supersessionLookup(false, errors.New("connection refused")))

	if _, err := ProcessMessages(context.Background(), cfg, blockUnavailableHandler()); err == nil {
		t.Fatal("an unconfirmed supersession must stay a batch error")
	}
	if got := consumer.deleted(); len(got) != 0 {
		t.Fatalf("deleted handles = %v, want none: permanence was never confirmed", got)
	}
	if recorder.CountWarn("could not establish") != 1 {
		t.Fatalf("want one WARN naming the failed check, got %v", recorder.MessagesAt(slog.LevelWarn))
	}
}

func TestProcessMessagesKeepsAMessageWithoutASupersessionHook(t *testing.T) {
	consumer := &mockConsumer{batches: [][]outbound.SQSMessage{{orphanedBlockMessage()}}}
	cfg, _ := recordingConfig(consumer)

	if _, err := ProcessMessages(context.Background(), cfg, blockUnavailableHandler()); err == nil {
		t.Fatal("without a hook the loop must behave exactly as before: a handler error is a batch error")
	}
	if got := consumer.deleted(); len(got) != 0 {
		t.Fatalf("deleted handles = %v, want none when no supersession hook is configured", got)
	}
}

func TestProcessMessagesDoesNotCheckSupersessionForAnUnrelatedHandlerError(t *testing.T) {
	consumer := &mockConsumer{batches: [][]outbound.SQSMessage{{orphanedBlockMessage()}}}
	consulted := false
	cfg, _ := discardConfig(consumer, func(context.Context, int64, string) (bool, error) {
		consulted = true
		return true, nil
	})

	handler := func(context.Context, outbound.BlockEvent) error { return errors.New("postgres lock wait") }
	if _, err := ProcessMessages(context.Background(), cfg, handler); err == nil {
		t.Fatal("expected the handler error to stay a batch error")
	}
	if consulted {
		t.Error("only a block-unavailable-at-hash failure may spend a lookup on a supersession check")
	}
	if got := consumer.deleted(); len(got) != 0 {
		t.Fatalf("deleted handles = %v, want none for an unrelated handler error", got)
	}
}

// The watcher dedupes on hash and un-orphans in place without republishing, so a
// discard here would leave a hole nothing refills.
func TestProcessMessagesKeepsAnOrphanedBlockWithNoPublishedSuccessor(t *testing.T) {
	consumer := &mockConsumer{batches: [][]outbound.SQSMessage{{orphanedBlockMessage()}}}
	cfg, _ := discardConfig(consumer, NewSupersededBlockLookup(canonicalRow(canonicalBlockHash, false)))

	if _, err := ProcessMessages(context.Background(), cfg, blockUnavailableHandler()); err == nil {
		t.Fatal("a height with no published replacement event must stay a batch error: discarding it is a permanent hole")
	}
	if got := consumer.deleted(); len(got) != 0 {
		t.Fatalf("deleted handles = %v, want none: nothing proves a replacement event was published for this height", got)
	}
}

func TestNewSupersededBlockLookup(t *testing.T) {
	tests := []struct {
		name   string
		record blockRecord
		want   bool
	}{
		{"a published canonical block under another hash supersedes it", canonicalRow(canonicalBlockHash, true), true},
		{"a published canonical block under the same hash does not", canonicalRow(orphanedBlockHash, true), false},
		{"an unpublished canonical block does not", canonicalRow(canonicalBlockHash, false), false},
		{"a height with no canonical block does not", blockRecord{}, false},
		{"a hash differing only in case does not", canonicalRow(strings.ToUpper(orphanedBlockHash), true), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewSupersededBlockLookup(tt.record)(context.Background(), orphanedBlockNumber, orphanedBlockHash)
			if err != nil {
				t.Fatalf("NewSupersededBlockLookup: %v", err)
			}
			if got != tt.want {
				t.Errorf("superseded = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewSupersededBlockLookupPropagatesAFailedRead(t *testing.T) {
	record := blockRecord{err: errors.New("connection refused")}

	got, err := NewSupersededBlockLookup(record)(context.Background(), orphanedBlockNumber, orphanedBlockHash)
	if err == nil {
		t.Fatal("a block record that cannot be read must not be reported as a supersession verdict")
	}
	if got {
		t.Error("superseded = true on a failed read; a discard would be unconfirmed")
	}
	if !strings.Contains(err.Error(), "25944523") {
		t.Errorf("error %q should name the height it asked about", err)
	}
}

func TestRunLoopSeedsEveryDiscardReasonAtZero(t *testing.T) {
	reader := installManualMeterProvider(t)
	cancel, done := startRunLoop(&mockConsumer{}, slog.Default(), noopHandler)
	cancel()
	awaitLoopExit(t, done)

	counts := collectDiscardCounter(t, reader)
	for _, reason := range []string{"non_canonical_block", "foreign_chain"} {
		key := discardKey{chain: "mainnet", reason: reason}
		got, seeded := counts[key]
		if !seeded || got != 0 {
			t.Errorf("sqs.message.discards.total%+v = %d (present=%v), want a seeded 0 so increase() can see the first discard",
				key, got, seeded)
		}
	}
}

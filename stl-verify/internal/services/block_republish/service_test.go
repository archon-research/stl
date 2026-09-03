package block_republish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const (
	testChainID    = int64(1)
	testBlock      = int64(25395651)
	testHash       = "0x1111111111111111111111111111111111111111111111111111111111111111"
	testParentHash = "0x2222222222222222222222222222222222222222222222222222222222222222"
	forkHash       = "0x3333333333333333333333333333333333333333333333333333333333333333"
	testTimestamp  = int64(0x68b0c0c0)
)

// stubClient answers only the three reads Republish issues. The embedded port is
// nil, so any other call panics rather than silently returning a zero value.
type stubClient struct {
	outbound.BlockchainClient

	headers     []headerReply
	headerCalls int

	data    outbound.BlockData
	dataErr error

	head    int64
	headErr error
}

type headerReply struct {
	raw json.RawMessage
	err error
}

func (c *stubClient) GetBlockByNumber(context.Context, int64, bool) (json.RawMessage, error) {
	if c.headerCalls >= len(c.headers) {
		panic(fmt.Sprintf("unexpected GetBlockByNumber call %d", c.headerCalls+1))
	}
	reply := c.headers[c.headerCalls]
	c.headerCalls++
	return reply.raw, reply.err
}

func (c *stubClient) GetBlockDataByHash(context.Context, int64, string, bool) (outbound.BlockData, error) {
	return c.data, c.dataErr
}

func (c *stubClient) GetCurrentBlockNumber(context.Context) (int64, error) {
	return c.head, c.headErr
}

func headerJSON(hash string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"number":"0x1836b83","hash":%q,"parentHash":%q,"timestamp":"0x%x"}`,
		hash, testParentHash, testTimestamp))
}

func newStubClient() *stubClient {
	return &stubClient{
		headers: []headerReply{{raw: headerJSON(testHash)}, {raw: headerJSON(testHash)}},
		data: outbound.BlockData{
			BlockNumber: testBlock,
			Block:       json.RawMessage(`{"hash":"0x11"}`),
			Receipts:    json.RawMessage(`[{"status":"0x1"}]`),
			Traces:      json.RawMessage(`[{"type":"call"}]`),
			Blobs:       json.RawMessage(`[{"index":"0x0"}]`),
		},
		head: testBlock + 5000,
	}
}

// stubArchive stands in for the raw archive, reporting the highest version a
// height already holds an object at.
type stubArchive struct {
	highest int
	found   bool
	err     error
	calls   int
}

func (a *stubArchive) HighestVersion(context.Context, int64) (int, bool, error) {
	a.calls++
	return a.highest, a.found, a.err
}

type failingCache struct{ err error }

func (c failingCache) SetBlockData(context.Context, int64, int64, int, outbound.BlockDataInput) error {
	return c.err
}
func (c failingCache) DeleteBlock(context.Context, int64, int64, int) error { return nil }
func (c failingCache) Close() error                                         { return nil }

type failingSink struct{ err error }

func (s failingSink) Publish(context.Context, outbound.Event) error { return s.err }
func (s failingSink) Close() error                                  { return nil }

type fixture struct {
	service *Service
	client  *stubClient
	archive *stubArchive
	cache   *memory.BlockCache
	sink    *memory.EventSink
}

func (f fixture) archiveHolds(highest int) {
	f.archive.highest, f.archive.found = highest, true
}

// testConfig is the mainnet watcher's shape: traces on, blobs off.
func testConfig() Config {
	return Config{
		ChainID:      testChainID,
		EnableTraces: true,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func newFixture(t *testing.T, client *stubClient) fixture {
	t.Helper()
	return newFixtureWith(t, testConfig(), client)
}

func newFixtureWith(t *testing.T, config Config, client *stubClient) fixture {
	t.Helper()
	archive := &stubArchive{}
	cache := memory.NewBlockCache()
	sink := memory.NewEventSink()
	service := newTestService(t, config, client, archive, cache, sink)
	return fixture{service: service, client: client, archive: archive, cache: cache, sink: sink}
}

func newTestService(t *testing.T, config Config, client outbound.BlockchainClient, archive outbound.ArchiveVersionReader, cache outbound.BlockCacheWriter, sink outbound.EventSink) *Service {
	t.Helper()
	service, err := NewService(config, client, archive, cache, sink)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return service
}

func (f fixture) cached(t *testing.T, dataType string, version int) json.RawMessage {
	t.Helper()
	readers := map[string]func(context.Context, int64, int64, int) (json.RawMessage, error){
		"block":    f.cache.GetBlock,
		"receipts": f.cache.GetReceipts,
		"traces":   f.cache.GetTraces,
		"blobs":    f.cache.GetBlobs,
	}
	read, ok := readers[dataType]
	if !ok {
		t.Fatalf("unknown data type %q", dataType)
	}
	raw, err := read(context.Background(), testChainID, testBlock, version)
	if err != nil {
		t.Fatalf("reading %s from cache: %v", dataType, err)
	}
	return raw
}

func (f fixture) publishedEvent(t *testing.T) outbound.BlockEvent {
	t.Helper()
	events := f.sink.GetBlockEvents()
	if len(events) != 1 {
		t.Fatalf("published %d block events, want 1", len(events))
	}
	return events[0]
}

func TestRepublish_CachesExactlyTheDataTypesTheChainsWatcherPublishes(t *testing.T) {
	tests := []struct {
		name        string
		enableBlobs bool
		want        []string
	}{
		{name: "mainnet: block, receipts and traces", want: []string{"block", "receipts", "traces"}},
		{name: "a chain whose watcher also fetches blobs", enableBlobs: true, want: []string{"block", "receipts", "traces", "blobs"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := testConfig()
			config.EnableBlobs = tc.enableBlobs
			f := newFixtureWith(t, config, newStubClient())

			result, err := f.service.Republish(context.Background(), testBlock, 1, nil)
			if err != nil {
				t.Fatalf("Republish: %v", err)
			}

			if got := fmt.Sprint(result.DataTypes); got != fmt.Sprint(tc.want) {
				t.Errorf("DataTypes = %v, want %v", got, tc.want)
			}
			for _, dataType := range []string{"block", "receipts", "traces", "blobs"} {
				cached := f.cached(t, dataType, 1)
				if slices.Contains(tc.want, dataType) == (cached == nil) {
					t.Errorf("cached %s = %s, want it present only when the watcher publishes it", dataType, cached)
				}
			}
		})
	}
}

func TestRepublish_PublishesTheBlockEventAsAReorgBackfillAtTheGivenVersion(t *testing.T) {
	f := newFixture(t, newStubClient())

	if _, err := f.service.Republish(context.Background(), testBlock, 2, nil); err != nil {
		t.Fatalf("Republish: %v", err)
	}

	event := f.publishedEvent(t)
	want := outbound.BlockEvent{
		ChainID:        testChainID,
		BlockNumber:    testBlock,
		Version:        2,
		BlockHash:      testHash,
		ParentHash:     testParentHash,
		BlockTimestamp: testTimestamp,
		IsReorg:        true,
		IsBackfill:     true,
	}
	got := event
	got.ReceivedAt = want.ReceivedAt
	if got != want {
		t.Errorf("published event = %+v, want %+v (ReceivedAt ignored)", got, want)
	}
	if event.ReceivedAt.IsZero() {
		t.Error("published event has a zero ReceivedAt")
	}
}

// The phases are what a caller turns into activity heartbeats, so a worker that
// dies mid-block is noticed in seconds and the Temporal UI says which step a slow
// one is in.
func TestRepublish_ReportsEveryPhaseItEnters(t *testing.T) {
	f := newFixture(t, newStubClient())
	var phases []Phase

	_, err := f.service.Republish(context.Background(), testBlock, 1, func(_ context.Context, phase Phase) {
		phases = append(phases, phase)
	})
	if err != nil {
		t.Fatalf("Republish: %v", err)
	}

	want := []Phase{PhaseFetching, PhaseCaching, PhasePublishing}
	if fmt.Sprint(phases) != fmt.Sprint(want) {
		t.Errorf("phases = %v, want %v", phases, want)
	}
}

func TestRepublish_ReportsTheBlockItRepublished(t *testing.T) {
	f := newFixture(t, newStubClient())

	result, err := f.service.Republish(context.Background(), testBlock, 2, nil)
	if err != nil {
		t.Fatalf("Republish: %v", err)
	}

	want := Result{
		BlockNumber:    testBlock,
		BlockHash:      testHash,
		ParentHash:     testParentHash,
		BlockTimestamp: testTimestamp,
		Version:        2,
		DataTypes:      []string{"block", "receipts", "traces"},
	}
	if fmt.Sprint(result) != fmt.Sprint(want) {
		t.Errorf("result = %+v, want %+v", result, want)
	}
}

func TestRepublish_RefusesWhenTheHeightsCanonicalHashMovedBetweenTheTwoReads(t *testing.T) {
	client := newStubClient()
	client.headers[1] = headerReply{raw: headerJSON(forkHash)}
	f := newFixture(t, client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

	if !errors.Is(err, ErrCanonicalHashMoved) {
		t.Fatalf("error = %v, want ErrCanonicalHashMoved", err)
	}
	if errors.Is(err, ErrStructuralData) {
		t.Error("a live reorg must stay retryable, not be tagged structural")
	}
	if got := f.cached(t, "block", 1); got != nil {
		t.Errorf("cached %s despite the reorg", got)
	}
	if count := f.sink.GetEventCount(); count != 0 {
		t.Errorf("published %d events despite the reorg", count)
	}
}

// The finality guard already proved this height sits far below the head a synced
// node reported, so a null here is a lagging replica, not a height that is not
// there. Killing the block would take a repairable one out of the run for good.
func TestRepublish_LeavesANullFirstReadRetryable(t *testing.T) {
	client := newStubClient()
	client.headers[0] = headerReply{err: fmt.Errorf("eth_getBlockByNumber: %w", rpcutil.ErrUpstreamNullResult)}
	f := newFixtureWith(t, testConfig(), client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

	if err == nil {
		t.Fatal("Republish succeeded against a node with no block at the height")
	}
	if errors.Is(err, ErrStructuralData) {
		t.Errorf("error = %v, want it left retryable", err)
	}
	for _, want := range []string{fmt.Sprint(testBlock), "below the chain head"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to mention %q", err, want)
		}
	}
}

// A height still inside the reorg window can pass the canonical check and be
// orphaned moments later, writing a second losing fork into the slot meant to
// correct the first.
func TestRepublish_RefusesAHeightTooCloseToTheChainHead(t *testing.T) {
	tests := []struct {
		name string
		head int64
	}{
		{name: "above the head", head: testBlock - 1},
		{name: "at the head", head: testBlock},
		{name: "inside the reorg window", head: testBlock + finalityDepth - 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newStubClient()
			client.head = tc.head
			f := newFixtureWith(t, testConfig(), client)

			_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

			if !errors.Is(err, ErrStructuralData) {
				t.Fatalf("error = %v, want ErrStructuralData", err)
			}
			if f.client.headerCalls != 0 {
				t.Errorf("read the chain %d times for a refused height", f.client.headerCalls)
			}
		})
	}
}

func TestRepublish_AcceptsTheShallowestSafeHeight(t *testing.T) {
	client := newStubClient()
	client.head = testBlock + finalityDepth
	f := newFixtureWith(t, testConfig(), client)

	if _, err := f.service.Republish(context.Background(), testBlock, 1, nil); err != nil {
		t.Fatalf("Republish: %v", err)
	}
}

// A throttled or timed-out head read says nothing about the block, so tagging it
// structural would kill a run that the next attempt would complete.
func TestRepublish_LeavesAFailedHeadReadRetryable(t *testing.T) {
	client := newStubClient()
	client.headErr = errors.New("429 Too Many Requests")
	f := newFixtureWith(t, testConfig(), client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

	if err == nil {
		t.Fatal("Republish succeeded without reading the chain head")
	}
	if errors.Is(err, ErrStructuralData) {
		t.Errorf("error = %v, want it left retryable", err)
	}
}

func TestRepublish_IsStructuralWhenTheNodeAnswersNullForAnExpectedDataType(t *testing.T) {
	client := newStubClient()
	client.data.Receipts = nil
	f := newFixture(t, client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
	if count := f.sink.GetEventCount(); count != 0 {
		t.Errorf("published %d events for an incomplete payload", count)
	}
}

// The archive decides the slot: one past whatever it already holds, and the
// first correction slot at a height it holds nothing for. Version 0 is never a
// target — it carries the data being corrected.
func TestNextFreeVersion_DerivesTheSlotFromWhatTheArchiveHolds(t *testing.T) {
	tests := []struct {
		name    string
		archive stubArchive
		want    int
	}{
		{name: "an orphan-only height the archive never received", want: 1},
		{name: "the live version alone", archive: stubArchive{found: true}, want: 1},
		{name: "a slot already taken by an earlier correction", archive: stubArchive{highest: 1, found: true}, want: 2},
		{name: "a height corrected repeatedly", archive: stubArchive{highest: 4, found: true}, want: 5},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, newStubClient())
			*f.archive = tc.archive

			version, err := f.service.NextFreeVersion(context.Background(), testBlock)
			if err != nil {
				t.Fatalf("NextFreeVersion: %v", err)
			}

			if version != tc.want {
				t.Errorf("version = %d, want %d", version, tc.want)
			}
		})
	}
}

func TestNextFreeVersion_LogsTheVersionItChose(t *testing.T) {
	var logged strings.Builder
	config := testConfig()
	config.Logger = slog.New(slog.NewTextHandler(&logged, nil))
	f := newFixtureWith(t, config, newStubClient())
	f.archiveHolds(2)

	if _, err := f.service.NextFreeVersion(context.Background(), testBlock); err != nil {
		t.Fatalf("NextFreeVersion: %v", err)
	}

	for _, want := range []string{fmt.Sprintf("block=%d", testBlock), "version=3"} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("logs = %s, want a line carrying %q", logged.String(), want)
		}
	}
}

// A throttled listing says nothing about the height. Killing it as structural
// would take a repairable block out of the run for good, and republishing
// regardless would write over a slot that is already occupied.
func TestNextFreeVersion_LeavesAFailedArchiveReadRetryable(t *testing.T) {
	f := newFixture(t, newStubClient())
	f.archive.err = errors.New("503 SlowDown")

	_, err := f.service.NextFreeVersion(context.Background(), testBlock)

	if err == nil {
		t.Fatal("NextFreeVersion succeeded without reading the archive")
	}
	if errors.Is(err, ErrStructuralData) {
		t.Errorf("error = %v, want it left retryable", err)
	}
}

// Nothing under the height's own prefix should be unreadable, and no retry can
// change that, so the run must stop on it rather than burn its envelope.
func TestNextFreeVersion_IsStructuralWhenTheArchiveHoldsAKeyItCannotRead(t *testing.T) {
	f := newFixture(t, newStubClient())
	f.archive.err = fmt.Errorf("listing s3://bucket/25395000-25395999/25395651_: %w", s3key.ErrUnrecognisedKey)

	_, err := f.service.NextFreeVersion(context.Background(), testBlock)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
}

func TestNextFreeVersion_RejectsANonPositiveHeightWithoutListing(t *testing.T) {
	f := newFixture(t, newStubClient())

	_, err := f.service.NextFreeVersion(context.Background(), 0)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
	if f.archive.calls != 0 {
		t.Errorf("listed the archive %d times for a height that is not one", f.archive.calls)
	}
}

// The version is settled before a republish starts and handed to it. Reading the
// archive here would move the slot on a retry — the block's own publish is what
// fills the one it was given.
func TestRepublish_NeverReadsTheArchive(t *testing.T) {
	f := newFixture(t, newStubClient())

	if _, err := f.service.Republish(context.Background(), testBlock, 2, nil); err != nil {
		t.Fatalf("Republish: %v", err)
	}

	if f.archive.calls != 0 {
		t.Errorf("listed the archive %d times while republishing", f.archive.calls)
	}
}

func TestRepublish_IsStructuralWhenAskedForVersionZero(t *testing.T) {
	f := newFixture(t, newStubClient())

	_, err := f.service.Republish(context.Background(), testBlock, 0, nil)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
	if f.client.headerCalls != 0 {
		t.Errorf("read the chain %d times for a rejected version", f.client.headerCalls)
	}
}

func TestRepublish_LeavesATransientRPCFailureRetryable(t *testing.T) {
	client := newStubClient()
	client.dataErr = errors.New("429 Too Many Requests")
	f := newFixture(t, client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

	if err == nil {
		t.Fatal("Republish succeeded against a throttled node")
	}
	if errors.Is(err, ErrStructuralData) {
		t.Errorf("error = %v, want it left retryable", err)
	}
}

func TestRepublish_DoesNotPublishWhenTheCacheWriteFails(t *testing.T) {
	sink := memory.NewEventSink()
	service := newTestService(t, testConfig(), newStubClient(), &stubArchive{}, failingCache{err: errors.New("redis unavailable")}, sink)

	_, err := service.Republish(context.Background(), testBlock, 1, nil)

	if err == nil {
		t.Fatal("Republish succeeded against an unwritable cache")
	}
	if count := sink.GetEventCount(); count != 0 {
		t.Errorf("published %d events without a cached payload", count)
	}
}

func TestRepublish_SurfacesAPublishFailure(t *testing.T) {
	service := newTestService(t, testConfig(), newStubClient(), &stubArchive{}, memory.NewBlockCache(), failingSink{err: errors.New("sns unavailable")})

	_, err := service.Republish(context.Background(), testBlock, 1, nil)

	if err == nil {
		t.Fatal("Republish succeeded when the sink refused the event")
	}
}

func TestNewService_RequiresEveryDependency(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		client  outbound.BlockchainClient
		archive outbound.ArchiveVersionReader
		cache   outbound.BlockCacheWriter
		sink    outbound.EventSink
		wantErr string
	}{
		{
			name:    "missing client",
			config:  Config{ChainID: testChainID},
			archive: &stubArchive{},
			cache:   memory.NewBlockCache(),
			sink:    memory.NewEventSink(),
			wantErr: "client is required",
		},
		{
			name:    "missing archive reader",
			config:  Config{ChainID: testChainID},
			client:  newStubClient(),
			cache:   memory.NewBlockCache(),
			sink:    memory.NewEventSink(),
			wantErr: "archive version reader is required",
		},
		{
			name:    "missing cache",
			config:  Config{ChainID: testChainID},
			client:  newStubClient(),
			archive: &stubArchive{},
			sink:    memory.NewEventSink(),
			wantErr: "cache is required",
		},
		{
			name:    "missing event sink",
			config:  Config{ChainID: testChainID},
			client:  newStubClient(),
			archive: &stubArchive{},
			cache:   memory.NewBlockCache(),
			wantErr: "event sink is required",
		},
		{
			name:    "non-positive chain ID",
			config:  Config{},
			client:  newStubClient(),
			archive: &stubArchive{},
			cache:   memory.NewBlockCache(),
			sink:    memory.NewEventSink(),
			wantErr: "ChainID",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewService(tc.config, tc.client, tc.archive, tc.cache, tc.sink)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestRepublish_CachesBlobsWhenTheChainsWatcherFetchesThem(t *testing.T) {
	config := testConfig()
	config.EnableBlobs = true
	f := newFixtureWith(t, config, newStubClient())

	result, err := f.service.Republish(context.Background(), testBlock, 1, nil)
	if err != nil {
		t.Fatalf("Republish: %v", err)
	}

	if got := string(f.cached(t, "blobs", 1)); got != `[{"index":"0x0"}]` {
		t.Errorf("cached blobs = %s", got)
	}
	if got, want := fmt.Sprint(result.DataTypes), fmt.Sprint([]string{"block", "receipts", "traces", "blobs"}); got != want {
		t.Errorf("DataTypes = %v, want %v", got, want)
	}
}

func TestRepublish_RejectsANonPositiveBlockNumber(t *testing.T) {
	f := newFixture(t, newStubClient())

	_, err := f.service.Republish(context.Background(), 0, 1, nil)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
}

func TestRepublish_IsStructuralWhenTheHeaderCannotBeDecoded(t *testing.T) {
	tests := []struct {
		name string
		raw  json.RawMessage
	}{
		{name: "not an object", raw: json.RawMessage(`["0x1"]`)},
		{name: "no hash", raw: json.RawMessage(`{"parentHash":"0x2","timestamp":"0x1"}`)},
		{name: "unparseable timestamp", raw: json.RawMessage(`{"hash":"0x1","parentHash":"0x2","timestamp":"later"}`)},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newStubClient()
			client.headers[0] = headerReply{raw: tc.raw}
			f := newFixtureWith(t, testConfig(), client)

			_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

			if !errors.Is(err, ErrStructuralData) {
				t.Fatalf("error = %v, want ErrStructuralData", err)
			}
		})
	}
}

func TestRepublish_LeavesAFailedNumberReadRetryable(t *testing.T) {
	tests := []struct {
		name  string
		index int
	}{
		{name: "the first read", index: 0},
		{name: "the confirming read", index: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newStubClient()
			client.headers[tc.index] = headerReply{err: errors.New("dial tcp: connection refused")}
			f := newFixtureWith(t, testConfig(), client)

			_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

			if err == nil {
				t.Fatal("Republish succeeded against an unreachable node")
			}
			if errors.Is(err, ErrStructuralData) {
				t.Errorf("error = %v, want it left retryable", err)
			}
		})
	}
}

func TestRepublish_TreatsTheHeightVanishingOnTheConfirmingReadAsAReorg(t *testing.T) {
	client := newStubClient()
	client.headers[1] = headerReply{err: fmt.Errorf("eth_getBlockByNumber: %w", rpcutil.ErrUpstreamNullResult)}
	f := newFixtureWith(t, testConfig(), client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

	if !errors.Is(err, ErrCanonicalHashMoved) {
		t.Fatalf("error = %v, want ErrCanonicalHashMoved", err)
	}
}

func TestRepublish_IsStructuralWhenTheConfirmingReadCannotBeDecoded(t *testing.T) {
	client := newStubClient()
	client.headers[1] = headerReply{raw: json.RawMessage(`{"parentHash":"0x2","timestamp":"0x1"}`)}
	f := newFixtureWith(t, testConfig(), client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
}

func TestRepublish_LeavesAPerDataTypeRPCFailureRetryable(t *testing.T) {
	client := newStubClient()
	client.data.ReceiptsErr = errors.New("504 Gateway Timeout")
	f := newFixtureWith(t, testConfig(), client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, nil)

	if err == nil {
		t.Fatal("Republish succeeded with a failed receipts fetch")
	}
	if errors.Is(err, ErrStructuralData) {
		t.Errorf("error = %v, want it left retryable", err)
	}
}

func TestNewService_RunsWithoutALogger(t *testing.T) {
	service, err := NewService(Config{ChainID: testChainID, EnableTraces: true},
		newStubClient(), &stubArchive{}, memory.NewBlockCache(), memory.NewEventSink())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if _, err := service.Republish(context.Background(), testBlock, 1, nil); err != nil {
		t.Fatalf("Republish: %v", err)
	}
}

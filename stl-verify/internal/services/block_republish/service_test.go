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
	"github.com/archon-research/stl/stl-verify/internal/pkg/archiveblock"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	testChainID    = int64(1)
	testBlock      = int64(25395651)
	testHash       = "0x1111111111111111111111111111111111111111111111111111111111111111"
	testParentHash = "0x2222222222222222222222222222222222222222222222222222222222222222"
	forkHash       = "0x3333333333333333333333333333333333333333333333333333333333333333"
	testTimestamp  = int64(0x68b0c0c0)
	archivedKey    = "25395000-25395999/25395651_1_block.json.gz"
)

// stubClient answers only the by-number reads Republish issues. The embedded
// port is nil, so any other call panics rather than silently returning a zero
// value.
type stubClient struct {
	outbound.BlockchainClient

	headers     []headerReply
	headerCalls int

	payloads outbound.BlockData
	fetches  int

	head    int64
	headErr error
}

type headerReply struct {
	raw json.RawMessage
	err error
}

// GetBlockByNumber serves the header reads from the scripted replies, and the
// full-transaction read from the payload set.
func (c *stubClient) GetBlockByNumber(_ context.Context, _ int64, fullTx bool) (json.RawMessage, error) {
	if fullTx {
		c.fetches++
		return c.payloads.Block, c.payloads.BlockErr
	}
	if c.headerCalls >= len(c.headers) {
		panic(fmt.Sprintf("unexpected GetBlockByNumber call %d", c.headerCalls+1))
	}
	reply := c.headers[c.headerCalls]
	c.headerCalls++
	return reply.raw, reply.err
}

func (c *stubClient) GetBlockReceipts(context.Context, int64) (json.RawMessage, error) {
	c.fetches++
	return c.payloads.Receipts, c.payloads.ReceiptsErr
}

func (c *stubClient) GetBlockTraces(context.Context, int64) (json.RawMessage, error) {
	c.fetches++
	return c.payloads.Traces, c.payloads.TracesErr
}

func (c *stubClient) GetBlobSidecars(context.Context, int64) (json.RawMessage, error) {
	c.fetches++
	return c.payloads.Blobs, c.payloads.BlobsErr
}

// Alchemy serves trace_block by hash only near the head, and every height this
// repairs is far older than that.
func (c *stubClient) GetBlockDataByHash(context.Context, int64, string, bool) (outbound.BlockData, error) {
	panic("Republish fetched by hash")
}

func (c *stubClient) GetCurrentBlockNumber(context.Context) (int64, error) {
	return c.head, c.headErr
}

func headerJSON(hash string) json.RawMessage {
	return headerJSONAt(testBlock, hash)
}

func headerJSONAt(number int64, hash string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"number":"0x%x","hash":%q,"parentHash":%q,"timestamp":"0x%x"}`,
		number, hash, testParentHash, testTimestamp))
}

// blockPayload is the full-transaction block a node serves by number: it holds
// one transaction, so an empty receipt or trace list for it is a lagging answer.
// It carries the fields the event does, which the republish now reads from here
// rather than from a header of its own.
func blockPayload(hash string) json.RawMessage {
	return blockPayloadAt(testBlock, hash, `[{"hash":"0xaa"}]`)
}

func emptyBlockPayload(hash string) json.RawMessage {
	return blockPayloadAt(testBlock, hash, `[]`)
}

func blockPayloadAt(number int64, hash, transactions string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(
		`{"number":"0x%x","hash":%q,"parentHash":%q,"timestamp":"0x%x","transactions":%s}`,
		number, hash, testParentHash, testTimestamp, transactions))
}

func receiptsPayload(blockHash string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`[{"blockHash":%q,"status":"0x1"}]`, blockHash))
}

func tracesPayload(blockHash string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`[{"blockHash":%q,"type":"call"}]`, blockHash))
}

func newStubClient() *stubClient {
	return &stubClient{
		headers: []headerReply{{raw: headerJSON(testHash)}, {raw: headerJSON(testHash)}},
		payloads: outbound.BlockData{
			BlockNumber: testBlock,
			Block:       blockPayload(testHash),
			Receipts:    receiptsPayload(testHash),
			Traces:      tracesPayload(testHash),
			Blobs:       json.RawMessage(`[{"index":"0x0"}]`),
		},
		head: testBlock + 5000,
	}
}

// stubArchive stands in for the raw archive: the highest version a height already
// holds an object at, and the block that version names.
type stubArchive struct {
	highest int
	found   bool
	err     error
	calls   int

	hash      string
	hashFound bool
	hashErr   error
}

func (a *stubArchive) HighestVersion(context.Context, int64) (int, bool, error) {
	a.calls++
	return a.highest, a.found, a.err
}

func (a *stubArchive) BlockHashAt(context.Context, int64, int) (string, bool, error) {
	return a.hash, a.hashFound, a.hashErr
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

// archiveHoldsFork is a height whose top archived version names a losing fork:
// the shape a repair exists for.
func (f fixture) archiveHoldsFork(highest int) {
	f.archiveHolds(highest)
	f.archive.hash, f.archive.hashFound = forkHash, true
}

// archiveHoldsCanonical is the state a bulk-downloader repair leaves behind: the
// archive holds the canonical block and no indexer was told.
func (f fixture) archiveHoldsCanonical(highest int) {
	f.archiveHolds(highest)
	f.archive.hash, f.archive.hashFound = testHash, true
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

func newTestService(t *testing.T, config Config, client outbound.BlockchainClient, archive outbound.ArchiveReader, cache outbound.BlockCacheWriter, sink outbound.EventSink) *Service {
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

			result, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)
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

	if _, err := f.service.Republish(context.Background(), testBlock, 2, testHash, nil); err != nil {
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

	_, err := f.service.Republish(context.Background(), testBlock, 1, testHash, func(_ context.Context, phase Phase) {
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

	result, err := f.service.Republish(context.Background(), testBlock, 2, testHash, nil)
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

// The derivation already proved a synced node knows this height, so a null data
// type is a replica behind the head it read. Nothing is cached or published.
func TestRepublish_LeavesANullDataTypeRetryable(t *testing.T) {
	tests := []struct {
		name  string
		serve func(*stubClient)
	}{
		{name: "a missing receipts payload", serve: func(c *stubClient) { c.payloads.Receipts = nil }},
		{name: "a literal null traces payload", serve: func(c *stubClient) { c.payloads.Traces = json.RawMessage(`null`) }},
		{
			name: "the adapter's null sentinel on the block",
			serve: func(c *stubClient) {
				c.payloads.BlockErr = fmt.Errorf("eth_getBlockByNumber: %w", rpcutil.ErrUpstreamNullResult)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newStubClient()
			tc.serve(client)
			f := newFixture(t, client)

			_, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)

			if err == nil {
				t.Fatal("Republish succeeded on an incomplete payload")
			}
			if errors.Is(err, ErrStructuralData) {
				t.Errorf("error = %v, want it left retryable", err)
			}
			if got := f.cached(t, "block", 1); got != nil {
				t.Errorf("cached %s from an incomplete payload", got)
			}
			if count := f.sink.GetEventCount(); count != 0 {
				t.Errorf("published %d events for an incomplete payload", count)
			}
		})
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
			f.archive.hash, f.archive.hashFound = forkHash, tc.archive.found

			version, _, err := f.service.NextFreeVersion(context.Background(), testBlock)
			if err != nil {
				t.Fatalf("NextFreeVersion: %v", err)
			}

			if version != tc.want {
				t.Errorf("version = %d, want %d", version, tc.want)
			}
			if version < s3key.FirstCorrectionVersion {
				t.Errorf("version = %d, want at least %d — version 0 carries the data being corrected",
					version, s3key.FirstCorrectionVersion)
			}
		})
	}
}

// Republishing a height whose archive already holds the canonical block appends
// an identical correction that every reader then prefers — permanently, in S3 and
// in every indexer. No retry changes that, so the height is refused outright.
func TestNextFreeVersion_RefusesAHeightAlreadyCanonicalInTheArchive(t *testing.T) {
	f := newFixture(t, newStubClient())
	f.archiveHolds(1)
	f.archive.hash, f.archive.hashFound = strings.ToUpper(testHash), true

	_, _, err := f.service.NextFreeVersion(context.Background(), testBlock)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
	for _, want := range []string{fmt.Sprint(testBlock), "version 1", testHash} {
		if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(want)) {
			t.Errorf("error = %v, want it to mention %q", err, want)
		}
	}
}

func TestNextFreeVersion_ProceedsWhenTheArchivedBlockIsAFork(t *testing.T) {
	f := newFixture(t, newStubClient())
	f.archiveHoldsFork(1)

	version, _, err := f.service.NextFreeVersion(context.Background(), testBlock)
	if err != nil {
		t.Fatalf("NextFreeVersion: %v", err)
	}

	if version != 2 {
		t.Errorf("version = %d, want the slot above the fork", version)
	}
}

// A top version nothing can name a block from — no block object, no receipts, or
// only a data type this binary does not know — is a height to repair.
func TestNextFreeVersion_ProceedsWhenTheArchiveNamesNoBlockAtTheTopVersion(t *testing.T) {
	f := newFixture(t, newStubClient())
	f.archiveHolds(4)

	version, _, err := f.service.NextFreeVersion(context.Background(), testBlock)
	if err != nil {
		t.Fatalf("NextFreeVersion: %v", err)
	}

	if version != 5 {
		t.Errorf("version = %d, want the slot above the unreadable one", version)
	}
	if f.client.headerCalls != 1 {
		t.Errorf("read the chain %d times, want the one read the republish verifies against", f.client.headerCalls)
	}
}

func TestNextFreeVersion_LeavesAFailedHashReadRetryable(t *testing.T) {
	f := newFixture(t, newStubClient())
	f.archiveHolds(1)
	f.archive.hashErr = errors.New("503 SlowDown")

	_, _, err := f.service.NextFreeVersion(context.Background(), testBlock)

	if err == nil {
		t.Fatal("NextFreeVersion succeeded without reading the archived block")
	}
	if errors.Is(err, ErrStructuralData) {
		t.Errorf("error = %v, want it left retryable", err)
	}
}

// An archived object that will not decompress, or whose hash sits beyond the
// prefix, answers the same on every attempt. Retrying it burns the whole
// envelope on a fault only a repaired object can clear, so the height stops.
func TestNextFreeVersion_IsStructuralWhenTheArchivedObjectCannotBeRead(t *testing.T) {
	f := newFixture(t, newStubClient())
	f.archiveHolds(1)
	f.archive.hashErr = fmt.Errorf("reading %s: %w", archivedKey, archiveblock.ErrUnreadable)

	_, _, err := f.service.NextFreeVersion(context.Background(), testBlock)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
	if !strings.Contains(err.Error(), archivedKey) {
		t.Errorf("error = %v, want it to name the key", err)
	}
}

func TestNextFreeVersion_RefusesAHeightTooCloseToTheChainHead(t *testing.T) {
	client := newStubClient()
	client.head = testBlock + finalityDepth - 1
	f := newFixtureWith(t, testConfig(), client)

	_, _, err := f.service.NextFreeVersion(context.Background(), testBlock)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
	if f.archive.calls != 0 {
		t.Errorf("listed the archive %d times for a refused height", f.archive.calls)
	}
}

func TestNextFreeVersion_LeavesAFailedHeadReadRetryable(t *testing.T) {
	client := newStubClient()
	client.headErr = errors.New("429 Too Many Requests")
	f := newFixtureWith(t, testConfig(), client)

	_, _, err := f.service.NextFreeVersion(context.Background(), testBlock)

	if err == nil {
		t.Fatal("NextFreeVersion succeeded without reading the chain head")
	}
	if errors.Is(err, ErrStructuralData) {
		t.Errorf("error = %v, want it left retryable", err)
	}
}

// The comparison needs the canonical hash, so a by-number read that fails must
// not read as "the archive differs".
func TestNextFreeVersion_SurfacesAFailedCanonicalRead(t *testing.T) {
	client := newStubClient()
	client.headers[0] = headerReply{err: errors.New("dial tcp: connection refused")}
	f := newFixtureWith(t, testConfig(), client)
	f.archiveHoldsFork(1)

	_, _, err := f.service.NextFreeVersion(context.Background(), testBlock)

	if err == nil {
		t.Fatal("NextFreeVersion succeeded without the canonical hash to compare")
	}
	if errors.Is(err, ErrStructuralData) {
		t.Errorf("error = %v, want it left retryable", err)
	}
}

func TestNextFreeVersion_LogsTheVersionItChose(t *testing.T) {
	var logged strings.Builder
	config := testConfig()
	config.Logger = slog.New(slog.NewTextHandler(&logged, nil))
	f := newFixtureWith(t, config, newStubClient())
	f.archiveHoldsFork(2)

	if _, _, err := f.service.NextFreeVersion(context.Background(), testBlock); err != nil {
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

	_, _, err := f.service.NextFreeVersion(context.Background(), testBlock)

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

	_, _, err := f.service.NextFreeVersion(context.Background(), testBlock)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
}

func TestNextFreeVersion_RejectsANonPositiveHeightWithoutListing(t *testing.T) {
	f := newFixture(t, newStubClient())

	_, _, err := f.service.NextFreeVersion(context.Background(), 0)

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

	if _, err := f.service.Republish(context.Background(), testBlock, 2, testHash, nil); err != nil {
		t.Fatalf("Republish: %v", err)
	}

	if f.archive.calls != 0 {
		t.Errorf("listed the archive %d times while republishing", f.archive.calls)
	}
}

func TestRepublish_IsStructuralWhenAskedForANegativeVersion(t *testing.T) {
	f := newFixture(t, newStubClient())

	_, err := f.service.Republish(context.Background(), testBlock, -1, testHash, nil)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
	if f.client.fetches != 0 {
		t.Errorf("read the chain %d times for a rejected version", f.client.fetches)
	}
}

func TestRepublish_DoesNotPublishWhenTheCacheWriteFails(t *testing.T) {
	sink := memory.NewEventSink()
	service := newTestService(t, testConfig(), newStubClient(), &stubArchive{}, failingCache{err: errors.New("redis unavailable")}, sink)

	_, err := service.Republish(context.Background(), testBlock, 1, testHash, nil)

	if err == nil {
		t.Fatal("Republish succeeded against an unwritable cache")
	}
	if count := sink.GetEventCount(); count != 0 {
		t.Errorf("published %d events without a cached payload", count)
	}
}

func TestRepublish_SurfacesAPublishFailure(t *testing.T) {
	service := newTestService(t, testConfig(), newStubClient(), &stubArchive{}, memory.NewBlockCache(), failingSink{err: errors.New("sns unavailable")})

	_, err := service.Republish(context.Background(), testBlock, 1, testHash, nil)

	if err == nil {
		t.Fatal("Republish succeeded when the sink refused the event")
	}
}

func TestNewService_RequiresEveryDependency(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		client  outbound.BlockchainClient
		archive outbound.ArchiveReader
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

	result, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)
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

	_, err := f.service.Republish(context.Background(), 0, 1, testHash, nil)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
}

func TestRepublish_LeavesAFailedPayloadReadRetryable(t *testing.T) {
	tests := []struct {
		name string
		fail func(*stubClient)
	}{
		{name: "the block", fail: func(c *stubClient) { c.payloads.BlockErr = errors.New("504 Gateway Timeout") }},
		{name: "the receipts", fail: func(c *stubClient) { c.payloads.ReceiptsErr = errors.New("504 Gateway Timeout") }},
		{name: "the traces", fail: func(c *stubClient) { c.payloads.TracesErr = errors.New("504 Gateway Timeout") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newStubClient()
			tc.fail(client)
			f := newFixtureWith(t, testConfig(), client)

			_, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)

			if err == nil {
				t.Fatal("Republish succeeded with a failed fetch")
			}
			if errors.Is(err, ErrStructuralData) {
				t.Errorf("error = %v, want it left retryable", err)
			}
			if count := f.sink.GetEventCount(); count != 0 {
				t.Errorf("published %d events for a block it could not fetch", count)
			}
		})
	}
}

func TestNewService_RunsWithoutALogger(t *testing.T) {
	service, err := NewService(Config{ChainID: testChainID, EnableTraces: true},
		newStubClient(), &stubArchive{}, memory.NewBlockCache(), memory.NewEventSink())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if _, err := service.Republish(context.Background(), testBlock, 1, testHash, nil); err != nil {
		t.Fatalf("Republish: %v", err)
	}
}

func TestNextFreeVersion_RefusesAHeaderDescribingADifferentHeight(t *testing.T) {
	client := newStubClient()
	client.headers[0] = headerReply{raw: headerJSONAt(testBlock-1, testHash)}
	f := newFixtureWith(t, testConfig(), client)
	f.archiveHoldsFork(1)

	_, _, err := f.service.NextFreeVersion(context.Background(), testBlock)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
}

// A height the archive holds nothing for skips the already-canonical comparison
// — there is no archived object to compare against — so version 1 is taken on
// the operator's word alone, and the run has to say so.
func TestNextFreeVersion_WarnsWhenTheArchiveHoldsNothingToCompareAgainst(t *testing.T) {
	recorder := &testutil.SlogRecorder{}
	config := testConfig()
	config.Logger = slog.New(recorder)
	f := newFixtureWith(t, config, newStubClient())

	version, _, err := f.service.NextFreeVersion(context.Background(), testBlock)
	if err != nil {
		t.Fatalf("NextFreeVersion: %v", err)
	}

	if version != s3key.FirstCorrectionVersion {
		t.Errorf("version = %d, want %d", version, s3key.FirstCorrectionVersion)
	}
	if got := recorder.CountWarn("no archived"); got != 1 {
		t.Errorf("warnings = %d, want 1; warn messages: %v", got, recorder.MessagesAt(slog.LevelWarn))
	}
	if !recorder.ContainsAttr(fmt.Sprint(testBlock)) {
		t.Error("the warning names no height, so an operator cannot tell which block it covers")
	}
}

// A height whose archive does hold an object is compared against the chain, so
// there is nothing to warn about.
func TestNextFreeVersion_DoesNotWarnWhenTheArchiveHoldsTheHeight(t *testing.T) {
	recorder := &testutil.SlogRecorder{}
	config := testConfig()
	config.Logger = slog.New(recorder)
	f := newFixtureWith(t, config, newStubClient())
	f.archiveHoldsFork(1)

	if _, _, err := f.service.NextFreeVersion(context.Background(), testBlock); err != nil {
		t.Fatalf("NextFreeVersion: %v", err)
	}

	if got := recorder.MessagesAt(slog.LevelWarn); len(got) != 0 {
		t.Errorf("warn messages = %v, want none", got)
	}
}

// A #849 repair fixes S3 alone and tells no indexer, so the event has to go out
// AT the version the repaired objects sit in: the backup worker's if-not-exists
// write is then a no-op and every indexer appends that version.
func TestArchivedVersion_PublishesAtTheVersionTheRepairedArchiveHolds(t *testing.T) {
	tests := []struct {
		name    string
		highest int
	}{
		{name: "a height the repair archived fresh, at version 0", highest: 0},
		{name: "a correction the repair wrote above the fork", highest: 1},
		{name: "a height repaired above earlier corrections", highest: 4},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, newStubClient())
			f.archiveHoldsCanonical(tc.highest)

			version, _, err := f.service.ArchivedVersion(context.Background(), testBlock)
			if err != nil {
				t.Fatalf("ArchivedVersion: %v", err)
			}

			if version != tc.highest {
				t.Errorf("version = %d, want the version the archive already holds, %d", version, tc.highest)
			}
		})
	}
}

// archiveRepaired is a claim about the archive, and every shape that contradicts
// it reproduces on every attempt: publishing anyway would put a fork at the
// version the canonical block was supposed to occupy.
func TestArchivedVersion_RefusesAnArchiveThatDoesNotHoldTheCanonicalBlock(t *testing.T) {
	tests := []struct {
		name     string
		archive  func(f fixture)
		mentions []string
	}{
		{
			name:     "a height the archive holds nothing at",
			archive:  func(fixture) {},
			mentions: []string{"archiveRepaired", "holds nothing", "25395651"},
		},
		{
			name:     "a top version that names no block",
			archive:  func(f fixture) { f.archiveHolds(2) },
			mentions: []string{"version 2", "names no block"},
		},
		{
			name:     "a top version that is still the losing fork",
			archive:  func(f fixture) { f.archiveHoldsFork(2) },
			mentions: []string{"version 2", "not the canonical block", "drop archiveRepaired"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, newStubClient())
			tc.archive(f)

			_, _, err := f.service.ArchivedVersion(context.Background(), testBlock)

			if !errors.Is(err, ErrStructuralData) {
				t.Fatalf("error = %v, want ErrStructuralData", err)
			}
			for _, want := range tc.mentions {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error = %v, want it to mention %q", err, want)
				}
			}
		})
	}
}

func TestArchivedVersion_RefusesAHeightTooCloseToTheChainHead(t *testing.T) {
	client := newStubClient()
	client.head = testBlock + finalityDepth - 1
	f := newFixtureWith(t, testConfig(), client)

	_, _, err := f.service.ArchivedVersion(context.Background(), testBlock)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
	if f.archive.calls != 0 {
		t.Errorf("listed the archive %d times for a refused height", f.archive.calls)
	}
}

func TestArchivedVersion_LeavesAFailedHashReadRetryable(t *testing.T) {
	f := newFixture(t, newStubClient())
	f.archiveHolds(1)
	f.archive.hashErr = errors.New("503 SlowDown")

	_, _, err := f.service.ArchivedVersion(context.Background(), testBlock)

	if err == nil {
		t.Fatal("ArchivedVersion succeeded without reading the archived block")
	}
	if errors.Is(err, ErrStructuralData) {
		t.Errorf("error = %v, want it left retryable", err)
	}
}

// The flag is what makes this run land where it does, so the line that records
// the decision has to name it alongside the version and the hash it matched.
func TestArchivedVersion_LogsTheRepairedDerivation(t *testing.T) {
	var logged strings.Builder
	config := testConfig()
	config.Logger = slog.New(slog.NewTextHandler(&logged, nil))
	f := newFixtureWith(t, config, newStubClient())
	f.archiveHoldsCanonical(2)

	if _, _, err := f.service.ArchivedVersion(context.Background(), testBlock); err != nil {
		t.Fatalf("ArchivedVersion: %v", err)
	}

	for _, want := range []string{"archiveRepaired=true", "version=2", "hash=" + testHash} {
		if !strings.Contains(logged.String(), want) {
			t.Errorf("logs = %s, want a line carrying %q", logged.String(), want)
		}
	}
}

// Version 0 is a real target for an archiveRepaired run at a height #849
// archived fresh, so Republish takes the version its run derived rather than
// re-deciding which slots are legal.
func TestRepublish_PublishesAtVersionZeroWhenTheRunDerivedIt(t *testing.T) {
	f := newFixture(t, newStubClient())

	result, err := f.service.Republish(context.Background(), testBlock, 0, testHash, nil)
	if err != nil {
		t.Fatalf("Republish: %v", err)
	}

	if result.Version != 0 {
		t.Errorf("version = %d, want 0", result.Version)
	}
	if got := f.publishedEvent(t).Version; got != 0 {
		t.Errorf("published version = %d, want 0", got)
	}
	if got := f.cached(t, "block", 0); got == nil {
		t.Error("cached nothing at version 0")
	}
}

// Fetching by number lets a replica behind the head answer with another block's
// receipts or traces, and caching those would publish one block's logs under
// another's hash. Retryable: the next attempt asks again.
func TestRepublish_LeavesAPayloadDescribingAnotherBlockRetryable(t *testing.T) {
	tests := []struct {
		name     string
		serve    func(*stubClient)
		mentions []string
	}{
		{
			name:     "receipts naming another block",
			serve:    func(c *stubClient) { c.payloads.Receipts = receiptsPayload(forkHash) },
			mentions: []string{"receipts", forkHash, testHash},
		},
		{
			name:     "traces naming another block",
			serve:    func(c *stubClient) { c.payloads.Traces = tracesPayload(forkHash) },
			mentions: []string{"traces", forkHash, testHash},
		},
		{
			name:     "receipts naming no block at all",
			serve:    func(c *stubClient) { c.payloads.Receipts = json.RawMessage(`[{"status":"0x1"}]`) },
			mentions: []string{"receipts", "no block"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newStubClient()
			tc.serve(client)
			f := newFixtureWith(t, testConfig(), client)

			_, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)

			if err == nil {
				t.Fatal("Republish succeeded on a payload describing another block")
			}
			if errors.Is(err, ErrStructuralData) {
				t.Errorf("error = %v, want it left retryable", err)
			}
			for _, want := range tc.mentions {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error = %v, want it to mention %q", err, want)
				}
			}
			if got := f.cached(t, "block", 1); got != nil {
				t.Errorf("cached %s from a payload set describing another block", got)
			}
			if count := f.sink.GetEventCount(); count != 0 {
				t.Errorf("published %d events from a payload set describing another block", count)
			}
		})
	}
}

// An empty list is what a lagging replica answers with, and the only block it is
// the truth for is one with no transactions.
func TestRepublish_LeavesAnEmptyPayloadForABlockWithTransactionsRetryable(t *testing.T) {
	tests := []struct {
		name  string
		serve func(*stubClient)
	}{
		{name: "no receipts", serve: func(c *stubClient) { c.payloads.Receipts = json.RawMessage(`[]`) }},
		{name: "no traces", serve: func(c *stubClient) { c.payloads.Traces = json.RawMessage(`[]`) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newStubClient()
			tc.serve(client)
			f := newFixtureWith(t, testConfig(), client)

			_, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)

			if err == nil {
				t.Fatal("Republish succeeded on an empty payload for a block with transactions")
			}
			if errors.Is(err, ErrStructuralData) {
				t.Errorf("error = %v, want it left retryable", err)
			}
			if !strings.Contains(err.Error(), "transactions") {
				t.Errorf("error = %v, want it to say the block has transactions", err)
			}
			if got := f.cached(t, "block", 1); got != nil {
				t.Errorf("cached %s from an incomplete payload set", got)
			}
			if count := f.sink.GetEventCount(); count != 0 {
				t.Errorf("published %d events from an incomplete payload set", count)
			}
		})
	}
}

func TestRepublish_AcceptsEmptyReceiptsAndTracesForABlockWithNoTransactions(t *testing.T) {
	client := newStubClient()
	client.payloads.Block = emptyBlockPayload(testHash)
	client.payloads.Receipts = json.RawMessage(`[]`)
	client.payloads.Traces = json.RawMessage(`[]`)
	f := newFixtureWith(t, testConfig(), client)

	result, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)
	if err != nil {
		t.Fatalf("Republish: %v", err)
	}

	if got, want := fmt.Sprint(result.DataTypes), fmt.Sprint([]string{"block", "receipts", "traces"}); got != want {
		t.Errorf("DataTypes = %v, want %v", got, want)
	}
	for _, dataType := range []string{"receipts", "traces"} {
		if got := string(f.cached(t, dataType, 1)); got != `[]` {
			t.Errorf("cached %s = %s, want the empty list the node served", dataType, got)
		}
	}
}

func TestRepublish_CachesThePayloadsItFetchedAtTheVersion(t *testing.T) {
	f := newFixture(t, newStubClient())

	if _, err := f.service.Republish(context.Background(), testBlock, 3, testHash, nil); err != nil {
		t.Fatalf("Republish: %v", err)
	}

	want := map[string]json.RawMessage{
		"block":    blockPayload(testHash),
		"receipts": receiptsPayload(testHash),
		"traces":   tracesPayload(testHash),
	}
	for dataType, payload := range want {
		if got := string(f.cached(t, dataType, 3)); got != string(payload) {
			t.Errorf("cached %s = %s, want %s", dataType, got, payload)
		}
	}
	if got := f.publishedEvent(t).Version; got != 3 {
		t.Errorf("published version = %d, want 3", got)
	}
}

// Without the hash its derivation read, a republish has nothing to hold the
// payloads to — and the run that derived one is the only caller.
func TestRepublish_RejectsARunThatDerivedNoCanonicalHash(t *testing.T) {
	f := newFixture(t, newStubClient())

	_, err := f.service.Republish(context.Background(), testBlock, 1, "", nil)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
	if f.client.fetches != 0 {
		t.Errorf("read the chain %d times with nothing to verify against", f.client.fetches)
	}
}

// The height reorged after its version was derived, or the node answered with an
// orphan. Either way the block is not the one this run is repairing to, and
// publishing it would enshrine a second losing fork.
func TestRepublish_RefusesABlockThatIsNotTheOneTheRunDerived(t *testing.T) {
	client := newStubClient()
	client.payloads.Block = blockPayload(forkHash)
	f := newFixtureWith(t, testConfig(), client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)

	if !errors.Is(err, ErrCanonicalHashMoved) {
		t.Fatalf("error = %v, want ErrCanonicalHashMoved", err)
	}
	if errors.Is(err, ErrStructuralData) {
		t.Error("a height that moved must stay retryable, not be tagged structural")
	}
	for _, want := range []string{forkHash, testHash} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to name %q", err, want)
		}
	}
	if got := f.cached(t, "block", 1); got != nil {
		t.Errorf("cached %s from a block the run did not derive", got)
	}
	if count := f.sink.GetEventCount(); count != 0 {
		t.Errorf("published %d events from a block the run did not derive", count)
	}
}

func TestRepublish_LeavesABlockPayloadNamingNoBlockRetryable(t *testing.T) {
	client := newStubClient()
	client.payloads.Block = json.RawMessage(`{"number":"0x1"}`)
	f := newFixtureWith(t, testConfig(), client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)

	if err == nil {
		t.Fatal("Republish succeeded on a payload naming no block")
	}
	if errors.Is(err, ErrStructuralData) {
		t.Errorf("error = %v, want it left retryable", err)
	}
	if !strings.Contains(err.Error(), testHash) {
		t.Errorf("error = %v, want it to name the hash the run derived", err)
	}
}

// A replica answering by number with a neighbouring height would have its block
// cached and published under the height that was asked for.
func TestRepublish_RefusesABlockPayloadDescribingADifferentHeight(t *testing.T) {
	client := newStubClient()
	client.payloads.Block = blockPayloadAt(testBlock-1, testHash, `[]`)
	f := newFixtureWith(t, testConfig(), client)

	_, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)

	if !errors.Is(err, ErrStructuralData) {
		t.Fatalf("error = %v, want ErrStructuralData", err)
	}
	for _, want := range []string{fmt.Sprint(testBlock), fmt.Sprint(testBlock - 1)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to name %q", err, want)
		}
	}
	if got := f.cached(t, "block", 1); got != nil {
		t.Errorf("cached %s from a payload describing another height", got)
	}
}

// The event's timestamp comes from the payload now, so a payload that cannot
// answer for it stops the height rather than publishing a zero.
func TestRepublish_IsStructuralWhenTheBlockPayloadCannotBeDecoded(t *testing.T) {
	tests := []struct {
		name    string
		payload json.RawMessage
	}{
		{name: "no timestamp", payload: json.RawMessage(fmt.Sprintf(`{"number":"0x%x","hash":%q,"transactions":[]}`, testBlock, testHash))},
		{name: "an unparseable timestamp", payload: json.RawMessage(fmt.Sprintf(`{"number":"0x%x","hash":%q,"timestamp":"later","transactions":[]}`, testBlock, testHash))},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newStubClient()
			client.payloads.Block = tc.payload
			f := newFixtureWith(t, testConfig(), client)

			_, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil)

			if !errors.Is(err, ErrStructuralData) {
				t.Fatalf("error = %v, want ErrStructuralData", err)
			}
		})
	}
}

// The three payload reads are the whole RPC cost of a republish: the hash comes
// from the derivation, and at this depth a second read of it can only confirm
// what the first one saw.
func TestRepublish_ReadsTheChainThreeTimes(t *testing.T) {
	f := newFixture(t, newStubClient())

	if _, err := f.service.Republish(context.Background(), testBlock, 1, testHash, nil); err != nil {
		t.Fatalf("Republish: %v", err)
	}

	if f.client.fetches != 3 {
		t.Errorf("issued %d payload reads, want block, receipts and traces", f.client.fetches)
	}
	if f.client.headerCalls != 0 {
		t.Errorf("read the canonical header %d times, want it taken from the derivation", f.client.headerCalls)
	}
}

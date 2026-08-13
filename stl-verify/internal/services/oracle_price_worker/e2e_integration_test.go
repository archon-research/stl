//go:build integration

// VEC-242: end-to-end test that the watcher refuses to publish a block whose
// JSON-RPC body came back as the literal `null`, and that — once the upstream
// recovers — the backfill retry loop publishes the block and the oracle price
// worker (acting as a downstream consumer) processes it without seeing a null
// payload.
//
// What is mocked:
//   - Alchemy HTTP JSON-RPC (a small `nullableRPCServer` declared in this file)
//   - Alchemy WebSocket subscriber (testutil.MockSubscriber)
//   - SQS bus (an in-process bridge from memory.EventSink to outbound.SQSConsumer)
//
// Everything else is the real production code: real `alchemy.Client`, real
// `live_data.LiveService`, real `backfill_gaps.BackfillService`, real
// `oracle_price_worker.Service`, real Postgres (testcontainer), and a real
// `outbound.BlockCache` shared between the watcher's writer and the worker's
// reader. The fix lives in the alchemy adapter and watcher, so all the bits
// the fix changes are exercised by their real implementations here.

package oracle_price_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/backfill_gaps"
	"github.com/archon-research/stl/stl-verify/internal/services/live_data"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Test helpers (scoped to this file)
// ---------------------------------------------------------------------------

// nullableRPCServer is a minimal Alchemy-compatible JSON-RPC HTTP server. It
// accepts both single and batched requests and, for each request, returns
// either the configured payload or a literal `null` result when the block is
// flagged as racy. The null flag is per-block-number so a single instance can
// flip a recovering block while leaving its neighbours intact — that is what
// makes it possible to exercise the gap-fill path (`eth_getBlockByNumber`),
// the by-hash live path (`eth_getBlockByHash`), and the retry path
// independently.
//
// Unlike a permissive mock, this server fails the test on any unknown RPC
// method: silent default-null returns would mask future adapter changes (e.g.
// adding a new RPC) by returning a fake-but-acceptable result.
type nullableRPCServer struct {
	t      *testing.T
	server *httptest.Server

	mu sync.Mutex
	// returnNullForNum[blockNum] == true → all RPC calls for this block
	// number/hash return `{"result": null}`. Flip back to false to simulate
	// upstream propagation completing.
	returnNullForNum map[int64]bool
	// stored block data, keyed by decimal block number.
	blockByNum    map[int64]json.RawMessage
	receiptsByNum map[int64]json.RawMessage
	// hash → number index so by-hash lookups resolve to the same payload.
	numByHash map[string]int64
}

func newNullableRPCServer(t *testing.T) *nullableRPCServer {
	t.Helper()
	s := &nullableRPCServer{
		t:                t,
		returnNullForNum: make(map[int64]bool),
		blockByNum:       make(map[int64]json.RawMessage),
		receiptsByNum:    make(map[int64]json.RawMessage),
		numByHash:        make(map[string]int64),
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

func (s *nullableRPCServer) URL() string { return s.server.URL }
func (s *nullableRPCServer) Close()      { s.server.Close() }

// SetBlockData registers block+receipts JSON to be returned for the given
// block, indexed by both decimal number and hash so eth_getBlockByHash /
// eth_getBlockByNumber / eth_getBlockReceipts all resolve to it.
func (s *nullableRPCServer) SetBlockData(blockNum int64, hash string, block, receipts json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockByNum[blockNum] = block
	s.receiptsByNum[blockNum] = receipts
	s.numByHash[hash] = blockNum
}

// SetReturnNull toggles whether RPC calls for this block (by hash or number)
// respond with `{"result": null}` (true) or with the stored data (false).
func (s *nullableRPCServer) SetReturnNull(blockNum int64, enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.returnNullForNum[blockNum] = enabled
}

func (s *nullableRPCServer) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if len(raw) > 0 && raw[0] == '[' {
		s.handleBatch(w, raw)
		return
	}
	s.handleSingle(w, raw)
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      int             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
}

type rpcRequest struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      int               `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}

func (s *nullableRPCServer) handleSingle(w http.ResponseWriter, raw json.RawMessage) {
	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: s.resultFor(req.Method, req.Params)}
	_ = json.NewEncoder(w).Encode(resp)
}

func (s *nullableRPCServer) handleBatch(w http.ResponseWriter, raw json.RawMessage) {
	var reqs []rpcRequest
	if err := json.Unmarshal(raw, &reqs); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	responses := make([]rpcResponse, len(reqs))
	for i, req := range reqs {
		responses[i] = rpcResponse{JSONRPC: "2.0", ID: req.ID, Result: s.resultFor(req.Method, req.Params)}
	}
	_ = json.NewEncoder(w).Encode(responses)
}

// resultFor returns the JSON-RPC result for a single (method, params) call.
// It mirrors what real Alchemy returns when the requested block has not yet
// propagated through the backend ("null") versus when it has (the data). The
// helper fails the test on any unknown method so a future adapter that adds a
// new RPC call cannot silently get away with a fabricated null reply.
func (s *nullableRPCServer) resultFor(method string, params []json.RawMessage) json.RawMessage {
	switch method {
	case "eth_blockNumber":
		return json.RawMessage(`"0x64"`)
	case "eth_getBlockByHash":
		return s.resolveBlock(s.numForHash(params), params)
	case "eth_getBlockByNumber":
		return s.resolveBlock(parseHexBlockNumber(params), params)
	case "eth_getBlockReceipts":
		return s.resolveReceipts(s.numForHashOrHex(params))
	case "trace_block":
		// Synthesise an empty traces array for unknown blocks; null when racy.
		num := s.numForHashOrHex(params)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.returnNullForNum[num] {
			return json.RawMessage("null")
		}
		return json.RawMessage(`[]`)
	case "eth_getBlobSidecars":
		num := s.numForHashOrHex(params)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.returnNullForNum[num] {
			return json.RawMessage("null")
		}
		return json.RawMessage(`[]`)
	default:
		s.t.Errorf("nullableRPCServer: unexpected RPC method %q (params=%v) — extend the mock to handle it", method, params)
		return json.RawMessage("null")
	}
}

func (s *nullableRPCServer) resolveBlock(num int64, _ []json.RawMessage) json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.returnNullForNum[num] {
		return json.RawMessage("null")
	}
	if data, ok := s.blockByNum[num]; ok {
		return data
	}
	return json.RawMessage("null")
}

func (s *nullableRPCServer) resolveReceipts(num int64) json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.returnNullForNum[num] {
		return json.RawMessage("null")
	}
	if data, ok := s.receiptsByNum[num]; ok {
		return data
	}
	return json.RawMessage(`[]`)
}

// numForHash resolves a hash-typed first param to a stored block number, or
// -1 (an impossible block number) when unknown. -1 routes the call to the
// "null" branch above so unknown hashes consistently surface as null.
func (s *nullableRPCServer) numForHash(params []json.RawMessage) int64 {
	hash := parseStringParam(params)
	s.mu.Lock()
	defer s.mu.Unlock()
	if num, ok := s.numByHash[hash]; ok {
		return num
	}
	return -1
}

// numForHashOrHex accepts either a 32-byte hash or a hex block number (the
// receipts/traces/blobs endpoints take both). Hex numbers are parsed
// directly; hashes go through numByHash.
func (s *nullableRPCServer) numForHashOrHex(params []json.RawMessage) int64 {
	raw := parseStringParam(params)
	if len(raw) >= 2 && raw[0] == '0' && (raw[1] == 'x' || raw[1] == 'X') {
		if len(raw) == 66 { // 0x + 32 bytes (block hash)
			s.mu.Lock()
			defer s.mu.Unlock()
			if num, ok := s.numByHash[raw]; ok {
				return num
			}
			return -1
		}
		if n, err := strconv.ParseInt(raw[2:], 16, 64); err == nil {
			return n
		}
	}
	return -1
}

func parseHexBlockNumber(params []json.RawMessage) int64 {
	raw := parseStringParam(params)
	if len(raw) >= 2 && raw[0] == '0' && (raw[1] == 'x' || raw[1] == 'X') {
		if n, err := strconv.ParseInt(raw[2:], 16, 64); err == nil {
			return n
		}
	}
	return -1
}

func parseStringParam(params []json.RawMessage) string {
	if len(params) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(params[0], &s); err != nil {
		return ""
	}
	return s
}

// eventSinkConsumer bridges memory.EventSink (which the LiveService publishes
// to) into outbound.SQSConsumer (which the oracle price worker pulls from).
// Each published BlockEvent is JSON-serialised and queued so the worker sees
// the same wire shape it would see in production. Failures in the bridge
// surface via t.Errorf so a future event-shape regression cannot make the
// test pass by simply dropping the event.
type eventSinkConsumer struct {
	t  *testing.T
	mu sync.Mutex
	// pending is the queue of un-Received messages.
	pending []outbound.SQSMessage
	// deleted is an in-order log of every successful DeleteMessage receipt
	// handle. Unbounded slice so the test can grow without silent drops.
	deleted []string
	closed  bool
}

func newEventSinkConsumer(t *testing.T, sink *memory.EventSink) *eventSinkConsumer {
	t.Helper()
	c := &eventSinkConsumer{t: t}
	sink.OnPublish(func(e outbound.Event) {
		be, ok := e.(outbound.BlockEvent)
		if !ok {
			t.Errorf("eventSinkConsumer: published event is not a BlockEvent (type %T) — extend the bridge", e)
			return
		}
		body, err := json.Marshal(be)
		if err != nil {
			t.Errorf("eventSinkConsumer: marshal BlockEvent: %v", err)
			return
		}
		c.mu.Lock()
		defer c.mu.Unlock()
		c.pending = append(c.pending, outbound.SQSMessage{
			MessageID:     fmt.Sprintf("evt-%d-%d", be.BlockNumber, be.Version),
			Body:          string(body),
			ReceiptHandle: fmt.Sprintf("rh-%d-%d", be.BlockNumber, be.Version),
		})
	})
	return c
}

func (c *eventSinkConsumer) ReceiveMessages(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(c.pending) == 0 {
		return nil, nil
	}
	n := len(c.pending)
	if n > maxMessages {
		n = maxMessages
	}
	out := append([]outbound.SQSMessage(nil), c.pending[:n]...)
	c.pending = c.pending[n:]
	return out, nil
}

func (c *eventSinkConsumer) DeleteMessage(_ context.Context, receiptHandle string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.deleted = append(c.deleted, receiptHandle)
	return nil
}

func (c *eventSinkConsumer) DeleteCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.deleted)
}

func (c *eventSinkConsumer) VisibilityTimeout() time.Duration {
	return 300 * time.Second
}

func (c *eventSinkConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

// waitUntilWatcherSettled polls until either (a) the watcher has finished
// processing the block by publishing an event (pre-fix path) or (b) two
// consecutive 100ms-apart samples agree that the block_states row exists,
// BlockPublished is false, and no event has been published (post-fix path).
// Fails the test on overall deadline. The settling-window approach removes
// the fixed-sleep flake risk noted by the reviewers.
func waitUntilWatcherSettled(t *testing.T, ctx context.Context, repo *postgres.BlockStateRepository, hash string, sink *memory.EventSink, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	var lastEventCount int
	stableTicks := 0
	for time.Now().Before(end) {
		bs, err := repo.GetBlockByHash(ctx, hash)
		if err == nil && bs != nil {
			events := len(sink.GetBlockEvents())
			if bs.BlockPublished || events > 0 {
				return // watcher published — settled
			}
			if events == lastEventCount {
				stableTicks++
				if stableTicks >= 2 {
					return // two stable samples — rejection settled
				}
			} else {
				stableTicks = 0
			}
			lastEventCount = events
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("watcher did not settle within %v (block %s)", deadline, hash)
}

// ---------------------------------------------------------------------------
// Test
// ---------------------------------------------------------------------------

// TestE2E_VEC242_NullBlockBodyRejected_RetryThenWorkerProcesses spans the full
// pipeline: a header arrives via WebSocket → the watcher fetches the block →
// upstream returns null → backfill retries → upstream recovers → worker
// processes the (now-valid) cached block.
//
// Without the fix, the watcher caches `[]byte("null")` and publishes a
// BlockEvent on the first attempt, marking block_published=true. The retry
// loop skips the block forever (it filters `WHERE NOT block_published`), the
// cached payload remains null, and the worker chokes on the null body — it
// either parses an empty timestamp string (failing) or it deletes the SQS
// message on a successful "no-op" run after publish-time mishaps. Either way,
// no price row appears for the block, and the test times out at the
// WaitForCondition below.
//
// With the fix, the watcher refuses the null body, the backfill retry loop
// catches the unpublished row on its next tick, refetches once upstream has
// recovered, and only then caches+publishes. The worker then processes a
// valid block and writes prices to the database.
func TestE2E_VEC242_NullBlockBodyRejected_RetryThenWorkerProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// --- Schema, oracle/token fixtures, repositories ---
	pool, _, schemaCleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(schemaCleanup)

	logger := testutil.DiscardLogger()

	testutil.DisableAllOracles(t, ctx, pool)
	oracleID := testutil.SeedOracle(t, ctx, pool, "vec242-test", "VEC-242 Test", 1, "0x0000000000000000000000000000000000000AAA")
	tokenID := testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000051", "VTK", 18)
	testutil.SeedOracleAsset(t, ctx, pool, oracleID, tokenID)

	priceRepo, err := postgres.NewOnchainPriceRepository(pool, logger, 0, 100)
	if err != nil {
		t.Fatalf("NewOnchainPriceRepository: %v", err)
	}
	blockStateRepo := postgres.NewBlockStateRepository(pool, 1, logger)

	// --- In-memory pieces shared across the watcher and worker ---
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// --- Mock Alchemy HTTP RPC ---
	const blockNum = int64(100)
	blockHash := "0x" + strings.Repeat("a", 64)
	parentHash := "0x" + strings.Repeat("b", 64)
	blockTS := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	blockJSON := json.RawMessage(fmt.Sprintf(
		`{"number":"0x%x","hash":"%s","parentHash":"%s","timestamp":"0x%x"}`,
		blockNum, blockHash, parentHash, blockTS,
	))
	receiptsJSON := json.RawMessage(`[]`)

	rpc := newNullableRPCServer(t)
	rpc.SetBlockData(blockNum, blockHash, blockJSON, receiptsJSON)
	rpc.SetReturnNull(blockNum, true) // start in the racy state
	t.Cleanup(rpc.Close)

	alchemyClient, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL:    rpc.URL(),
		MaxRetries: 1, // keep the test fast — null is not retried in-adapter anyway
		Logger:     logger,
	})
	if err != nil {
		t.Fatalf("alchemy.NewClient: %v", err)
	}

	// --- Mock WebSocket subscriber for new headers ---
	mockSub := testutil.NewMockSubscriber()

	// --- LiveService (real) ---
	liveCfg := live_data.LiveConfig{
		ChainID:            1,
		FinalityBlockCount: 2,
		Logger:             logger,
	}
	liveSvc, err := live_data.NewLiveService(liveCfg, mockSub, alchemyClient, blockStateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("NewLiveService: %v", err)
	}
	if err := liveSvc.Start(ctx); err != nil {
		t.Fatalf("liveSvc.Start: %v", err)
	}
	t.Cleanup(func() { _ = liveSvc.Stop() })

	// --- BackfillService (real, tight retry loop for the test) ---
	bfCfg := backfill_gaps.BackfillConfig{
		ChainID:            1,
		BatchSize:          5,
		PollInterval:       100 * time.Millisecond,
		BoundaryCheckDepth: -1,
		Logger:             logger,
	}
	bfSvc, err := backfill_gaps.NewBackfillService(bfCfg, alchemyClient, blockStateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("NewBackfillService: %v", err)
	}
	if err := bfSvc.Start(ctx); err != nil {
		t.Fatalf("bfSvc.Start: %v", err)
	}
	t.Cleanup(func() { _ = bfSvc.Stop() })

	// --- Oracle worker (real) bridged to the event sink via a fake SQS consumer ---
	consumer := newEventSinkConsumer(t, eventSink)
	mc := integrationMulticaller(t, []*big.Int{new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))})

	workerCfg := shared.SQSConsumerConfig{
		PollInterval: 10 * time.Millisecond,
		Logger:       logger,
		ChainID:      1,
	}
	workerSvc, err := NewService(workerCfg, consumer, cache, priceRepo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService (worker): %v", err)
	}
	if err := workerSvc.Start(ctx); err != nil {
		t.Fatalf("workerSvc.Start: %v", err)
	}
	t.Cleanup(func() { _ = workerSvc.Stop() })

	// --- Phase 1: emit header, expect rejection on null body ---
	header := outbound.BlockHeader{
		Number:     fmt.Sprintf("0x%x", blockNum),
		Hash:       blockHash,
		ParentHash: parentHash,
		Timestamp:  fmt.Sprintf("0x%x", blockTS),
	}
	mockSub.SendHeader(header)

	// Wait for the watcher to both land the block_states row AND finish its
	// publish attempt. The row appears synchronously after SaveBlock; we then
	// poll for a settling window in which two consecutive samples agree on
	// the (false) BlockPublished state and no event has leaked. This is more
	// robust than a fixed sleep — fast paths exit immediately, slow CI runs
	// keep polling — while still bounded by an overall deadline.
	waitUntilWatcherSettled(t, ctx, blockStateRepo, blockHash, eventSink, 3*time.Second)

	t.Run("phase1_no_publish_on_null", func(t *testing.T) {
		bs, err := blockStateRepo.GetBlockByHash(ctx, blockHash)
		if err != nil {
			t.Fatalf("GetBlockByHash: %v", err)
		}
		if bs == nil {
			t.Fatal("expected block_states row to exist")
		}
		if bs.BlockPublished {
			t.Error("expected block_published=false when RPC returned null; got true")
		}
		if got := len(eventSink.GetBlockEvents()); got != 0 {
			t.Errorf("expected 0 BlockEvents published while upstream returned null, got %d", got)
		}

		// And nothing of the null payload should have leaked into the cache.
		cached, err := cache.GetBlock(ctx, 1, blockNum, 0)
		if err != nil {
			t.Fatalf("cache.GetBlock: %v", err)
		}
		if !rpcutil.IsNullOrEmpty(cached) {
			t.Errorf("expected cache to be empty/null while watcher rejected the body, got %q", string(cached))
		}
	})

	// --- Phase 2: upstream recovers; backfill retry catches it; worker processes ---
	rpc.SetReturnNull(blockNum, false)

	t.Run("phase2_worker_writes_price_after_retry", func(t *testing.T) {
		testutil.WaitForCondition(t, 30*time.Second, func() bool {
			var count int
			if err := pool.QueryRow(ctx, `
				SELECT COUNT(*) FROM onchain_token_price
				WHERE oracle_id = $1 AND block_number = $2
			`, oracleID, blockNum).Scan(&count); err != nil {
				return false
			}
			return count >= 1
		}, "oracle worker to write at least one price for block 100")
	})

	t.Run("phase2_block_published_flag_eventually_true", func(t *testing.T) {
		testutil.WaitForCondition(t, 5*time.Second, func() bool {
			bs, err := blockStateRepo.GetBlockByHash(ctx, blockHash)
			return err == nil && bs != nil && bs.BlockPublished
		}, "block_published to flip to true after retry succeeded")
	})

	t.Run("phase2_cached_block_has_valid_timestamp", func(t *testing.T) {
		bs, err := blockStateRepo.GetBlockByHash(ctx, blockHash)
		if err != nil {
			t.Fatalf("GetBlockByHash: %v", err)
		}
		cached, err := cache.GetBlock(ctx, 1, blockNum, bs.Version)
		if err != nil {
			t.Fatalf("cache.GetBlock: %v", err)
		}
		if rpcutil.IsNullOrEmpty(cached) {
			t.Fatalf("expected valid cached block JSON after retry, got %q", string(cached))
		}
		var parsed struct {
			Timestamp string `json:"timestamp"`
		}
		if err := json.Unmarshal(cached, &parsed); err != nil {
			t.Fatalf("unmarshal cached block: %v", err)
		}
		if parsed.Timestamp == "" {
			t.Error("expected cached block to carry a non-empty timestamp field")
		}
	})
}

// TestE2E_VEC242_GapFillRetriesNull exercises the gap-fill path
// (`BackfillService.processBatch` → `client.GetBlocksBatch`). Pre-fix this
// path silently passed `[]byte("null")` through into `processBlockData`,
// which then saved a `block_states` row with `hash = ""`. We seed block 1,
// inject a gap at block 2 (so the gap-finder must call `GetBlocksBatch` to
// retrieve it), keep upstream null until the worker has had time to retry,
// then flip to valid and assert the gap fills cleanly with the correct hash.
func TestE2E_VEC242_GapFillRetriesNull(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	pool, _, schemaCleanup := testutil.SetupTestSchema(t, sharedDSN)
	t.Cleanup(schemaCleanup)
	logger := testutil.DiscardLogger()
	testutil.DisableAllOracles(t, ctx, pool)
	_ = testutil.SeedOracle(t, ctx, pool, "vec242-gapfill", "VEC-242 GapFill", 1, "0x0000000000000000000000000000000000000BBB")
	_ = testutil.SeedToken(t, ctx, pool, 1, "0x0000000000000000000000000000000000000061", "VTK2", 18)

	blockStateRepo := postgres.NewBlockStateRepository(pool, 1, logger)
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	// Seed block 1 as already processed so the gap-finder sees block 2 as a gap.
	const block1Num, block2Num = int64(1), int64(2)
	block1Hash := "0x" + strings.Repeat("c", 64)
	block2Hash := "0x" + strings.Repeat("d", 64)
	block1Parent := "0x" + strings.Repeat("0", 64)
	block2Parent := block1Hash
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()
	block1JSON := json.RawMessage(fmt.Sprintf(`{"number":"0x%x","hash":"%s","parentHash":"%s","timestamp":"0x%x"}`, block1Num, block1Hash, block1Parent, ts))
	block2JSON := json.RawMessage(fmt.Sprintf(`{"number":"0x%x","hash":"%s","parentHash":"%s","timestamp":"0x%x"}`, block2Num, block2Hash, block2Parent, ts+12))

	if _, err := blockStateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: block1Num, Hash: block1Hash, ParentHash: block1Parent,
		ReceivedAt: ts, BlockTimestamp: ts,
	}); err != nil {
		t.Fatalf("seed block 1: %v", err)
	}
	if err := blockStateRepo.MarkPublishComplete(ctx, block1Hash); err != nil {
		t.Fatalf("mark block 1 published: %v", err)
	}
	// Block 3 sits above the gap so block 2 is the only missing height.
	const block3Num = int64(3)
	block3Hash := "0x" + strings.Repeat("e", 64)
	if _, err := blockStateRepo.SaveBlock(ctx, outbound.BlockState{
		Number: block3Num, Hash: block3Hash, ParentHash: block2Hash,
		ReceivedAt: ts + 24, BlockTimestamp: ts + 24,
	}); err != nil {
		t.Fatalf("seed block 3: %v", err)
	}
	if err := blockStateRepo.MarkPublishComplete(ctx, block3Hash); err != nil {
		t.Fatalf("mark block 3 published: %v", err)
	}

	rpc := newNullableRPCServer(t)
	rpc.SetBlockData(block2Num, block2Hash, block2JSON, json.RawMessage(`[]`))
	rpc.SetReturnNull(block2Num, true) // upstream is racy on the missing block
	// Seed block 1 and 3 in the mock too — verifyBoundaryBlocks may query them.
	rpc.SetBlockData(block1Num, block1Hash, block1JSON, json.RawMessage(`[]`))
	rpc.SetBlockData(block3Num, block3Hash, json.RawMessage(fmt.Sprintf(`{"number":"0x%x","hash":"%s","parentHash":"%s","timestamp":"0x%x"}`, block3Num, block3Hash, block2Hash, ts+24)), json.RawMessage(`[]`))
	t.Cleanup(rpc.Close)

	alchemyClient, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL: rpc.URL(), MaxRetries: 1, Logger: logger,
	})
	if err != nil {
		t.Fatalf("alchemy.NewClient: %v", err)
	}

	// Only run BackfillService — no LiveService — so the gap-fill path is the
	// sole exerciser of the fix.
	bfCfg := backfill_gaps.BackfillConfig{
		ChainID:            1,
		BatchSize:          5,
		PollInterval:       100 * time.Millisecond,
		BoundaryCheckDepth: -1,
		Logger:             logger,
	}
	bfSvc, err := backfill_gaps.NewBackfillService(bfCfg, alchemyClient, blockStateRepo, cache, eventSink)
	if err != nil {
		t.Fatalf("NewBackfillService: %v", err)
	}
	if err := bfSvc.Start(ctx); err != nil {
		t.Fatalf("bfSvc.Start: %v", err)
	}
	t.Cleanup(func() { _ = bfSvc.Stop() })

	t.Run("phase1_null_does_not_persist_empty_hash_row", func(t *testing.T) {
		// Bounded poll instead of a fixed sleep: the gap-fill loop ticks every
		// PollInterval (100ms), so a 1s window covers ~10 attempts on any
		// reasonable runner. We fail the test the instant a corrupted row
		// appears at any point in the window; otherwise we proceed once the
		// window elapses cleanly. This is deterministic on fast machines
		// (asserts fire as soon as state is wrong) and robust on slow CI
		// runners (the window is long enough for several ticks).
		const window = 1 * time.Second
		const pollEvery = 50 * time.Millisecond
		deadline := time.Now().Add(window)
		for time.Now().Before(deadline) {
			bs, err := blockStateRepo.GetBlockByNumber(ctx, block2Num)
			if err != nil {
				t.Fatalf("GetBlockByNumber: %v", err)
			}
			if bs != nil {
				if bs.Hash == "" {
					t.Fatal("backfill persisted a row with empty hash from a null upstream — VEC-242 regression")
				}
				if bs.Hash != block2Hash {
					t.Fatalf("unexpected hash %q at block %d while upstream null", bs.Hash, block2Num)
				}
			}
			time.Sleep(pollEvery)
		}
	})

	t.Run("phase2_recovery_fills_gap_with_correct_hash", func(t *testing.T) {
		rpc.SetReturnNull(block2Num, false)
		testutil.WaitForCondition(t, 30*time.Second, func() bool {
			bs, err := blockStateRepo.GetBlockByNumber(ctx, block2Num)
			return err == nil && bs != nil && bs.Hash == block2Hash && bs.BlockPublished
		}, "backfill to fill gap at block 2 once upstream recovered")

		bs, err := blockStateRepo.GetBlockByNumber(ctx, block2Num)
		if err != nil {
			t.Fatalf("GetBlockByNumber: %v", err)
		}
		if bs == nil {
			t.Fatal("expected block 2 in state repo after recovery")
		}
		if bs.Hash != block2Hash {
			t.Errorf("expected hash %s, got %s", block2Hash, bs.Hash)
		}
		if !bs.BlockPublished {
			t.Error("expected block_published=true after gap-fill success")
		}
	})
}

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

// nullableRPCServer is a minimal Alchemy-compatible JSON-RPC HTTP server.
// It accepts batched requests (which is how the watcher's GetBlockDataByHash
// fetcher talks to upstream) and, for each request, returns either a valid
// JSON-RPC result for the configured block hash OR a literal `null` result
// when the hash is marked as "should return null". The null-mode flag can be
// flipped at runtime to simulate the propagation race: header arrives first,
// HTTP body is null on the first fetch, valid on the second.
type nullableRPCServer struct {
	t      *testing.T
	server *httptest.Server

	mu sync.Mutex
	// returnNullForHash[hash] == true → eth_getBlockByHash/eth_getBlockReceipts
	// for that hash returns `{"result": null}`. When the flag is flipped back
	// to false, subsequent calls return the stored block data.
	returnNullForHash map[string]bool
	// stored block data, keyed by hash. Required fields: block, receipts.
	blockByHash    map[string]json.RawMessage
	receiptsByHash map[string]json.RawMessage
}

func newNullableRPCServer(t *testing.T) *nullableRPCServer {
	t.Helper()
	s := &nullableRPCServer{
		t:                 t,
		returnNullForHash: make(map[string]bool),
		blockByHash:       make(map[string]json.RawMessage),
		receiptsByHash:    make(map[string]json.RawMessage),
	}
	s.server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

func (s *nullableRPCServer) URL() string { return s.server.URL }
func (s *nullableRPCServer) Close()      { s.server.Close() }

// SetBlockData registers block+receipts JSON to be returned for the given hash.
func (s *nullableRPCServer) SetBlockData(hash string, block, receipts json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockByHash[hash] = block
	s.receiptsByHash[hash] = receipts
}

// SetReturnNullForHash toggles whether RPC calls referring to this hash
// respond with `{"result": null}` (true) or with the stored data (false).
func (s *nullableRPCServer) SetReturnNullForHash(hash string, enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.returnNullForHash[hash] = enabled
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
// It mirrors what real Alchemy returns when the requested hash has not yet
// propagated through the backend ("null") versus when it has (the data).
func (s *nullableRPCServer) resultFor(method string, params []json.RawMessage) json.RawMessage {
	switch method {
	case "eth_blockNumber":
		return json.RawMessage(`"0x64"`)
	case "eth_getBlockByHash":
		hash := parseHashParam(params)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.returnNullForHash[hash] {
			return json.RawMessage("null")
		}
		if data, ok := s.blockByHash[hash]; ok {
			return data
		}
		return json.RawMessage("null")
	case "eth_getBlockReceipts":
		hash := parseHashParam(params)
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.returnNullForHash[hash] {
			return json.RawMessage("null")
		}
		if data, ok := s.receiptsByHash[hash]; ok {
			return data
		}
		return json.RawMessage(`[]`)
	default:
		return json.RawMessage("null")
	}
}

func parseHashParam(params []json.RawMessage) string {
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
// the same wire shape it would see in production.
type eventSinkConsumer struct {
	mu       sync.Mutex
	pending  []outbound.SQSMessage
	deleteCh chan string
	closed   bool
}

func newEventSinkConsumer(sink *memory.EventSink) *eventSinkConsumer {
	c := &eventSinkConsumer{deleteCh: make(chan string, 256)}
	sink.OnPublish(func(e outbound.Event) {
		be, ok := e.(outbound.BlockEvent)
		if !ok {
			return
		}
		body, err := json.Marshal(be)
		if err != nil {
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
	select {
	case c.deleteCh <- receiptHandle:
	default:
	}
	return nil
}

func (c *eventSinkConsumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
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
	rpc.SetBlockData(blockHash, blockJSON, receiptsJSON)
	rpc.SetReturnNullForHash(blockHash, true) // start in the racy state
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
	consumer := newEventSinkConsumer(eventSink)
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

	// Wait for the watcher to land the block_states row but refuse to publish.
	// The row appears synchronously after SaveBlock; the publish attempt then
	// fails (or — pre-fix — succeeds incorrectly).
	if !testutil.WaitFor(t, 5*time.Second, 25*time.Millisecond, func() bool {
		bs, err := blockStateRepo.GetBlockByHash(ctx, blockHash)
		return err == nil && bs != nil
	}) {
		t.Fatal("expected block_states row after watcher processed the header, none appeared")
	}

	// Give the watcher a beat to flush its publish path before we sample state.
	// The publish attempt is async w.r.t. SaveBlock only via the prefetch wait,
	// which is bounded by the RPC round-trip; 250ms is plenty against an
	// in-process httptest server.
	time.Sleep(250 * time.Millisecond)

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
	rpc.SetReturnNullForHash(blockHash, false)

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

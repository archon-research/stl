//go:build integration

package data_validator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/blockverifier"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// baseChainID is Base mainnet — a non-Ethereum chain. VEC-208 requires the data
// validator to be proven end-to-end on at least one non-Ethereum chain.
const baseChainID int64 = 8453

// TestValidate_BaseChain_Integration proves the data validator works for Base
// (chain ID 8453, a non-Ethereum chain) along the real path:
//
//   - the chain-routing factory (blockverifier.New) selects the Etherscan
//     verifier for 8453 — this is what confirms routing, not a hand-built client;
//   - that verifier talks to a MOCK Etherscan V2 proxy HTTP server;
//   - the service reads block state from a REAL Postgres (testcontainers harness
//     via testutil.SetupTimescaleDB, which applies all migrations);
//   - the produced report is a success.
//
// Only the Etherscan HTTP endpoint is mocked (a data source we do not control),
// in line with the "mock only what we cannot control" rule from CLAUDE.md.
func TestValidate_BaseChain_Integration(t *testing.T) {
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Real Postgres with migrations applied (own container, no shared TestMain).
	pool, _, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	repo := postgres.NewBlockStateRepository(pool, baseChainID, logger)

	// Seed a small parent-linked chain (blocks 100..110) for chain 8453.
	// Each block's parent_hash equals the previous block's hash so the chain
	// integrity check passes; we record number->hash so the mock can echo them.
	// No reorg events are seeded, so reorg validation passes trivially.
	const firstBlock, lastBlock int64 = 100, 110
	hashByNumber := make(map[int64]string)
	for number := firstBlock; number <= lastBlock; number++ {
		hashByNumber[number] = blockHash(number)
	}

	now := time.Now().Unix()
	for number := firstBlock; number <= lastBlock; number++ {
		parentHash := blockHash(number - 1) // genesis's parent need not be seeded
		if _, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         number,
			Hash:           hashByNumber[number],
			ParentHash:     parentHash,
			ReceivedAt:     now,
			BlockTimestamp: now,
		}); err != nil {
			t.Fatalf("seeding block %d: %v", number, err)
		}
	}

	// Mock Etherscan V2 proxy server: answers for ANY chainid.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		etherscanProxyHandler(t, w, r, hashByNumber)
	}))
	defer server.Close()

	// Build the verifier THROUGH the factory: this proves 8453 routes to the
	// Etherscan adapter, and points that adapter at the mock server.
	verifier, err := blockverifier.New(baseChainID, blockverifier.Options{
		EtherscanAPIKey:  "test-key",
		EtherscanBaseURL: server.URL,
		Logger:           logger,
	})
	if err != nil {
		t.Fatalf("blockverifier.New(%d): %v", baseChainID, err)
	}
	if verifier.Name() != "etherscan" {
		t.Fatalf("expected etherscan verifier for chain %d, got %q", baseChainID, verifier.Name())
	}

	cfg := DefaultConfig()
	cfg.SpotCheckCount = int(lastBlock - firstBlock + 1) // 11 blocks seeded; check all deterministically
	cfg.Logger = logger
	svc, err := NewService(cfg, repo, verifier)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	report, err := svc.Validate(ctx)
	if err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}

	if !report.Success() {
		t.Fatalf("expected validation success for Base chain, got failure (failed=%d, errors=%d):\n%s",
			report.Failed, report.Errors, report.FormatText())
	}
}

// etherscanProxyHandler emulates the Etherscan V2 proxy module the adapter calls.
// It serves eth_blockNumber and eth_getBlockByNumber for any chainid, echoing the
// seeded hash for known blocks and a JSON null result for unknown ones.
func etherscanProxyHandler(t *testing.T, w http.ResponseWriter, r *http.Request, hashByNumber map[int64]string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")

	action := r.URL.Query().Get("action")
	switch action {
	case "eth_blockNumber":
		// Highest seeded block, as a hex string result.
		var highest int64
		for number := range hashByNumber {
			if number > highest {
				highest = number
			}
		}
		fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":"0x%x"}`, highest)

	case "eth_getBlockByNumber":
		number, err := parseHexTag(r.URL.Query().Get("tag"))
		if err != nil {
			t.Errorf("mock could not parse tag %q: %v", r.URL.Query().Get("tag"), err)
			http.Error(w, "bad tag", http.StatusBadRequest)
			return
		}
		hash, ok := hashByNumber[number]
		if !ok {
			// Unknown block: Etherscan returns a JSON null result.
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":null}`)
			return
		}
		fmt.Fprintf(w,
			`{"jsonrpc":"2.0","id":1,"result":{"number":"0x%x","hash":"%s","timestamp":"0x%x"}}`,
			number, hash, time.Now().Unix())

	default:
		t.Errorf("mock received unexpected action %q", action)
		http.Error(w, "unexpected action", http.StatusBadRequest)
	}
}

// blockHash returns a deterministic, unique, 0x-prefixed 32-byte hash for a block
// number, matching the format the validator compares against.
func blockHash(number int64) string {
	return fmt.Sprintf("0x%064x", number)
}

// parseHexTag parses an Etherscan "tag" (a 0x-prefixed hex block number).
func parseHexTag(tag string) (int64, error) {
	return strconv.ParseInt(strings.TrimPrefix(tag, "0x"), 16, 64)
}

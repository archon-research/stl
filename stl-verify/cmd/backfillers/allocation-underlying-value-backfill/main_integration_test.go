//go:build integration

package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(testutil.RunShared(m, testutil.Shared{TimescaleDSN: &sharedDSN}))
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

// seedChain idempotently inserts the chain row allocation_position's FK needs.
func seedChain(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO chain (chain_id, name) VALUES (1, 'mainnet') ON CONFLICT (chain_id) DO NOTHING`); err != nil {
		t.Fatalf("seed chain: %v", err)
	}
}

// sparkPrimeID looks up the migration-seeded 'spark' prime row.
func sparkPrimeID(t *testing.T, ctx context.Context, pool *pgxpool.Pool) int64 {
	t.Helper()
	var id int64
	if err := pool.QueryRow(ctx, `SELECT id FROM prime WHERE name = 'spark'`).Scan(&id); err != nil {
		t.Fatalf("look up spark prime: %v", err)
	}
	return id
}

// historicalPosition is one pre-cutover allocation_position row this backfill
// is meant to correct: underlying_value/underlying_token_id are never set here.
type historicalPosition struct {
	tokenID      int64
	primeID      int64
	proxyAddress common.Address
	balance      string // numeric literal text, e.g. "1000.500000000000000000"
	blockNumber  int64
	blockVersion int32
	txHash       string // 0x-prefixed, 32 bytes
	logIndex     int32
	txAmount     string
	direction    string
	fromAddress  *common.Address
	toAddress    *common.Address
	createdAt    time.Time
}

// insertHistoricalPosition seeds one row as the live indexer would have
// written it before the underlying_value column existed. balance/txAmount are
// interpolated as bare SQL numeric literals (test-controlled, not user input)
// because pgx has no default encoding for a Go string bound to a numeric
// column; every other field is a normal parameter.
func insertHistoricalPosition(t *testing.T, ctx context.Context, pool *pgxpool.Pool, p historicalPosition) {
	t.Helper()
	txHashBytes, err := testutil.HexToBytes(p.txHash)
	if err != nil {
		t.Fatalf("parse tx hash %s: %v", p.txHash, err)
	}

	var fromBytes, toBytes []byte
	if p.fromAddress != nil {
		fromBytes = p.fromAddress.Bytes()
	}
	if p.toAddress != nil {
		toBytes = p.toAddress.Bytes()
	}

	query := fmt.Sprintf(`
		INSERT INTO allocation_position
			(chain_id, token_id, prime_id, proxy_address, balance, block_number, block_version,
			 tx_hash, log_index, tx_amount, direction, created_at, from_address, to_address)
		VALUES (1, $1, $2, $3, %s, $4, $5, $6, $7, %s, $8, $9, $10, $11)
	`, p.balance, p.txAmount)

	if _, err := pool.Exec(ctx, query,
		p.tokenID, p.primeID, p.proxyAddress.Bytes(),
		p.blockNumber, p.blockVersion, txHashBytes, p.logIndex,
		p.direction, p.createdAt, fromBytes, toBytes,
	); err != nil {
		t.Fatalf("insert historical position: %v", err)
	}
}

func mustParseTime(t *testing.T, s string) time.Time {
	t.Helper()
	ts, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("parse time %s: %v", s, err)
	}
	return ts
}

// ---------------------------------------------------------------------------
// Mock archive RPC: answers convertToAssets(shares) multicalls. Every test
// using it seeds exactly one block's worth of candidates, so one eth_call
// carries the whole batch and per-call ordering never needs to be tracked.
// ---------------------------------------------------------------------------

func startConvertToAssetsRPC(t *testing.T, results []bool, assets []*big.Int) *httptest.Server {
	t.Helper()
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("GetMulticall3ABI: %v", err)
	}
	erc4626ABI, err := abis.GetERC4626ABI()
	if err != nil {
		t.Fatalf("GetERC4626ABI: %v", err)
	}

	type call3Result struct {
		Success    bool
		ReturnData []byte
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req rpcutil.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			testutil.WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}
		if req.Method != "eth_call" {
			testutil.WriteRPCError(w, req.ID, -32601, "method not found: "+req.Method)
			return
		}

		n := testutil.CountMulticallInnerCalls(req.Params)
		callResults := make([]call3Result, n)
		for i := 0; i < n; i++ {
			success := i < len(results) && results[i]
			var data []byte
			if success {
				data, _ = erc4626ABI.Methods["convertToAssets"].Outputs.Pack(assets[i])
			}
			callResults[i] = call3Result{Success: success, ReturnData: data}
		}
		agg, err := multicallABI.Methods["aggregate3"].Outputs.Pack(callResults)
		if err != nil {
			t.Fatalf("packing aggregate3 response: %v", err)
		}
		resultHex, _ := json.Marshal("0x" + hex.EncodeToString(agg))
		testutil.WriteRPCResult(w, req.ID, resultHex)
	}))
}

// ---------------------------------------------------------------------------
// Scaling fix: a direct (non-receipt-token) holding round-trips through
// toEntity/SavePositions unchanged. Before VEC-759's fix, an unset
// TokenDecimals on the rewritten entity made the repository store the RAW
// on-chain integer in a column that holds human-normalized values -- balance
// inflated by 10^18 for an 18-decimal token.
// ---------------------------------------------------------------------------

func TestRunIntegration_ScalingFix(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	seedChain(t, ctx, pool)
	primeID := sparkPrimeID(t, ctx, pool)
	tokenID := testutil.SeedToken(t, ctx, pool, 1, "0x1111111111111111111111111111111111111111", "sUSDS", 18)
	proxy := common.HexToAddress("0x2222222222222222222222222222222222222222")

	const balanceHuman = "1402923191.117714747284001290"
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: tokenID, primeID: primeID, proxyAddress: proxy,
		balance: balanceHuman, blockNumber: 25_000_000,
		txHash: "0x" + strings.Repeat("aa", 32), logIndex: 0,
		txAmount: balanceHuman, direction: "sweep",
		createdAt: mustParseTime(t, "2026-01-01T00:00:00Z"),
	})

	if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"}); err != nil {
		t.Fatalf("run: %v", err)
	}

	var writtenBalance, writtenUnderlying string
	if err := pool.QueryRow(ctx,
		`SELECT balance::text, underlying_value::text FROM allocation_position WHERE token_id = $1 AND processing_version > 0`,
		tokenID,
	).Scan(&writtenBalance, &writtenUnderlying); err != nil {
		t.Fatalf("query corrected row: %v", err)
	}

	if writtenBalance != balanceHuman {
		t.Errorf("balance = %q, want %q (human-normalized, not raw*10^18)", writtenBalance, balanceHuman)
	}
	if writtenUnderlying != balanceHuman {
		t.Errorf("underlying_value = %q, want %q (self-referencing direct holding)", writtenUnderlying, balanceHuman)
	}
}

// ---------------------------------------------------------------------------
// Aave-family matcher: every registered name variant resolves 1:1, and a
// non-Aave registration (even one sharing a display symbol with an Aave-family
// token, like spDAI) does not.
// ---------------------------------------------------------------------------

func TestRunIntegration_AaveFamilyMatcher(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	seedChain(t, ctx, pool)
	primeID := sparkPrimeID(t, ctx, pool)

	protocolNames := []string{"SparkLend", "Aave V2", "Aave V3", "Aave V3 Lido", "Aave V3 Base"}
	for i, name := range protocolNames {
		protocolID := testutil.SeedProtocol(t, ctx, pool, 1,
			fmt.Sprintf("0x%040d", i+1), name, "lending", 100, "")
		underlyingID := testutil.SeedToken(t, ctx, pool, 1, fmt.Sprintf("0x%040d", 1000+i), "DAI", 18)
		vaultAddr := fmt.Sprintf("0x%040d", 2000+i)
		vaultID := testutil.SeedToken(t, ctx, pool, 1, vaultAddr, "aDAI", 18)
		testutil.SeedReceiptToken(t, ctx, pool, 1, vaultAddr, protocolID, underlyingID, "aDAI")

		proxy := common.HexToAddress(fmt.Sprintf("0x%040d", 3000+i))
		insertHistoricalPosition(t, ctx, pool, historicalPosition{
			tokenID: vaultID, primeID: primeID, proxyAddress: proxy,
			balance: "500.000000000000000000", blockNumber: int64(25_000_000 + i),
			txHash: fmt.Sprintf("0x%064d", i), logIndex: 0,
			txAmount: "500.000000000000000000", direction: "sweep",
			createdAt: mustParseTime(t, fmt.Sprintf("2026-01-01T00:00:%02dZ", i)),
		})

		t.Run(name, func(t *testing.T) {
			// Each subtest reruns the whole backfill over a growing table;
			// harmless (and a light idempotency check in its own right) since
			// the NOT EXISTS guard makes repeats no-ops for already-corrected rows.
			if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "100"}); err != nil {
				t.Fatalf("run: %v", err)
			}
			var underlyingValue string
			var underlyingTokenID int64
			if err := pool.QueryRow(ctx,
				`SELECT underlying_value::text, underlying_token_id FROM allocation_position WHERE token_id = $1 AND processing_version > 0`,
				vaultID,
			).Scan(&underlyingValue, &underlyingTokenID); err != nil {
				t.Fatalf("query corrected row for %s: %v", name, err)
			}
			if underlyingValue != "500.000000000000000000" {
				t.Errorf("%s: underlying_value = %q, want 1:1 with balance", name, underlyingValue)
			}
			if underlyingTokenID != underlyingID {
				t.Errorf("%s: underlying_token_id = %d, want %d", name, underlyingTokenID, underlyingID)
			}
		})
	}
}

// TestRunIntegration_NonAaveRegistrationIsNotTreated1to1 guards the classifier
// against keying off a token's display symbol: spDAI is a real, live example
// of two different contracts sharing one symbol -- a SparkLend aToken (1:1)
// and a Morpho Blue vault (genuinely variable ratio). The Morpho-registered
// address must go through the real conversion, never the 1:1 shortcut, no
// matter what it is named.
func TestRunIntegration_NonAaveRegistrationIsNotTreated1to1(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	seedChain(t, ctx, pool)
	primeID := sparkPrimeID(t, ctx, pool)

	protocolID := testutil.SeedProtocol(t, ctx, pool, 1, "0x009000000000000000000000000000000000000a", "Morpho Blue", "vault", 100, "")
	underlyingID := testutil.SeedToken(t, ctx, pool, 1, "0x009000000000000000000000000000000000000b", "DAI", 18)
	vaultAddr := "0x009000000000000000000000000000000000000c"
	vaultID := testutil.SeedToken(t, ctx, pool, 1, vaultAddr, "spDAI", 18)
	testutil.SeedReceiptToken(t, ctx, pool, 1, vaultAddr, protocolID, underlyingID, "spDAI")

	proxy := common.HexToAddress("0x009000000000000000000000000000000000000d")
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: vaultID, primeID: primeID, proxyAddress: proxy,
		balance: "1000.000000000000000000", blockNumber: 25_100_000,
		txHash: fmt.Sprintf("0x%064d", 1), logIndex: 0,
		txAmount: "1000.000000000000000000", direction: "sweep",
		createdAt: mustParseTime(t, "2026-01-01T00:00:00Z"),
	})

	twoThousandAt18Decimals, _ := new(big.Int).SetString("2000000000000000000000", 10)         // 2000.0 at 18 decimals
	rpcServer := startConvertToAssetsRPC(t, []bool{true}, []*big.Int{twoThousandAt18Decimals}) // 2:1, NOT 1:1
	defer rpcServer.Close()
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"}); err != nil {
		t.Fatalf("run: %v", err)
	}

	var underlyingValue string
	if err := pool.QueryRow(ctx,
		`SELECT underlying_value::text FROM allocation_position WHERE token_id = $1 AND processing_version > 0`,
		vaultID,
	).Scan(&underlyingValue); err != nil {
		t.Fatalf("query corrected row: %v", err)
	}
	if underlyingValue == "1000.000000000000000000" {
		t.Fatalf("underlying_value = %q: a Morpho-Blue-registered token must never be treated 1:1", underlyingValue)
	}
	if underlyingValue != "2000.000000000000000000" {
		t.Errorf("underlying_value = %q, want the real conversion result 2000.000000000000000000", underlyingValue)
	}
}

// ---------------------------------------------------------------------------
// erc4626 fallback: when the archive call reverts, the price-ratio derivation
// from onchain_token_price takes over -- and only then.
// ---------------------------------------------------------------------------

func TestRunIntegration_ERC4626FallsBackToPriceRatioWhenArchiveReverts(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	seedChain(t, ctx, pool)
	primeID := sparkPrimeID(t, ctx, pool)

	protocolID := testutil.SeedProtocol(t, ctx, pool, 1, "0x00800000000000000000000000000000000000aa", "Morpho Blue", "vault", 100, "")
	underlyingID := testutil.SeedToken(t, ctx, pool, 1, "0x00800000000000000000000000000000000000bb", "DAI", 18)
	vaultAddr := "0x00800000000000000000000000000000000000cc"
	vaultID := testutil.SeedToken(t, ctx, pool, 1, vaultAddr, "spDAI", 18)
	testutil.SeedReceiptToken(t, ctx, pool, 1, vaultAddr, protocolID, underlyingID, "spDAI")

	const blockNumber = 25_200_000
	oracleID := testutil.SeedFeedOracle(t, ctx, pool, "test-oracle-fallback", "Test Oracle", "chainlink", 1, 8)
	// share_price/underlying_price = 2.0/1.0 -- balance 100 shares -> 200 DAI.
	if _, err := pool.Exec(ctx, `
		INSERT INTO onchain_token_price (token_id, oracle_id, block_number, timestamp, price_usd)
		VALUES ($1, $2, $3, $4, 2.0), ($5, $2, $3, $4, 1.0)
	`, vaultID, oracleID, int64(blockNumber), mustParseTime(t, "2026-01-01T00:00:00Z"), underlyingID); err != nil {
		t.Fatalf("seed onchain_token_price: %v", err)
	}

	proxy := common.HexToAddress("0x00800000000000000000000000000000000000dd")
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: vaultID, primeID: primeID, proxyAddress: proxy,
		balance: "100.000000000000000000", blockNumber: blockNumber,
		txHash: fmt.Sprintf("0x%064d", 2), logIndex: 0,
		txAmount: "100.000000000000000000", direction: "sweep",
		createdAt: mustParseTime(t, "2026-01-02T00:00:00Z"),
	})

	rpcServer := startConvertToAssetsRPC(t, []bool{false}, []*big.Int{nil}) // reverts
	defer rpcServer.Close()
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)

	if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"}); err != nil {
		t.Fatalf("run: %v", err)
	}

	var underlyingValue string
	if err := pool.QueryRow(ctx,
		`SELECT underlying_value::text FROM allocation_position WHERE token_id = $1 AND processing_version > 0`,
		vaultID,
	).Scan(&underlyingValue); err != nil {
		t.Fatalf("query corrected row: %v", err)
	}
	if underlyingValue != "200.000000000000000000" {
		t.Errorf("underlying_value = %q, want the price-ratio derivation 200.0 at 18 decimals (100 shares * 2.0/1.0)", underlyingValue)
	}
}

// ---------------------------------------------------------------------------
// Idempotency: running the backfill twice over the same rows produces exactly
// one correction per identity, never two.
// ---------------------------------------------------------------------------

func TestRunIntegration_Idempotent(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	seedChain(t, ctx, pool)
	primeID := sparkPrimeID(t, ctx, pool)
	protocolID := testutil.SeedProtocol(t, ctx, pool, 1, "0x007000000000000000000000000000000000000a", "SparkLend", "lending", 100, "")
	underlyingID := testutil.SeedToken(t, ctx, pool, 1, "0x007000000000000000000000000000000000000b", "DAI", 18)
	vaultAddr := "0x007000000000000000000000000000000000000c"
	vaultID := testutil.SeedToken(t, ctx, pool, 1, vaultAddr, "spDAI", 18)
	testutil.SeedReceiptToken(t, ctx, pool, 1, vaultAddr, protocolID, underlyingID, "spDAI")

	proxy := common.HexToAddress("0x007000000000000000000000000000000000000d")
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: vaultID, primeID: primeID, proxyAddress: proxy,
		balance: "42.000000000000000000", blockNumber: 25_300_000,
		txHash: fmt.Sprintf("0x%064d", 3), logIndex: 0,
		txAmount: "42.000000000000000000", direction: "sweep",
		createdAt: mustParseTime(t, "2026-01-03T00:00:00Z"),
	})

	args := []string{"-db", dbURL, "-dry-run=false", "-limit", "100"}
	if err := run(ctx, args); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := run(ctx, args); err != nil {
		t.Fatalf("second run: %v", err)
	}

	var total, corrected int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM allocation_position WHERE token_id = $1`, vaultID).Scan(&total); err != nil {
		t.Fatalf("count total rows: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM allocation_position WHERE token_id = $1 AND processing_version > 0`, vaultID).Scan(&corrected); err != nil {
		t.Fatalf("count corrected rows: %v", err)
	}
	if total != 2 {
		t.Errorf("total rows for this identity = %d, want 2 (original + exactly one correction)", total)
	}
	if corrected != 1 {
		t.Errorf("corrected rows = %d, want exactly 1 -- running the backfill twice must not double-correct", corrected)
	}
}

// ---------------------------------------------------------------------------
// Sweep/in-out predicate: a sweep needs no transfer parties, an in/out row
// needs both, and a pre-cutover in/out row missing them is left alone rather
// than treated as valid.
// ---------------------------------------------------------------------------

func TestRunIntegration_SweepInOutPredicate(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	seedChain(t, ctx, pool)
	primeID := sparkPrimeID(t, ctx, pool)
	protocolID := testutil.SeedProtocol(t, ctx, pool, 1, "0x006000000000000000000000000000000000000a", "SparkLend", "lending", 100, "")
	underlyingID := testutil.SeedToken(t, ctx, pool, 1, "0x006000000000000000000000000000000000000b", "DAI", 18)
	vaultAddr := "0x006000000000000000000000000000000000000c"
	vaultID := testutil.SeedToken(t, ctx, pool, 1, vaultAddr, "spDAI", 18)
	testutil.SeedReceiptToken(t, ctx, pool, 1, vaultAddr, protocolID, underlyingID, "spDAI")

	proxy := common.HexToAddress("0x006000000000000000000000000000000000000d")
	counterparty := common.HexToAddress("0x006000000000000000000000000000000000000e")

	// Sweep, no parties: valid, must be corrected.
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: vaultID, primeID: primeID, proxyAddress: proxy,
		balance: "1.000000000000000000", blockNumber: 25_400_001,
		txHash: fmt.Sprintf("0x%064d", 11), logIndex: 0,
		txAmount: "1.000000000000000000", direction: "sweep",
		createdAt: mustParseTime(t, "2026-01-04T00:00:01Z"),
	})
	// Pre-cutover "in" with no recovered parties: invalid entity, must be
	// left alone (never selected as a candidate at all).
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: vaultID, primeID: primeID, proxyAddress: proxy,
		balance: "2.000000000000000000", blockNumber: 25_400_002,
		txHash: fmt.Sprintf("0x%064d", 12), logIndex: 0,
		txAmount: "2.000000000000000000", direction: "in",
		createdAt: mustParseTime(t, "2026-01-04T00:00:02Z"),
	})
	// "in" with both parties present: valid, must be corrected.
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: vaultID, primeID: primeID, proxyAddress: proxy,
		balance: "3.000000000000000000", blockNumber: 25_400_003,
		txHash: fmt.Sprintf("0x%064d", 13), logIndex: 0,
		txAmount: "3.000000000000000000", direction: "in",
		fromAddress: &counterparty, toAddress: &proxy,
		createdAt: mustParseTime(t, "2026-01-04T00:00:03Z"),
	})

	if err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "100"}); err != nil {
		t.Fatalf("run: %v", err)
	}

	corrected := map[int64]bool{}
	rows, err := pool.Query(ctx, `SELECT block_number FROM allocation_position WHERE token_id = $1 AND processing_version > 0`, vaultID)
	if err != nil {
		t.Fatalf("query corrected rows: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var bn int64
		if err := rows.Scan(&bn); err != nil {
			t.Fatalf("scan: %v", err)
		}
		corrected[bn] = true
	}

	if !corrected[25_400_001] {
		t.Error("sweep row (block 25400001) should have been corrected")
	}
	if corrected[25_400_002] {
		t.Error("pre-cutover 'in' row missing both transfer parties (block 25400002) must NOT be corrected")
	}
	if !corrected[25_400_003] {
		t.Error("'in' row with both transfer parties present (block 25400003) should have been corrected")
	}
	if len(corrected) != 2 {
		t.Errorf("corrected %d rows, want exactly 2", len(corrected))
	}
}

// ---------------------------------------------------------------------------
// Dry run: candidates are classified and logged, but nothing is written.
// ---------------------------------------------------------------------------

func TestRunIntegration_DryRunWritesNothing(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	seedChain(t, ctx, pool)
	primeID := sparkPrimeID(t, ctx, pool)
	protocolID := testutil.SeedProtocol(t, ctx, pool, 1, "0x0050000000000000000000000000000000000a", "SparkLend", "lending", 100, "")
	underlyingID := testutil.SeedToken(t, ctx, pool, 1, "0x0050000000000000000000000000000000000b", "DAI", 18)
	vaultAddr := "0x0050000000000000000000000000000000000c"
	vaultID := testutil.SeedToken(t, ctx, pool, 1, vaultAddr, "spDAI", 18)
	testutil.SeedReceiptToken(t, ctx, pool, 1, vaultAddr, protocolID, underlyingID, "spDAI")

	proxy := common.HexToAddress("0x0050000000000000000000000000000000000d")
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: vaultID, primeID: primeID, proxyAddress: proxy,
		balance: "9.000000000000000000", blockNumber: 25_500_000,
		txHash: fmt.Sprintf("0x%064d", 21), logIndex: 0,
		txAmount: "9.000000000000000000", direction: "sweep",
		createdAt: mustParseTime(t, "2026-01-05T00:00:00Z"),
	})

	// dry-run defaults to true; explicit here for clarity.
	if err := run(ctx, []string{"-db", dbURL, "-dry-run=true", "-limit", "100"}); err != nil {
		t.Fatalf("run: %v", err)
	}

	var corrected int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM allocation_position WHERE token_id = $1 AND processing_version > 0`, vaultID).Scan(&corrected); err != nil {
		t.Fatalf("count corrected rows: %v", err)
	}
	if corrected != 0 {
		t.Errorf("dry run wrote %d rows, want 0", corrected)
	}
}

// ---------------------------------------------------------------------------
// Error paths: an unreachable database must fail loudly.
// ---------------------------------------------------------------------------

func TestRunIntegration_BadDatabaseURL(t *testing.T) {
	err := run(context.Background(), []string{
		"-db", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1",
	})
	if err == nil {
		t.Fatal("expected error for an unreachable database")
	}
}

// TestRunIntegration_MissingArchiveCredentialsFailsLoud: an erc4626 candidate
// with no ALCHEMY_API_KEY in the environment must abort the run rather than
// silently fall back -- the fallback is only for a per-row archive miss, never
// for the archive being unreachable at all.
func TestRunIntegration_MissingArchiveCredentialsFailsLoud(t *testing.T) {
	pool, dbURL, cleanup := testutil.SetupTestDB(t, sharedDSN)
	defer cleanup()
	ctx := context.Background()

	seedChain(t, ctx, pool)
	primeID := sparkPrimeID(t, ctx, pool)
	protocolID := testutil.SeedProtocol(t, ctx, pool, 1, "0x0040000000000000000000000000000000000a", "Morpho Blue", "vault", 100, "")
	underlyingID := testutil.SeedToken(t, ctx, pool, 1, "0x0040000000000000000000000000000000000b", "DAI", 18)
	vaultAddr := "0x0040000000000000000000000000000000000c"
	vaultID := testutil.SeedToken(t, ctx, pool, 1, vaultAddr, "vault", 18)
	testutil.SeedReceiptToken(t, ctx, pool, 1, vaultAddr, protocolID, underlyingID, "vault")

	proxy := common.HexToAddress("0x0040000000000000000000000000000000000d")
	insertHistoricalPosition(t, ctx, pool, historicalPosition{
		tokenID: vaultID, primeID: primeID, proxyAddress: proxy,
		balance: "1.000000000000000000", blockNumber: 25_600_000,
		txHash: fmt.Sprintf("0x%064d", 22), logIndex: 0,
		txAmount: "1.000000000000000000", direction: "sweep",
		createdAt: mustParseTime(t, "2026-01-06T00:00:00Z"),
	})

	t.Setenv("ALCHEMY_API_KEY", "") // explicitly absent

	err := run(ctx, []string{"-db", dbURL, "-dry-run=false", "-limit", "10"})
	if err == nil {
		t.Fatal("expected an error: no archive credentials means the erc4626 row can never be resolved")
	}
}

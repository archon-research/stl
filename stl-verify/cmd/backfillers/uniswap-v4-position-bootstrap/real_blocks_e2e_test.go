//go:build integration && e2e

// Hand-run end-to-end gate for the posm transfer backfill. It drives the DEPLOYED
// wiring — register, loadConfig, the real Alchemy client, the real S3 archive
// reader and the real repository — against mainnet and a real raw archive, so the
// version stamped on each row comes from archived objects rather than a fixture.
//
// `make e2e-v4-posm-transfer` is the entry point. SCAN_BLOCKS bounds the slice,
// because a whole posm history is ~4.3M blocks and hours of archive reads.
package main

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	// posmDeployBlockMainnet is uniswap_v4_position_manager.deploy_block for chain
	// 1, where the backfill starts its scan whatever the pin is.
	posmDeployBlockMainnet = int64(21689089)
	defaultE2EScanBlocks   = int64(30_000)
	// Below this height the raw archive is bulk-downloader-only and so version-1
	// only; from here up its hash-verified 0/1 twins are both legitimate.
	mainnetDualVersionBandStart = int64(24_250_000)
	// The workflow test environment defaults to 3s, which only a mock chain meets.
	e2eWorkflowTimeout = 10 * time.Minute
)

func requireE2EEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Skipf("%s must be set to run the real-chain e2e", key)
	}
	return value
}

func e2eScanBlocks(t *testing.T) int64 {
	t.Helper()
	raw := os.Getenv("E2E_SCAN_BLOCKS")
	if raw == "" {
		return defaultE2EScanBlocks
	}
	blocks, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || blocks <= 0 {
		t.Fatalf("E2E_SCAN_BLOCKS = %q, want a positive block count", raw)
	}
	return blocks
}

// e2eHeadBlock reads the head through the same client the deployment uses, so the
// scan span is taken from where the chain is now: FINALITY_DEPTH is a distance
// below the head, and a fixed one would cover a longer range every month.
func e2eHeadBlock(t *testing.T) int64 {
	t.Helper()
	rpcURL, err := chainutil.AlchemyRPCURL(1)
	if err != nil {
		t.Fatalf("resolving the mainnet RPC URL: %v", err)
	}
	client, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL: rpcURL,
		Timeout: 60 * time.Second,
		Logger:  testutil.DiscardLogger(),
	})
	if err != nil {
		t.Fatalf("building the Alchemy client: %v", err)
	}
	head, err := client.GetCurrentBlockNumber(context.Background())
	if err != nil {
		t.Fatalf("reading the chain head: %v", err)
	}
	return head
}

// setRealChainWorkerEnv is setWorkerEnv's shape with the mock chain and LocalStack
// swapped for the real ones. awsconfig builds its static credentials with an empty
// session token, so an SSO profile has to reach the SDK through the default
// credential chain.
func setRealChainWorkerEnv(t *testing.T) (pin int64) {
	t.Helper()
	t.Setenv("CHAIN_ID", "1")
	// Chain 1 only: AlchemyRPCURL appends the key to its mainnet default.
	t.Setenv("ALCHEMY_API_KEY", requireE2EEnv(t, "ALCHEMY_API_KEY"))
	t.Setenv("BUILD_GIT_HASH", "e2e-real-chain")

	pin = posmDeployBlockMainnet + e2eScanBlocks(t)
	depth := e2eHeadBlock(t) - pin
	if depth <= 0 {
		t.Fatalf("SCAN_BLOCKS reaches past the head: pin %d", pin)
	}
	// FINALITY_DEPTH is what bounds the scan: the pin is head - FINALITY_DEPTH.
	t.Setenv("FINALITY_DEPTH", strconv.FormatInt(depth, 10))
	t.Setenv("INITIAL_WINDOW", "10000")
	t.Setenv("MAX_WINDOW", "10000")

	t.Setenv("DEPLOY_ENV", requireE2EEnv(t, "E2E_DEPLOY_ENV"))
	t.Setenv("S3_BUCKET", requireE2EEnv(t, "E2E_S3_BUCKET"))
	t.Setenv("AWS_REGION", "eu-west-1")
	t.Setenv("AWS_PROFILE", requireE2EEnv(t, "AWS_PROFILE"))
	return pin
}

type e2eTransferRow struct {
	blockNumber  int64
	blockVersion int
	logIndex     int
	tokenID      string
}

func e2eTransferRows(t *testing.T, db *pgxpool.Pool) []e2eTransferRow {
	t.Helper()
	rows, err := db.Query(context.Background(), `
		SELECT block_number, block_version, log_index, token_id::text
		FROM uniswap_v4_position_nft_transfer
		ORDER BY block_number, log_index`)
	if err != nil {
		t.Fatalf("reading the persisted transfers: %v", err)
	}
	defer rows.Close()

	var out []e2eTransferRow
	for rows.Next() {
		var r e2eTransferRow
		if err := rows.Scan(&r.blockNumber, &r.blockVersion, &r.logIndex, &r.tokenID); err != nil {
			t.Fatalf("scanning a transfer: %v", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterating transfers: %v", err)
	}
	return out
}

func runE2ETransferWorkflow(t *testing.T, db *pgxpool.Pool) {
	t.Helper()
	env, err := registerWorker(t, db)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	env.SetTestTimeout(e2eWorkflowTimeout)
	env.ExecuteWorkflow(transferWorkflowTypeName)
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("transfer backfill: %v", err)
	}
}

func assertRowsInsideScannedRange(t *testing.T, rows []e2eTransferRow, pin int64) {
	t.Helper()
	for _, r := range rows {
		if r.blockNumber < posmDeployBlockMainnet || r.blockNumber > pin {
			t.Errorf("block_number %d is outside the scanned range [%d, %d]",
				r.blockNumber, posmDeployBlockMainnet, pin)
		}
	}
}

// assertArchiveStampedVersions holds each row to what the archive could hold at its
// height; below the dual-version band that is the bulk downloader's 1 and nothing else.
func assertArchiveStampedVersions(t *testing.T, rows []e2eTransferRow) {
	t.Helper()
	for _, r := range rows {
		if r.blockNumber >= mainnetDualVersionBandStart {
			if r.blockVersion < 0 {
				t.Errorf("block %d: block_version = %d, want a version the archive could hold",
					r.blockNumber, r.blockVersion)
			}
			continue
		}
		// A 0 below the band is the zero value, not an archived version: the run
		// stopped reading the archive.
		if r.blockVersion != 1 {
			t.Errorf("block %d: block_version = %d, want the 1 the archive holds below block %d",
				r.blockNumber, r.blockVersion, mainnetDualVersionBandStart)
		}
	}
}

// TestE2E_PosmTransferBackfillOverRealBlocks is the whole path: eth_getLogs
// against mainnet, each height's block_version resolved from the real archive with
// its hash verified against the block being replayed, and rows written through the
// real repository into a migrated TimescaleDB.
func TestE2E_PosmTransferBackfillOverRealBlocks(t *testing.T) {
	db, _, cleanup := testutil.SetupTestDB(t, sharedDSN)
	t.Cleanup(cleanup)
	pin := setRealChainWorkerEnv(t)

	runE2ETransferWorkflow(t, db)

	rows := e2eTransferRows(t, db)
	if len(rows) == 0 {
		t.Fatal("no transfers written: the scanned range should hold posm Transfer logs")
	}
	t.Logf("wrote %d transfer rows over blocks %d..%d (pin %d)",
		len(rows), rows[0].blockNumber, rows[len(rows)-1].blockNumber, pin)

	assertRowsInsideScannedRange(t, rows, pin)
	assertArchiveStampedVersions(t, rows)

	// A rerun is a new workflow execution carrying no heartbeat details, so it
	// rescans the range from the start; on the same build every row conflicts away.
	before := len(rows)
	runE2ETransferWorkflow(t, db)
	if after := len(e2eTransferRows(t, db)); after != before {
		t.Errorf("a rerun on the same build changed the row count: %d -> %d", before, after)
	} else {
		t.Logf("rerun on the same build wrote no new rows (%d)", after)
	}
}

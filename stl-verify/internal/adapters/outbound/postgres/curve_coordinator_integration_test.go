//go:build integration

package postgres

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/curveindexer"
	"github.com/archon-research/stl/stl-verify/internal/services/dexconsumer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// curveCoordChainID is a dedicated chain id for the coordinator full-block tests
// so their seeds never collide with the repo-level tests on chain 999.
const curveCoordChainID = int64(998)

// stableswapCallCountResults is a fake Multicaller for the coordinator full-block
// tests. The number of multicall calls is deterministic per pool kind (a 2-coin
// pre-NG pool issues 21 calls, a 2-coin NG pool 27), so it returns the matching
// canned result set keyed on len(calls). An unexpected count fails the test.
type stableswapCallCountResults struct {
	t   *testing.T
	pre []outbound.Result
	ng  []outbound.Result
}

func (m *stableswapCallCountResults) Execute(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
	return m.resultsFor(calls)
}

// ExecuteAtHash is the path CurveService.snapshotPools actually calls
// (state snapshots are pinned to the block hash); Execute is kept to satisfy
// outbound.Multicaller for callers that only have a block number.
func (m *stableswapCallCountResults) ExecuteAtHash(_ context.Context, calls []outbound.Call, _ common.Hash) ([]outbound.Result, error) {
	return m.resultsFor(calls)
}

func (m *stableswapCallCountResults) resultsFor(calls []outbound.Call) ([]outbound.Result, error) {
	switch len(calls) {
	case len(m.pre):
		return m.pre, nil
	case len(m.ng):
		return m.ng, nil
	default:
		m.t.Fatalf("unexpected snapshot call count %d (pre=%d ng=%d)", len(calls), len(m.pre), len(m.ng))
		return nil, nil
	}
}

func (m *stableswapCallCountResults) Address() common.Address { return common.Address{} }

// coordPreNGResults mirrors the curveindexer unit-test canned results for a
// 2-coin pre-NG pool (21 reads). Kept local to the postgres package because the
// curveindexer fixtures are test-only and not importable.
func coordPreNGResults() []outbound.Result {
	pack := func(v int64) outbound.Result {
		return outbound.Result{Success: true, ReturnData: common.LeftPadBytes(big.NewInt(v).Bytes(), 32)}
	}
	return []outbound.Result{
		pack(1000000000000000000), // balances(0)
		pack(1000000000000000000), // balances(1)
		pack(1001000000000000000), // get_virtual_price
		pack(2000000000000000000), // totalSupply
		pack(900),                 // A
		pack(4000000),             // fee
		pack(999000000000000000),  // get_dy(0,1)
		pack(998000000000000000),  // get_dy(1,0)
		pack(90000),               // A_precise
		pack(167049139334410),     // admin_balances(0)
		pack(200000),              // admin_balances(1)
		pack(1754802761188011498), // calc_token_amount
		pack(1139623037379637920), // calc_withdraw_one_coin(0)
		pack(1138000000000000000), // calc_withdraw_one_coin(1)
		pack(20000),               // initial_A
		pack(1731805535),          // initial_A_time
		pack(90000),               // future_A
		pack(1732495784),          // future_A_time
		pack(5000000000),          // admin_fee
		pack(1000000),             // future_fee
		pack(5000000000),          // future_admin_fee
	}
}

// coordNGResults mirrors the curveindexer unit-test canned results for a 2-coin
// NG pool (27 reads); stored_rates (index 16) returns a uint256[2].
func coordNGResults(t *testing.T) []outbound.Result {
	t.Helper()
	pack := func(v int64) outbound.Result {
		return outbound.Result{Success: true, ReturnData: common.LeftPadBytes(big.NewInt(v).Bytes(), 32)}
	}
	arrT, err := abi.NewType("uint256[2]", "", nil)
	if err != nil {
		t.Fatalf("uint256[2] type: %v", err)
	}
	args := abi.Arguments{{Type: arrT}}
	storedRates, err := args.Pack([2]*big.Int{big.NewInt(1000000000000000000), big.NewInt(1000000000000000000)})
	if err != nil {
		t.Fatalf("packing stored_rates: %v", err)
	}
	return []outbound.Result{
		pack(1000000000000000000),                // balances(0)
		pack(1000000000000000000),                // balances(1)
		pack(1001000000000000000),                // get_virtual_price
		pack(2000000000000000000),                // totalSupply
		pack(150000),                             // A
		pack(4000000),                            // fee
		pack(999000000000000000),                 // get_dy(0,1)
		pack(998000000000000000),                 // get_dy(1,0)
		pack(1000100000000000000),                // price_oracle
		pack(1000050000000000000),                // last_price
		pack(150000),                             // A_precise
		pack(3102741508070431),                   // admin_balances(0)
		pack(300000),                             // admin_balances(1)
		pack(1858424247096721508),                // calc_token_amount
		pack(1076093673587716682),                // calc_withdraw_one_coin(0)
		pack(1075000000000000000),                // calc_withdraw_one_coin(1)
		{Success: true, ReturnData: storedRates}, // stored_rates (uint256[2])
		pack(999924337681242600),                 // ema_price
		pack(999923231149457753),                 // get_p
		pack(20000),                              // initial_A
		pack(1731805535),                         // initial_A_time
		pack(150000),                             // future_A
		pack(1732495784),                         // future_A_time
		pack(5000000000),                         // admin_fee
		pack(1000000),                            // future_fee
		pack(2597),                               // ma_exp_time
		pack(0),                                  // oracle_method
	}
}

// buildCurveEventLog ABI-encodes an event's non-indexed args into Data and places
// the given indexed topics (already 32-byte hashes) after topic0.
func buildCurveEventLog(
	t *testing.T,
	a *abi.ABI,
	eventName string,
	addr common.Address,
	txHash common.Hash,
	logIndex uint,
	indexedTopics []common.Hash,
	nonIndexedValues ...any,
) shared.Log {
	t.Helper()
	ev, ok := a.Events[eventName]
	if !ok {
		t.Fatalf("event %s not in ABI", eventName)
	}
	var nonIndexed abi.Arguments
	for _, arg := range ev.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}
	var data []byte
	if len(nonIndexed) > 0 {
		packed, err := nonIndexed.Pack(nonIndexedValues...)
		if err != nil {
			t.Fatalf("packing %s data: %v", eventName, err)
		}
		data = packed
	}
	topics := []string{ev.ID.Hex()}
	for _, h := range indexedTopics {
		topics = append(topics, h.Hex())
	}
	return shared.Log{
		Address:         addr.Hex(),
		Topics:          topics,
		Data:            "0x" + hex.EncodeToString(data),
		TransactionHash: txHash.Hex(),
		LogIndex:        fmt.Sprintf("0x%x", logIndex),
	}
}

func addrTopicHash(a common.Address) common.Hash { return common.BytesToHash(a.Bytes()) }

// seedCurveCoordPreNGPool inserts a 2-coin pre-NG pool with a SEPARATE LP-token
// contract on chain 998 and returns (poolID, poolAddr, lpTokenAddr). Idempotent.
func seedCurveCoordPreNGPool(t *testing.T, ctx context.Context) (int64, common.Address, common.Address) {
	t.Helper()
	poolAddr := common.HexToAddress("0xC0FFEE0000000000000000000000000000000001")
	lpAddr := common.HexToAddress("0x06325440D014e39736583c165C2963BA99fAf14E")
	protoID := seedCurveCoordPrereqs(t, ctx)

	tokenID0 := seedCurveCoordToken(t, ctx, "0xBEEF000000000000000000000000000000000010", "PREA", 18)
	tokenID1 := seedCurveCoordToken(t, ctx, "0xBEEF000000000000000000000000000000000011", "PREB", 18)

	var poolID int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO curve_pool (chain_id, protocol_id, pool_address, pool_kind, n_coins, deploy_block, lp_token_address, has_a_precise)
		 VALUES ($1, $2, $3, 'plain_pre_ng', 2, 100, $4, TRUE)
		 ON CONFLICT (chain_id, pool_address) DO UPDATE SET lp_token_address = EXCLUDED.lp_token_address, pool_kind = EXCLUDED.pool_kind, has_a_precise = EXCLUDED.has_a_precise
		 RETURNING id`,
		curveCoordChainID, protoID, poolAddr.Bytes(), lpAddr.Bytes(),
	).Scan(&poolID); err != nil {
		t.Fatalf("seed pre-ng pool: %v", err)
	}
	seedCurveCoordCoins(t, ctx, poolID, tokenID0, tokenID1)
	return poolID, poolAddr, lpAddr
}

// seedCurveCoordNGPool inserts a 2-coin NG pool (its own LP token, NULL
// lp_token_address) on chain 998 and returns (poolID, poolAddr). Idempotent.
func seedCurveCoordNGPool(t *testing.T, ctx context.Context) (int64, common.Address) {
	t.Helper()
	poolAddr := common.HexToAddress("0xC0FFEE0000000000000000000000000000000002")
	protoID := seedCurveCoordPrereqs(t, ctx)

	tokenID0 := seedCurveCoordToken(t, ctx, "0xBEEF000000000000000000000000000000000020", "NGA", 18)
	tokenID1 := seedCurveCoordToken(t, ctx, "0xBEEF000000000000000000000000000000000021", "NGB", 18)

	var poolID int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO curve_pool (chain_id, protocol_id, pool_address, pool_kind, n_coins, deploy_block, has_a_precise)
		 VALUES ($1, $2, $3, 'plain_ng', 2, 100, TRUE)
		 ON CONFLICT (chain_id, pool_address) DO UPDATE SET pool_kind = EXCLUDED.pool_kind, lp_token_address = NULL, has_a_precise = EXCLUDED.has_a_precise
		 RETURNING id`,
		curveCoordChainID, protoID, poolAddr.Bytes(),
	).Scan(&poolID); err != nil {
		t.Fatalf("seed ng pool: %v", err)
	}
	seedCurveCoordCoins(t, ctx, poolID, tokenID0, tokenID1)
	return poolID, poolAddr
}

func seedCurveCoordPrereqs(t *testing.T, ctx context.Context) int64 {
	t.Helper()
	if _, err := curveTestPool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES ($1, 'coordchain')
		 ON CONFLICT (chain_id) DO NOTHING`, curveCoordChainID,
	); err != nil {
		t.Fatalf("seed chain: %v", err)
	}
	var protoID int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block, updated_at, metadata)
		 VALUES ($1, '\xC0DEC0DEC0DEC0DEC0DEC0DEC0DEC0DEC0DEC0DE'::bytea, 'CurveCoord', 'dex', 0, NOW(), '{}'::jsonb)
		 ON CONFLICT (chain_id, address) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`, curveCoordChainID,
	).Scan(&protoID); err != nil {
		t.Fatalf("seed protocol: %v", err)
	}
	return protoID
}

func seedCurveCoordToken(t *testing.T, ctx context.Context, addr, symbol string, decimals int) int64 {
	t.Helper()
	var id int64
	if err := curveTestPool.QueryRow(ctx,
		`INSERT INTO token (chain_id, address, symbol, decimals)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (chain_id, address) DO UPDATE SET symbol = EXCLUDED.symbol, decimals = EXCLUDED.decimals
		 RETURNING id`,
		curveCoordChainID, common.HexToAddress(addr).Bytes(), symbol, decimals,
	).Scan(&id); err != nil {
		t.Fatalf("seed token %s: %v", symbol, err)
	}
	return id
}

func seedCurveCoordCoins(t *testing.T, ctx context.Context, poolID, tokenID0, tokenID1 int64) {
	t.Helper()
	if _, err := curveTestPool.Exec(ctx,
		`INSERT INTO curve_pool_coin (curve_pool_id, coin_index, token_id, precision)
		 VALUES ($1, 0, $2, 1), ($1, 1, $3, 1)
		 ON CONFLICT (curve_pool_id, coin_index) DO NOTHING`,
		poolID, tokenID0, tokenID1,
	); err != nil {
		t.Fatalf("seed coins: %v", err)
	}
}

// newCurveCurveService wires a real CurveRepository, EventRepository, TxManager,
// and the canned-result multicaller into a curveindexer.CurveService over the
// pools seeded on chain 998.
func newCurveCurveService(t *testing.T, ctx context.Context) (*curveindexer.CurveService, *stableswapCallCountResults) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	repo, err := NewCurveRepository(curveTestPool, logger, buildregistry.BuildID(1))
	if err != nil {
		t.Fatalf("NewCurveRepository: %v", err)
	}
	pools, err := repo.LoadPools(ctx, curveCoordChainID)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}
	registered := curveindexer.IndexPoolsByAddress(pools)

	stableABI, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading stableswap ABI: %v", err)
	}

	// The seeded stableswap pools carry has_a_precise=TRUE (see seedCurveCoord*Pool),
	// so LoadPools reports HasAPrecise=true and the snapshot issues the gated
	// A_precise call, keeping the 21/27 canned result counts.
	cryptoABI, err := abis.CurveCryptoswapABI()
	if err != nil {
		t.Fatalf("loading cryptoswap ABI: %v", err)
	}
	stableHandler := curveindexer.NewStableswapHandler(stableABI)
	cryptoHandler := curveindexer.NewCryptoswapHandler(cryptoABI)
	handlers := curveindexer.NewHandlerRegistry(stableHandler, cryptoHandler)

	txMgr, err := NewTxManager(curveTestPool, logger)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}

	var protocolID int64
	if err := curveTestPool.QueryRow(ctx,
		`SELECT id FROM protocol WHERE chain_id = $1 AND address = '\xC0DEC0DEC0DEC0DEC0DEC0DEC0DEC0DEC0DEC0DE'::bytea`,
		curveCoordChainID,
	).Scan(&protocolID); err != nil {
		t.Fatalf("resolve protocol id: %v", err)
	}
	eventWriter := dexconsumer.NewProtocolEventWriter(protocolID, NewEventRepository(logger, buildregistry.BuildID(1)))

	mc := &stableswapCallCountResults{t: t, pre: coordPreNGResults(), ng: coordNGResults(t)}

	coord, err := curveindexer.NewCurveService(curveindexer.CurveServiceDeps{
		Pools:         registered,
		Handlers:      handlers,
		StableHandler: stableHandler,
		CryptoHandler: cryptoHandler,
		Multicaller:   mc,
		Repo:          repo,
		EventWriter:   eventWriter,
		TxManager:     txMgr,
		SweepBlocks:   0,
		ChainID:       curveCoordChainID,
		Logger:        logger,
	})
	if err != nil {
		t.Fatalf("NewCurveService: %v", err)
	}
	return coord, mc
}

// truncateCurveCoordTables clears the fact tables plus protocol_event so each
// coordinator test starts clean regardless of sibling test order. It also
// registers a cleanup that removes this test's chain-998 pools and their coins,
// so the global curve_pool_coin count (asserted by TestCurveMigration) is left
// exactly as found and these tests carry no order dependency.
func truncateCurveCoordTables(t *testing.T, ctx context.Context) {
	t.Helper()
	truncateCurveFactTables(t, ctx)
	if _, err := curveTestPool.Exec(ctx, "DELETE FROM protocol_event"); err != nil {
		t.Fatalf("truncate protocol_event: %v", err)
	}
	deleteCurveCoordPools(t, ctx)
	t.Cleanup(func() { deleteCurveCoordPools(t, context.Background()) })
}

// deleteCurveCoordPools removes the chain-998 pools, their coins, and any fact
// rows referencing them, so these tests leave no rows behind in the shared schema.
func deleteCurveCoordPools(t *testing.T, ctx context.Context) {
	t.Helper()
	poolSubquery := `SELECT id FROM curve_pool WHERE chain_id = $1`
	for _, table := range []string{
		"curve_swap", "curve_liquidity_event", "curve_stableswap_state",
		"curve_cryptoswap_state", "curve_stableswap_config", "curve_cryptoswap_config",
		"curve_parameter_event", "curve_lp_token_event", "curve_pool_coin",
	} {
		if _, err := curveTestPool.Exec(ctx,
			"DELETE FROM "+table+" WHERE curve_pool_id IN ("+poolSubquery+")",
			curveCoordChainID,
		); err != nil {
			t.Fatalf("delete coord %s: %v", table, err)
		}
	}
	if _, err := curveTestPool.Exec(ctx,
		`DELETE FROM curve_pool WHERE chain_id = $1`, curveCoordChainID,
	); err != nil {
		t.Fatalf("delete coord pools: %v", err)
	}
}

// TestCurveCurveService_FullBlock_RoutesLpTokenLogsAndPopulatesEveryTable feeds a
// single block through the coordinator that carries, for the pre-NG pool, a swap,
// a liquidity add, a parameter event (RampA), and LP Transfer + Approval emitted
// on its SEPARATE LP-token contract, plus an LP Transfer on the NG pool's own
// address. It asserts every curve fact table is populated, the pre-NG LP transfer
// is attributed to the pre-NG pool id, both pools are state-snapshotted, and the
// protocol_event capture row for an LP-token log keeps the LP-token contract
// address (not the pool address).
func TestCurveCurveService_FullBlock_RoutesLpTokenLogsAndPopulatesEveryTable(t *testing.T) {
	ctx := context.Background()
	truncateCurveCoordTables(t, ctx)

	preID, preAddr, lpAddr := seedCurveCoordPreNGPool(t, ctx)
	ngID, ngAddr := seedCurveCoordNGPool(t, ctx)

	stableABI, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading stableswap ABI: %v", err)
	}

	coord, _ := newCurveCurveService(t, ctx)

	const bn = int64(12_000_000)
	txHash := common.HexToHash("0xfeedface00000000000000000000000000000000000000000000000000000001")
	buyer := common.HexToAddress("0xAbc0000000000000000000000000000000000001")
	provider := common.HexToAddress("0xAbc0000000000000000000000000000000000002")
	lpFrom := common.HexToAddress("0x1111111111111111111111111111111111111111")
	lpTo := common.HexToAddress("0x2222222222222222222222222222222222222222")

	// Pool-address logs for the pre-NG pool: swap, add liquidity, RampA.
	swapLog := buildCurveEventLog(t, stableABI, "TokenExchange", preAddr, txHash, 0,
		[]common.Hash{addrTopicHash(buyer)},
		big.NewInt(0), big.NewInt(1_000_000_000_000_000_000), big.NewInt(1), big.NewInt(990_000_000_000_000_000))
	addLog := buildCurveEventLog(t, stableABI, "AddLiquidity", preAddr, txHash, 1,
		[]common.Hash{addrTopicHash(provider)},
		[]*big.Int{big.NewInt(1_000_000_000_000_000_000), big.NewInt(990_000_000_000_000_000)},
		[]*big.Int{big.NewInt(0), big.NewInt(0)},
		big.NewInt(2_000_000_000_000_000_000),
		big.NewInt(1_999_000_000_000_000_000))
	rampLog := buildCurveEventLog(t, stableABI, "RampA", preAddr, txHash, 2, nil,
		big.NewInt(20000), big.NewInt(90000), big.NewInt(100), big.NewInt(200))

	// LP Transfer + Approval on the SEPARATE LP-token contract for the pre-NG pool.
	lpTransferLog := buildCurveEventLog(t, stableABI, "Transfer", lpAddr, txHash, 3,
		[]common.Hash{addrTopicHash(lpFrom), addrTopicHash(lpTo)}, big.NewInt(500))
	lpApprovalLog := buildCurveEventLog(t, stableABI, "Approval", lpAddr, txHash, 4,
		[]common.Hash{addrTopicHash(lpFrom), addrTopicHash(lpTo)}, big.NewInt(700))

	// LP Transfer on the NG pool's OWN address (NG pool is its own LP token).
	ngTransferLog := buildCurveEventLog(t, stableABI, "Transfer", ngAddr, txHash, 5,
		[]common.Hash{addrTopicHash(lpFrom), addrTopicHash(lpTo)}, big.NewInt(900))

	receipt := shared.TransactionReceipt{
		Logs:            []shared.Log{swapLog, addLog, rampLog, lpTransferLog, lpApprovalLog, ngTransferLog},
		TransactionHash: txHash.Hex(),
	}
	event := outbound.BlockEvent{
		ChainID:        curveCoordChainID,
		BlockNumber:    bn,
		Version:        0,
		BlockTimestamp: 1_700_000_000,
		BlockHash:      common.HexToHash("0x01").Hex(),
	}

	if err := coord.BlockHandler()(ctx, event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	// Every fact table populated for the pre-NG pool.
	assertCount(t, ctx, "curve_swap", preID, bn, 1)
	assertCount(t, ctx, "curve_liquidity_event", preID, bn, 1)
	assertCount(t, ctx, "curve_parameter_event", preID, bn, 1)
	assertCount(t, ctx, "curve_stableswap_state", preID, bn, 1)
	assertCount(t, ctx, "curve_stableswap_config", preID, bn, 1)

	// Pre-NG LP transfer (and approval) attributed to the pre-NG pool, NOT the NG pool.
	assertCount(t, ctx, "curve_lp_token_event", preID, bn, 2)

	// NG pool: LP transfer on its own address + its state snapshot.
	assertCount(t, ctx, "curve_lp_token_event", ngID, bn, 1)
	assertCount(t, ctx, "curve_stableswap_state", ngID, bn, 1)

	// The capture row for the LP-token Transfer keeps the LP-token contract
	// address, while the swap's capture row keeps the pool address.
	assertProtocolEventAddress(t, ctx, bn, 3, lpAddr)
	assertProtocolEventAddress(t, ctx, bn, 0, preAddr)
	// The NG LP transfer's capture row is the NG pool's own address.
	assertProtocolEventAddress(t, ctx, bn, 5, ngAddr)
}

// TestCurveCurveService_PreNGPoolTouchedOnlyByLpTokenLog_GetsStateSnapshot feeds a
// block whose only relevant log is an LP Transfer on the pre-NG pool's SEPARATE
// LP-token contract. The pool has no pool-address activity, yet it must still be
// considered touched and receive a state snapshot.
func TestCurveCurveService_PreNGPoolTouchedOnlyByLpTokenLog_GetsStateSnapshot(t *testing.T) {
	ctx := context.Background()
	truncateCurveCoordTables(t, ctx)

	preID, _, lpAddr := seedCurveCoordPreNGPool(t, ctx)
	seedCurveCoordNGPool(t, ctx) // present in registry but not touched this block

	stableABI, err := abis.CurveStableswapABI()
	if err != nil {
		t.Fatalf("loading stableswap ABI: %v", err)
	}
	coord, _ := newCurveCurveService(t, ctx)

	const bn = int64(12_000_100)
	txHash := common.HexToHash("0xfeedface00000000000000000000000000000000000000000000000000000002")
	lpFrom := common.HexToAddress("0x1111111111111111111111111111111111111111")
	lpTo := common.HexToAddress("0x2222222222222222222222222222222222222222")
	lpTransferLog := buildCurveEventLog(t, stableABI, "Transfer", lpAddr, txHash, 0,
		[]common.Hash{addrTopicHash(lpFrom), addrTopicHash(lpTo)}, big.NewInt(500))

	receipt := shared.TransactionReceipt{
		Logs:            []shared.Log{lpTransferLog},
		TransactionHash: txHash.Hex(),
	}
	event := outbound.BlockEvent{
		ChainID:        curveCoordChainID,
		BlockNumber:    bn,
		Version:        0,
		BlockTimestamp: 1_700_000_100,
		BlockHash:      common.HexToHash("0x02").Hex(),
	}

	if err := coord.BlockHandler()(ctx, event, []shared.TransactionReceipt{receipt}); err != nil {
		t.Fatalf("BlockHandler: %v", err)
	}

	assertCount(t, ctx, "curve_lp_token_event", preID, bn, 1)
	// Touched only by the LP-token log, but still snapshotted.
	assertCount(t, ctx, "curve_stableswap_state", preID, bn, 1)
}

func assertCount(t *testing.T, ctx context.Context, table string, poolID, bn int64, want int) {
	t.Helper()
	var got int
	if err := curveTestPool.QueryRow(ctx,
		"SELECT count(*) FROM "+table+" WHERE curve_pool_id=$1 AND block_number=$2",
		poolID, bn,
	).Scan(&got); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	if got != want {
		t.Errorf("%s rows for pool %d block %d = %d, want %d", table, poolID, bn, got, want)
	}
}

func assertProtocolEventAddress(t *testing.T, ctx context.Context, bn int64, logIndex int, want common.Address) {
	t.Helper()
	var addr []byte
	if err := curveTestPool.QueryRow(ctx,
		`SELECT contract_address FROM protocol_event
		 WHERE chain_id=$1 AND block_number=$2 AND log_index=$3`,
		curveCoordChainID, bn, logIndex,
	).Scan(&addr); err != nil {
		t.Fatalf("read protocol_event (log_index %d): %v", logIndex, err)
	}
	if got := common.BytesToAddress(addr); got != want {
		t.Errorf("protocol_event log_index %d contract_address = %s, want %s", logIndex, got, want)
	}
}

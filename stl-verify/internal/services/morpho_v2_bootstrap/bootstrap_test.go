package morpho_v2_bootstrap

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/morpho_indexer"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// mainnetVaultV2DeployBlock is the sweep's lower bound on chain 1 — the block
// the VaultV2 factory was deployed at. Duplicated from morpho_indexer so the
// tests assert the sweep starts where it must, not merely where the code says.
const mainnetVaultV2DeployBlock = int64(23_375_073)

var (
	testVaultAddr   = common.HexToAddress("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee")
	testAdapterAddr = common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")
	testTxHash      = common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333")
)

func TestNewService_RejectsInvalidConfig(t *testing.T) {
	valid := ConfigDefaults()
	valid.ChainID = 1
	valid.Logger = discardLogger()

	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "missing chain id", mutate: func(c *Config) { c.ChainID = 0 }},
		{name: "non-positive block chunk size", mutate: func(c *Config) { c.BlockChunkSize = 0 }},
		{name: "non-positive address batch size", mutate: func(c *Config) { c.AddressBatchSize = 0 }},
		{name: "missing logger", mutate: func(c *Config) { c.Logger = nil }},
		{name: "chain with no known VaultV2 factory", mutate: func(c *Config) { c.ChainID = 999999 }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid
			tc.mutate(&cfg)
			if _, err := NewService(cfg, &fakeChainReader{}, &recordingReplayer{}); err == nil {
				t.Fatal("expected NewService to reject the config")
			}
		})
	}
}

// TestRun_ReplaysHistoryThenSeedsAdapters is the end-to-end pass: a persisted V2
// vault with no adapter rows gets its historical AddAdapter replayed through the
// REAL morpho-indexer handler path (NewReplayService → ReplayMetaMorphoLog), not
// a stand-in, and then its current adapter set enumerated and snapshotted at the
// finalized head.
func TestRun_ReplaysHistoryThenSeedsAdapters(t *testing.T) {
	h := newBootstrapHarness(t)

	const headBlock = int64(24_000_000)
	const addAdapterBlock = uint64(23_400_000)
	realAssets := big.NewInt(4242)

	head := h.chain.setFinalizedHead(headBlock, 1_770_000_000)
	logBlockHash := h.chain.addBlock(addAdapterBlock, 1_760_000_000)
	h.chain.logs = []ethtypes.Log{h.addAdapterLog(addAdapterBlock, logBlockHash, 7)}
	h.wireAdapterReads(head.Hash(), realAssets)

	if err := h.service.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Pass 1 — the replay: the historical AddAdapter reached the real handler,
	// which registered the adapter at its true block and wrote the audit row.
	replayed := h.adaptersAt(int64(addAdapterBlock))
	if len(replayed) != 1 {
		t.Fatalf("adapters registered at the AddAdapter block = %d, want 1 (the replayed event)", len(replayed))
	}
	if len(h.auditEvents) != 1 || h.auditEvents[0].EventName != "AddAdapter" {
		t.Fatalf("audit events = %+v, want a single AddAdapter protocol_event", h.auditEvents)
	}
	if h.auditEvents[0].BlockNumber != int64(addAdapterBlock) || h.auditEvents[0].LogIndex != 7 {
		t.Errorf("audit event coordinates = (block %d, index %d), want (%d, 7)",
			h.auditEvents[0].BlockNumber, h.auditEvents[0].LogIndex, addAdapterBlock)
	}

	// Pass 2 — the seed: enumeration at the pinned head, plus a state row so
	// VEC-219's composition probe no longer sees adapter_data_missing.
	seeded := h.adaptersAt(headBlock)
	if len(seeded) != 1 {
		t.Fatalf("adapters registered at the head block = %d, want 1 (the enumeration seed)", len(seeded))
	}
	if !bytes.Equal(seeded[0].Address, testAdapterAddr.Bytes()) {
		t.Errorf("seeded adapter = %x, want %s", seeded[0].Address, testAdapterAddr.Hex())
	}
	if seeded[0].AdapterType != entity.MorphoAdapterTypeMarketV1 {
		t.Errorf("seeded adapter type = %v, want MarketV1", seeded[0].AdapterType)
	}
	// Two snapshots: the registration path takes one at the AddAdapter's own
	// block, the seed pass one at the pinned head.
	stateBlocks := make([]int64, 0, len(h.adapterStates))
	for _, state := range h.adapterStates {
		stateBlocks = append(stateBlocks, state.BlockNumber)
		if state.RealAssets.Cmp(realAssets) != 0 {
			t.Errorf("adapter_state at block %d has realAssets %s, want %s",
				state.BlockNumber, state.RealAssets, realAssets)
		}
	}
	slices.Sort(stateBlocks)
	if want := []int64{int64(addAdapterBlock), headBlock}; !slices.Equal(stateBlocks, want) {
		t.Errorf("adapter_state blocks = %v, want %v", stateBlocks, want)
	}
}

// TestRun_SweepsFromTheFactoryDeployBlockToTheFinalizedHead pins the sweep's
// bounds and its hash-pinning. Starting later than the factory deploy block
// would silently skip a vault's earliest governance events; pinning the seed to
// anything but the finalized head risks recording state from a reorged block.
func TestRun_SweepsFromTheFactoryDeployBlockToTheFinalizedHead(t *testing.T) {
	h := newBootstrapHarness(t)
	const headBlock = int64(23_400_000)
	head := h.chain.setFinalizedHead(headBlock, 1_770_000_000)
	h.wireAdapterReads(head.Hash(), big.NewInt(1))

	if err := h.service.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(h.chain.requestedHeadNumbers) != 1 || h.chain.requestedHeadNumbers[0].Int64() != int64(rpc.FinalizedBlockNumber) {
		t.Fatalf("head resolution requested %v, want a single finalized-block header read", h.chain.requestedHeadNumbers)
	}
	if len(h.chain.queries) == 0 {
		t.Fatal("no eth_getLogs request was issued")
	}
	first, last := h.chain.queries[0], h.chain.queries[len(h.chain.queries)-1]
	if first.FromBlock.Int64() != mainnetVaultV2DeployBlock {
		t.Errorf("sweep starts at block %d, want the VaultV2 factory deploy block %d", first.FromBlock, mainnetVaultV2DeployBlock)
	}
	if last.ToBlock.Int64() != headBlock {
		t.Errorf("sweep ends at block %d, want the finalized head %d", last.ToBlock, headBlock)
	}
	if len(first.Topics) != 1 || len(first.Topics[0]) != 10 {
		t.Errorf("topic filter = %v, want a single position holding the 10 config topics", first.Topics)
	}
	if len(first.Addresses) != 1 || first.Addresses[0] != testVaultAddr {
		t.Errorf("address filter = %v, want the single known V2 vault", first.Addresses)
	}
}

// TestRun_NoV2VaultsFailsTheRun: this job is only triggered because V2 vaults are
// known to be missing rows, so finding none means it is pointed at the wrong
// chain or database. Returning nil there would report a green run that healed
// nothing — the one failure mode nobody would notice.
func TestRun_NoV2VaultsFailsTheRun(t *testing.T) {
	h := newBootstrapHarness(t)
	h.morphoRepo.GetAllVaultsFn = func(context.Context, int64) (map[common.Address]*entity.MorphoVault, error) {
		return map[common.Address]*entity.MorphoVault{
			testVaultAddr: {ID: 7, ChainID: 1, Address: testVaultAddr.Bytes(), VaultVersion: entity.MorphoVaultV1},
		}, nil
	}
	h.chain.setFinalizedHead(24_000_000, 1_770_000_000)

	err := h.service.Run(context.Background())
	if err == nil {
		t.Fatal("expected a run that found no V2 vaults to fail rather than report success")
	}
	if !strings.Contains(err.Error(), "no VaultV2 vaults") {
		t.Errorf("error %q should say no V2 vaults were found", err)
	}
	if len(h.chain.queries) != 0 {
		t.Errorf("issued %d eth_getLogs requests for a chain with no V2 vaults, want 0", len(h.chain.queries))
	}
}

// TestRun_FinalizedHeadBelowDeployBlockFailsTheRun: an inverted sweep range
// yields zero chunks, so without this guard the run would replay nothing and
// still return nil. It means the RPC endpoint is not on the configured chain.
func TestRun_FinalizedHeadBelowDeployBlockFailsTheRun(t *testing.T) {
	h := newBootstrapHarness(t)
	h.chain.setFinalizedHead(mainnetVaultV2DeployBlock-1, 1_770_000_000)

	err := h.service.Run(context.Background())
	if err == nil {
		t.Fatal("expected a finalized head below the factory deploy block to fail the run")
	}
	if !strings.Contains(err.Error(), "not on the configured chain") {
		t.Errorf("error %q should point at a chain mismatch", err)
	}
	if len(h.chain.queries) != 0 {
		t.Errorf("issued %d eth_getLogs requests, want 0", len(h.chain.queries))
	}
}

// TestRun_ReplayFailureStopsBeforeSeed pins the load-bearing pass order from the
// other end: the seed converges onto whatever incarnation the replay built, so a
// failed replay must stop the run before the seed writes anything. Seeding on top
// of a half-replayed history is how a state row ends up on the wrong adapter
// incarnation.
func TestRun_ReplayFailureStopsBeforeSeed(t *testing.T) {
	h := newBootstrapHarness(t)
	replayer := &recordingReplayer{v2Vaults: map[common.Address]struct{}{testVaultAddr: {}}}
	service, err := NewService(h.config, h.chain, replayer)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	h.chain.setFinalizedHead(24_000_000, 1_770_000_000)
	h.chain.filterErr = errors.New("401 Unauthorized: invalid api key")

	if err := service.Run(context.Background()); err == nil {
		t.Fatal("expected the replay failure to fail the run")
	}
	if len(replayer.seeded) != 0 {
		t.Errorf("seeded %d vaults after the replay failed, want 0", len(replayer.seeded))
	}
}

// TestRun_SeedFailureFailsTheRun: the seed is the last pass, so its failure has
// nothing left to guard — it just has to fail loudly rather than let the run
// report success with an adapter left stateless.
func TestRun_SeedFailureFailsTheRun(t *testing.T) {
	h := newBootstrapHarness(t)
	h.chain.setFinalizedHead(24_000_000, 1_770_000_000)
	h.multicaller.ExecuteAtHashFn = func(context.Context, []outbound.Call, common.Hash) ([]outbound.Result, error) {
		return nil, errors.New("rpc unavailable")
	}

	if err := h.service.Run(context.Background()); err == nil {
		t.Fatal("expected the seed failure to fail the run")
	}
}

// TestRun_ReplayedLogsArriveInChainOrder: the handlers require an adapter's
// AddAdapter before its RemoveAdapter, and the per-address-batch requests inside
// a chunk return interleaved. The service must restore (block, logIndex) order.
func TestRun_ReplayedLogsArriveInChainOrder(t *testing.T) {
	h := newBootstrapHarness(t)
	replayer := &recordingReplayer{v2Vaults: map[common.Address]struct{}{testVaultAddr: {}}}
	service, err := NewService(h.config, h.chain, replayer)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	h.chain.setFinalizedHead(23_400_000, 1_770_000_000)
	blockA := h.chain.addBlock(23_380_000, 1_760_000_000)
	blockB := h.chain.addBlock(23_390_000, 1_760_000_500)
	h.chain.logs = []ethtypes.Log{
		{BlockNumber: 23_390_000, BlockHash: blockB, Index: 1, Address: testVaultAddr, Topics: []common.Hash{{0x01}}},
		{BlockNumber: 23_380_000, BlockHash: blockA, Index: 9, Address: testVaultAddr, Topics: []common.Hash{{0x02}}},
		{BlockNumber: 23_380_000, BlockHash: blockA, Index: 2, Address: testVaultAddr, Topics: []common.Hash{{0x03}}},
	}

	if err := service.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := []struct {
		block uint64
		index string
	}{
		{23_380_000, "0x2"},
		{23_380_000, "0x9"},
		{23_390_000, "0x1"},
	}
	if len(replayer.replayed) != len(want) {
		t.Fatalf("replayed %d logs, want %d", len(replayer.replayed), len(want))
	}
	for i, w := range want {
		got := replayer.replayed[i]
		if uint64(got.blockNumber) != w.block || got.log.LogIndex != w.index {
			t.Fatalf("replayed[%d] = (block %d, index %s), want (block %d, index %s)",
				i, got.blockNumber, got.log.LogIndex, w.block, w.index)
		}
	}
}

// TestRun_RemovedLogFailsTheRun: the sweep's upper bound is a finalized block, so
// a reorged-out log is a node anomaly. Replaying it would persist state from a
// non-canonical block; filtering it away silently would hide the anomaly.
func TestRun_RemovedLogFailsTheRun(t *testing.T) {
	h := newBootstrapHarness(t)
	replayer := &recordingReplayer{v2Vaults: map[common.Address]struct{}{testVaultAddr: {}}}
	service, err := NewService(h.config, h.chain, replayer)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	h.chain.setFinalizedHead(23_400_000, 1_770_000_000)
	blockHash := h.chain.addBlock(23_380_000, 1_760_000_000)
	h.chain.logs = []ethtypes.Log{
		{BlockNumber: 23_380_000, BlockHash: blockHash, Index: 0, Address: testVaultAddr, Topics: []common.Hash{{0x01}}, Removed: true},
	}

	if err := service.Run(context.Background()); err == nil {
		t.Fatal("expected a removed log inside the finalized range to fail the run")
	}
	if len(replayer.replayed) != 0 {
		t.Errorf("replayed %d logs, want 0 — a removed log must never reach a handler", len(replayer.replayed))
	}
}

// TestRun_NarrowsRangeOnProviderCap: a provider result cap rides HTTP 200 so the
// retrying transport cannot see it. The sweep must halve its range and still
// deliver every log in the original range exactly once.
func TestRun_NarrowsRangeOnProviderCap(t *testing.T) {
	h := newBootstrapHarness(t)
	replayer := &recordingReplayer{v2Vaults: map[common.Address]struct{}{testVaultAddr: {}}}
	h.config.BlockChunkSize = 1_000_000
	service, err := NewService(h.config, h.chain, replayer)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	h.chain.setFinalizedHead(mainnetVaultV2DeployBlock+7, 1_770_000_000)
	blockHash := h.chain.addBlock(uint64(mainnetVaultV2DeployBlock+5), 1_760_000_000)
	h.chain.logs = []ethtypes.Log{
		{BlockNumber: uint64(mainnetVaultV2DeployBlock + 5), BlockHash: blockHash, Index: 0, Address: testVaultAddr, Topics: []common.Hash{{0x01}}},
	}
	// Refuse any request wider than 2 blocks, forcing repeated halving.
	h.chain.maxQueryWidth = 2

	if err := service.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(replayer.replayed) != 1 {
		t.Fatalf("replayed %d logs after range narrowing, want exactly 1", len(replayer.replayed))
	}
	if h.chain.cappedQueries == 0 {
		t.Fatal("the provider cap never triggered; the test is not exercising narrowing")
	}
}

// TestRun_UnrelatedGetLogsErrorBubbles: only the result/range cap is worked
// around. Any other failure must stop the run so it is not mistaken for an
// empty history.
func TestRun_UnrelatedGetLogsErrorBubbles(t *testing.T) {
	h := newBootstrapHarness(t)
	replayer := &recordingReplayer{v2Vaults: map[common.Address]struct{}{testVaultAddr: {}}}
	service, err := NewService(h.config, h.chain, replayer)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	h.chain.setFinalizedHead(mainnetVaultV2DeployBlock+10, 1_770_000_000)
	h.chain.filterErr = errors.New("401 Unauthorized: invalid api key")

	if err := service.Run(context.Background()); err == nil {
		t.Fatal("expected an unrelated eth_getLogs failure to fail the run")
	}
}

// --- harness -----------------------------------------------------------------

// bootstrapHarness wires the service against the REAL morpho-indexer replay
// service so a test exercises the production handler path, with only the node
// and the repositories faked.
type bootstrapHarness struct {
	t             *testing.T
	config        Config
	service       *Service
	chain         *fakeChainReader
	multicaller   *testutil.MockMulticaller
	morphoRepo    *testutil.MockMorphoRepository
	adapters      []*entity.MorphoAdapter
	adapterStates []*entity.MorphoAdapterState
	auditEvents   []*entity.ProtocolEvent
}

func newBootstrapHarness(t *testing.T) *bootstrapHarness {
	t.Helper()
	h := &bootstrapHarness{t: t, chain: newFakeChainReader()}

	h.multicaller = testutil.NewMockMulticaller()
	h.morphoRepo = &testutil.MockMorphoRepository{}
	h.morphoRepo.GetAllVaultsFn = func(context.Context, int64) (map[common.Address]*entity.MorphoVault, error) {
		return map[common.Address]*entity.MorphoVault{
			testVaultAddr: {
				ID: 7, ChainID: 1, ProtocolID: 1, Address: testVaultAddr.Bytes(),
				Name: "Test Vault", Symbol: "tVAULT", AssetTokenID: 1,
				VaultVersion: entity.MorphoVaultV2, CreatedAtBlock: 23_400_000,
			},
		}, nil
	}
	var nextAdapterID int64
	h.morphoRepo.GetOrCreateAdapterFn = func(_ context.Context, _ pgx.Tx, a *entity.MorphoAdapter) (int64, error) {
		nextAdapterID++
		h.adapters = append(h.adapters, a)
		return nextAdapterID, nil
	}
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoAdapterState) error {
		h.adapterStates = append(h.adapterStates, s)
		return nil
	}
	eventRepo := &testutil.MockEventRepository{
		SaveEventFn: func(_ context.Context, _ pgx.Tx, e *entity.ProtocolEvent) error {
			h.auditEvents = append(h.auditEvents, e)
			return nil
		},
	}

	svcConfig := morpho_indexer.ConfigDefaults()
	svcConfig.ChainID = 1
	svcConfig.Logger = discardLogger()
	replay, err := morpho_indexer.NewReplayService(
		svcConfig, h.multicaller, &testutil.MockTxManager{},
		&testutil.MockProtocolRepository{}, h.morphoRepo, eventRepo,
	)
	if err != nil {
		t.Fatalf("NewReplayService: %v", err)
	}

	h.config = ConfigDefaults()
	h.config.ChainID = 1
	h.config.Logger = discardLogger()

	h.service, err = NewService(h.config, h.chain, replay)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return h
}

func (h *bootstrapHarness) wireAdapterReads(headHash common.Hash, realAssets *big.Int) {
	h.t.Helper()
	wireAdapterReads(h.t, h.multicaller, headHash, testVaultAddr, testAdapterAddr, realAssets)
}

// wireAdapterReads answers the enumeration + realAssets reads (hash-pinned to
// headHash) and the number-pinned adapter type probe for a vault holding exactly
// one MarketV1 adapter.
func wireAdapterReads(t *testing.T, mc *testutil.MockMulticaller, headHash common.Hash, vault, adapter common.Address, realAssets *big.Int) {
	t.Helper()
	vaultV2ABI, err := abis.GetVaultV2ReadABI()
	if err != nil {
		t.Fatalf("GetVaultV2ReadABI: %v", err)
	}
	adapterABI, err := abis.GetVaultV2AdapterReadABI()
	if err != nil {
		t.Fatalf("GetVaultV2AdapterReadABI: %v", err)
	}
	pack := func(args abi.Arguments, values ...any) []byte {
		data, err := args.Pack(values...)
		if err != nil {
			t.Fatalf("packing return data: %v", err)
		}
		return data
	}

	// The seed's enumeration must be pinned to the run's finalized head. An
	// adapter's realAssets is read on the registration path too, pinned to the
	// AddAdapter's own block, so only the enumeration is head-checked.
	mc.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		requireHeadPinned := func() error {
			if blockHash != headHash {
				return errors.New("adapter enumeration is not pinned to the run's finalized head")
			}
			return nil
		}
		switch {
		case len(calls) == 1 && calls[0].Target == vault && hasSelector(calls[0], vaultV2ABI.Methods["adaptersLength"].ID):
			if err := requireHeadPinned(); err != nil {
				return nil, err
			}
			return []outbound.Result{{Success: true, ReturnData: pack(vaultV2ABI.Methods["adaptersLength"].Outputs, big.NewInt(1))}}, nil
		case len(calls) == 1 && calls[0].Target == vault:
			if err := requireHeadPinned(); err != nil {
				return nil, err
			}
			return []outbound.Result{{Success: true, ReturnData: pack(vaultV2ABI.Methods["adapters"].Outputs, adapter)}}, nil
		case len(calls) == 1 && calls[0].Target == adapter:
			return []outbound.Result{{Success: true, ReturnData: pack(adapterABI.Methods["realAssets"].Outputs, realAssets)}}, nil
		}
		return nil, errors.New("unexpected hash-pinned multicall")
	}
	// The adapter type probe is number-pinned: morpho() succeeds, morphoVaultV1()
	// reverts ⇒ MarketV1.
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != 2 || calls[0].Target != adapter {
			return nil, errors.New("unexpected number-pinned multicall")
		}
		return []outbound.Result{
			{Success: true, ReturnData: pack(adapterABI.Methods["morpho"].Outputs, common.HexToAddress("0x1"))},
			{Success: false},
		}, nil
	}
}

// addAdapterLog builds the historical AddAdapter log the sweep is expected to
// find, derived from the registered ABI so it stays in sync with the real event.
func (h *bootstrapHarness) addAdapterLog(blockNumber uint64, blockHash common.Hash, index uint) ethtypes.Log {
	h.t.Helper()
	eventsABI, err := abis.GetVaultV2EventsABI()
	if err != nil {
		h.t.Fatalf("GetVaultV2EventsABI: %v", err)
	}
	event := eventsABI.Events["AddAdapter"]
	return ethtypes.Log{
		Address:     testVaultAddr,
		Topics:      []common.Hash{event.ID, common.BytesToHash(common.LeftPadBytes(testAdapterAddr.Bytes(), 32))},
		BlockNumber: blockNumber,
		BlockHash:   blockHash,
		TxHash:      testTxHash,
		Index:       index,
	}
}

// adaptersAt returns the adapter rows registered with the given added_at_block.
func (h *bootstrapHarness) adaptersAt(block int64) []*entity.MorphoAdapter {
	var out []*entity.MorphoAdapter
	for _, a := range h.adapters {
		if a.AddedAtBlock == block {
			out = append(out, a)
		}
	}
	return out
}

func hasSelector(call outbound.Call, selector []byte) bool {
	return len(call.CallData) >= 4 && bytes.Equal(call.CallData[:4], selector)
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// --- fakes -------------------------------------------------------------------

// fakeChainReader answers the three node reads the bootstrap issues and records
// what it was asked for, so tests can assert on the sweep's bounds and pinning.
type fakeChainReader struct {
	headers              map[common.Hash]*ethtypes.Header
	finalized            *ethtypes.Header
	logs                 []ethtypes.Log
	queries              []ethereum.FilterQuery
	requestedHeadNumbers []*big.Int
	filterErr            error
	// maxQueryWidth, when non-zero, rejects any wider request with a provider
	// result-cap error, exercising the sweep's range narrowing.
	maxQueryWidth int64
	cappedQueries int
}

func newFakeChainReader() *fakeChainReader {
	return &fakeChainReader{headers: make(map[common.Hash]*ethtypes.Header)}
}

// setFinalizedHead registers the block HeaderByNumber(finalized) resolves to and
// returns it, so callers can pin their multicall expectations to its real hash.
func (f *fakeChainReader) setFinalizedHead(number int64, unixTime uint64) *ethtypes.Header {
	header := &ethtypes.Header{Number: big.NewInt(number), Time: unixTime}
	f.headers[header.Hash()] = header
	f.finalized = header
	return header
}

// addBlock registers a header a replayed log can be dated from, returning its hash.
func (f *fakeChainReader) addBlock(number uint64, unixTime uint64) common.Hash {
	header := &ethtypes.Header{Number: new(big.Int).SetUint64(number), Time: unixTime}
	hash := header.Hash()
	f.headers[hash] = header
	return hash
}

func (f *fakeChainReader) HeaderByNumber(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
	f.requestedHeadNumbers = append(f.requestedHeadNumbers, number)
	if f.finalized == nil {
		return nil, errors.New("no finalized head configured")
	}
	return f.finalized, nil
}

func (f *fakeChainReader) HeaderByHash(_ context.Context, hash common.Hash) (*ethtypes.Header, error) {
	header, ok := f.headers[hash]
	if !ok {
		return nil, errors.New("unknown block hash " + hash.Hex())
	}
	return header, nil
}

func (f *fakeChainReader) FilterLogs(_ context.Context, q ethereum.FilterQuery) ([]ethtypes.Log, error) {
	if f.filterErr != nil {
		return nil, f.filterErr
	}
	if f.maxQueryWidth > 0 && q.ToBlock.Int64()-q.FromBlock.Int64()+1 > f.maxQueryWidth {
		f.cappedQueries++
		return nil, errors.New("query returned more than 10000 results")
	}
	f.queries = append(f.queries, q)

	var out []ethtypes.Log
	for _, l := range f.logs {
		if int64(l.BlockNumber) < q.FromBlock.Int64() || int64(l.BlockNumber) > q.ToBlock.Int64() {
			continue
		}
		out = append(out, l)
	}
	return out, nil
}

// recordingReplayer stands in for the morpho-indexer service in the tests that
// assert on WHAT the sweep feeds it (order, filtering) rather than on what the
// handlers then do with it.
type recordingReplayer struct {
	v2Vaults map[common.Address]struct{}
	seeded   []common.Address
	replayed []replayedLog
}

type replayedLog struct {
	log         shared.Log
	blockNumber int64
}

func (r *recordingReplayer) LoadVaultRegistry(context.Context) error { return nil }

func (r *recordingReplayer) V2VaultAddresses() map[common.Address]struct{} { return r.v2Vaults }

func (r *recordingReplayer) SeedV2VaultAdapters(_ context.Context, vaultAddress common.Address, _ int64, _ common.Hash, _ int, _ time.Time) error {
	r.seeded = append(r.seeded, vaultAddress)
	return nil
}

func (r *recordingReplayer) ReplayMetaMorphoLog(_ context.Context, log shared.Log, blockNumber int64, _ common.Hash, _ int, _ time.Time) error {
	r.replayed = append(r.replayed, replayedLog{log: log, blockNumber: blockNumber})
	return nil
}

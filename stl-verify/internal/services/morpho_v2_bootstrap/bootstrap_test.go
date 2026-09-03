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
	secondVaultAddr = common.HexToAddress("0xdddddddddddddddddddddddddddddddddddddddd")
	thirdVaultAddr  = common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc")
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
			if _, err := NewService(cfg, &fakeChainReader{}, &recordingReplayer{}, &fakeProgressStore{}); err == nil {
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
	// which recorded the transition at its true block and wrote the audit row.
	replayed := h.observationsAt(int64(addAdapterBlock))
	if len(replayed) != 1 {
		t.Fatalf("observations recorded at the AddAdapter block = %d, want 1 (the replayed event)", len(replayed))
	}
	if replayed[0].Membership.ObservedVia != entity.MembershipFromAddAdapter {
		t.Errorf("replayed observedVia = %q, want add_adapter_event", replayed[0].Membership.ObservedVia)
	}
	if !bytes.Equal(replayed[0].Identity.Address, testAdapterAddr.Bytes()) {
		t.Errorf("replayed adapter = %x, want %s", replayed[0].Identity.Address, testAdapterAddr.Hex())
	}
	if got := replayed[0].Membership.AdapterType; got == nil || *got != entity.MorphoAdapterTypeMarketV1 {
		t.Errorf("replayed adapter type = %v, want MarketV1", got)
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
	//
	// It records NO membership observation, and that is the point. The seed is an
	// ASSERTION about what the adapter set contains at the head, and the replay in
	// pass 1 already established the same answer there — so there is nothing to
	// add. Under the old registry every run wrote a fresh "added at the finalized
	// head" row and hung the seed snapshot off it, which is bootstrap issue #1.
	if seeded := h.observationsAt(headBlock); len(seeded) != 0 {
		t.Errorf("observations recorded at the head block = %d, want 0: the replay already answered there — got %+v", len(seeded), seeded)
	}
	if len(h.adapters) != 1 {
		t.Errorf("total observations = %d, want 1 (the replayed AddAdapter alone)", len(h.adapters))
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
	if !strings.Contains(err.Error(), "wrong chain or database") {
		t.Errorf("error %q should name the misconfiguration an empty, undeferred set implies", err)
	}
	if len(h.chain.queries) != 0 {
		t.Errorf("issued %d eth_getLogs requests for a chain with no V2 vaults, want 0", len(h.chain.queries))
	}
}

// TestRun_EveryVaultDeferredTellsTheOperatorToReRun: an empty scope whose cause
// is deferral is a lagging finalized head, not a misconfiguration. Reporting the
// wrong chain or database sends the operator to check a CHAIN_ID and a
// DATABASE_URL that are both correct.
func TestRun_EveryVaultDeferredTellsTheOperatorToReRun(t *testing.T) {
	h := newBootstrapHarness(t)
	const headBlock = int64(24_000_000)
	replayer := &recordingReplayer{v2Vaults: map[common.Address]int64{
		testVaultAddr:   headBlock + 1,
		secondVaultAddr: headBlock + 500,
	}}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	h.chain.setFinalizedHead(headBlock, 1_770_000_000)

	err = service.Run(context.Background())

	if err == nil {
		t.Fatal("expected a run with nothing in scope to fail rather than report success")
	}
	if strings.Contains(err.Error(), "wrong chain or database") {
		t.Errorf("error %q blames the configuration for what is a head that has not finalized far enough", err)
	}
	if !strings.Contains(err.Error(), "re-run once finality passes them") {
		t.Errorf("error %q should tell the operator the next run heals this", err)
	}
	if !strings.Contains(err.Error(), "2") {
		t.Errorf("error %q should count the deferred vaults", err)
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

// TestRun_ReplayFailureStopsBeforeSeed pins that a failed replay ends the run. The
// pass ORDER is no longer load-bearing (seedAdapterState says why), but stopping is:
// the seed is the pass that succeeds trivially, so running it after a failed replay
// would let a run whose history is half-swept end in a green Temporal execution with
// only the head state on record. The whole point of this job is that its outcome is
// what an operator reads.
func TestRun_ReplayFailureStopsBeforeSeed(t *testing.T) {
	h := newBootstrapHarness(t)
	replayer := &recordingReplayer{v2Vaults: map[common.Address]int64{testVaultAddr: 0}}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
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

// TestRun_DefersAVaultFirstSeenAboveThePinnedHead: morpho_vault.created_at_block
// is where discovery FIRST SAW the vault, so a vault whose value sits above the
// run's pinned head cannot be shown to have existed there. Probing it anyway asks
// a contract-less address for its adapters, and recording that as a failure would
// make an ordinary "the vault is newer than the head" a red run forever.
func TestRun_DefersAVaultFirstSeenAboveThePinnedHead(t *testing.T) {
	h := newBootstrapHarness(t)
	const headBlock = int64(24_000_000)
	logger, logs := capturingLogger()
	h.config.Logger = logger

	replayer := &recordingReplayer{v2Vaults: map[common.Address]int64{
		testVaultAddr:   23_400_000,
		secondVaultAddr: headBlock + 1,
	}}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	h.chain.setFinalizedHead(headBlock, 1_770_000_000)

	if err := service.Run(context.Background()); err != nil {
		t.Fatalf("a vault newer than the head must be deferred, not fail the run: %v", err)
	}

	if want := []common.Address{testVaultAddr}; !slices.Equal(replayer.seeded, want) {
		t.Errorf("seeded %v, want only the vault that existed at the head %v", replayer.seeded, want)
	}
	written := logs()
	if !strings.Contains(written, secondVaultAddr.Hex()) {
		t.Errorf("the run never named the deferred vault %s in its logs:\n%s", secondVaultAddr.Hex(), written)
	}
	if !strings.Contains(written, "deferredVaults=1") {
		t.Errorf("the completion log carries no deferred count:\n%s", written)
	}
}

// TestRun_ALaterHeadPicksUpAPreviouslyDeferredVault: deferral is not exclusion.
// Live indexing has owned the vault since its first event, and the next run pins
// a later finalized head that includes it — which is what makes skipping safe
// rather than a permanent hole.
func TestRun_ALaterHeadPicksUpAPreviouslyDeferredVault(t *testing.T) {
	h := newBootstrapHarness(t)
	const firstHead = int64(24_000_000)

	replayer := &recordingReplayer{v2Vaults: map[common.Address]int64{
		testVaultAddr:   23_400_000,
		secondVaultAddr: firstHead + 1,
	}}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	h.chain.setFinalizedHead(firstHead, 1_770_000_000)
	if err := service.Run(context.Background()); err != nil {
		t.Fatalf("first run: %v", err)
	}
	h.chain.setFinalizedHead(firstHead+1_000, 1_770_100_000)
	if err := service.Run(context.Background()); err != nil {
		t.Fatalf("second run: %v", err)
	}

	if !slices.Contains(replayer.seeded, secondVaultAddr) {
		t.Errorf("seeded %v, want the deferred vault %s once the head reached it", replayer.seeded, secondVaultAddr.Hex())
	}
}

// TestRun_AHeadThatAdmitsADeferredVaultReSweepsTheWholeRange: the resume record
// is scoped to the vault set it was fetched for, and the filter is part of that
// set. A later head admitting a vault must therefore change the digest and force
// the full sweep — the recorded chunks were read through an address filter that
// never mentioned it.
func TestRun_AHeadThatAdmitsADeferredVaultReSweepsTheWholeRange(t *testing.T) {
	h := newBootstrapHarness(t)
	const firstHead = int64(24_000_000)

	replayer := &recordingReplayer{v2Vaults: map[common.Address]int64{
		testVaultAddr:   23_400_000,
		secondVaultAddr: firstHead + 1,
	}}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	h.chain.setFinalizedHead(firstHead, 1_770_000_000)
	if err := service.Run(context.Background()); err != nil {
		t.Fatalf("first run: %v", err)
	}
	firstDigest := h.progress.saved[len(h.progress.saved)-1].VaultsDigest
	queriesBefore := len(h.chain.queries)

	h.chain.setFinalizedHead(firstHead+1_000, 1_770_100_000)
	if err := service.Run(context.Background()); err != nil {
		t.Fatalf("second run: %v", err)
	}

	if got := h.progress.saved[len(h.progress.saved)-1].VaultsDigest; got == firstDigest {
		t.Errorf("the digest is unchanged at %s although the later head admitted a vault", got)
	}
	if from := h.chain.queries[queriesBefore].FromBlock.Int64(); from != mainnetVaultV2DeployBlock {
		t.Errorf("the second run resumed at block %d, want a full re-sweep from the factory deploy block %d", from, mainnetVaultV2DeployBlock)
	}
}

// TestRun_SeedHealsEveryVaultPastAFailingOneAndStillFailsTheRun: a vault-shaped
// contract that cannot be probed fails identically forever, so aborting the seed
// at the first one would leave every vault after it unhealed on every future run
// too — one poison pill blocking the repair job permanently. Heal what can be
// healed, then fail loudly with what could not be.
func TestRun_SeedHealsEveryVaultPastAFailingOneAndStillFailsTheRun(t *testing.T) {
	h := newBootstrapHarness(t)
	replayer := &recordingReplayer{
		v2Vaults: map[common.Address]int64{testVaultAddr: 0, secondVaultAddr: 0, thirdVaultAddr: 0},
		// Sorted by address, secondVaultAddr sits between thirdVaultAddr and
		// testVaultAddr, so a vault is seeded both before and after the failure.
		seedErr: func(vault common.Address) error {
			if vault == secondVaultAddr {
				return errors.New("execution reverted")
			}
			return nil
		},
	}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	h.chain.setFinalizedHead(24_000_000, 1_770_000_000)

	err = service.Run(context.Background())

	if err == nil {
		t.Fatal("expected the un-seedable vault to fail the run rather than pass unmentioned")
	}
	if !strings.Contains(err.Error(), secondVaultAddr.Hex()) {
		t.Errorf("error %q should name the vault that could not be seeded", err)
	}
	if !strings.Contains(err.Error(), "1 of 3") {
		t.Errorf("error %q should count the failures against the vault set", err)
	}
	want := []common.Address{thirdVaultAddr, testVaultAddr}
	if !slices.Equal(replayer.seeded, want) {
		t.Errorf("seeded %v, want every healable vault %v — including the one after the failure", replayer.seeded, want)
	}
}

// TestRun_SeedStopsAtOnceWhenTheRunIsCancelled: continuing is for a vault whose
// own probe is broken. A cancelled run (a pod kill, an expiring activity) fails
// every remaining vault identically, and collecting those would bury the cause
// under one error per vault.
func TestRun_SeedStopsAtOnceWhenTheRunIsCancelled(t *testing.T) {
	h := newBootstrapHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	attempts := 0
	replayer := &recordingReplayer{
		v2Vaults: map[common.Address]int64{testVaultAddr: 0, secondVaultAddr: 0, thirdVaultAddr: 0},
		seedErr: func(common.Address) error {
			attempts++
			cancel()
			return context.Canceled
		},
	}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	h.chain.setFinalizedHead(24_000_000, 1_770_000_000)

	if err := service.Run(ctx); err == nil {
		t.Fatal("expected a cancelled run to fail")
	}
	if attempts != 1 {
		t.Errorf("seed attempted %d vaults after cancellation, want 1", attempts)
	}
}

// TestRun_SeedCancellationStillReportsTheFailuresAlreadyCollected: the vaults
// that failed on their own before the cancellation are the run's only durable
// record of them — Temporal shows the operator the returned error, not the log
// lines. Dropping them there hides a hole behind a cancellation.
func TestRun_SeedCancellationStillReportsTheFailuresAlreadyCollected(t *testing.T) {
	h := newBootstrapHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Sorted by address, thirdVaultAddr is seeded first and secondVaultAddr next.
	replayer := &recordingReplayer{
		v2Vaults: map[common.Address]int64{testVaultAddr: 0, secondVaultAddr: 0, thirdVaultAddr: 0},
		seedErr: func(vault common.Address) error {
			switch vault {
			case thirdVaultAddr:
				return errors.New("execution reverted")
			case secondVaultAddr:
				cancel()
				return context.Canceled
			}
			return nil
		},
	}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	h.chain.setFinalizedHead(24_000_000, 1_770_000_000)

	err = service.Run(ctx)

	if err == nil {
		t.Fatal("expected a cancelled run to fail")
	}
	if !strings.Contains(err.Error(), thirdVaultAddr.Hex()) {
		t.Errorf("error %q dropped the vault that failed before the cancellation", err)
	}
	if !strings.Contains(err.Error(), secondVaultAddr.Hex()) {
		t.Errorf("error %q should still name the vault the cancellation interrupted", err)
	}
}

// TestRun_ReplayedLogsArriveInChainOrder: the per-address-batch requests inside a
// chunk return interleaved, and the service must restore (block, logIndex) order —
// not for correctness (see sortLogs) but so a replay does not manufacture the
// inferred-membership WARN that means a discovery gap.
func TestRun_ReplayedLogsArriveInChainOrder(t *testing.T) {
	h := newBootstrapHarness(t)
	replayer := &recordingReplayer{v2Vaults: map[common.Address]int64{testVaultAddr: 0}}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
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
	replayer := &recordingReplayer{v2Vaults: map[common.Address]int64{testVaultAddr: 0}}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
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
	replayer := &recordingReplayer{v2Vaults: map[common.Address]int64{testVaultAddr: 0}}
	h.config.BlockChunkSize = 1_000_000
	service, err := NewService(h.config, h.chain, replayer, h.progress)
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
	replayer := &recordingReplayer{v2Vaults: map[common.Address]int64{testVaultAddr: 0}}
	service, err := NewService(h.config, h.chain, replayer, h.progress)
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
	progress      *fakeProgressStore
	multicaller   *testutil.MockMulticaller
	morphoRepo    *testutil.MockMorphoRepository
	adapters      []*entity.MorphoAdapterObservation
	adapterStates []*entity.MorphoAdapterState
	auditEvents   []*entity.ProtocolEvent
}

func newBootstrapHarness(t *testing.T) *bootstrapHarness {
	t.Helper()
	h := &bootstrapHarness{t: t, chain: newFakeChainReader(), progress: &fakeProgressStore{}}

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
	// One identity per (vault, address), and an assertion that repeats an answer
	// the log already gives appends nothing — the same contract the real
	// repository implements.
	adapterIDs := map[string]int64{}
	member := map[string]bool{}
	h.morphoRepo.ObserveAdapterMembershipFn = func(_ context.Context, _ pgx.Tx, obs *entity.MorphoAdapterObservation) (int64, bool, error) {
		key := string(obs.Identity.Address)
		id, known := adapterIDs[key]
		if !known {
			id = int64(len(adapterIDs) + 1)
			adapterIDs[key] = id
		}
		if !obs.Membership.ObservedVia.IsTransition() && known && member[key] == obs.Membership.IsMember {
			return id, false, nil
		}
		member[key] = obs.Membership.IsMember
		h.adapters = append(h.adapters, obs)
		return id, true, nil
	}
	h.morphoRepo.SaveAdapterStateFn = func(_ context.Context, _ pgx.Tx, s *entity.MorphoAdapterState) (bool, error) {
		h.adapterStates = append(h.adapterStates, s)
		return true, nil
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

	h.service, err = NewService(h.config, h.chain, replay, h.progress)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	return h
}

// wireEmptyAdapterSets answers the seed's enumeration with "no adapters" for
// every vault, for tests that care about the sweep rather than the seed.
func (h *bootstrapHarness) wireEmptyAdapterSets(headHash common.Hash) {
	h.t.Helper()
	vaultV2ABI, err := abis.GetVaultV2ReadABI()
	if err != nil {
		h.t.Fatalf("GetVaultV2ReadABI: %v", err)
	}
	h.multicaller.ExecuteAtHashFn = func(_ context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
		if blockHash != headHash {
			return nil, errors.New("state read is not pinned to the run's finalized head")
		}
		length, err := vaultV2ABI.Methods["adaptersLength"].Outputs.Pack(big.NewInt(0))
		if err != nil {
			return nil, err
		}
		results := make([]outbound.Result, len(calls))
		for i := range results {
			results[i] = outbound.Result{Success: true, ReturnData: length}
		}
		return results, nil
	}
}

// addV2Vault extends the registry with another VaultV2, as live indexing would
// while a bootstrap run is in flight.
func (h *bootstrapHarness) addV2Vault(address common.Address) {
	h.t.Helper()
	previous := h.morphoRepo.GetAllVaultsFn
	h.morphoRepo.GetAllVaultsFn = func(ctx context.Context, chainID int64) (map[common.Address]*entity.MorphoVault, error) {
		vaults, err := previous(ctx, chainID)
		if err != nil {
			return nil, err
		}
		vaults[address] = &entity.MorphoVault{
			ID: 8, ChainID: 1, ProtocolID: 1, Address: address.Bytes(),
			Name: "Second Vault", Symbol: "sVAULT", AssetTokenID: 1,
			VaultVersion: entity.MorphoVaultV2, CreatedAtBlock: 23_400_000,
		}
		return vaults, nil
	}
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
	// The adapter type probe is number-pinned: morpho() succeeds, every other
	// marker reverts ⇒ MarketV1.
	prober, err := morpho_indexer.NewAdapterProber()
	if err != nil {
		t.Fatalf("NewAdapterProber: %v", err)
	}
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		if len(calls) != prober.NumProbeCalls() || calls[0].Target != adapter {
			return nil, errors.New("unexpected number-pinned multicall")
		}
		results := make([]outbound.Result, len(calls))
		results[0] = outbound.Result{Success: true, ReturnData: pack(adapterABI.Methods["morpho"].Outputs, common.HexToAddress("0x1"))}
		return results, nil
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

// observationsAt returns the membership observations recorded at the given block.
func (h *bootstrapHarness) observationsAt(block int64) []*entity.MorphoAdapterObservation {
	var out []*entity.MorphoAdapterObservation
	for _, obs := range h.adapters {
		if obs.Membership.BlockNumber == block {
			out = append(out, obs)
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

// capturingLogger returns a logger and a reader over everything written to it,
// for the assertions that are about what a run TELLS an operator.
func capturingLogger() (*slog.Logger, func() string) {
	var out bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return logger, out.String
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
	// failFilterAfter, when non-zero, fails every request past the first N,
	// standing in for a run that dies part-way through its sweep.
	failFilterAfter int
	servedQueries   int
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
	f.servedQueries++
	if f.failFilterAfter > 0 && f.servedQueries > f.failFilterAfter {
		return nil, errors.New("connection reset by peer")
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
	v2Vaults map[common.Address]int64
	// seedErr, when set, decides each vault's seed outcome; only the vaults it
	// lets through land in seeded.
	seedErr  func(common.Address) error
	seeded   []common.Address
	replayed []replayedLog
}

type replayedLog struct {
	log         shared.Log
	blockNumber int64
}

func (r *recordingReplayer) LoadVaultRegistry(context.Context) error { return nil }

func (r *recordingReplayer) V2VaultsFirstSeen() map[common.Address]int64 { return r.v2Vaults }

func (r *recordingReplayer) SeedV2VaultAdapters(_ context.Context, vaultAddress common.Address, _ int64, _ common.Hash, _ int, _ time.Time) error {
	if r.seedErr != nil {
		if err := r.seedErr(vaultAddress); err != nil {
			return err
		}
	}
	r.seeded = append(r.seeded, vaultAddress)
	return nil
}

func (r *recordingReplayer) ReplayMetaMorphoLog(_ context.Context, log shared.Log, blockNumber int64, _ common.Hash, _ int, _ time.Time) error {
	r.replayed = append(r.replayed, replayedLog{log: log, blockNumber: blockNumber})
	return nil
}

// fakeProgressStore stands in for the Temporal heartbeat-details store: it keeps
// the last record saved, so two Run calls against one harness model two attempts
// of the same activity.
type fakeProgressStore struct {
	record  *SweepProgress
	saved   []SweepProgress
	loadErr error
}

func (f *fakeProgressStore) SaveProgress(_ context.Context, progress SweepProgress) error {
	stored := progress
	f.record = &stored
	f.saved = append(f.saved, progress)
	return nil
}

func (f *fakeProgressStore) LoadProgress(context.Context) (SweepProgress, bool, error) {
	if f.loadErr != nil {
		return SweepProgress{}, false, f.loadErr
	}
	if f.record == nil {
		return SweepProgress{}, false, nil
	}
	return *f.record, true, nil
}

// savedTo lists the sweep positions recorded, in the order they were recorded.
func (f *fakeProgressStore) savedTo() []int64 {
	out := make([]int64, 0, len(f.saved))
	for _, s := range f.saved {
		out = append(out, s.LastCompletedTo)
	}
	return out
}

// TestRun_RecordsProgressOnlyAtChunkBoundaries: a chunk is recorded once every log
// in it has replayed, so a resumed run can never restart mid-chunk. Recording a
// block partway through would claim coverage the run does not have, and the resume
// would skip the rest of that chunk for good.
func TestRun_RecordsProgressOnlyAtChunkBoundaries(t *testing.T) {
	h := newBootstrapHarness(t)
	const headBlock = mainnetVaultV2DeployBlock + 25_000
	head := h.chain.setFinalizedHead(headBlock, 1_770_000_000)
	h.wireAdapterReads(head.Hash(), big.NewInt(1))

	if err := h.service.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	want := []int64{
		mainnetVaultV2DeployBlock + 9_999,
		mainnetVaultV2DeployBlock + 19_999,
		headBlock,
	}
	if got := h.progress.savedTo(); !slices.Equal(got, want) {
		t.Fatalf("recorded sweep positions %v, want the chunk boundaries %v", got, want)
	}
}

// TestRun_ResumesAfterTheLastCompletedChunk: an attempt that dies mid-sweep
// leaves a record behind, and the next attempt starts at the block after the
// last chunk that completed rather than redoing hours of eth_getLogs.
func TestRun_ResumesAfterTheLastCompletedChunk(t *testing.T) {
	h := newBootstrapHarness(t)
	const headBlock = mainnetVaultV2DeployBlock + 25_000
	head := h.chain.setFinalizedHead(headBlock, 1_770_000_000)
	h.wireAdapterReads(head.Hash(), big.NewInt(1))

	h.chain.failFilterAfter = 1
	if err := h.service.Run(context.Background()); err == nil {
		t.Fatal("expected the interrupted attempt to fail")
	}

	h.chain.failFilterAfter, h.chain.queries = 0, nil
	if err := h.service.Run(context.Background()); err != nil {
		t.Fatalf("resumed Run: %v", err)
	}

	if len(h.chain.queries) == 0 {
		t.Fatal("the resumed run issued no eth_getLogs request")
	}
	if got, want := h.chain.queries[0].FromBlock.Int64(), int64(mainnetVaultV2DeployBlock+10_000); got != want {
		t.Errorf("resumed sweep starts at block %d, want %d (the block after the completed chunk)", got, want)
	}
}

// TestRun_IgnoresProgressRecordedForAnotherVaultSet: the recorded chunks were
// fetched with an address filter that never mentioned a vault discovered since,
// so trusting them would silently lose that vault's governance history. A
// changed vault set must replay the whole range again.
func TestRun_IgnoresProgressRecordedForAnotherVaultSet(t *testing.T) {
	h := newBootstrapHarness(t)
	const headBlock = mainnetVaultV2DeployBlock + 25_000
	head := h.chain.setFinalizedHead(headBlock, 1_770_000_000)
	h.wireEmptyAdapterSets(head.Hash())

	h.chain.failFilterAfter = 1
	if err := h.service.Run(context.Background()); err == nil {
		t.Fatal("expected the interrupted attempt to fail")
	}

	h.addV2Vault(secondVaultAddr)
	h.chain.failFilterAfter, h.chain.queries = 0, nil
	if err := h.service.Run(context.Background()); err != nil {
		t.Fatalf("Run after the vault set grew: %v", err)
	}

	if len(h.chain.queries) == 0 {
		t.Fatal("the run issued no eth_getLogs request")
	}
	if got := h.chain.queries[0].FromBlock.Int64(); got != mainnetVaultV2DeployBlock {
		t.Errorf("sweep starts at block %d, want the factory deploy block %d — a grown vault set cannot reuse the earlier scope",
			got, int64(mainnetVaultV2DeployBlock))
	}
}

// TestRun_ProgressLoadFailureFailsTheRun: an unreadable record is not "start
// from the beginning". It means the resume decision cannot be made, and guessing
// either redoes hours of work or skips blocks that were never swept.
func TestRun_ProgressLoadFailureFailsTheRun(t *testing.T) {
	h := newBootstrapHarness(t)
	head := h.chain.setFinalizedHead(mainnetVaultV2DeployBlock+1_000, 1_770_000_000)
	h.wireAdapterReads(head.Hash(), big.NewInt(1))
	h.progress.loadErr = errors.New("details decode failed")

	if err := h.service.Run(context.Background()); err == nil {
		t.Fatal("expected an unreadable progress record to fail the run")
	}
}

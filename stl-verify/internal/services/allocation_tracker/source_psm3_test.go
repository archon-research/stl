package allocation_tracker

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ---------------------------------------------------------------------------
// Fake Psm3PositionReader.
// ---------------------------------------------------------------------------

type fakePsm3Reader struct {
	shares map[string]outbound.Psm3AlmShareSnapshot
	err    error
}

func newFakePsm3Reader() *fakePsm3Reader {
	return &fakePsm3Reader{shares: make(map[string]outbound.Psm3AlmShareSnapshot)}
}

func psm3ShareKey(chainID int64, pool, alm common.Address) string {
	return fmt.Sprintf("%d|%s|%s", chainID, pool.Hex(), alm.Hex())
}

func (f *fakePsm3Reader) setShare(chainID int64, pool, alm common.Address, shares, assetValue *big.Int) {
	f.shares[psm3ShareKey(chainID, pool, alm)] = outbound.Psm3AlmShareSnapshot{Shares: shares, AssetValue: assetValue}
}

func (f *fakePsm3Reader) AlmShareAtBlock(
	ctx context.Context, chainID int64, psm3Address, almAddress common.Address, blockNumber int64, blockTimestamp time.Time,
) (*outbound.Psm3AlmShareSnapshot, error) {
	if f.err != nil {
		return nil, f.err
	}
	s, ok := f.shares[psm3ShareKey(chainID, psm3Address, almAddress)]
	if !ok {
		return nil, nil
	}
	return &s, nil
}

// ---------------------------------------------------------------------------
// Fixtures.
// ---------------------------------------------------------------------------

var (
	psm3ChainID   = int64(42161) // arbitrum
	psm3PoolAddr  = common.HexToAddress("0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266")
	psm3AlmAddr   = common.HexToAddress("0x92afd6F2385a90e44da3a8B60fe36f6cBe1D8709")
	psm3AssetAddr = common.HexToAddress("0xaf88d065e77c8cC2239327C5EDb3A432268e5831") // USDC
	psm3BlockHash = common.HexToHash("0x5555555555555555555555555555555555555555555555555555555555555a")
	psm3BlockNum  = int64(300_000_000)
)

func psm3Entry() *TokenEntry {
	asset := psm3AssetAddr
	return &TokenEntry{
		ContractAddress: psm3PoolAddr,
		WalletAddress:   psm3AlmAddr,
		AssetAddress:    &asset,
		Star:            "spark",
		Chain:           "arbitrum",
		Protocol:        "psm3",
		TokenType:       "psm3",
	}
}

func newPsm3BlockState(t *testing.T, hash common.Hash, number int64) outbound.BlockHashResolver {
	t.Helper()
	repo := memory.NewBlockStateRepository()
	if _, err := repo.SaveBlock(context.Background(), outbound.BlockState{
		Number:         number,
		Hash:           hash.Hex(),
		BlockTimestamp: 1_700_000_000,
	}); err != nil {
		t.Fatalf("seed block state: %v", err)
	}
	return repo
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestPSM3Source_Name(t *testing.T) {
	src := NewPSM3Source(nil, nil, quietLogger())
	if got := src.Name(); got != "psm3" {
		t.Errorf("Name() = %q, want %q", got, "psm3")
	}
}

func TestPSM3Source_Supports(t *testing.T) {
	src := NewPSM3Source(nil, nil, quietLogger())
	if !src.Supports("psm3", "psm3") {
		t.Error("expected psm3/psm3 to be supported")
	}
	if src.Supports("erc20", "psm3") {
		t.Error("erc20 must not be supported")
	}
}

func TestPSM3Source_FetchBalances_EmptyEntries(t *testing.T) {
	src := NewPSM3Source(nil, nil, quietLogger())
	result, err := src.FetchBalances(context.Background(), nil, psm3BlockHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Balances) != 0 {
		t.Errorf("expected empty balances, got %d", len(result.Balances))
	}
}

func TestPSM3Source_FetchBalances_UnresolvedBlockHash_Error(t *testing.T) {
	src := NewPSM3Source(newFakePsm3Reader(), memory.NewBlockStateRepository(), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{psm3Entry()}, psm3BlockHash)
	if err == nil {
		t.Fatal("expected error when the block hash is not in block_states")
	}
}

func TestPSM3Source_FetchBalances_UnknownChain_Error(t *testing.T) {
	entry := psm3Entry()
	entry.Chain = "not-a-real-chain"
	src := NewPSM3Source(newFakePsm3Reader(), newPsm3BlockState(t, psm3BlockHash, psm3BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{entry}, psm3BlockHash)
	if err == nil {
		t.Fatal("expected error for unrecognised chain")
	}
}

func TestPSM3Source_FetchBalances_ValuesRawSharesAndParValue(t *testing.T) {
	reader := newFakePsm3Reader()
	shares := big.NewInt(999_000_000_000_000_000)       // 0.999e18 shares
	assetValue := big.NewInt(1_249_994_141_260_000_000) // ~1.25e18 par
	reader.setShare(psm3ChainID, psm3PoolAddr, psm3AlmAddr, shares, assetValue)

	src := NewPSM3Source(reader, newPsm3BlockState(t, psm3BlockHash, psm3BlockNum), quietLogger())
	result, err := src.FetchBalances(context.Background(), []*TokenEntry{psm3Entry()}, psm3BlockHash)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
	bal, ok := result.Balances[psm3Entry().Key()]
	if !ok {
		t.Fatal("no balance recorded for the psm3 entry")
	}
	if bal.Balance.Cmp(shares) != 0 {
		t.Errorf("Balance = %s, want raw shares %s", bal.Balance, shares)
	}
	if bal.ScaledBalance.Cmp(shares) != 0 {
		t.Errorf("ScaledBalance = %s, want raw shares %s", bal.ScaledBalance, shares)
	}
	if bal.UnderlyingValue.Cmp(assetValue) != 0 {
		t.Errorf("UnderlyingValue = %s, want raw par value %s (rescaling happens in the handler, not the source)", bal.UnderlyingValue, assetValue)
	}
}

func TestPSM3Source_FetchBalances_MissingIndexedShare_Error(t *testing.T) {
	src := NewPSM3Source(newFakePsm3Reader(), newPsm3BlockState(t, psm3BlockHash, psm3BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{psm3Entry()}, psm3BlockHash)
	if err == nil {
		t.Fatal("expected error for a tracked ALM with no indexed share reading")
	}
}

func TestPSM3Source_FetchBalances_ReaderError(t *testing.T) {
	reader := newFakePsm3Reader()
	reader.err = errors.New("db unavailable")
	src := NewPSM3Source(reader, newPsm3BlockState(t, psm3BlockHash, psm3BlockNum), quietLogger())
	_, err := src.FetchBalances(context.Background(), []*TokenEntry{psm3Entry()}, psm3BlockHash)
	if err == nil {
		t.Fatal("expected the reader error to propagate")
	}
}

// TestPSM3Source_FetchBalances_MultipleEntries proves every entry in the batch
// is valued independently (no discovery/registry step, unlike UniV4Source).
func TestPSM3Source_FetchBalances_MultipleEntries(t *testing.T) {
	reader := newFakePsm3Reader()
	entryA := psm3Entry()
	entryB := psm3Entry()
	entryB.Chain = "base"
	poolB := common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E")
	almB := common.HexToAddress("0x2917956eFF0B5eaF030abDB4EF4296DF775009cA")
	entryB.ContractAddress = poolB
	entryB.WalletAddress = almB

	reader.setShare(psm3ChainID, psm3PoolAddr, psm3AlmAddr, big.NewInt(100), big.NewInt(101))
	reader.setShare(8453, poolB, almB, big.NewInt(200), big.NewInt(202))

	src := NewPSM3Source(reader, newPsm3BlockState(t, psm3BlockHash, psm3BlockNum), quietLogger())
	result, err := src.FetchBalances(context.Background(), []*TokenEntry{entryA, entryB}, psm3BlockHash)
	if err != nil {
		t.Fatalf("FetchBalances: %v", err)
	}
	if len(result.Balances) != 2 {
		t.Fatalf("expected 2 balances, got %d", len(result.Balances))
	}
	if result.Balances[entryA.Key()].Balance.Cmp(big.NewInt(100)) != 0 {
		t.Errorf("entryA balance = %s, want 100", result.Balances[entryA.Key()].Balance)
	}
	if result.Balances[entryB.Key()].Balance.Cmp(big.NewInt(200)) != 0 {
		t.Errorf("entryB balance = %s, want 200", result.Balances[entryB.Key()].Balance)
	}
}

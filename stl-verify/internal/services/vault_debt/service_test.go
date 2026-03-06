package vault_debt_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/vault_debt"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeBlockQuerier returns a fixed block number.
type fakeBlockQuerier struct {
	mu       sync.Mutex
	blockNum uint64
	err      error
}

func newFakeBlockQuerier(blockNum uint64) *fakeBlockQuerier {
	return &fakeBlockQuerier{blockNum: blockNum}
}

func (f *fakeBlockQuerier) BlockNumber(_ context.Context) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blockNum, f.err
}

// fakeSQSConsumer is a controllable in-memory SQS consumer for unit tests.
type fakeSQSConsumer struct {
	mu       sync.Mutex
	messages []outbound.SQSMessage
	served   int
}

func newFakeSQSConsumer(events []outbound.BlockEvent) *fakeSQSConsumer {
	msgs := make([]outbound.SQSMessage, len(events))
	for i, e := range events {
		body, _ := json.Marshal(e)
		msgs[i] = outbound.SQSMessage{
			MessageID:     fmt.Sprintf("msg-%d", i),
			ReceiptHandle: fmt.Sprintf("handle-%d", i),
			Body:          string(body),
		}
	}
	return &fakeSQSConsumer{messages: msgs}
}

func (f *fakeSQSConsumer) ReceiveMessages(_ context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.served >= len(f.messages) {
		return nil, nil
	}

	end := min(f.served+maxMessages, len(f.messages))

	batch := f.messages[f.served:end]
	f.served = end
	return batch, nil
}

func (f *fakeSQSConsumer) DeleteMessage(_ context.Context, _ string) error {
	return nil
}

func (f *fakeSQSConsumer) Close() error {
	return nil
}

// fakeVatCaller is a controllable in-memory VatCaller for unit tests.
type fakeVatCaller struct {
	mu sync.Mutex

	// ilkByVault maps vault address → ilk bytes32 for ResolveIlks.
	ilkByVault map[common.Address][32]byte
	resolveErr error // if set, ResolveIlks returns this error

	// rate / art are returned for all vaults in ReadDebts.
	rate *big.Int
	art  *big.Int

	// Per-vault error injection for ReadDebts.
	debtErrByVault map[common.Address]error
	readDebtsErr   error // if set, ReadDebts returns this error (whole batch fails)
}

func newFakeVatCaller() *fakeVatCaller {
	rayVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil)
	wadVal := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	art1000 := new(big.Int).Mul(big.NewInt(1000), wadVal)

	return &fakeVatCaller{
		ilkByVault:     make(map[common.Address][32]byte),
		debtErrByVault: make(map[common.Address]error),
		rate:           rayVal,
		art:            art1000,
	}
}

func (f *fakeVatCaller) setIlk(vault common.Address, ilk [32]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ilkByVault[vault] = ilk
}

func (f *fakeVatCaller) setDebtError(vault common.Address, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.debtErrByVault[vault] = err
}

func (f *fakeVatCaller) ResolveIlks(_ context.Context, vaults []common.Address, _ *big.Int) (map[common.Address][32]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.resolveErr != nil {
		return nil, f.resolveErr
	}

	result := make(map[common.Address][32]byte, len(vaults))
	for _, v := range vaults {
		ilk, ok := f.ilkByVault[v]
		if !ok {
			return nil, errors.New("ilk not found for " + v.Hex())
		}
		result[v] = ilk
	}
	return result, nil
}

func (f *fakeVatCaller) ReadDebts(_ context.Context, queries []outbound.DebtQuery, _ *big.Int) ([]outbound.DebtResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.readDebtsErr != nil {
		return nil, f.readDebtsErr
	}

	results := make([]outbound.DebtResult, len(queries))
	for i, q := range queries {
		results[i].VaultAddress = q.VaultAddress

		if err, ok := f.debtErrByVault[q.VaultAddress]; ok {
			results[i].Err = err
			continue
		}

		results[i].Rate = new(big.Int).Set(f.rate)
		results[i].Art = new(big.Int).Set(f.art)
	}
	return results, nil
}

// fakePrimeDebtRepository is a controllable in-memory repository.
type fakePrimeDebtRepository struct {
	mu sync.Mutex

	primes    []entity.Prime
	primesErr error

	saved   [][]*entity.PrimeDebt
	saveErr error
}

func (r *fakePrimeDebtRepository) GetPrimes(_ context.Context) ([]entity.Prime, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.primes, r.primesErr
}

func (r *fakePrimeDebtRepository) SaveDebtSnapshots(_ context.Context, debts []*entity.PrimeDebt) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.saveErr != nil {
		return r.saveErr
	}
	cp := make([]*entity.PrimeDebt, len(debts))
	copy(cp, debts)
	r.saved = append(r.saved, cp)
	return nil
}

func (r *fakePrimeDebtRepository) savedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, batch := range r.saved {
		total += len(batch)
	}
	return total
}

func (r *fakePrimeDebtRepository) allSaved() []*entity.PrimeDebt {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []*entity.PrimeDebt
	for _, batch := range r.saved {
		out = append(out, batch...)
	}
	return out
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func ilkFrom(name string) [32]byte {
	var b [32]byte
	copy(b[:], name)
	return b
}

func sparkPrime() entity.Prime {
	return entity.Prime{
		ID:           1,
		Name:         "spark",
		VaultAddress: common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA"),
	}
}

const testBlockNum = 21000000
const testChainID int64 = 1

func makeBlockEvents(startBlock int64, count int) []outbound.BlockEvent {
	events := make([]outbound.BlockEvent, count)
	for i := range events {
		events[i] = outbound.BlockEvent{
			ChainID:        testChainID,
			BlockNumber:    startBlock + int64(i),
			Version:        0,
			BlockHash:      fmt.Sprintf("0x%064x", startBlock+int64(i)),
			ParentHash:     fmt.Sprintf("0x%064x", startBlock+int64(i)-1),
			BlockTimestamp: time.Now().Unix(),
			ReceivedAt:     time.Now(),
		}
	}
	return events
}

func defaultConfig(sweepEveryN int) vault_debt.Config {
	return vault_debt.Config{
		SweepEveryNBlocks: sweepEveryN,
		ChainID:           testChainID,
		MaxMessages:       10,
		PollInterval:      10 * time.Millisecond,
	}
}

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestNewVaultDebtService_NilCaller(t *testing.T) {
	consumer := newFakeSQSConsumer(nil)
	_, err := vault_debt.NewVaultDebtService(defaultConfig(75), nil, &fakePrimeDebtRepository{}, consumer, newFakeBlockQuerier(testBlockNum))
	if err == nil {
		t.Fatal("expected error for nil caller")
	}
}

func TestNewVaultDebtService_NilRepository(t *testing.T) {
	consumer := newFakeSQSConsumer(nil)
	_, err := vault_debt.NewVaultDebtService(defaultConfig(75), newFakeVatCaller(), nil, consumer, newFakeBlockQuerier(testBlockNum))
	if err == nil {
		t.Fatal("expected error for nil repository")
	}
}

func TestNewVaultDebtService_NilSQSConsumer(t *testing.T) {
	_, err := vault_debt.NewVaultDebtService(defaultConfig(75), newFakeVatCaller(), &fakePrimeDebtRepository{}, nil, newFakeBlockQuerier(testBlockNum))
	if err == nil {
		t.Fatal("expected error for nil sqs consumer")
	}
}

func TestNewVaultDebtService_NilBlockQuerier(t *testing.T) {
	consumer := newFakeSQSConsumer(nil)
	_, err := vault_debt.NewVaultDebtService(defaultConfig(75), newFakeVatCaller(), &fakePrimeDebtRepository{}, consumer, nil)
	if err == nil {
		t.Fatal("expected error for nil block querier")
	}
}

func TestNewVaultDebtService_ZeroChainID(t *testing.T) {
	consumer := newFakeSQSConsumer(nil)
	cfg := defaultConfig(75)
	cfg.ChainID = 0
	_, err := vault_debt.NewVaultDebtService(cfg, newFakeVatCaller(), &fakePrimeDebtRepository{}, consumer, newFakeBlockQuerier(testBlockNum))
	if err == nil {
		t.Fatal("expected error for zero chain ID")
	}
}

func TestNewVaultDebtService_Defaults(t *testing.T) {
	consumer := newFakeSQSConsumer(nil)
	svc, err := vault_debt.NewVaultDebtService(defaultConfig(75), newFakeVatCaller(), &fakePrimeDebtRepository{}, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil service")
	}
}

// ---------------------------------------------------------------------------
// Lifecycle tests
// ---------------------------------------------------------------------------

func TestStart_NoPrimes(t *testing.T) {
	consumer := newFakeSQSConsumer(nil)
	repo := &fakePrimeDebtRepository{}
	svc, err := vault_debt.NewVaultDebtService(defaultConfig(75), newFakeVatCaller(), repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when no primes are registered")
	}
	if !containsAny(err.Error(), "no primes", "primes") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestStart_IlkResolutionError(t *testing.T) {
	caller := newFakeVatCaller()
	caller.resolveErr = errors.New("RPC unavailable")

	consumer := newFakeSQSConsumer(nil)
	repo := &fakePrimeDebtRepository{primes: []entity.Prime{sparkPrime()}}
	svc, err := vault_debt.NewVaultDebtService(defaultConfig(75), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	err = svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when ilk resolution fails")
	}
}

func TestStart_Stop_Clean(t *testing.T) {
	caller := newFakeVatCaller()
	caller.setIlk(
		common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA"),
		ilkFrom("ALLOCATOR-SPARK-A"),
	)

	consumer := newFakeSQSConsumer(nil)
	repo := &fakePrimeDebtRepository{primes: []entity.Prime{sparkPrime()}}
	svc, err := vault_debt.NewVaultDebtService(defaultConfig(75), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Start is non-blocking now
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	// Give the goroutine time to stop
	time.Sleep(50 * time.Millisecond)

	if err := svc.Stop(); err != nil {
		t.Fatalf("Stop returned error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Sync / snapshot tests
// ---------------------------------------------------------------------------

func TestSync_WritesSnapshotPerPrime(t *testing.T) {
	primes := []entity.Prime{
		{ID: 1, Name: "spark", VaultAddress: common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA")},
		{ID: 2, Name: "grove", VaultAddress: common.HexToAddress("0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0")},
	}

	caller := newFakeVatCaller()
	for _, p := range primes {
		caller.setIlk(p.VaultAddress, ilkFrom("ALLOCATOR-"+p.Name+"-A"))
	}

	// Generate enough events to trigger a sweep (sweep-blocks=1)
	events := makeBlockEvents(testBlockNum, 3)
	consumer := newFakeSQSConsumer(events)

	repo := &fakePrimeDebtRepository{primes: primes}
	svc, err := vault_debt.NewVaultDebtService(defaultConfig(1), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		if repo.savedCount() >= len(primes) {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: want %d snapshots, got %d", len(primes), repo.savedCount())
		case <-time.After(20 * time.Millisecond):
		}
	}

	for _, snap := range repo.allSaved() {
		if snap.IlkName == "" {
			t.Errorf("prime_id %d: empty ilk_name", snap.PrimeID)
		}
		if snap.DebtWad == nil || snap.DebtWad.Sign() == 0 {
			t.Errorf("prime_id %d: nil or zero debt_wad", snap.PrimeID)
		}
		if snap.PrimeID == 0 {
			t.Error("expected non-zero prime_id")
		}
	}

	cancel()
	_ = svc.Stop()
}

func TestSync_PartialFailure_OtherPrimesStillSaved(t *testing.T) {
	spark := entity.Prime{ID: 1, Name: "spark", VaultAddress: common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA")}
	grove := entity.Prime{ID: 2, Name: "grove", VaultAddress: common.HexToAddress("0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0")}

	caller := newFakeVatCaller()
	caller.setIlk(spark.VaultAddress, ilkFrom("ALLOCATOR-SPARK-A"))
	caller.setIlk(grove.VaultAddress, ilkFrom("ALLOCATOR-GROVE-A"))
	caller.setDebtError(grove.VaultAddress, errors.New("simulated RPC failure"))

	events := makeBlockEvents(testBlockNum, 3)
	consumer := newFakeSQSConsumer(events)

	repo := &fakePrimeDebtRepository{primes: []entity.Prime{spark, grove}}
	svc, err := vault_debt.NewVaultDebtService(defaultConfig(1), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		snaps := repo.allSaved()
		for _, s := range snaps {
			if s.PrimeID == spark.ID {
				goto found
			}
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for spark snapshot despite grove failure")
		case <-time.After(20 * time.Millisecond):
		}
	}
found:
	cancel()
	_ = svc.Stop()
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if len(s) >= len(sub) {
			for i := 0; i <= len(s)-len(sub); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
		}
	}
	return false
}

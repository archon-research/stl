package vault_debt_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/vault_debt"
)

// ---------------------------------------------------------------------------
// Fakes
// ---------------------------------------------------------------------------

// fakeVatCaller is a controllable in-memory VatCaller for unit tests.
type fakeVatCaller struct {
	mu sync.Mutex

	// ilkByVault returns the ilk for a given vault address, or an error.
	ilkByVault map[common.Address][32]byte
	ilkErr     map[common.Address]error

	// rate / art are returned for all ilks.
	rate    *big.Int
	art     *big.Int
	rateErr error
	artErr  error
}

func newFakeVatCaller() *fakeVatCaller {
	return &fakeVatCaller{
		ilkByVault: make(map[common.Address][32]byte),
		ilkErr:     make(map[common.Address]error),
		rate:       big.NewInt(1e9), // non-zero so debt is non-zero
		art:        big.NewInt(1e9),
	}
}

func (f *fakeVatCaller) setIlk(vault common.Address, ilk [32]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ilkByVault[vault] = ilk
}

func (f *fakeVatCaller) setIlkError(vault common.Address, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.ilkErr[vault] = err
}

func (f *fakeVatCaller) GetIlk(_ context.Context, vaultAddress common.Address) ([32]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err, ok := f.ilkErr[vaultAddress]; ok {
		return [32]byte{}, err
	}
	return f.ilkByVault[vaultAddress], nil
}

func (f *fakeVatCaller) GetRate(_ context.Context, _ [32]byte) (*big.Int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.rateErr != nil {
		return nil, f.rateErr
	}
	return new(big.Int).Set(f.rate), nil
}

func (f *fakeVatCaller) GetNormalizedDebt(_ context.Context, _ [32]byte, _ common.Address) (*big.Int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.artErr != nil {
		return nil, f.artErr
	}
	return new(big.Int).Set(f.art), nil
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
// Helper
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
		VaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA",
	}
}

// ---------------------------------------------------------------------------
// Constructor tests
// ---------------------------------------------------------------------------

func TestNewVaultDebtService_NilCaller(t *testing.T) {
	_, err := vault_debt.NewVaultDebtService(vault_debt.Config{}, nil, &fakePrimeDebtRepository{})
	if err == nil {
		t.Fatal("expected error for nil caller")
	}
}

func TestNewVaultDebtService_NilRepository(t *testing.T) {
	_, err := vault_debt.NewVaultDebtService(vault_debt.Config{}, newFakeVatCaller(), nil)
	if err == nil {
		t.Fatal("expected error for nil repository")
	}
}

func TestNewVaultDebtService_NegativePollInterval(t *testing.T) {
	_, err := vault_debt.NewVaultDebtService(
		vault_debt.Config{PollInterval: -1 * time.Second},
		newFakeVatCaller(),
		&fakePrimeDebtRepository{},
	)
	if err == nil {
		t.Fatal("expected error for negative poll interval")
	}
}

func TestNewVaultDebtService_DefaultPollInterval(t *testing.T) {
	svc, err := vault_debt.NewVaultDebtService(
		vault_debt.Config{}, // zero PollInterval → should default
		newFakeVatCaller(),
		&fakePrimeDebtRepository{},
	)
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
	repo := &fakePrimeDebtRepository{} // empty primes
	svc, err := vault_debt.NewVaultDebtService(vault_debt.Config{}, newFakeVatCaller(), repo)
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
	vaultAddr := common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA")
	caller := newFakeVatCaller()
	caller.setIlkError(vaultAddr, errors.New("RPC unavailable"))

	repo := &fakePrimeDebtRepository{primes: []entity.Prime{sparkPrime()}}
	svc, _ := vault_debt.NewVaultDebtService(vault_debt.Config{}, caller, repo)

	err := svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when ilk resolution fails")
	}
}

func TestStart_Stop_Clean(t *testing.T) {
	vaultAddr := common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA")
	caller := newFakeVatCaller()
	caller.setIlk(vaultAddr, ilkFrom("ALLOCATOR-SPARK-A"))

	repo := &fakePrimeDebtRepository{primes: []entity.Prime{sparkPrime()}}
	svc, err := vault_debt.NewVaultDebtService(
		vault_debt.Config{PollInterval: 10 * time.Minute}, // long interval — only initial sync fires
		caller,
		repo,
	)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	if err := svc.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Give the initial sync goroutine a moment to fire.
	time.Sleep(50 * time.Millisecond)

	cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = svc.Stop()
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return within 5 seconds")
	}
}

// ---------------------------------------------------------------------------
// Sync / snapshot tests
// ---------------------------------------------------------------------------

func TestSync_WritesSnapshotPerPrime(t *testing.T) {
	primes := []entity.Prime{
		{ID: 1, Name: "spark", VaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA"},
		{ID: 2, Name: "grove", VaultAddress: "0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0"},
	}

	caller := newFakeVatCaller()
	for _, p := range primes {
		caller.setIlk(common.HexToAddress(p.VaultAddress), ilkFrom("ALLOCATOR-"+p.Name+"-A"))
	}

	repo := &fakePrimeDebtRepository{primes: primes}
	svc, err := vault_debt.NewVaultDebtService(
		vault_debt.Config{PollInterval: 10 * time.Minute},
		caller,
		repo,
	)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Wait for the initial sync batch.
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
			t.Errorf("prime %q: empty ilk_name", snap.PrimeName)
		}
		if snap.DebtWad == "" {
			t.Errorf("prime %q: empty debt_wad", snap.PrimeName)
		}
	}
}

func TestSync_PartialFailure_OtherPrimesStillSaved(t *testing.T) {
	// spark → ilk resolution succeeds; grove → GetIlk fails at startup.
	// Since ilk resolution is per-prime and grove fails, Start itself should
	// fail. To test partial poll failure instead, we make grove's GetRate fail.
	spark := entity.Prime{ID: 1, Name: "spark", VaultAddress: "0x691A6c29e9e96Dd897718305427Ad5D534db16BA"}
	grove := entity.Prime{ID: 2, Name: "grove", VaultAddress: "0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0"}

	sparkAddr := common.HexToAddress(spark.VaultAddress)
	groveAddr := common.HexToAddress(grove.VaultAddress)

	caller := newFakeVatCaller()
	caller.setIlk(sparkAddr, ilkFrom("ALLOCATOR-SPARK-A"))
	caller.setIlk(groveAddr, ilkFrom("ALLOCATOR-GROVE-A"))

	// Make GetRate fail for grove's ilk by making it fail globally after ilk
	// resolution. We simulate this by making artErr fire — grove's art read fails,
	// spark's succeeds because we only fail on second call.
	callN := 0
	var callMu sync.Mutex
	fakeCaller := &partialFailCaller{
		inner: caller,
		artFailOn: func(ilk [32]byte) bool {
			callMu.Lock()
			defer callMu.Unlock()
			callN++
			// fail every other GetNormalizedDebt call — grove is second prime
			return callN%2 == 0
		},
	}

	repo := &fakePrimeDebtRepository{primes: []entity.Prime{spark, grove}}
	svc, err := vault_debt.NewVaultDebtService(
		vault_debt.Config{PollInterval: 10 * time.Minute},
		fakeCaller,
		repo,
	)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("start: %v", err)
	}

	// At least spark's snapshot should be saved (grove fails its art call).
	deadline := time.After(5 * time.Second)
	for {
		snaps := repo.allSaved()
		for _, s := range snaps {
			if s.PrimeName == "spark" {
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
}

// ---------------------------------------------------------------------------
// partialFailCaller wraps fakeVatCaller to inject per-call art failures.
// ---------------------------------------------------------------------------

type partialFailCaller struct {
	inner     *fakeVatCaller
	artFailOn func(ilk [32]byte) bool
}

func (p *partialFailCaller) GetIlk(ctx context.Context, vaultAddress common.Address) ([32]byte, error) {
	return p.inner.GetIlk(ctx, vaultAddress)
}

func (p *partialFailCaller) GetRate(ctx context.Context, ilk [32]byte) (*big.Int, error) {
	return p.inner.GetRate(ctx, ilk)
}

func (p *partialFailCaller) GetNormalizedDebt(ctx context.Context, ilk [32]byte, urnAddress common.Address) (*big.Int, error) {
	if p.artFailOn(ilk) {
		return nil, errors.New("simulated RPC failure")
	}
	return p.inner.GetNormalizedDebt(ctx, ilk, urnAddress)
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

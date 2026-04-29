package prime_debt_test

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
	"github.com/archon-research/stl/stl-verify/internal/services/prime_debt"
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
	deleted  []string
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

func (f *fakeSQSConsumer) DeleteMessage(_ context.Context, receiptHandle string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleted = append(f.deleted, receiptHandle)
	return nil
}

func (f *fakeSQSConsumer) Close() error {
	return nil
}

func (f *fakeSQSConsumer) deleteCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.deleted)
}

// fakeRPCErr implements go-ethereum's rpc.Error interface
// (Error() string; ErrorCode() int). Used to synthesize EVM reverts in
// tests for per-prime error classification.
type fakeRPCErr struct {
	code int
	msg  string
}

func (e *fakeRPCErr) Error() string  { return e.msg }
func (e *fakeRPCErr) ErrorCode() int { return e.code }

// fakeVatCaller is a controllable in-memory VatCaller for unit tests.
type fakeVatCaller struct {
	mu sync.Mutex

	// ilkByVault maps vault address → ilk bytes32 for ResolveIlks.
	ilkByVault     map[common.Address][32]byte
	resolveErr     error // if set, ResolveIlks returns this error
	partialResolve bool  // if true, skip missing vaults instead of erroring

	// rate / art are returned for all vaults in ReadDebts.
	rate *big.Int
	art  *big.Int

	// Per-vault error injection for ReadDebts.
	debtErrByVault  map[common.Address]error
	readDebtsErr    error // if set, ReadDebts returns this error (whole batch fails)
	truncateResults int   // if > 0, return only this many results (to test short result handling)
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
			if f.partialResolve {
				continue // skip missing — return partial map
			}
			return nil, errors.New("ilk not found for " + v.Hex())
		}
		result[v] = ilk
	}
	return result, nil
}

func (f *fakeVatCaller) ReadDebts(_ context.Context, queries []entity.DebtQuery, _ *big.Int) ([]entity.DebtResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.readDebtsErr != nil {
		return nil, f.readDebtsErr
	}

	results := make([]entity.DebtResult, len(queries))
	for i, q := range queries {
		results[i].VaultAddress = q.VaultAddress

		if err, ok := f.debtErrByVault[q.VaultAddress]; ok {
			results[i].Err = err
			continue
		}

		results[i].Rate = new(big.Int).Set(f.rate)
		results[i].Art = new(big.Int).Set(f.art)
	}

	if f.truncateResults > 0 && f.truncateResults < len(results) {
		results = results[:f.truncateResults]
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

func defaultConfig(sweepEveryN int) prime_debt.Config {
	return prime_debt.Config{
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
	_, err := prime_debt.NewVaultDebtService(defaultConfig(75), nil, &fakePrimeDebtRepository{}, consumer, newFakeBlockQuerier(testBlockNum))
	if err == nil {
		t.Fatal("expected error for nil caller")
	}
}

func TestNewVaultDebtService_NilRepository(t *testing.T) {
	consumer := newFakeSQSConsumer(nil)
	_, err := prime_debt.NewVaultDebtService(defaultConfig(75), newFakeVatCaller(), nil, consumer, newFakeBlockQuerier(testBlockNum))
	if err == nil {
		t.Fatal("expected error for nil repository")
	}
}

func TestNewVaultDebtService_NilSQSConsumer(t *testing.T) {
	_, err := prime_debt.NewVaultDebtService(defaultConfig(75), newFakeVatCaller(), &fakePrimeDebtRepository{}, nil, newFakeBlockQuerier(testBlockNum))
	if err == nil {
		t.Fatal("expected error for nil sqs consumer")
	}
}

func TestNewVaultDebtService_NilBlockQuerier(t *testing.T) {
	consumer := newFakeSQSConsumer(nil)
	_, err := prime_debt.NewVaultDebtService(defaultConfig(75), newFakeVatCaller(), &fakePrimeDebtRepository{}, consumer, nil)
	if err == nil {
		t.Fatal("expected error for nil block querier")
	}
}

func TestNewVaultDebtService_ZeroChainID(t *testing.T) {
	consumer := newFakeSQSConsumer(nil)
	cfg := defaultConfig(75)
	cfg.ChainID = 0
	_, err := prime_debt.NewVaultDebtService(cfg, newFakeVatCaller(), &fakePrimeDebtRepository{}, consumer, newFakeBlockQuerier(testBlockNum))
	if err == nil {
		t.Fatal("expected error for zero chain ID")
	}
}

func TestNewVaultDebtService_Defaults(t *testing.T) {
	consumer := newFakeSQSConsumer(nil)
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(75), newFakeVatCaller(), &fakePrimeDebtRepository{}, consumer, newFakeBlockQuerier(testBlockNum))
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
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(75), newFakeVatCaller(), repo, consumer, newFakeBlockQuerier(testBlockNum))
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
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(75), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	err = svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when ilk resolution fails")
	}
}

func TestStart_PartialIlkResolution(t *testing.T) {
	spark := entity.Prime{ID: 1, Name: "spark", VaultAddress: common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA")}
	grove := entity.Prime{ID: 2, Name: "grove", VaultAddress: common.HexToAddress("0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0")}

	caller := newFakeVatCaller()
	// Only register ilk for spark, not grove
	caller.setIlk(spark.VaultAddress, ilkFrom("ALLOCATOR-SPARK-A"))
	caller.partialResolve = true // return partial map instead of error

	consumer := newFakeSQSConsumer(nil)
	repo := &fakePrimeDebtRepository{primes: []entity.Prime{spark, grove}}
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(75), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	err = svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when ilk is missing for some primes")
	}
	if !containsAny(err.Error(), "ilk not resolved", "grove") {
		t.Errorf("expected 'ilk not resolved' error mentioning grove, got: %v", err)
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
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(75), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
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
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(1), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
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

// TestSync_PartialRevert_OtherPrimesStillSaved verifies the
// AllowFailure-compatible revert path: if one vault's debt call reverts
// (the contract's "no data this block" answer), that prime is skipped
// and the remaining primes are still snapshotted. The classification
// added in this branch narrows the skip to revert-shaped errors only;
// transport errors now propagate (covered by
// TestSyncAll_ErrorsOnTransportFailure).
func TestSync_PartialRevert_OtherPrimesStillSaved(t *testing.T) {
	spark := entity.Prime{ID: 1, Name: "spark", VaultAddress: common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA")}
	grove := entity.Prime{ID: 2, Name: "grove", VaultAddress: common.HexToAddress("0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0")}

	caller := newFakeVatCaller()
	caller.setIlk(spark.VaultAddress, ilkFrom("ALLOCATOR-SPARK-A"))
	caller.setIlk(grove.VaultAddress, ilkFrom("ALLOCATOR-GROVE-A"))
	// Simulate a contract-level revert (geth code 3) for grove.
	caller.setDebtError(grove.VaultAddress, &fakeRPCErr{code: 3, msg: "execution reverted"})

	events := makeBlockEvents(testBlockNum, 3)
	consumer := newFakeSQSConsumer(events)

	repo := &fakePrimeDebtRepository{primes: []entity.Prime{spark, grove}}
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(1), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
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
			t.Fatal("timed out waiting for spark snapshot despite grove revert")
		case <-time.After(20 * time.Millisecond):
		}
	}
found:
	cancel()
	_ = svc.Stop()
}

// TestSyncAll_ErrorsOnTransportFailure codifies VEC-188 Finding 5: a
// per-prime transport error (classified by rpcerr.IsEVMRevert as
// non-revert) must propagate out of syncAll so the SQS message NACKs.
// A plain non-rpc.Error (e.g. from a network timeout) is not a revert,
// so no snapshots should be saved and the message should not be deleted.
func TestSyncAll_ErrorsOnTransportFailure(t *testing.T) {
	spark := entity.Prime{ID: 1, Name: "spark", VaultAddress: common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA")}
	grove := entity.Prime{ID: 2, Name: "grove", VaultAddress: common.HexToAddress("0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0")}

	caller := newFakeVatCaller()
	caller.setIlk(spark.VaultAddress, ilkFrom("ALLOCATOR-SPARK-A"))
	caller.setIlk(grove.VaultAddress, ilkFrom("ALLOCATOR-GROVE-A"))
	// Plain non-rpc.Error — IsEVMRevert returns false, so syncAll must
	// surface this as a top-level error, NACKing the SQS message.
	caller.setDebtError(spark.VaultAddress, errors.New("429 Too Many Requests"))

	events := makeBlockEvents(testBlockNum, 1)
	consumer := newFakeSQSConsumer(events)

	repo := &fakePrimeDebtRepository{primes: []entity.Prime{spark, grove}}
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(1), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// Give the loop time to process the single message.
	time.Sleep(300 * time.Millisecond)

	cancel()
	_ = svc.Stop()

	// No snapshots must be saved — syncAll should have errored out before
	// writing anything. This is the heart of the bug fix: if ANY per-prime
	// result is a transport error, the entire sync must fail so the
	// message is redelivered.
	if got := repo.savedCount(); got != 0 {
		t.Errorf("expected 0 saved snapshots on transport failure, got %d", got)
	}

	// NACK assertion: the message must not have been deleted. handler
	// returning an error is the NACK signal in sqsutil.ProcessMessages.
	if got := consumer.deleteCount(); got != 0 {
		t.Errorf("expected 0 deleted messages (NACK on transport error), got %d", got)
	}
}

// TestSyncAll_SkipsOnContractRevert verifies the revert path stays
// AllowFailure-compatible: an EVM revert (code 3 "execution reverted")
// is treated as the contract's definitive answer of "no debt this block"
// — the prime is skipped and syncAll succeeds with the remaining
// snapshots. The SQS message is deleted (ACK).
func TestSyncAll_SkipsOnContractRevert(t *testing.T) {
	spark := entity.Prime{ID: 1, Name: "spark", VaultAddress: common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA")}
	grove := entity.Prime{ID: 2, Name: "grove", VaultAddress: common.HexToAddress("0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0")}

	caller := newFakeVatCaller()
	caller.setIlk(spark.VaultAddress, ilkFrom("ALLOCATOR-SPARK-A"))
	caller.setIlk(grove.VaultAddress, ilkFrom("ALLOCATOR-GROVE-A"))
	// Contract-level revert (geth code 3) — IsEVMRevert returns true,
	// so syncAll must log+skip and still save the healthy prime.
	caller.setDebtError(grove.VaultAddress, &fakeRPCErr{code: 3, msg: "execution reverted"})

	events := makeBlockEvents(testBlockNum, 1)
	consumer := newFakeSQSConsumer(events)

	repo := &fakePrimeDebtRepository{primes: []entity.Prime{spark, grove}}
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(1), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	deadline := time.After(3 * time.Second)
	for {
		if repo.savedCount() >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for spark snapshot; grove revert should not have blocked save")
		case <-time.After(20 * time.Millisecond):
		}
	}

	cancel()
	_ = svc.Stop()

	// Spark must have been saved; grove must not have been.
	snaps := repo.allSaved()
	var sawSpark, sawGrove bool
	for _, s := range snaps {
		if s.PrimeID == spark.ID {
			sawSpark = true
		}
		if s.PrimeID == grove.ID {
			sawGrove = true
		}
	}
	if !sawSpark {
		t.Error("expected spark snapshot to be saved on grove revert")
	}
	if sawGrove {
		t.Error("expected grove snapshot to NOT be saved (it reverted)")
	}

	// ACK assertion: the message must have been deleted (syncAll returned nil).
	if got := consumer.deleteCount(); got != 1 {
		t.Errorf("expected 1 deleted message (ACK on revert skip), got %d", got)
	}
}

// ---------------------------------------------------------------------------
// latestBlock error path
// ---------------------------------------------------------------------------

func TestStart_BlockQuerierError(t *testing.T) {
	caller := newFakeVatCaller()
	bq := newFakeBlockQuerier(testBlockNum)
	bq.err = errors.New("RPC node unreachable")

	consumer := newFakeSQSConsumer(nil)
	repo := &fakePrimeDebtRepository{primes: []entity.Prime{sparkPrime()}}
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(75), caller, repo, consumer, bq)
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	err = svc.Start(context.Background())
	if err == nil {
		t.Fatal("expected error when block querier fails")
	}
	if !containsAny(err.Error(), "block number", "RPC node") {
		t.Errorf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// ReadDebts batch error
// ---------------------------------------------------------------------------

func TestSync_ReadDebtsBatchError(t *testing.T) {
	caller := newFakeVatCaller()
	caller.setIlk(sparkPrime().VaultAddress, ilkFrom("ALLOCATOR-SPARK-A"))
	caller.readDebtsErr = errors.New("multicall RPC failure")

	events := makeBlockEvents(testBlockNum, 3)
	consumer := newFakeSQSConsumer(events)

	repo := &fakePrimeDebtRepository{primes: []entity.Prime{sparkPrime()}}
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(1), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// Give time for processing
	time.Sleep(500 * time.Millisecond)

	// No snapshots should have been saved due to batch error
	if repo.savedCount() > 0 {
		t.Errorf("expected 0 saved snapshots, got %d", repo.savedCount())
	}

	cancel()
	_ = svc.Stop()
}

// ---------------------------------------------------------------------------
// Missing result for prime (truncated results)
// ---------------------------------------------------------------------------

func TestSync_MissingResultForPrime(t *testing.T) {
	primes := []entity.Prime{
		{ID: 1, Name: "spark", VaultAddress: common.HexToAddress("0x691A6c29e9e96Dd897718305427Ad5D534db16BA")},
		{ID: 2, Name: "grove", VaultAddress: common.HexToAddress("0xD5Bf3F08Ac13f4A2e2b1A70741d5c94E2b4Eb6E0")},
	}

	caller := newFakeVatCaller()
	for _, p := range primes {
		caller.setIlk(p.VaultAddress, ilkFrom("ALLOCATOR-"+p.Name+"-A"))
	}
	// Return only 1 result for 2 primes
	caller.truncateResults = 1

	events := makeBlockEvents(testBlockNum, 3)
	consumer := newFakeSQSConsumer(events)

	repo := &fakePrimeDebtRepository{primes: primes}
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(1), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	deadline := time.After(3 * time.Second)
	for {
		if repo.savedCount() >= 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for partial snapshot")
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Only spark (index 0) should be saved; grove (index 1) is missing from results
	for _, snap := range repo.allSaved() {
		if snap.PrimeID == 2 {
			t.Error("grove should not have been saved — its result was truncated")
		}
	}

	cancel()
	_ = svc.Stop()
}

// ---------------------------------------------------------------------------
// SaveDebtSnapshots error
// ---------------------------------------------------------------------------

func TestSync_SaveSnapshotsError(t *testing.T) {
	caller := newFakeVatCaller()
	caller.setIlk(sparkPrime().VaultAddress, ilkFrom("ALLOCATOR-SPARK-A"))

	events := makeBlockEvents(testBlockNum, 3)
	consumer := newFakeSQSConsumer(events)

	repo := &fakePrimeDebtRepository{
		primes:  []entity.Prime{sparkPrime()},
		saveErr: errors.New("database write failed"),
	}
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(1), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// Give time for processing attempts
	time.Sleep(500 * time.Millisecond)

	// Nothing should be saved since repo always returns error
	if repo.savedCount() > 0 {
		t.Errorf("expected 0 saved snapshots, got %d", repo.savedCount())
	}

	cancel()
	_ = svc.Stop()
}

// ---------------------------------------------------------------------------
// Sweep cadence N > 1
// ---------------------------------------------------------------------------

func TestSync_SweepCadence(t *testing.T) {
	caller := newFakeVatCaller()
	caller.setIlk(sparkPrime().VaultAddress, ilkFrom("ALLOCATOR-SPARK-A"))

	// Send 10 block events with sweep every 3 blocks.
	// First block triggers immediate read (blocksSinceSweep initialized to N-1).
	// Then blocks 2,3 skip, block 4 reads, blocks 5,6 skip, block 7 reads, etc.
	// Expected reads: block 1, 4, 7, 10 = 4 syncs.
	const sweepN = 3
	const numBlocks = 10
	events := makeBlockEvents(testBlockNum, numBlocks)
	consumer := newFakeSQSConsumer(events)

	repo := &fakePrimeDebtRepository{primes: []entity.Prime{sparkPrime()}}
	svc, err := prime_debt.NewVaultDebtService(defaultConfig(sweepN), caller, repo, consumer, newFakeBlockQuerier(testBlockNum))
	if err != nil {
		t.Fatalf("create service: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}

	// Wait for all messages to be consumed
	deadline := time.After(5 * time.Second)
	for {
		// 4 expected syncs: blocks 1, 4, 7, 10
		if repo.savedCount() >= 4 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("timed out: expected 4 snapshots, got %d", repo.savedCount())
		case <-time.After(20 * time.Millisecond):
		}
	}

	// Should have exactly 4 syncs (not 10)
	count := repo.savedCount()
	if count < 3 || count > 5 {
		t.Errorf("expected ~4 snapshots with sweep every %d blocks over %d blocks, got %d", sweepN, numBlocks, count)
	}

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

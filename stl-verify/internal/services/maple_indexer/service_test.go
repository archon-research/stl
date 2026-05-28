package maple_indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// -----------------------------------------------------------------------------
// Harness
// -----------------------------------------------------------------------------

type harness struct {
	t           testing.TB
	svc         *Service
	multicaller *multicallStub
	txMgr       *testutil.MockTxManager
	userRepo    *userRepoStub
	mapleRepo   *mapleRepoStub
	cache       *testutil.MockBlockCache
	consumer    *testutil.MockSQSConsumer
}

type userRepoStub struct {
	mu          sync.Mutex
	addrToID    map[common.Address]int64
	nextID      int64
	createdRows []entity.User
	err         error
}

func newUserRepoStub() *userRepoStub {
	return &userRepoStub{addrToID: make(map[common.Address]int64)}
}

func (s *userRepoStub) GetOrCreateUser(_ context.Context, _ pgx.Tx, u entity.User) (int64, error) {
	if s.err != nil {
		return 0, s.err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if id, ok := s.addrToID[u.Address]; ok {
		return id, nil
	}
	s.nextID++
	s.addrToID[u.Address] = s.nextID
	s.createdRows = append(s.createdRows, u)
	return s.nextID, nil
}

func (s *userRepoStub) GetOrCreateUsers(ctx context.Context, tx pgx.Tx, users []entity.User) (map[common.Address]int64, error) {
	out := make(map[common.Address]int64, len(users))
	for _, u := range users {
		id, err := s.GetOrCreateUser(ctx, tx, u)
		if err != nil {
			return nil, err
		}
		out[u.Address] = id
	}
	return out, nil
}

func (s *userRepoStub) UpsertUserProtocolMetadata(_ context.Context, _ []*entity.UserProtocolMetadata) error {
	return nil
}

var _ outbound.UserRepository = (*userRepoStub)(nil)

// newHarness returns a Service wired with mocks and the SyrupUSDC vault
// pre-loaded into the registry.
func newHarness(t *testing.T) *harness {
	t.Helper()
	vaultAddr := common.HexToAddress(syrupUSDCAddr)
	vault := newMapleVault(t, vaultAddr)
	vault.ID = 1

	mapleRepo := newRepoStub(map[common.Address]*entity.MapleVault{vaultAddr: vault}, nil)
	userRepo := newUserRepoStub()
	mc := &multicallStub{}
	tx := &testutil.MockTxManager{}
	cache := testutil.NewMockBlockCache()
	consumer := &testutil.MockSQSConsumer{}

	cfg := ConfigDefaults()
	cfg.ChainID = 1
	cfg.Logger = quietLogger()
	cfg.Telemetry = nil

	svc, err := NewService(cfg, consumer, cache, mc, tx, userRepo, mapleRepo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	// Load the registry directly without invoking Start (which would also
	// fire off the SQS RunLoop goroutine).
	if err := svc.registry.LoadFromDB(context.Background(), mapleRepo, 1); err != nil {
		t.Fatalf("registry.LoadFromDB: %v", err)
	}

	return &harness{
		t:           t,
		svc:         svc,
		multicaller: mc,
		txMgr:       tx,
		userRepo:    userRepo,
		mapleRepo:   mapleRepo,
		cache:       cache,
		consumer:    consumer,
	}
}

// makeBlockEvent constructs a representative block event for tests.
func makeBlockEvent(blockNumber int64) outbound.BlockEvent {
	return outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        0,
		BlockHash:      "0xdeadbeef",
		BlockTimestamp: time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC).Unix(),
		ReceivedAt:     time.Now().UTC(),
	}
}

// receiptWithLogs builds a single-receipt cache payload containing the given logs.
func receiptWithLogs(t *testing.T, logs []shared.Log) json.RawMessage {
	t.Helper()
	receipts := []shared.TransactionReceipt{{
		TransactionHash: "0xabc",
		Logs:            logs,
	}}
	raw, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}
	return raw
}

// queueVaultStateAndPositions seeds the multicall stub with responses for
// one FetchVaultState + one FetchUserPositions(N users) call sequence.
func (h *harness) queueVaultStateAndPositions(totalAssets, totalSupply, sharePrice int64, balances, assets []int64) {
	// FetchVaultState — 3 calls.
	h.multicaller.Responses = append(h.multicaller.Responses,
		encodeUint256(big.NewInt(totalAssets)),
		encodeUint256(big.NewInt(totalSupply)),
		encodeUint256(big.NewInt(sharePrice)),
	)
	// FetchUserPositions — 2 batches, N each.
	for _, b := range balances {
		h.multicaller.Responses = append(h.multicaller.Responses, encodeUint256(big.NewInt(b)))
	}
	for _, a := range assets {
		h.multicaller.Responses = append(h.multicaller.Responses, encodeUint256(big.NewInt(a)))
	}
}

// -----------------------------------------------------------------------------
// Constructor + dependency validation
// -----------------------------------------------------------------------------

func TestNewService_RequiresDependencies(t *testing.T) {
	mc := &multicallStub{}
	tx := &testutil.MockTxManager{}
	ur := newUserRepoStub()
	mr := newRepoStub(nil, nil)
	cache := testutil.NewMockBlockCache()
	consumer := &testutil.MockSQSConsumer{}

	cfg := ConfigDefaults()
	cfg.ChainID = 1
	cfg.Logger = quietLogger()

	if _, err := NewService(cfg, nil, cache, mc, tx, ur, mr); err == nil {
		t.Fatal("expected error for nil consumer")
	}
	if _, err := NewService(cfg, consumer, nil, mc, tx, ur, mr); err == nil {
		t.Fatal("expected error for nil cache")
	}
	if _, err := NewService(cfg, consumer, cache, nil, tx, ur, mr); err == nil {
		t.Fatal("expected error for nil multicaller")
	}
	if _, err := NewService(cfg, consumer, cache, mc, nil, ur, mr); err == nil {
		t.Fatal("expected error for nil txManager")
	}
	if _, err := NewService(cfg, consumer, cache, mc, tx, nil, mr); err == nil {
		t.Fatal("expected error for nil userRepo")
	}
	if _, err := NewService(cfg, consumer, cache, mc, tx, ur, nil); err == nil {
		t.Fatal("expected error for nil mapleRepo")
	}
}

func TestNewService_RejectsUnsupportedChain(t *testing.T) {
	mc := &multicallStub{}
	tx := &testutil.MockTxManager{}
	ur := newUserRepoStub()
	mr := newRepoStub(nil, nil)
	cache := testutil.NewMockBlockCache()
	consumer := &testutil.MockSQSConsumer{}

	cfg := ConfigDefaults()
	cfg.ChainID = 999_999 // not in mapleSyrupDeployBlocks
	cfg.Logger = quietLogger()

	if _, err := NewService(cfg, consumer, cache, mc, tx, ur, mr); err == nil {
		t.Fatal("expected error for unsupported chain")
	}
}

func TestService_Start_FailsOnEmptyRegistry(t *testing.T) {
	mc := &multicallStub{}
	tx := &testutil.MockTxManager{}
	ur := newUserRepoStub()
	mr := newRepoStub(map[common.Address]*entity.MapleVault{}, nil) // empty vault map
	cache := testutil.NewMockBlockCache()
	consumer := &testutil.MockSQSConsumer{}

	cfg := ConfigDefaults()
	cfg.ChainID = 1 // mainnet — valid chain, but no vaults seeded
	cfg.Logger = quietLogger()

	svc, err := NewService(cfg, consumer, cache, mc, tx, ur, mr)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if err := svc.Start(context.Background()); err == nil {
		t.Fatal("expected error when vault registry is empty")
	}
}

// -----------------------------------------------------------------------------
// fetchAndProcessReceipts — golden path + edge cases
// -----------------------------------------------------------------------------

func TestService_ProcessBlock_NoCachedReceipts_Errors(t *testing.T) {
	h := newHarness(t)
	event := makeBlockEvent(18_500_000)
	// No cache.SetReceipts call → mock returns nil, nil.
	err := h.svc.processBlockEvent(context.Background(), event)
	if err == nil {
		t.Fatal("expected error when cache returns nil receipts")
	}
}

func TestService_ProcessBlock_CacheError_Propagates(t *testing.T) {
	h := newHarness(t)
	event := makeBlockEvent(18_500_000)
	wantErr := errors.New("redis down")
	h.cache.SetError(wantErr)
	err := h.svc.processBlockEvent(context.Background(), event)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped cache error, got %v", err)
	}
}

func TestService_ProcessBlock_NoRelevantLogs_NoWrites(t *testing.T) {
	h := newHarness(t)
	event := makeBlockEvent(18_500_000)
	// One receipt with an unrelated log (zero topic).
	logs := []shared.Log{{
		Address: otherAddr,
		Topics:  []string{"0x0000000000000000000000000000000000000000000000000000000000000001"},
	}}
	h.cache.SetReceipts(1, event.BlockNumber, 0, receiptWithLogs(t, logs))

	if err := h.svc.processBlockEvent(context.Background(), event); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(h.mapleRepo.stateRows) != 0 {
		t.Fatalf("expected no state writes, got %d", len(h.mapleRepo.stateRows))
	}
	if len(h.mapleRepo.posRows) != 0 {
		t.Fatalf("expected no position writes, got %d", len(h.mapleRepo.posRows))
	}
	if len(h.multicaller.Calls) != 0 {
		t.Fatal("multicaller invoked despite no relevant logs")
	}
}

func TestService_ProcessBlock_Deposit_WritesStateAndPosition(t *testing.T) {
	h := newHarness(t)
	event := makeBlockEvent(18_500_000)

	// Build a deposit log: sender=userA, owner=userB.
	depositTopic := h.svc.extractor.DepositTopic().Hex()
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics:  []string{depositTopic, topicForAddress(userA), topicForAddress(userB)},
	}}
	h.cache.SetReceipts(1, event.BlockNumber, 0, receiptWithLogs(t, logs))

	// Users sort by raw bytes: userA=0x11.., userB=0x22.. → A first.
	h.queueVaultStateAndPositions(
		1_000_000_000_000, // totalAssets
		900_000_000_000,   // totalSupply
		1_111_111,         // sharePrice
		[]int64{500, 750}, // balances A,B
		[]int64{550, 825}, // assets   A,B
	)

	if err := h.svc.processBlockEvent(context.Background(), event); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}

	// One vault_state row.
	if len(h.mapleRepo.stateRows) != 1 {
		t.Fatalf("state rows=%d, want 1", len(h.mapleRepo.stateRows))
	}
	st := h.mapleRepo.stateRows[0]
	if st.MapleVaultID != 1 || st.BlockNumber != 18_500_000 || st.BlockVersion != 0 {
		t.Fatalf("state row mis-set: %+v", st)
	}
	if st.TotalAssets.Cmp(big.NewInt(1_000_000_000_000)) != 0 {
		t.Fatalf("TotalAssets=%s", st.TotalAssets)
	}
	if st.SharePrice.Cmp(big.NewInt(1_111_111)) != 0 {
		t.Fatalf("SharePrice=%s", st.SharePrice)
	}

	// Two position rows in user-address order (A then B).
	if len(h.mapleRepo.posRows) != 2 {
		t.Fatalf("position rows=%d, want 2", len(h.mapleRepo.posRows))
	}
	wantAddrs := []common.Address{common.HexToAddress(userA), common.HexToAddress(userB)}
	for i, want := range wantAddrs {
		gotUser := h.userIDFor(want)
		if h.mapleRepo.posRows[i].UserID != gotUser {
			t.Fatalf("position[%d].UserID=%d, want %d (addr %s)",
				i, h.mapleRepo.posRows[i].UserID, gotUser, want.Hex())
		}
	}
	if h.mapleRepo.posRows[0].Shares.Cmp(big.NewInt(500)) != 0 {
		t.Fatalf("user A shares=%s", h.mapleRepo.posRows[0].Shares)
	}
	if h.mapleRepo.posRows[1].Assets.Cmp(big.NewInt(825)) != 0 {
		t.Fatalf("user B assets=%s", h.mapleRepo.posRows[1].Assets)
	}
}

func TestService_ProcessBlock_VaultStateFetchError_Propagates(t *testing.T) {
	h := newHarness(t)
	event := makeBlockEvent(18_500_000)
	depositTopic := h.svc.extractor.DepositTopic().Hex()
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics:  []string{depositTopic, topicForAddress(userA), topicForAddress(userB)},
	}}
	h.cache.SetReceipts(1, event.BlockNumber, 0, receiptWithLogs(t, logs))
	wantErr := errors.New("alchemy 502")
	h.multicaller.Err = wantErr

	err := h.svc.processBlockEvent(context.Background(), event)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped multicall error, got %v", err)
	}
	if len(h.mapleRepo.stateRows) != 0 || len(h.mapleRepo.posRows) != 0 {
		t.Fatal("no rows should be written on error")
	}
}

func TestService_ProcessBlock_VaultStateSaveError_Propagates(t *testing.T) {
	h := newHarness(t)
	event := makeBlockEvent(18_500_000)
	depositTopic := h.svc.extractor.DepositTopic().Hex()
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics:  []string{depositTopic, topicForAddress(userA), topicForAddress(userB)},
	}}
	h.cache.SetReceipts(1, event.BlockNumber, 0, receiptWithLogs(t, logs))
	h.queueVaultStateAndPositions(1, 1, 1, []int64{1, 1}, []int64{1, 1})
	wantErr := errors.New("constraint violation")
	h.mapleRepo.stateErr = wantErr

	err := h.svc.processBlockEvent(context.Background(), event)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped state save error, got %v", err)
	}
}

func TestService_ProcessBlock_UserRepoError_Propagates(t *testing.T) {
	h := newHarness(t)
	event := makeBlockEvent(18_500_000)
	depositTopic := h.svc.extractor.DepositTopic().Hex()
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics:  []string{depositTopic, topicForAddress(userA), topicForAddress(userB)},
	}}
	h.cache.SetReceipts(1, event.BlockNumber, 0, receiptWithLogs(t, logs))
	h.queueVaultStateAndPositions(1, 1, 1, []int64{1, 1}, []int64{1, 1})
	wantErr := errors.New("user table corrupted")
	h.userRepo.err = wantErr

	err := h.svc.processBlockEvent(context.Background(), event)
	if err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("expected wrapped user error, got %v", err)
	}
}

func TestService_ProcessBlock_DedupesUsersAcrossLogs(t *testing.T) {
	h := newHarness(t)
	event := makeBlockEvent(18_500_000)
	depositTopic := h.svc.extractor.DepositTopic().Hex()
	transferTopic := h.svc.extractor.TransferTopic().Hex()
	// Two events touching userA — should still produce one balanceOf call for A.
	logs := []shared.Log{
		{Address: syrupUSDCAddr, Topics: []string{depositTopic, topicForAddress(userA), topicForAddress(userB)}},
		{Address: syrupUSDCAddr, Topics: []string{transferTopic, topicForAddress(userA), topicForAddress(userC)}},
	}
	h.cache.SetReceipts(1, event.BlockNumber, 0, receiptWithLogs(t, logs))
	// 3 unique users (A, B, C) — sorted A < B < C.
	h.queueVaultStateAndPositions(1, 1, 1,
		[]int64{500, 600, 700},
		[]int64{550, 660, 770})

	if err := h.svc.processBlockEvent(context.Background(), event); err != nil {
		t.Fatalf("processBlockEvent: %v", err)
	}
	if len(h.mapleRepo.posRows) != 3 {
		t.Fatalf("position rows=%d, want 3 (dedup A across two logs)", len(h.mapleRepo.posRows))
	}
}

func TestService_ProcessBlock_OneTxPerVault(t *testing.T) {
	h := newHarness(t)
	event := makeBlockEvent(18_500_000)
	depositTopic := h.svc.extractor.DepositTopic().Hex()
	logs := []shared.Log{{
		Address: syrupUSDCAddr,
		Topics:  []string{depositTopic, topicForAddress(userA), topicForAddress(userB)},
	}}
	h.cache.SetReceipts(1, event.BlockNumber, 0, receiptWithLogs(t, logs))
	h.queueVaultStateAndPositions(1, 1, 1, []int64{1, 1}, []int64{1, 1})

	var txCount int
	h.txMgr.WithTransactionFn = func(ctx context.Context, fn func(tx pgx.Tx) error) error {
		txCount++
		return fn(nil)
	}
	if err := h.svc.processBlockEvent(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	if txCount != 1 {
		t.Fatalf("WithTransaction calls=%d, want 1 (one tx per vault per block)", txCount)
	}
}

// -----------------------------------------------------------------------------
// touchedVaults — direct behaviour
// -----------------------------------------------------------------------------

func TestTouchedVaults_ReturnsOnlyAffectedVaults(t *testing.T) {
	h := newHarness(t)
	depositTopic := h.svc.extractor.DepositTopic().Hex()
	receipts := []shared.TransactionReceipt{{
		Logs: []shared.Log{
			{Address: syrupUSDCAddr, Topics: []string{depositTopic, topicForAddress(userA), topicForAddress(userB)}},
			{Address: otherAddr, Topics: []string{depositTopic, topicForAddress(userA), topicForAddress(userB)}},
		},
	}}
	touched := h.svc.touchedVaults(receipts)
	if len(touched) != 1 {
		t.Fatalf("expected exactly 1 touched vault, got %d", len(touched))
	}
	usersForVault := touched[common.HexToAddress(syrupUSDCAddr)]
	if len(usersForVault) != 2 {
		t.Fatalf("expected 2 touched users, got %d", len(usersForVault))
	}
}

// -----------------------------------------------------------------------------
// Stop releases the cancel func
// -----------------------------------------------------------------------------

func TestService_Stop_NoStart_NoPanic(t *testing.T) {
	h := newHarness(t)
	// cancel is nil prior to Start — Stop must not panic.
	if err := h.svc.Stop(); err != nil {
		t.Fatalf("Stop() returned err: %v", err)
	}
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func (h *harness) userIDFor(addr common.Address) int64 {
	h.userRepo.mu.Lock()
	defer h.userRepo.mu.Unlock()
	return h.userRepo.addrToID[addr]
}

// sanity: byte-order of the test users matches the sort assumption (A < B < C).
func TestSanity_UserOrdering(t *testing.T) {
	a := common.HexToAddress(userA).Bytes()
	b := common.HexToAddress(userB).Bytes()
	c := common.HexToAddress(userC).Bytes()
	if bytes.Compare(a, b) >= 0 {
		t.Fatal("userA must sort before userB")
	}
	if bytes.Compare(b, c) >= 0 {
		t.Fatal("userB must sort before userC")
	}
}

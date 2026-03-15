package transfer_user_discovery

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Spec coverage: plan-transfer-user-discovery.md §5, §5 block processing loop,
// and §6 service acceptance criteria.

type ensureUserCall struct {
	user entity.User
	tx   pgx.Tx
}

type snapshotUserCall struct {
	tx              pgx.Tx
	chainID         int64
	protocolAddress common.Address
	userAddress     common.Address
	blockNumber     int64
	blockVersion    int
	eventType       string
	txHash          []byte
}

type mockSnapshotter struct {
	SnapshotUserWithTxFn func(
		ctx context.Context,
		tx pgx.Tx,
		chainID int64,
		protocolAddress common.Address,
		userAddress common.Address,
		blockNumber int64,
		blockVersion int,
		eventType string,
		txHash []byte,
	) error
	calls []snapshotUserCall
}

func (m *mockSnapshotter) SnapshotUserWithTx(
	ctx context.Context,
	tx pgx.Tx,
	chainID int64,
	protocolAddress common.Address,
	userAddress common.Address,
	blockNumber int64,
	blockVersion int,
	eventType string,
	txHash []byte,
) error {
	m.calls = append(m.calls, snapshotUserCall{
		tx:              tx,
		chainID:         chainID,
		protocolAddress: protocolAddress,
		userAddress:     userAddress,
		blockNumber:     blockNumber,
		blockVersion:    blockVersion,
		eventType:       eventType,
		txHash:          txHash,
	})

	if m.SnapshotUserWithTxFn != nil {
		return m.SnapshotUserWithTxFn(ctx, tx, chainID, protocolAddress, userAddress, blockNumber, blockVersion, eventType, txHash)
	}

	return nil
}

type serviceTestDeps struct {
	config      shared.SQSConsumerConfig
	consumer    outbound.SQSConsumer
	cacheReader outbound.BlockCacheReader
	txManager   outbound.TxManager
	userRepo    outbound.UserRepository
	snapshotter *mockSnapshotter
	tokens      []outbound.TrackedReceiptToken
}

func TestProcessBlockEvent_NewUserSnapshotsTransferCandidate(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §5 requires EnsureUser-created users to
	// trigger a snapshot with eventType "Transfer" for the discovered candidate.
	ctx := context.Background()
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 19000010, Version: 2}
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	protocolAddress := common.HexToAddress("0x2222222222222222222222222222222222222222")
	userAddress := common.HexToAddress("0x3333333333333333333333333333333333333333")
	txHash := common.HexToHash("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(event.ChainID, event.BlockNumber, event.Version, mustMarshalReceipts(t, []shared.TransactionReceipt{
		makeReceipt(txHash, makeTransferLog(trackedToken, common.Address{}, userAddress, bigIntFromInt64(123), 7)),
	}))

	var ensureCalls []ensureUserCall
	userRepo := &testutil.MockUserRepository{
		EnsureUserFn: func(_ context.Context, tx pgx.Tx, user entity.User) (int64, bool, error) {
			ensureCalls = append(ensureCalls, ensureUserCall{user: user, tx: tx})
			return 101, true, nil
		},
	}

	snapshotter := &mockSnapshotter{}
	svc := newTestService(t, serviceTestDeps{
		config:      validServiceConfig(),
		consumer:    &testutil.MockSQSConsumer{},
		cacheReader: cache,
		txManager:   &testutil.MockTxManager{},
		userRepo:    userRepo,
		snapshotter: snapshotter,
		tokens: []outbound.TrackedReceiptToken{{
			ReceiptTokenAddress: trackedToken,
			ProtocolAddress:     protocolAddress,
		}},
	})

	if err := svc.processBlockEvent(ctx, event); err != nil {
		t.Fatalf("processBlockEvent returned unexpected error: %v", err)
	}

	if len(ensureCalls) != 1 {
		t.Fatalf("expected 1 EnsureUser call, got %d", len(ensureCalls))
	}
	if ensureCalls[0].user.ChainID != event.ChainID {
		t.Fatalf("expected EnsureUser chainID %d, got %d", event.ChainID, ensureCalls[0].user.ChainID)
	}
	if ensureCalls[0].user.Address != userAddress {
		t.Fatalf("expected EnsureUser address %s, got %s", userAddress.Hex(), ensureCalls[0].user.Address.Hex())
	}
	if ensureCalls[0].user.FirstSeenBlock != event.BlockNumber {
		t.Fatalf("expected first_seen_block %d, got %d", event.BlockNumber, ensureCalls[0].user.FirstSeenBlock)
	}

	if len(snapshotter.calls) != 1 {
		t.Fatalf("expected 1 SnapshotUserWithTx call, got %d", len(snapshotter.calls))
	}

	call := snapshotter.calls[0]
	if call.chainID != event.ChainID {
		t.Fatalf("expected snapshot chainID %d, got %d", event.ChainID, call.chainID)
	}
	if call.protocolAddress != protocolAddress {
		t.Fatalf("expected snapshot protocol %s, got %s", protocolAddress.Hex(), call.protocolAddress.Hex())
	}
	if call.userAddress != userAddress {
		t.Fatalf("expected snapshot user %s, got %s", userAddress.Hex(), call.userAddress.Hex())
	}
	if call.blockNumber != event.BlockNumber {
		t.Fatalf("expected snapshot block %d, got %d", event.BlockNumber, call.blockNumber)
	}
	if call.blockVersion != event.Version {
		t.Fatalf("expected snapshot block version %d, got %d", event.Version, call.blockVersion)
	}
	if call.eventType != "Transfer" {
		t.Fatalf("expected event type Transfer, got %q", call.eventType)
	}
	if common.BytesToHash(call.txHash) != txHash {
		t.Fatalf("expected tx hash %s, got %s", txHash.Hex(), common.BytesToHash(call.txHash).Hex())
	}
}

func TestProcessBlockEvent_ExistingUserDoesNotSnapshot(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §5 requires created=false to skip snapshotting.
	ctx := context.Background()
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 19000011, Version: 0}
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	protocolAddress := common.HexToAddress("0x2222222222222222222222222222222222222222")
	userAddress := common.HexToAddress("0x4444444444444444444444444444444444444444")

	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(event.ChainID, event.BlockNumber, event.Version, mustMarshalReceipts(t, []shared.TransactionReceipt{
		makeReceipt(common.HexToHash("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"), makeTransferLog(trackedToken, common.Address{}, userAddress, bigIntFromInt64(1), 0)),
	}))

	var ensureCalls []ensureUserCall
	userRepo := &testutil.MockUserRepository{
		EnsureUserFn: func(_ context.Context, tx pgx.Tx, user entity.User) (int64, bool, error) {
			ensureCalls = append(ensureCalls, ensureUserCall{user: user, tx: tx})
			return 102, false, nil
		},
	}

	snapshotter := &mockSnapshotter{}
	svc := newTestService(t, serviceTestDeps{
		config:      validServiceConfig(),
		consumer:    &testutil.MockSQSConsumer{},
		cacheReader: cache,
		txManager:   &testutil.MockTxManager{},
		userRepo:    userRepo,
		snapshotter: snapshotter,
		tokens: []outbound.TrackedReceiptToken{{
			ReceiptTokenAddress: trackedToken,
			ProtocolAddress:     protocolAddress,
		}},
	})

	if err := svc.processBlockEvent(ctx, event); err != nil {
		t.Fatalf("processBlockEvent returned unexpected error: %v", err)
	}

	if len(ensureCalls) != 1 {
		t.Fatalf("expected 1 EnsureUser call, got %d", len(ensureCalls))
	}
	if len(snapshotter.calls) != 0 {
		t.Fatalf("expected no snapshot calls when EnsureUser returns created=false, got %d", len(snapshotter.calls))
	}
}

func TestProcessBlockEvent_ProcessesBothFromAndToCandidates(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §5 and testing plan require both from and to
	// to be processed when both are non-zero addresses.
	ctx := context.Background()
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 19000012, Version: 1}
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	protocolAddress := common.HexToAddress("0x2222222222222222222222222222222222222222")
	fromAddress := common.HexToAddress("0x5555555555555555555555555555555555555555")
	toAddress := common.HexToAddress("0x6666666666666666666666666666666666666666")

	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(event.ChainID, event.BlockNumber, event.Version, mustMarshalReceipts(t, []shared.TransactionReceipt{
		makeReceipt(common.HexToHash("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"), makeTransferLog(trackedToken, fromAddress, toAddress, bigIntFromInt64(10), 1)),
	}))

	var ensureCalls []ensureUserCall
	userRepo := &testutil.MockUserRepository{
		EnsureUserFn: func(_ context.Context, tx pgx.Tx, user entity.User) (int64, bool, error) {
			ensureCalls = append(ensureCalls, ensureUserCall{user: user, tx: tx})
			return 103, true, nil
		},
	}

	snapshotter := &mockSnapshotter{}
	svc := newTestService(t, serviceTestDeps{
		config:      validServiceConfig(),
		consumer:    &testutil.MockSQSConsumer{},
		cacheReader: cache,
		txManager:   &testutil.MockTxManager{},
		userRepo:    userRepo,
		snapshotter: snapshotter,
		tokens: []outbound.TrackedReceiptToken{{
			ReceiptTokenAddress: trackedToken,
			ProtocolAddress:     protocolAddress,
		}},
	})

	if err := svc.processBlockEvent(ctx, event); err != nil {
		t.Fatalf("processBlockEvent returned unexpected error: %v", err)
	}

	if len(ensureCalls) != 2 {
		t.Fatalf("expected 2 EnsureUser calls, got %d", len(ensureCalls))
	}
	if len(snapshotter.calls) != 2 {
		t.Fatalf("expected 2 snapshot calls, got %d", len(snapshotter.calls))
	}

	ensured := map[common.Address]bool{}
	for _, call := range ensureCalls {
		ensured[call.user.Address] = true
	}
	if !ensured[fromAddress] {
		t.Fatalf("expected from address %s to be ensured", fromAddress.Hex())
	}
	if !ensured[toAddress] {
		t.Fatalf("expected to address %s to be ensured", toAddress.Hex())
	}
}

func TestProcessBlockEvent_SkipsZeroAddressCandidates(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §5 requires zero-address filtering at the service layer.
	ctx := context.Background()
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 19000013, Version: 0}
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	protocolAddress := common.HexToAddress("0x2222222222222222222222222222222222222222")
	fromAddress := common.HexToAddress("0x7777777777777777777777777777777777777777")
	toAddress := common.HexToAddress("0x8888888888888888888888888888888888888888")

	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(event.ChainID, event.BlockNumber, event.Version, mustMarshalReceipts(t, []shared.TransactionReceipt{
		makeReceipt(
			common.HexToHash("0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"),
			makeTransferLog(trackedToken, common.Address{}, toAddress, bigIntFromInt64(5), 1),
			makeTransferLog(trackedToken, fromAddress, common.Address{}, bigIntFromInt64(6), 2),
		),
	}))

	var ensureCalls []ensureUserCall
	userRepo := &testutil.MockUserRepository{
		EnsureUserFn: func(_ context.Context, tx pgx.Tx, user entity.User) (int64, bool, error) {
			ensureCalls = append(ensureCalls, ensureUserCall{user: user, tx: tx})
			return 104, false, nil
		},
	}

	svc := newTestService(t, serviceTestDeps{
		config:      validServiceConfig(),
		consumer:    &testutil.MockSQSConsumer{},
		cacheReader: cache,
		txManager:   &testutil.MockTxManager{},
		userRepo:    userRepo,
		snapshotter: &mockSnapshotter{},
		tokens: []outbound.TrackedReceiptToken{{
			ReceiptTokenAddress: trackedToken,
			ProtocolAddress:     protocolAddress,
		}},
	})

	if err := svc.processBlockEvent(ctx, event); err != nil {
		t.Fatalf("processBlockEvent returned unexpected error: %v", err)
	}

	if len(ensureCalls) != 2 {
		t.Fatalf("expected 2 non-zero EnsureUser calls, got %d", len(ensureCalls))
	}
	for _, call := range ensureCalls {
		if call.user.Address == (common.Address{}) {
			t.Fatal("zero address must not be passed to EnsureUser")
		}
	}

	ensured := map[common.Address]bool{}
	for _, call := range ensureCalls {
		ensured[call.user.Address] = true
	}
	if !ensured[fromAddress] || !ensured[toAddress] {
		t.Fatalf("expected ensured addresses to contain %s and %s, got %#v", fromAddress.Hex(), toAddress.Hex(), ensured)
	}
}

func TestProcessBlockEvent_DeduplicatesUserProtocolCandidatesWithinBlock(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §5 requires deduplication by
	// (userAddress, protocolAddress) within a block.
	ctx := context.Background()
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 19000014, Version: 0}
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	protocolAddress := common.HexToAddress("0x2222222222222222222222222222222222222222")
	userAddress := common.HexToAddress("0x9999999999999999999999999999999999999999")

	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(event.ChainID, event.BlockNumber, event.Version, mustMarshalReceipts(t, []shared.TransactionReceipt{
		makeReceipt(
			common.HexToHash("0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"),
			makeTransferLog(trackedToken, common.Address{}, userAddress, bigIntFromInt64(1), 1),
			makeTransferLog(trackedToken, common.Address{}, userAddress, bigIntFromInt64(2), 2),
		),
	}))

	var ensureCalls []ensureUserCall
	userRepo := &testutil.MockUserRepository{
		EnsureUserFn: func(_ context.Context, tx pgx.Tx, user entity.User) (int64, bool, error) {
			ensureCalls = append(ensureCalls, ensureUserCall{user: user, tx: tx})
			return 105, true, nil
		},
	}

	snapshotter := &mockSnapshotter{}
	svc := newTestService(t, serviceTestDeps{
		config:      validServiceConfig(),
		consumer:    &testutil.MockSQSConsumer{},
		cacheReader: cache,
		txManager:   &testutil.MockTxManager{},
		userRepo:    userRepo,
		snapshotter: snapshotter,
		tokens: []outbound.TrackedReceiptToken{{
			ReceiptTokenAddress: trackedToken,
			ProtocolAddress:     protocolAddress,
		}},
	})

	if err := svc.processBlockEvent(ctx, event); err != nil {
		t.Fatalf("processBlockEvent returned unexpected error: %v", err)
	}

	if len(ensureCalls) != 1 {
		t.Fatalf("expected 1 EnsureUser call after deduplication, got %d", len(ensureCalls))
	}
	if len(snapshotter.calls) != 1 {
		t.Fatalf("expected 1 snapshot call after deduplication, got %d", len(snapshotter.calls))
	}
}

func TestProcessBlockEvent_SnapshotFailureReturnsError(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §5 requires snapshot failures to fail the block.
	ctx := context.Background()
	event := outbound.BlockEvent{ChainID: 1, BlockNumber: 19000015, Version: 0}
	trackedToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	protocolAddress := common.HexToAddress("0x2222222222222222222222222222222222222222")
	userAddress := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	snapshotErr := errors.New("snapshot failed")

	cache := testutil.NewMockBlockCache()
	cache.SetReceipts(event.ChainID, event.BlockNumber, event.Version, mustMarshalReceipts(t, []shared.TransactionReceipt{
		makeReceipt(common.HexToHash("0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"), makeTransferLog(trackedToken, common.Address{}, userAddress, bigIntFromInt64(1), 0)),
	}))

	userRepo := &testutil.MockUserRepository{
		EnsureUserFn: func(_ context.Context, _ pgx.Tx, _ entity.User) (int64, bool, error) {
			return 106, true, nil
		},
	}

	snapshotter := &mockSnapshotter{
		SnapshotUserWithTxFn: func(
			_ context.Context,
			_ pgx.Tx,
			_ int64,
			_ common.Address,
			_ common.Address,
			_ int64,
			_ int,
			_ string,
			_ []byte,
		) error {
			return snapshotErr
		},
	}

	svc := newTestService(t, serviceTestDeps{
		config:      validServiceConfig(),
		consumer:    &testutil.MockSQSConsumer{},
		cacheReader: cache,
		txManager:   &testutil.MockTxManager{},
		userRepo:    userRepo,
		snapshotter: snapshotter,
		tokens: []outbound.TrackedReceiptToken{{
			ReceiptTokenAddress: trackedToken,
			ProtocolAddress:     protocolAddress,
		}},
	})

	err := svc.processBlockEvent(ctx, event)
	if err == nil {
		t.Fatal("expected snapshot failure to be returned, got nil")
	}
	if !errors.Is(err, snapshotErr) {
		t.Fatalf("expected error to wrap snapshot failure, got %v", err)
	}
}

func TestProcessBlockEvent_CacheMissReturnsError(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §5 requires cache misses to fail block processing.
	svc := newTestService(t, serviceTestDeps{
		config:      validServiceConfig(),
		consumer:    &testutil.MockSQSConsumer{},
		cacheReader: testutil.NewMockBlockCache(),
		txManager:   &testutil.MockTxManager{},
		userRepo:    &testutil.MockUserRepository{},
		snapshotter: &mockSnapshotter{},
		tokens:      nil,
	})

	err := svc.processBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 19000016, Version: 0})
	if err == nil {
		t.Fatal("expected cache miss to return an error, got nil")
	}
	if !strings.Contains(err.Error(), "receipts") {
		t.Fatalf("expected cache miss error to mention receipts, got %v", err)
	}
}

func TestProcessBlockEvent_CacheLoadFailureReturnsError(t *testing.T) {
	// Spec: plan-transfer-user-discovery.md §5 requires cache load failures to fail block processing.
	cacheErr := errors.New("cache unavailable")
	cache := testutil.NewMockBlockCache()
	cache.SetError(cacheErr)

	svc := newTestService(t, serviceTestDeps{
		config:      validServiceConfig(),
		consumer:    &testutil.MockSQSConsumer{},
		cacheReader: cache,
		txManager:   &testutil.MockTxManager{},
		userRepo:    &testutil.MockUserRepository{},
		snapshotter: &mockSnapshotter{},
		tokens:      nil,
	})

	err := svc.processBlockEvent(context.Background(), outbound.BlockEvent{ChainID: 1, BlockNumber: 19000017, Version: 0})
	if err == nil {
		t.Fatal("expected cache load failure to return an error, got nil")
	}
	if !errors.Is(err, cacheErr) {
		t.Fatalf("expected error to wrap cache failure, got %v", err)
	}
}

func newTestService(t *testing.T, deps serviceTestDeps) *Service {
	t.Helper()

	service, err := NewService(
		deps.config,
		deps.consumer,
		deps.cacheReader,
		deps.txManager,
		deps.userRepo,
		deps.snapshotter,
		deps.tokens,
		testutil.DiscardLogger(),
	)
	if err != nil {
		t.Fatalf("NewService returned unexpected error: %v", err)
	}

	return service
}

func validServiceConfig() shared.SQSConsumerConfig {
	return shared.SQSConsumerConfig{
		ChainID: 1,
		Logger:  testutil.DiscardLogger(),
	}
}

func mustMarshalReceipts(t *testing.T, receipts []shared.TransactionReceipt) json.RawMessage {
	t.Helper()

	data, err := json.Marshal(receipts)
	if err != nil {
		t.Fatalf("marshal receipts: %v", err)
	}

	return data
}

func bigIntFromInt64(value int64) *big.Int {
	return big.NewInt(value)
}

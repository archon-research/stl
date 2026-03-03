package maple_indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

type mockConsumer = testutil.MockSQSConsumer

type mockMapleClient struct {
	mu                            sync.Mutex
	getAllActiveLoansAtBlockFn    func(ctx context.Context, blockNumber uint64) ([]outbound.MapleActiveLoan, error)
	getAllActiveLoansAtBlockCalls int
}

func (m *mockMapleClient) GetAllActiveLoansAtBlock(ctx context.Context, blockNumber uint64) ([]outbound.MapleActiveLoan, error) {
	m.mu.Lock()
	m.getAllActiveLoansAtBlockCalls++
	m.mu.Unlock()
	if m.getAllActiveLoansAtBlockFn != nil {
		return m.getAllActiveLoansAtBlockFn(ctx, blockNumber)
	}
	return nil, nil
}

type mockMaplePositionRepo struct {
	mu                           sync.Mutex
	saveBorrowerSnapshotsFn      func(ctx context.Context, snapshots []*entity.MapleBorrower) error
	saveCollateralSnapshotsFn    func(ctx context.Context, snapshots []*entity.MapleCollateral) error
	saveBorrowerSnapshotsCalls   int
	saveCollateralSnapshotsCalls int
	lastSavedBorrowers           []*entity.MapleBorrower
	lastSavedCollateral          []*entity.MapleCollateral
}

func (m *mockMaplePositionRepo) SaveBorrowerSnapshots(ctx context.Context, snapshots []*entity.MapleBorrower) error {
	m.mu.Lock()
	m.saveBorrowerSnapshotsCalls++
	m.lastSavedBorrowers = snapshots
	m.mu.Unlock()
	if m.saveBorrowerSnapshotsFn != nil {
		return m.saveBorrowerSnapshotsFn(ctx, snapshots)
	}
	return nil
}

func (m *mockMaplePositionRepo) SaveCollateralSnapshots(ctx context.Context, snapshots []*entity.MapleCollateral) error {
	m.mu.Lock()
	m.saveCollateralSnapshotsCalls++
	m.lastSavedCollateral = snapshots
	m.mu.Unlock()
	if m.saveCollateralSnapshotsFn != nil {
		return m.saveCollateralSnapshotsFn(ctx, snapshots)
	}
	return nil
}

type mockUserRepo struct {
	mu                   sync.Mutex
	getOrCreateUserFn    func(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error)
	getOrCreateUserCalls int
}

func (m *mockUserRepo) UpsertUsers(ctx context.Context, users []*entity.User) error {
	return nil
}

func (m *mockUserRepo) GetOrCreateUser(ctx context.Context, tx pgx.Tx, user entity.User) (int64, error) {
	m.mu.Lock()
	m.getOrCreateUserCalls++
	m.mu.Unlock()
	if m.getOrCreateUserFn != nil {
		return m.getOrCreateUserFn(ctx, tx, user)
	}
	return 1, nil
}

func (m *mockUserRepo) UpsertUserProtocolMetadata(ctx context.Context, metadata []*entity.UserProtocolMetadata) error {
	return nil
}

type mockTxManager struct {
	withTransactionFn    func(ctx context.Context, fn func(tx pgx.Tx) error) error
	withTransactionCalls int
}

func (m *mockTxManager) WithTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	m.withTransactionCalls++
	if m.withTransactionFn != nil {
		return m.withTransactionFn(ctx, fn)
	}
	return fn(nil)
}

type mockProtocolRepo struct {
	mu                     sync.Mutex
	getProtocolByAddrFn    func(ctx context.Context, chainID int64, address common.Address) (*entity.Protocol, error)
	getProtocolByAddrCalls int
}

func (m *mockProtocolRepo) GetProtocolByAddress(ctx context.Context, chainID int64, address common.Address) (*entity.Protocol, error) {
	m.mu.Lock()
	m.getProtocolByAddrCalls++
	m.mu.Unlock()
	if m.getProtocolByAddrFn != nil {
		return m.getProtocolByAddrFn(ctx, chainID, address)
	}
	return nil, nil
}

func (m *mockProtocolRepo) UpsertReserveData(ctx context.Context, tx pgx.Tx, data []*entity.SparkLendReserveData) error {
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func validServiceConfig() Config {
	return Config{
		ChainID:         1,
		ProtocolAddress: common.HexToAddress("0x804a6F5F667170F545Bf14e5DDB48C70B788390C"),
		Logger:          testutil.DiscardLogger(),
	}
}

func defaultProtocolRepo() *mockProtocolRepo {
	return &mockProtocolRepo{
		getProtocolByAddrFn: func(_ context.Context, chainID int64, address common.Address) (*entity.Protocol, error) {
			return &entity.Protocol{
				ID:             1,
				ChainID:        int(chainID),
				Address:        address.Bytes(),
				Name:           "Maple Finance",
				ProtocolType:   "rwa",
				CreatedAtBlock: 11964925,
				Metadata:       map[string]any{},
			}, nil
		},
	}
}

// testPoolAddress returns a common.Address for "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b".
func testPoolAddress() common.Address {
	return common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b")
}

func makeMapleBlockEventJSON(blockNumber int64, blockTimestamp int64) string {
	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        1,
		BlockHash:      "0xabc123",
		BlockTimestamp: blockTimestamp,
	}
	data, _ := json.Marshal(event)
	return string(data)
}

func defaultLoans() []outbound.MapleActiveLoan {
	return []outbound.MapleActiveLoan{
		{
			LoanID:        common.HexToAddress("0x00000000000000000000000000000000000000aa"),
			Borrower:      common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e"),
			State:         "Active",
			PrincipalOwed: big.NewInt(200_000_000),
			AcmRatio:      big.NewInt(1_650_000),
			Collateral: outbound.MapleLoanCollateral{
				Asset:            "BTC",
				AssetAmount:      big.NewInt(1_000_000_000),
				AssetValueUSD:    big.NewInt(60_000_000),
				Decimals:         8,
				State:            "Deposited",
				Custodian:        "ANCHORAGE",
				LiquidationLevel: big.NewInt(1_500_000),
			},
			PoolAddress:       testPoolAddress(),
			PoolName:          "Syrup USDC",
			PoolAssetSymbol:   "USDC",
			PoolAssetDecimals: 6,
		},
	}
}

func blockingConsumer() *mockConsumer {
	return &mockConsumer{
		ReceiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
}

func defaultClientSetup(client *mockMapleClient) {
	client.getAllActiveLoansAtBlockFn = func(_ context.Context, _ uint64) ([]outbound.MapleActiveLoan, error) {
		return defaultLoans(), nil
	}
}

func defaultTxManager() *mockTxManager {
	return &mockTxManager{}
}

func defaultUserRepo() *mockUserRepo {
	return &mockUserRepo{}
}

func defaultMaplePositionRepo() *mockMaplePositionRepo {
	return &mockMaplePositionRepo{}
}

// ---------------------------------------------------------------------------
// TestNewService
// ---------------------------------------------------------------------------

func TestNewService(t *testing.T) {
	consumer := &mockConsumer{}
	mapleAPI := &mockMapleClient{}
	maplePositionRepo := &mockMaplePositionRepo{}
	userRepo := &mockUserRepo{}
	txManager := &mockTxManager{}
	protocolRepo := &mockProtocolRepo{}
	protocolRepo.getProtocolByAddrFn = func(_ context.Context, _ int64, _ common.Address) (*entity.Protocol, error) {
		return &entity.Protocol{
			ID:             1,
			ChainID:        1,
			Address:        common.HexToAddress("0x804a6F5F667170F545Bf14e5DDB48C70B788390C").Bytes(),
			Name:           "Maple Finance",
			ProtocolType:   "rwa",
			CreatedAtBlock: 11964925,
			Metadata:       map[string]any{},
		}, nil
	}

	tests := []struct {
		name              string
		config            Config
		consumer          outbound.SQSConsumer
		mapleAPI          outbound.MapleClient
		txManager         outbound.TxManager
		userRepo          outbound.UserRepository
		maplePositionRepo outbound.MaplePositionRepository
		protocolRepo      outbound.ProtocolRepository
		wantErr           bool
		errContains       string
	}{
		{
			name:              "success with all valid params",
			config:            validServiceConfig(),
			consumer:          consumer,
			mapleAPI:          mapleAPI,
			txManager:         txManager,
			userRepo:          userRepo,
			maplePositionRepo: maplePositionRepo,
			protocolRepo:      protocolRepo,
		},
		{
			name:              "error nil consumer",
			config:            validServiceConfig(),
			consumer:          nil,
			mapleAPI:          mapleAPI,
			txManager:         txManager,
			userRepo:          userRepo,
			maplePositionRepo: maplePositionRepo,
			protocolRepo:      protocolRepo,
			wantErr:           true,
			errContains:       "consumer cannot be nil",
		},
		{
			name:              "error nil mapleAPI",
			config:            validServiceConfig(),
			consumer:          consumer,
			mapleAPI:          nil,
			txManager:         txManager,
			userRepo:          userRepo,
			maplePositionRepo: maplePositionRepo,
			protocolRepo:      protocolRepo,
			wantErr:           true,
			errContains:       "mapleAPI cannot be nil",
		},
		{
			name:              "error nil txManager",
			config:            validServiceConfig(),
			consumer:          consumer,
			mapleAPI:          mapleAPI,
			txManager:         nil,
			userRepo:          userRepo,
			maplePositionRepo: maplePositionRepo,
			protocolRepo:      protocolRepo,
			wantErr:           true,
			errContains:       "txManager cannot be nil",
		},
		{
			name:              "error nil userRepo",
			config:            validServiceConfig(),
			consumer:          consumer,
			mapleAPI:          mapleAPI,
			txManager:         txManager,
			userRepo:          nil,
			maplePositionRepo: maplePositionRepo,
			protocolRepo:      protocolRepo,
			wantErr:           true,
			errContains:       "userRepo cannot be nil",
		},
		{
			name:              "error nil maplePositionRepo",
			config:            validServiceConfig(),
			consumer:          consumer,
			mapleAPI:          mapleAPI,
			txManager:         txManager,
			userRepo:          userRepo,
			maplePositionRepo: nil,
			protocolRepo:      protocolRepo,
			wantErr:           true,
			errContains:       "maplePositionRepo cannot be nil",
		},
		{
			name: "error protocol address zero",
			config: Config{
				ProtocolAddress: common.Address{},
				Logger:          testutil.DiscardLogger(),
			},
			consumer:          consumer,
			mapleAPI:          mapleAPI,
			txManager:         txManager,
			userRepo:          userRepo,
			maplePositionRepo: maplePositionRepo,
			protocolRepo:      protocolRepo,
			wantErr:           true,
			errContains:       "protocolAddress must be set",
		},
		{
			name: "error nil protocol repo",
			config: Config{
				ProtocolAddress: common.HexToAddress("0x804a6F5F667170F545Bf14e5DDB48C70B788390C"),
				Logger:          testutil.DiscardLogger(),
			},
			consumer:          consumer,
			mapleAPI:          mapleAPI,
			txManager:         txManager,
			userRepo:          userRepo,
			maplePositionRepo: maplePositionRepo,
			protocolRepo:      nil,
			wantErr:           true,
			errContains:       "protocolRepo cannot be nil",
		},
		{
			name: "defaults are applied for zero config",
			config: Config{
				ProtocolAddress: common.HexToAddress("0x804a6F5F667170F545Bf14e5DDB48C70B788390C"),
			},
			consumer:          consumer,
			mapleAPI:          mapleAPI,
			txManager:         txManager,
			userRepo:          userRepo,
			maplePositionRepo: maplePositionRepo,
			protocolRepo:      protocolRepo,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := NewService(
				tc.config,
				tc.consumer,
				tc.mapleAPI,
				tc.txManager,
				tc.userRepo,
				tc.maplePositionRepo,
				tc.protocolRepo,
			)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.errContains)
				}
				if svc != nil {
					t.Error("expected nil service on error")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if svc == nil {
				t.Fatal("expected non-nil service")
			}
			if svc.consumer == nil {
				t.Error("consumer should not be nil")
			}
			if svc.mapleAPI == nil {
				t.Error("mapleAPI should not be nil")
			}
			if svc.maplePositionRepo == nil {
				t.Error("maplePositionRepo should not be nil")
			}
			if svc.logger == nil {
				t.Error("logger should not be nil")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestStartStop
// ---------------------------------------------------------------------------

func TestStartStop(t *testing.T) {
	t.Run("start and stop", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			defaultMaplePositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if svc.ctx.Err() != nil {
			t.Error("context should not be cancelled before Stop")
		}

		if err := svc.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}

		if svc.ctx.Err() == nil {
			t.Error("context should be cancelled after Stop")
		}
	})

	t.Run("stop without start", func(t *testing.T) {
		protocolRepo := defaultProtocolRepo()
		consumer := &mockConsumer{}
		svc, err := NewService(
			validServiceConfig(),
			consumer,
			&mockMapleClient{},
			defaultTxManager(),
			defaultUserRepo(),
			defaultMaplePositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Stop(); err != nil {
			t.Errorf("Stop without Start should not error, got: %v", err)
		}

		consumer.Mu.Lock()
		if consumer.CloseCalls != 1 {
			t.Errorf("Close calls = %d, want 1", consumer.CloseCalls)
		}
		consumer.Mu.Unlock()
	})
}

// ---------------------------------------------------------------------------
// TestProcessBlock (end-to-end message processing via processBlock)
// ---------------------------------------------------------------------------

func TestProcessBlock(t *testing.T) {
	blockTimestamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	t.Run("success: borrowers and collateral persisted", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)
		positionRepo := defaultMaplePositionRepo()

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			positionRepo,
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{
			ChainID:        1,
			BlockNumber:    21000000,
			Version:        2,
			BlockHash:      "0xabc",
			BlockTimestamp: blockTimestamp,
		}

		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		positionRepo.mu.Lock()
		defer positionRepo.mu.Unlock()
		if positionRepo.saveBorrowerSnapshotsCalls != 1 {
			t.Errorf("SaveBorrowerSnapshots calls = %d, want 1", positionRepo.saveBorrowerSnapshotsCalls)
		}
		if len(positionRepo.lastSavedBorrowers) != 1 {
			t.Fatalf("borrowers count = %d, want 1", len(positionRepo.lastSavedBorrowers))
		}
		borrower := positionRepo.lastSavedBorrowers[0]
		if borrower.PoolAsset != "USDC" {
			t.Errorf("borrower PoolAsset = %q, want %q", borrower.PoolAsset, "USDC")
		}
		if borrower.PoolDecimals != 6 {
			t.Errorf("borrower PoolDecimals = %d, want 6", borrower.PoolDecimals)
		}
		if borrower.Amount.Cmp(big.NewInt(200_000_000)) != 0 {
			t.Errorf("borrower Amount = %v, want 200000000", borrower.Amount)
		}
		if borrower.BlockVersion != event.Version {
			t.Errorf("borrower BlockVersion = %d, want %d", borrower.BlockVersion, event.Version)
		}
		if positionRepo.saveCollateralSnapshotsCalls != 1 {
			t.Errorf("SaveCollateralSnapshots calls = %d, want 1", positionRepo.saveCollateralSnapshotsCalls)
		}
		if len(positionRepo.lastSavedCollateral) != 1 {
			t.Fatalf("collateral count = %d, want 1", len(positionRepo.lastSavedCollateral))
		}
		collateral := positionRepo.lastSavedCollateral[0]
		if collateral.CollateralAsset != "BTC" {
			t.Errorf("collateral CollateralAsset = %q, want %q", collateral.CollateralAsset, "BTC")
		}
		if collateral.CollateralDecimals != 8 {
			t.Errorf("collateral CollateralDecimals = %d, want 8", collateral.CollateralDecimals)
		}
		if collateral.Amount.Cmp(big.NewInt(1_000_000_000)) != 0 {
			t.Errorf("collateral Amount = %v, want 1000000000", collateral.Amount)
		}
		if collateral.Custodian != "ANCHORAGE" {
			t.Errorf("collateral Custodian = %q, want %q", collateral.Custodian, "ANCHORAGE")
		}
		if collateral.State != "Deposited" {
			t.Errorf("collateral State = %q, want %q", collateral.State, "Deposited")
		}
		if collateral.BlockVersion != event.Version {
			t.Errorf("collateral BlockVersion = %d, want %d", collateral.BlockVersion, event.Version)
		}
	})

	t.Run("no loans found: no saves", func(t *testing.T) {
		client := &mockMapleClient{}
		client.getAllActiveLoansAtBlockFn = func(_ context.Context, _ uint64) ([]outbound.MapleActiveLoan, error) {
			return nil, nil
		}
		positionRepo := defaultMaplePositionRepo()

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			positionRepo,
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		positionRepo.mu.Lock()
		if positionRepo.saveBorrowerSnapshotsCalls != 0 {
			t.Errorf("SaveBorrowerSnapshots calls = %d, want 0", positionRepo.saveBorrowerSnapshotsCalls)
		}
		if positionRepo.saveCollateralSnapshotsCalls != 0 {
			t.Errorf("SaveCollateralSnapshots calls = %d, want 0", positionRepo.saveCollateralSnapshotsCalls)
		}
		positionRepo.mu.Unlock()
	})

	t.Run("GetAllActiveLoansAtBlock error propagates", func(t *testing.T) {
		client := &mockMapleClient{}
		client.getAllActiveLoansAtBlockFn = func(_ context.Context, _ uint64) ([]outbound.MapleActiveLoan, error) {
			return nil, fmt.Errorf("graphql timeout")
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			defaultMaplePositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "graphql timeout") {
			t.Errorf("error %q should contain 'graphql timeout'", err.Error())
		}
	})

	t.Run("save borrowers error propagates", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)
		positionRepo := defaultMaplePositionRepo()
		positionRepo.saveBorrowerSnapshotsFn = func(_ context.Context, _ []*entity.MapleBorrower) error {
			return fmt.Errorf("database write failure")
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			positionRepo,
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "database write failure") {
			t.Errorf("error %q should contain 'database write failure'", err.Error())
		}
	})

	t.Run("save collateral error propagates", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)
		positionRepo := defaultMaplePositionRepo()
		positionRepo.saveCollateralSnapshotsFn = func(_ context.Context, _ []*entity.MapleCollateral) error {
			return fmt.Errorf("collateral write failure")
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			positionRepo,
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "collateral write failure") {
			t.Errorf("error %q should contain 'collateral write failure'", err.Error())
		}
	})

	t.Run("success: single transaction for users", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)
		txMgr := defaultTxManager()

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			txMgr,
			defaultUserRepo(),
			defaultMaplePositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		// User resolution and position persistence happen in a single transaction
		if txMgr.withTransactionCalls != 1 {
			t.Errorf("WithTransaction calls = %d, want 1", txMgr.withTransactionCalls)
		}
	})

	t.Run("deduplication: same borrower across loans calls GetOrCreateUser once", func(t *testing.T) {
		sameBorrower := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
		client := &mockMapleClient{}
		client.getAllActiveLoansAtBlockFn = func(_ context.Context, _ uint64) ([]outbound.MapleActiveLoan, error) {
			return []outbound.MapleActiveLoan{
				{
					LoanID:        common.HexToAddress("0xaa"),
					Borrower:      sameBorrower,
					State:         "Active",
					PrincipalOwed: big.NewInt(100_000_000),
					AcmRatio:      big.NewInt(1_500_000),
					Collateral: outbound.MapleLoanCollateral{
						Asset:       "BTC",
						AssetAmount: big.NewInt(500_000_000),
						Decimals:    8,
						State:       "Deposited",
					},
					PoolAddress:       testPoolAddress(),
					PoolName:          "Pool A",
					PoolAssetSymbol:   "USDC",
					PoolAssetDecimals: 6,
				},
				{
					LoanID:        common.HexToAddress("0xbb"),
					Borrower:      sameBorrower,
					State:         "Active",
					PrincipalOwed: big.NewInt(200_000_000),
					AcmRatio:      big.NewInt(1_600_000),
					Collateral: outbound.MapleLoanCollateral{
						Asset:       "BTC",
						AssetAmount: big.NewInt(300_000_000),
						Decimals:    8,
						State:       "Deposited",
					},
					PoolAddress:       testPoolAddress(),
					PoolName:          "Pool B",
					PoolAssetSymbol:   "USDC",
					PoolAssetDecimals: 6,
				},
			}, nil
		}
		userRepo := defaultUserRepo()
		positionRepo := defaultMaplePositionRepo()

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			userRepo,
			positionRepo,
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		userRepo.mu.Lock()
		defer userRepo.mu.Unlock()
		// Same borrower appears in 2 loans, but GetOrCreateUser should only be called once
		if userRepo.getOrCreateUserCalls != 1 {
			t.Errorf("GetOrCreateUser calls = %d, want 1 (deduplication)", userRepo.getOrCreateUserCalls)
		}

		positionRepo.mu.Lock()
		defer positionRepo.mu.Unlock()
		if len(positionRepo.lastSavedBorrowers) != 2 {
			t.Errorf("borrowers count = %d, want 2", len(positionRepo.lastSavedBorrowers))
		}
		if len(positionRepo.lastSavedCollateral) != 2 {
			t.Errorf("collateral count = %d, want 2", len(positionRepo.lastSavedCollateral))
		}
	})

	t.Run("user resolution error causes processBlock to fail", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)
		userRepo := &mockUserRepo{
			getOrCreateUserFn: func(_ context.Context, _ pgx.Tx, _ entity.User) (int64, error) {
				return 0, fmt.Errorf("database connection lost")
			},
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			userRepo,
			defaultMaplePositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error from user resolution failure")
		}
		if !strings.Contains(err.Error(), "resolving users") {
			t.Errorf("error %q should contain 'resolving users'", err.Error())
		}
		if !strings.Contains(err.Error(), "database connection lost") {
			t.Errorf("error %q should contain 'database connection lost'", err.Error())
		}
	})

	t.Run("chain mismatch returns error", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			defaultMaplePositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{ChainID: 999, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "unexpected chain ID") {
			t.Errorf("error %q should contain 'unexpected chain ID'", err.Error())
		}
	})

	t.Run("invalid block number returns error", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			defaultMaplePositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{ChainID: 1, BlockNumber: 0, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "invalid block number") {
			t.Errorf("error %q should contain 'invalid block number'", err.Error())
		}
	})

	t.Run("missing block hash returns error", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			defaultMaplePositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := outbound.BlockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "block hash missing") {
			t.Errorf("error %q should contain 'block hash missing'", err.Error())
		}
	})
}

// ---------------------------------------------------------------------------
// TestProcessMessages (SQS message handling)
// ---------------------------------------------------------------------------

func TestProcessMessages(t *testing.T) {
	blockTimestamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	t.Run("end-to-end: message processed and deleted", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)
		positionRepo := defaultMaplePositionRepo()

		messageDelivered := false
		body := makeMapleBlockEventJSON(21000000, blockTimestamp)
		consumer := &mockConsumer{
			ReceiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !messageDelivered {
					messageDelivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-1", Body: body, ReceiptHandle: "receipt-1"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validServiceConfig()
		cfg.PollInterval = 1 * time.Millisecond

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			cfg,
			consumer,
			client,
			defaultTxManager(),
			defaultUserRepo(),
			positionRepo,
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		testutil.WaitForCondition(t, 2*time.Second, func() bool {
			positionRepo.mu.Lock()
			defer positionRepo.mu.Unlock()
			return positionRepo.saveBorrowerSnapshotsCalls >= 1
		}, "SaveBorrowerSnapshots to be called")

		consumer.Mu.Lock()
		if consumer.DeleteMessageCalls < 1 {
			t.Errorf("DeleteMessage calls = %d, want >= 1", consumer.DeleteMessageCalls)
		}
		consumer.Mu.Unlock()

		_ = svc.Stop()
	})

	t.Run("SQS receive error: logged, not fatal", func(t *testing.T) {
		consumer := &mockConsumer{
			ReceiveMessagesFn: func(_ context.Context, _ int) ([]outbound.SQSMessage, error) {
				return nil, fmt.Errorf("SQS unavailable")
			},
		}

		cfg := validServiceConfig()
		cfg.PollInterval = 1 * time.Millisecond

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			cfg,
			consumer,
			&mockMapleClient{},
			defaultTxManager(),
			defaultUserRepo(),
			defaultMaplePositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		consumer.Mu.Lock()
		if consumer.ReceiveMessageCalls == 0 {
			t.Error("expected at least one ReceiveMessage call")
		}
		consumer.Mu.Unlock()

		_ = svc.Stop()
	})

	t.Run("empty messages: no processing", func(t *testing.T) {
		positionRepo := defaultMaplePositionRepo()
		consumer := &mockConsumer{
			ReceiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				return nil, nil
			},
		}

		cfg := validServiceConfig()
		cfg.PollInterval = 1 * time.Millisecond

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			cfg,
			consumer,
			&mockMapleClient{},
			defaultTxManager(),
			defaultUserRepo(),
			positionRepo,
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		positionRepo.mu.Lock()
		if positionRepo.saveBorrowerSnapshotsCalls != 0 {
			t.Errorf("SaveBorrowerSnapshots calls = %d, want 0", positionRepo.saveBorrowerSnapshotsCalls)
		}
		positionRepo.mu.Unlock()

		_ = svc.Stop()
	})

	t.Run("invalid JSON: message not deleted", func(t *testing.T) {
		delivered := false
		consumer := &mockConsumer{
			ReceiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-bad", Body: "not json{{{", ReceiptHandle: "receipt-bad"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validServiceConfig()
		cfg.PollInterval = 1 * time.Millisecond

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			cfg,
			consumer,
			&mockMapleClient{},
			defaultTxManager(),
			defaultUserRepo(),
			defaultMaplePositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		consumer.Mu.Lock()
		if consumer.DeleteMessageCalls != 0 {
			t.Errorf("DeleteMessage calls = %d, want 0 (invalid JSON)", consumer.DeleteMessageCalls)
		}
		consumer.Mu.Unlock()

		_ = svc.Stop()
	})

	t.Run("delete message error: logged but processing continues", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)
		positionRepo := defaultMaplePositionRepo()

		delivered := false
		body := makeMapleBlockEventJSON(21000000, blockTimestamp)
		consumer := &mockConsumer{
			ReceiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-del-err", Body: body, ReceiptHandle: "receipt-del-err"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
			DeleteMessageFn: func(_ context.Context, _ string) error {
				return fmt.Errorf("SQS delete failed")
			},
		}

		cfg := validServiceConfig()
		cfg.PollInterval = 1 * time.Millisecond

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			cfg,
			consumer,
			client,
			defaultTxManager(),
			defaultUserRepo(),
			positionRepo,
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		testutil.WaitForCondition(t, 2*time.Second, func() bool {
			positionRepo.mu.Lock()
			defer positionRepo.mu.Unlock()
			return positionRepo.saveBorrowerSnapshotsCalls >= 1
		}, "SaveBorrowerSnapshots to be called")

		consumer.Mu.Lock()
		if consumer.DeleteMessageCalls < 1 {
			t.Errorf("DeleteMessage calls = %d, want >= 1", consumer.DeleteMessageCalls)
		}
		consumer.Mu.Unlock()

		_ = svc.Stop()
	})
}

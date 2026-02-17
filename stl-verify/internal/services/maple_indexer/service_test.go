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
	listPoolsFn                   func(ctx context.Context) ([]outbound.MaplePoolInfo, error)
	getBorrowerCollateralFn       func(ctx context.Context, poolAddress common.Address, blockNumber uint64) ([]outbound.MapleBorrowerLoan, error)
	getAllActiveLoansAtBlockFn    func(ctx context.Context, blockNumber uint64) ([]outbound.MapleActiveLoan, error)
	listPoolsCalls                int
	getBorrowerCollateralCalls    int
	getAllActiveLoansAtBlockCalls int
}

func (m *mockMapleClient) ListPools(ctx context.Context) ([]outbound.MaplePoolInfo, error) {
	m.mu.Lock()
	m.listPoolsCalls++
	m.mu.Unlock()
	if m.listPoolsFn != nil {
		return m.listPoolsFn(ctx)
	}
	return nil, nil
}

func (m *mockMapleClient) GetBorrowerCollateralAtBlock(ctx context.Context, poolAddress common.Address, blockNumber uint64) ([]outbound.MapleBorrowerLoan, error) {
	m.mu.Lock()
	m.getBorrowerCollateralCalls++
	m.mu.Unlock()
	if m.getBorrowerCollateralFn != nil {
		return m.getBorrowerCollateralFn(ctx, poolAddress, blockNumber)
	}
	return nil, nil
}

func (m *mockMapleClient) GetPoolCollateral(ctx context.Context, poolAddress common.Address) (*outbound.MaplePoolData, error) {
	return nil, nil
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

type mockPositionRepo struct {
	mu                              sync.Mutex
	upsertBorrowersFn               func(ctx context.Context, borrowers []*entity.Borrower) error
	upsertBorrowersTxFn             func(ctx context.Context, tx pgx.Tx, borrowers []*entity.Borrower) error
	upsertBorrowerCollateralFn      func(ctx context.Context, collateral []*entity.BorrowerCollateral) error
	upsertBorrowerCollateralTxFn    func(ctx context.Context, tx pgx.Tx, collateral []*entity.BorrowerCollateral) error
	upsertBorrowersCalls            int
	upsertBorrowersTxCalls          int
	upsertBorrowerCollateralCalls   int
	upsertBorrowerCollateralTxCalls int
	lastUpsertedBorrowers           []*entity.Borrower
	lastUpsertedCollateral          []*entity.BorrowerCollateral
}

func (m *mockPositionRepo) UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error {
	m.mu.Lock()
	m.upsertBorrowersCalls++
	m.lastUpsertedBorrowers = borrowers
	m.mu.Unlock()
	if m.upsertBorrowersFn != nil {
		return m.upsertBorrowersFn(ctx, borrowers)
	}
	return nil
}

func (m *mockPositionRepo) UpsertBorrowersTx(ctx context.Context, tx pgx.Tx, borrowers []*entity.Borrower) error {
	m.mu.Lock()
	m.upsertBorrowersTxCalls++
	m.lastUpsertedBorrowers = borrowers
	m.mu.Unlock()
	if m.upsertBorrowersTxFn != nil {
		return m.upsertBorrowersTxFn(ctx, tx, borrowers)
	}
	return nil
}

func (m *mockPositionRepo) UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error {
	m.mu.Lock()
	m.upsertBorrowerCollateralCalls++
	m.lastUpsertedCollateral = collateral
	m.mu.Unlock()
	if m.upsertBorrowerCollateralFn != nil {
		return m.upsertBorrowerCollateralFn(ctx, collateral)
	}
	return nil
}

func (m *mockPositionRepo) UpsertBorrowerCollateralTx(ctx context.Context, tx pgx.Tx, collateral []*entity.BorrowerCollateral) error {
	m.mu.Lock()
	m.upsertBorrowerCollateralTxCalls++
	m.lastUpsertedCollateral = collateral
	m.mu.Unlock()
	if m.upsertBorrowerCollateralTxFn != nil {
		return m.upsertBorrowerCollateralTxFn(ctx, tx, collateral)
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

type mockTokenRepo struct {
	mu                      sync.Mutex
	getTokenIDBySymbolFn    func(ctx context.Context, chainID int64, symbol string) (int64, error)
	getTokenIDBySymbolCalls int
}

func (m *mockTokenRepo) UpsertTokens(ctx context.Context, tokens []*entity.Token) error {
	return nil
}

func (m *mockTokenRepo) UpsertReceiptTokens(ctx context.Context, tokens []*entity.ReceiptToken) error {
	return nil
}

func (m *mockTokenRepo) UpsertDebtTokens(ctx context.Context, tokens []*entity.DebtToken) error {
	return nil
}

func (m *mockTokenRepo) GetOrCreateToken(ctx context.Context, tx pgx.Tx, chainID int64, address common.Address, symbol string, decimals int, createdAtBlock int64) (int64, error) {
	return 1, nil
}

func (m *mockTokenRepo) GetTokenIDBySymbol(ctx context.Context, chainID int64, symbol string) (int64, error) {
	m.mu.Lock()
	m.getTokenIDBySymbolCalls++
	m.mu.Unlock()
	if m.getTokenIDBySymbolFn != nil {
		return m.getTokenIDBySymbolFn(ctx, chainID, symbol)
	}
	return 1, nil
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
	event := blockEvent{
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
				Asset:         "BTC",
				AssetAmount:   big.NewInt(1_000_000_000),
				AssetValueUSD: big.NewInt(60_000_000),
				Decimals:      8,
				State:         "Deposited",
				Custodian:     "ANCHORAGE",
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

func defaultTokenRepo() *mockTokenRepo {
	return &mockTokenRepo{
		getTokenIDBySymbolFn: func(_ context.Context, _ int64, symbol string) (int64, error) {
			switch strings.ToUpper(symbol) {
			case "USDC":
				return 1, nil
			case "BTC":
				return 2, nil
			default:
				return 0, fmt.Errorf("token not found for symbol %s", symbol)
			}
		},
	}
}

func defaultPositionRepo() *mockPositionRepo {
	return &mockPositionRepo{}
}

// ---------------------------------------------------------------------------
// TestNewService
// ---------------------------------------------------------------------------

func TestNewService(t *testing.T) {
	consumer := &mockConsumer{}
	mapleAPI := &mockMapleClient{}
	positionRepo := &mockPositionRepo{}
	userRepo := &mockUserRepo{}
	tokenRepo := &mockTokenRepo{}
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
		name         string
		config       Config
		consumer     outbound.SQSConsumer
		mapleAPI     outbound.MapleClient
		txManager    outbound.TxManager
		userRepo     outbound.UserRepository
		tokenRepo    outbound.TokenRepository
		positionRepo outbound.PositionRepository
		protocolRepo outbound.ProtocolRepository
		wantErr      bool
		errContains  string
	}{
		{
			name:         "success with all valid params",
			config:       validServiceConfig(),
			consumer:     consumer,
			mapleAPI:     mapleAPI,
			txManager:    txManager,
			userRepo:     userRepo,
			tokenRepo:    tokenRepo,
			positionRepo: positionRepo,
			protocolRepo: protocolRepo,
		},
		{
			name:         "error nil consumer",
			config:       validServiceConfig(),
			consumer:     nil,
			mapleAPI:     mapleAPI,
			txManager:    txManager,
			userRepo:     userRepo,
			tokenRepo:    tokenRepo,
			positionRepo: positionRepo,
			protocolRepo: protocolRepo,
			wantErr:      true,
			errContains:  "consumer cannot be nil",
		},
		{
			name:         "error nil mapleAPI",
			config:       validServiceConfig(),
			consumer:     consumer,
			mapleAPI:     nil,
			txManager:    txManager,
			userRepo:     userRepo,
			tokenRepo:    tokenRepo,
			positionRepo: positionRepo,
			protocolRepo: protocolRepo,
			wantErr:      true,
			errContains:  "mapleAPI cannot be nil",
		},
		{
			name:         "error nil txManager",
			config:       validServiceConfig(),
			consumer:     consumer,
			mapleAPI:     mapleAPI,
			txManager:    nil,
			userRepo:     userRepo,
			tokenRepo:    tokenRepo,
			positionRepo: positionRepo,
			protocolRepo: protocolRepo,
			wantErr:      true,
			errContains:  "txManager cannot be nil",
		},
		{
			name: "error protocol address zero",
			config: Config{
				ProtocolAddress: common.Address{},
				Logger:          testutil.DiscardLogger(),
			},
			consumer:     consumer,
			mapleAPI:     mapleAPI,
			txManager:    txManager,
			userRepo:     userRepo,
			tokenRepo:    tokenRepo,
			positionRepo: positionRepo,
			protocolRepo: protocolRepo,
			wantErr:      true,
			errContains:  "protocolAddress must be set",
		},
		{
			name: "error nil protocol repo",
			config: Config{
				ProtocolAddress: common.HexToAddress("0x804a6F5F667170F545Bf14e5DDB48C70B788390C"),
				Logger:          testutil.DiscardLogger(),
			},
			consumer:     consumer,
			mapleAPI:     mapleAPI,
			txManager:    txManager,
			userRepo:     userRepo,
			tokenRepo:    tokenRepo,
			positionRepo: positionRepo,
			protocolRepo: nil,
			wantErr:      true,
			errContains:  "protocolRepo cannot be nil",
		},
		{
			name: "defaults are applied for zero config",
			config: Config{
				ProtocolAddress: common.HexToAddress("0x804a6F5F667170F545Bf14e5DDB48C70B788390C"),
			},
			consumer:     consumer,
			mapleAPI:     mapleAPI,
			txManager:    txManager,
			userRepo:     userRepo,
			tokenRepo:    tokenRepo,
			positionRepo: positionRepo,
			protocolRepo: protocolRepo,
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
				tc.tokenRepo,
				tc.positionRepo,
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
			if svc.positionRepo == nil {
				t.Error("positionRepo should not be nil")
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
			defaultTokenRepo(),
			defaultPositionRepo(),
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
			defaultTokenRepo(),
			defaultPositionRepo(),
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
		positionRepo := defaultPositionRepo()

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			defaultTokenRepo(),
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

		event := blockEvent{
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
		if positionRepo.upsertBorrowersCalls != 0 {
			t.Errorf("UpsertBorrowers calls = %d, want 0", positionRepo.upsertBorrowersCalls)
		}
		if positionRepo.upsertBorrowersTxCalls != 1 {
			t.Errorf("UpsertBorrowersTx calls = %d, want 1", positionRepo.upsertBorrowersTxCalls)
		}
		if len(positionRepo.lastUpsertedBorrowers) != 1 {
			t.Errorf("borrowers count = %d, want 1", len(positionRepo.lastUpsertedBorrowers))
		}
		borrower := positionRepo.lastUpsertedBorrowers[0]
		if borrower.ID != 0 {
			t.Errorf("borrower ID = %d, want 0", borrower.ID)
		}
		if borrower.EventType != entity.EventMapleSnapshot {
			t.Errorf("borrower EventType = %v, want %v", borrower.EventType, entity.EventMapleSnapshot)
		}
		if borrower.BlockVersion != event.Version {
			t.Errorf("borrower BlockVersion = %d, want %d", borrower.BlockVersion, event.Version)
		}
		if positionRepo.upsertBorrowerCollateralCalls != 0 {
			t.Errorf("UpsertBorrowerCollateral calls = %d, want 0", positionRepo.upsertBorrowerCollateralCalls)
		}
		if positionRepo.upsertBorrowerCollateralTxCalls != 1 {
			t.Errorf("UpsertBorrowerCollateralTx calls = %d, want 1", positionRepo.upsertBorrowerCollateralTxCalls)
		}
		if len(positionRepo.lastUpsertedCollateral) != 1 {
			t.Errorf("collateral count = %d, want 1", len(positionRepo.lastUpsertedCollateral))
		}
		collateral := positionRepo.lastUpsertedCollateral[0]
		if collateral.ID != 0 {
			t.Errorf("collateral ID = %d, want 0", collateral.ID)
		}
		if collateral.EventType != entity.EventMapleSnapshot {
			t.Errorf("collateral EventType = %v, want %v", collateral.EventType, entity.EventMapleSnapshot)
		}
		if collateral.BlockVersion != event.Version {
			t.Errorf("collateral BlockVersion = %d, want %d", collateral.BlockVersion, event.Version)
		}
	})

	t.Run("no loans found: no upserts", func(t *testing.T) {
		client := &mockMapleClient{}
		client.getAllActiveLoansAtBlockFn = func(_ context.Context, _ uint64) ([]outbound.MapleActiveLoan, error) {
			return nil, nil
		}
		positionRepo := defaultPositionRepo()

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			defaultTokenRepo(),
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

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		positionRepo.mu.Lock()
		if positionRepo.upsertBorrowersTxCalls != 0 {
			t.Errorf("UpsertBorrowersTx calls = %d, want 0", positionRepo.upsertBorrowersTxCalls)
		}
		if positionRepo.upsertBorrowerCollateralTxCalls != 0 {
			t.Errorf("UpsertBorrowerCollateralTx calls = %d, want 0", positionRepo.upsertBorrowerCollateralTxCalls)
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
			defaultTokenRepo(),
			defaultPositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "graphql timeout") {
			t.Errorf("error %q should contain 'graphql timeout'", err.Error())
		}
	})

	t.Run("upsert borrowers error propagates", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)
		positionRepo := defaultPositionRepo()
		positionRepo.upsertBorrowersTxFn = func(_ context.Context, _ pgx.Tx, _ []*entity.Borrower) error {
			return fmt.Errorf("database write failure")
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			defaultTokenRepo(),
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

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "database write failure") {
			t.Errorf("error %q should contain 'database write failure'", err.Error())
		}
	})

	t.Run("upsert borrower collateral error propagates", func(t *testing.T) {
		client := &mockMapleClient{}
		defaultClientSetup(client)
		positionRepo := defaultPositionRepo()
		positionRepo.upsertBorrowerCollateralTxFn = func(_ context.Context, _ pgx.Tx, _ []*entity.BorrowerCollateral) error {
			return fmt.Errorf("collateral write failure")
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			defaultUserRepo(),
			defaultTokenRepo(),
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

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "collateral write failure") {
			t.Errorf("error %q should contain 'collateral write failure'", err.Error())
		}
	})

	t.Run("success: exactly 2 WithTransaction calls", func(t *testing.T) {
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
			defaultTokenRepo(),
			defaultPositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		// 1 for resolveUsers + 1 for position persistence = 2
		if txMgr.withTransactionCalls != 2 {
			t.Errorf("WithTransaction calls = %d, want 2", txMgr.withTransactionCalls)
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
		positionRepo := defaultPositionRepo()

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(
			validServiceConfig(),
			blockingConsumer(),
			client,
			defaultTxManager(),
			userRepo,
			defaultTokenRepo(),
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

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
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
		if len(positionRepo.lastUpsertedBorrowers) != 2 {
			t.Errorf("borrowers count = %d, want 2", len(positionRepo.lastUpsertedBorrowers))
		}
		if len(positionRepo.lastUpsertedCollateral) != 2 {
			t.Errorf("collateral count = %d, want 2", len(positionRepo.lastUpsertedCollateral))
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
			defaultTokenRepo(),
			defaultPositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
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
			defaultTokenRepo(),
			defaultPositionRepo(),
			protocolRepo,
		)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer func() { _ = svc.Stop() }()

		event := blockEvent{ChainID: 999, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "unexpected chain ID") {
			t.Errorf("error %q should contain 'unexpected chain ID'", err.Error())
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
		positionRepo := defaultPositionRepo()

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
			defaultTokenRepo(),
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
			return positionRepo.upsertBorrowersTxCalls >= 1
		}, "UpsertBorrowers to be called")

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
			defaultTokenRepo(),
			defaultPositionRepo(),
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
		positionRepo := defaultPositionRepo()
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
			defaultTokenRepo(),
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
		if positionRepo.upsertBorrowersCalls != 0 {
			t.Errorf("UpsertBorrowers calls = %d, want 0", positionRepo.upsertBorrowersCalls)
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
			defaultTokenRepo(),
			defaultPositionRepo(),
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
		positionRepo := defaultPositionRepo()

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
			defaultTokenRepo(),
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
			return positionRepo.upsertBorrowersTxCalls >= 1
		}, "UpsertBorrowers to be called")

		consumer.Mu.Lock()
		if consumer.DeleteMessageCalls < 1 {
			t.Errorf("DeleteMessage calls = %d, want >= 1", consumer.DeleteMessageCalls)
		}
		consumer.Mu.Unlock()

		_ = svc.Stop()
	})
}

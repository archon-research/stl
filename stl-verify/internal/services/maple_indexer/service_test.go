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
	mu                         sync.Mutex
	listPoolsFn                func(ctx context.Context) ([]outbound.MaplePoolInfo, error)
	getBorrowerCollateralFn    func(ctx context.Context, poolAddress common.Address, blockNumber uint64) ([]outbound.MapleBorrowerLoan, error)
	listPoolsCalls             int
	getBorrowerCollateralCalls int
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

type mockPositionRepo struct {
	mu                            sync.Mutex
	upsertBorrowersFn             func(ctx context.Context, borrowers []*entity.Borrower) error
	upsertBorrowerCollateralFn    func(ctx context.Context, collateral []*entity.BorrowerCollateral) error
	upsertBorrowersCalls          int
	upsertBorrowerCollateralCalls int
	lastUpsertedBorrowers         []*entity.Borrower
	lastUpsertedCollateral        []*entity.BorrowerCollateral
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

func defaultPools() []outbound.MaplePoolInfo {
	return []outbound.MaplePoolInfo{
		{
			Address:       testPoolAddress(),
			Name:          "Syrup USDC",
			AssetSymbol:   "USDC",
			AssetDecimals: 6,
		},
	}
}

func defaultLoans() []outbound.MapleBorrowerLoan {
	return []outbound.MapleBorrowerLoan{
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
		},
	}
}

func defaultPoolData() *outbound.MaplePoolData {
	return &outbound.MaplePoolData{
		TVL: big.NewInt(1_000_000_000_000_000), // $1B with 6 decimals
		Collaterals: []outbound.MapleCollateral{
			{Asset: "BTC", AssetValueUSD: big.NewInt(674_000_000_000_000), AssetDecimals: 8},
			{Asset: "XRP", AssetValueUSD: big.NewInt(317_000_000_000_000), AssetDecimals: 6},
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
	client.listPoolsFn = func(_ context.Context) ([]outbound.MaplePoolInfo, error) {
		return defaultPools(), nil
	}
	client.getBorrowerCollateralFn = func(_ context.Context, _ common.Address, _ uint64) ([]outbound.MapleBorrowerLoan, error) {
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
		defer svc.Stop()

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
		if positionRepo.upsertBorrowersCalls != 1 {
			t.Errorf("UpsertBorrowers calls = %d, want 1", positionRepo.upsertBorrowersCalls)
		}
		if len(positionRepo.lastUpsertedBorrowers) != 1 {
			t.Errorf("borrowers count = %d, want 1", len(positionRepo.lastUpsertedBorrowers))
		}
		borrower := positionRepo.lastUpsertedBorrowers[0]
		if borrower.EventType != entity.EventMapleSnapshot {
			t.Errorf("borrower EventType = %v, want %v", borrower.EventType, entity.EventMapleSnapshot)
		}
		if borrower.BlockVersion != event.Version {
			t.Errorf("borrower BlockVersion = %d, want %d", borrower.BlockVersion, event.Version)
		}
		if positionRepo.upsertBorrowerCollateralCalls != 1 {
			t.Errorf("UpsertBorrowerCollateral calls = %d, want 1", positionRepo.upsertBorrowerCollateralCalls)
		}
		if len(positionRepo.lastUpsertedCollateral) != 1 {
			t.Errorf("collateral count = %d, want 1", len(positionRepo.lastUpsertedCollateral))
		}
		collateral := positionRepo.lastUpsertedCollateral[0]
		if collateral.EventType != entity.EventMapleSnapshot {
			t.Errorf("collateral EventType = %v, want %v", collateral.EventType, entity.EventMapleSnapshot)
		}
		if collateral.BlockVersion != event.Version {
			t.Errorf("collateral BlockVersion = %d, want %d", collateral.BlockVersion, event.Version)
		}
	})

	t.Run("no pools found: no borrower fetches", func(t *testing.T) {
		client := &mockMapleClient{}
		client.listPoolsFn = func(_ context.Context) ([]outbound.MaplePoolInfo, error) {
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
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		client.mu.Lock()
		if client.getBorrowerCollateralCalls != 0 {
			t.Errorf("GetBorrowerCollateralAtBlock calls = %d, want 0", client.getBorrowerCollateralCalls)
		}
		client.mu.Unlock()
	})

	t.Run("borrower fetch error propagates", func(t *testing.T) {
		client := &mockMapleClient{}
		client.listPoolsFn = func(_ context.Context) ([]outbound.MaplePoolInfo, error) {
			return defaultPools(), nil
		}
		client.getBorrowerCollateralFn = func(_ context.Context, _ common.Address, _ uint64) ([]outbound.MapleBorrowerLoan, error) {
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
		defer svc.Stop()

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
		positionRepo.upsertBorrowersFn = func(_ context.Context, _ []*entity.Borrower) error {
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
		defer svc.Stop()

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
		positionRepo.upsertBorrowerCollateralFn = func(_ context.Context, _ []*entity.BorrowerCollateral) error {
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
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockHash: "0xabc", BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "collateral write failure") {
			t.Errorf("error %q should contain 'collateral write failure'", err.Error())
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
		defer svc.Stop()

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
			return positionRepo.upsertBorrowersCalls >= 1
		}, "UpsertBorrowers to be called")

		consumer.Mu.Lock()
		if consumer.DeleteMessageCalls < 1 {
			t.Errorf("DeleteMessage calls = %d, want >= 1", consumer.DeleteMessageCalls)
		}
		consumer.Mu.Unlock()

		svc.Stop()
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

		svc.Stop()
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

		svc.Stop()
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

		svc.Stop()
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
			return positionRepo.upsertBorrowersCalls >= 1
		}, "UpsertBorrowers to be called")

		consumer.Mu.Lock()
		if consumer.DeleteMessageCalls < 1 {
			t.Errorf("DeleteMessage calls = %d, want >= 1", consumer.DeleteMessageCalls)
		}
		consumer.Mu.Unlock()

		svc.Stop()
	})
}

// ---------------------------------------------------------------------------
// TestCalculateBackedBreakdown
// ---------------------------------------------------------------------------

func TestCalculateBackedBreakdown(t *testing.T) {
	tests := []struct {
		name           string
		lendingBalance *big.Int
		poolData       *outbound.MaplePoolData
		wantCount      int
		wantSymbols    []string
		checkValues    bool
		wantPercents   []float64
	}{
		{
			name:           "nil pool data",
			lendingBalance: big.NewInt(200_000_000_000_000),
			poolData:       nil,
			wantCount:      0,
		},
		{
			name:           "zero lending balance",
			lendingBalance: big.NewInt(0),
			poolData:       defaultPoolData(),
			wantCount:      0,
		},
		{
			name:           "zero TVL",
			lendingBalance: big.NewInt(200_000_000_000_000),
			poolData: &outbound.MaplePoolData{
				TVL:         big.NewInt(0),
				Collaterals: []outbound.MapleCollateral{{Asset: "BTC", AssetValueUSD: big.NewInt(100), AssetDecimals: 8}},
			},
			wantCount: 0,
		},
		{
			name:           "standard breakdown with two assets",
			lendingBalance: big.NewInt(200_000_000_000_000), // $200M
			poolData: &outbound.MaplePoolData{
				TVL: big.NewInt(1_000_000_000_000_000), // $1B
				Collaterals: []outbound.MapleCollateral{
					{Asset: "BTC", AssetValueUSD: big.NewInt(674_000_000_000_000), AssetDecimals: 8}, // 67.4%
					{Asset: "XRP", AssetValueUSD: big.NewInt(317_000_000_000_000), AssetDecimals: 6}, // 31.7%
				},
			},
			wantCount:    2,
			wantSymbols:  []string{"BTC", "XRP"},
			checkValues:  true,
			wantPercents: []float64{0.674, 0.317},
		},
		{
			name:           "zero-value collateral entries are filtered",
			lendingBalance: big.NewInt(200_000_000_000_000),
			poolData: &outbound.MaplePoolData{
				TVL: big.NewInt(1_000_000_000_000_000),
				Collaterals: []outbound.MapleCollateral{
					{Asset: "BTC", AssetValueUSD: big.NewInt(500_000_000_000_000), AssetDecimals: 8},
					{Asset: "ZERO", AssetValueUSD: big.NewInt(0), AssetDecimals: 6},
				},
			},
			wantCount:   1,
			wantSymbols: []string{"BTC"},
		},
		{
			name:           "single asset at 100%",
			lendingBalance: big.NewInt(100_000_000_000_000), // $100M
			poolData: &outbound.MaplePoolData{
				TVL: big.NewInt(100_000_000_000_000), // $100M
				Collaterals: []outbound.MapleCollateral{
					{Asset: "ETH", AssetValueUSD: big.NewInt(100_000_000_000_000), AssetDecimals: 18},
				},
			},
			wantCount:    1,
			wantSymbols:  []string{"ETH"},
			checkValues:  true,
			wantPercents: []float64{1.0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backings := CalculateBackedBreakdown(tc.lendingBalance, tc.poolData)

			if len(backings) != tc.wantCount {
				t.Fatalf("count = %d, want %d", len(backings), tc.wantCount)
			}

			for i, b := range backings {
				if i < len(tc.wantSymbols) && b.Symbol != tc.wantSymbols[i] {
					t.Errorf("backings[%d].Symbol = %q, want %q", i, b.Symbol, tc.wantSymbols[i])
				}
			}

			if tc.checkValues {
				for i, b := range backings {
					if i < len(tc.wantPercents) {
						diff := b.Percent - tc.wantPercents[i]
						if diff < -0.001 || diff > 0.001 {
							t.Errorf("backings[%d].Percent = %f, want ~%f", i, b.Percent, tc.wantPercents[i])
						}
					}

					// Verify ValueUSD is positive
					if b.ValueUSD <= 0 {
						t.Errorf("backings[%d].ValueUSD = %f, want > 0", i, b.ValueUSD)
					}

					// Verify TotalUSD is positive
					if b.TotalUSD <= 0 {
						t.Errorf("backings[%d].TotalUSD = %f, want > 0", i, b.TotalUSD)
					}
				}
			}
		})
	}

	t.Run("math precision: ValueUSD = Percent * lendingBalance / 1e6", func(t *testing.T) {
		lendingBalance := big.NewInt(200_000_000_000_000) // $200M (6 decimals)
		poolData := &outbound.MaplePoolData{
			TVL: big.NewInt(1_000_000_000_000_000), // $1B
			Collaterals: []outbound.MapleCollateral{
				{Asset: "BTC", AssetValueUSD: big.NewInt(500_000_000_000_000), AssetDecimals: 8}, // 50%
			},
		}

		backings := CalculateBackedBreakdown(lendingBalance, poolData)
		if len(backings) != 1 {
			t.Fatalf("count = %d, want 1", len(backings))
		}

		b := backings[0]
		// 50% of $200M = $100M
		expectedValueUSD := 100_000_000.0
		diff := b.ValueUSD - expectedValueUSD
		if diff < -1.0 || diff > 1.0 {
			t.Errorf("ValueUSD = %f, want ~%f", b.ValueUSD, expectedValueUSD)
		}

		// Total = $500M
		expectedTotalUSD := 500_000_000.0
		diff = b.TotalUSD - expectedTotalUSD
		if diff < -1.0 || diff > 1.0 {
			t.Errorf("TotalUSD = %f, want ~%f", b.TotalUSD, expectedTotalUSD)
		}
	})
}

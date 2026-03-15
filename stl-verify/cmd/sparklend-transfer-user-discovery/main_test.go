package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/lifecycle"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

func TestParseConfig(t *testing.T) {
	// Spec section 6: env/config should mirror other SQS worker cmds and reject
	// missing required env with a clear error.
	tests := []struct {
		name      string
		env       map[string]string
		wantCfg   cliConfig
		wantError string
	}{
		{
			name: "missing QUEUE_URL returns clear error",
			env: map[string]string{
				"DATABASE_URL":   "postgres://localhost/testdb",
				"REDIS_ADDR":     "localhost:6379",
				"ALCHEMY_WS_URL": "wss://eth.example/ws",
				"S3_BUCKET":      "raw-blocks",
				"DEPLOY_ENV":     "staging",
			},
			wantError: "QUEUE_URL",
		},
		{
			name: "missing DATABASE_URL returns clear error",
			env: map[string]string{
				"QUEUE_URL":      "https://sqs.us-east-1.amazonaws.com/123/queue",
				"REDIS_ADDR":     "localhost:6379",
				"ALCHEMY_WS_URL": "wss://eth.example/ws",
				"S3_BUCKET":      "raw-blocks",
				"DEPLOY_ENV":     "staging",
			},
			wantError: "DATABASE_URL",
		},
		{
			name: "missing REDIS_ADDR returns clear error",
			env: map[string]string{
				"QUEUE_URL":      "https://sqs.us-east-1.amazonaws.com/123/queue",
				"DATABASE_URL":   "postgres://localhost/testdb",
				"ALCHEMY_WS_URL": "wss://eth.example/ws",
				"S3_BUCKET":      "raw-blocks",
				"DEPLOY_ENV":     "staging",
			},
			wantError: "REDIS_ADDR",
		},
		{
			name: "missing ALCHEMY_WS_URL returns clear error",
			env: map[string]string{
				"QUEUE_URL":    "https://sqs.us-east-1.amazonaws.com/123/queue",
				"DATABASE_URL": "postgres://localhost/testdb",
				"REDIS_ADDR":   "localhost:6379",
				"S3_BUCKET":    "raw-blocks",
				"DEPLOY_ENV":   "staging",
			},
			wantError: "ALCHEMY_WS_URL",
		},
		{
			name: "missing S3_BUCKET returns clear error",
			env: map[string]string{
				"QUEUE_URL":      "https://sqs.us-east-1.amazonaws.com/123/queue",
				"DATABASE_URL":   "postgres://localhost/testdb",
				"REDIS_ADDR":     "localhost:6379",
				"ALCHEMY_WS_URL": "wss://eth.example/ws",
				"DEPLOY_ENV":     "staging",
			},
			wantError: "S3_BUCKET",
		},
		{
			name: "missing DEPLOY_ENV returns clear error",
			env: map[string]string{
				"QUEUE_URL":      "https://sqs.us-east-1.amazonaws.com/123/queue",
				"DATABASE_URL":   "postgres://localhost/testdb",
				"REDIS_ADDR":     "localhost:6379",
				"ALCHEMY_WS_URL": "wss://eth.example/ws",
				"S3_BUCKET":      "raw-blocks",
			},
			wantError: "DEPLOY_ENV",
		},
		{
			name: "valid env uses defaults for chain and max messages",
			env: map[string]string{
				"QUEUE_URL":      "https://sqs.us-east-1.amazonaws.com/123/queue",
				"DATABASE_URL":   "postgres://localhost/testdb",
				"REDIS_ADDR":     "localhost:6379",
				"ALCHEMY_WS_URL": "wss://eth.example/ws",
				"S3_BUCKET":      "raw-blocks",
				"DEPLOY_ENV":     "staging",
			},
			wantCfg: cliConfig{
				queueURL:    "https://sqs.us-east-1.amazonaws.com/123/queue",
				dbURL:       "postgres://localhost/testdb",
				redisAddr:   "localhost:6379",
				alchemyURL:  "wss://eth.example/ws",
				s3Bucket:    "raw-blocks",
				deployEnv:   "staging",
				maxMessages: 10,
				chainID:     1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range []string{
				"QUEUE_URL",
				"DATABASE_URL",
				"REDIS_ADDR",
				"REDIS_PASSWORD",
				"ALCHEMY_WS_URL",
				"S3_BUCKET",
				"DEPLOY_ENV",
				"CHAIN_ID",
				"MAX_MESSAGES",
				"AWS_REGION",
				"AWS_SQS_ENDPOINT",
			} {
				if _, ok := tt.env[key]; !ok {
					t.Setenv(key, "")
				}
			}

			for key, value := range tt.env {
				t.Setenv(key, value)
			}

			cfg, err := parseConfig(nil)
			if tt.wantError != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantError)
				}
				if !strings.Contains(err.Error(), tt.wantError) {
					t.Fatalf("expected error containing %q, got %q", tt.wantError, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if cfg.queueURL != tt.wantCfg.queueURL {
				t.Errorf("queueURL: got %q, want %q", cfg.queueURL, tt.wantCfg.queueURL)
			}
			if cfg.dbURL != tt.wantCfg.dbURL {
				t.Errorf("dbURL: got %q, want %q", cfg.dbURL, tt.wantCfg.dbURL)
			}
			if cfg.redisAddr != tt.wantCfg.redisAddr {
				t.Errorf("redisAddr: got %q, want %q", cfg.redisAddr, tt.wantCfg.redisAddr)
			}
			if cfg.alchemyURL != tt.wantCfg.alchemyURL {
				t.Errorf("alchemyURL: got %q, want %q", cfg.alchemyURL, tt.wantCfg.alchemyURL)
			}
			if cfg.s3Bucket != tt.wantCfg.s3Bucket {
				t.Errorf("s3Bucket: got %q, want %q", cfg.s3Bucket, tt.wantCfg.s3Bucket)
			}
			if cfg.deployEnv != tt.wantCfg.deployEnv {
				t.Errorf("deployEnv: got %q, want %q", cfg.deployEnv, tt.wantCfg.deployEnv)
			}
			if cfg.maxMessages != tt.wantCfg.maxMessages {
				t.Errorf("maxMessages: got %d, want %d", cfg.maxMessages, tt.wantCfg.maxMessages)
			}
			if cfg.chainID != tt.wantCfg.chainID {
				t.Errorf("chainID: got %d, want %d", cfg.chainID, tt.wantCfg.chainID)
			}
		})
	}
}

func TestRunWithConfig_FailsIfTrackedReceiptTokensCannotBeLoadedAtStartup(t *testing.T) {
	// Spec section 6 + acceptance criterion 2.
	ctx := context.Background()
	wantErr := errors.New("list tracked receipt tokens: boom")
	calledSnapshotter := false
	calledService := false
	calledLifecycle := false

	deps := runtimeDependencies{
		logger: discardLogger(),
		newConsumer: func(context.Context, cliConfig, *slog.Logger) (outbound.SQSConsumer, error) {
			return fakeSQSConsumer{}, nil
		},
		newCacheReader: func(context.Context, cliConfig, *slog.Logger) (outbound.BlockCacheReader, error) {
			return fakeBlockCacheReader{}, nil
		},
		openRepositories: func(context.Context, cliConfig, *slog.Logger) (runtimeRepos, error) {
			return runtimeRepos{
				txManager:        fakeTxManager{},
				userRepo:         fakeUserRepository{},
				protocolRepo:     fakeProtocolRepository{},
				tokenRepo:        fakeTokenRepository{},
				positionRepo:     fakePositionRepository{},
				receiptTokenRepo: fakeReceiptTokenRepository{err: wantErr},
			}, nil
		},
		newSnapshotter: func(context.Context, cliConfig, runtimeRepos, *slog.Logger) (*shared.PositionSnapshotter, error) {
			calledSnapshotter = true
			return &shared.PositionSnapshotter{}, nil
		},
		newService: func(shared.SQSConsumerConfig, outbound.SQSConsumer, outbound.BlockCacheReader, outbound.TxManager, outbound.UserRepository, *shared.PositionSnapshotter, []outbound.TrackedReceiptToken, *slog.Logger) (lifecycle.Service, error) {
			calledService = true
			return fakeLifecycleService{}, nil
		},
		lifecycleRun: func(context.Context, *slog.Logger, lifecycle.Service) error {
			calledLifecycle = true
			return nil
		},
	}

	err := runWithConfig(ctx, validConfigForRunTests(), deps)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "tracked receipt tokens") {
		t.Fatalf("expected tracked receipt token startup error, got %q", err.Error())
	}
	if !strings.Contains(err.Error(), wantErr.Error()) {
		t.Fatalf("expected wrapped repository error %q, got %q", wantErr.Error(), err.Error())
	}
	if calledSnapshotter {
		t.Error("snapshotter should not be created when tracked token startup load fails")
	}
	if calledService {
		t.Error("service should not be created when tracked token startup load fails")
	}
	if calledLifecycle {
		t.Error("lifecycle should not start when tracked token startup load fails")
	}
}

func TestRunWithConfig_ConstructsDependenciesAndStartsLifecycleWhenValid(t *testing.T) {
	// Spec section 6 + acceptance criterion 3.
	ctx := context.Background()
	trackedTokens := []outbound.TrackedReceiptToken{{
		ID:                  1,
		ProtocolID:          2,
		ProtocolAddress:     common.HexToAddress("0x00000000000000000000000000000000000000AA"),
		UnderlyingTokenID:   3,
		ReceiptTokenAddress: common.HexToAddress("0x00000000000000000000000000000000000000BB"),
		Symbol:              "spUSDC",
		ChainID:             1,
	}}

	consumer := fakeSQSConsumer{}
	cacheReader := fakeBlockCacheReader{}
	txManager := fakeTxManager{}
	userRepo := fakeUserRepository{}
	protocolRepo := fakeProtocolRepository{}
	tokenRepo := fakeTokenRepository{}
	positionRepo := fakePositionRepository{}
	snapshotter := &shared.PositionSnapshotter{}
	service := fakeLifecycleService{}

	var listedChainID int64
	var gotSnapshotterRepos runtimeRepos
	var gotServiceConfig shared.SQSConsumerConfig
	var gotServiceConsumer outbound.SQSConsumer
	var gotServiceCacheReader outbound.BlockCacheReader
	var gotServiceTxManager outbound.TxManager
	var gotServiceUserRepo outbound.UserRepository
	var gotServiceSnapshotter *shared.PositionSnapshotter
	var gotServiceTrackedTokens []outbound.TrackedReceiptToken
	var gotLifecycleService lifecycle.Service

	deps := runtimeDependencies{
		logger: discardLogger(),
		newConsumer: func(context.Context, cliConfig, *slog.Logger) (outbound.SQSConsumer, error) {
			return consumer, nil
		},
		newCacheReader: func(context.Context, cliConfig, *slog.Logger) (outbound.BlockCacheReader, error) {
			return cacheReader, nil
		},
		openRepositories: func(context.Context, cliConfig, *slog.Logger) (runtimeRepos, error) {
			return runtimeRepos{
				txManager:        txManager,
				userRepo:         userRepo,
				protocolRepo:     protocolRepo,
				tokenRepo:        tokenRepo,
				positionRepo:     positionRepo,
				receiptTokenRepo: fakeReceiptTokenRepository{tokens: trackedTokens, listFn: func(chainID int64) { listedChainID = chainID }},
			}, nil
		},
		newSnapshotter: func(_ context.Context, _ cliConfig, repos runtimeRepos, _ *slog.Logger) (*shared.PositionSnapshotter, error) {
			gotSnapshotterRepos = repos
			return snapshotter, nil
		},
		newService: func(cfg shared.SQSConsumerConfig, gotConsumer outbound.SQSConsumer, gotCacheReader outbound.BlockCacheReader, gotTxManager outbound.TxManager, gotUserRepo outbound.UserRepository, gotSnapshotter *shared.PositionSnapshotter, gotTrackedTokens []outbound.TrackedReceiptToken, _ *slog.Logger) (lifecycle.Service, error) {
			gotServiceConfig = cfg
			gotServiceConsumer = gotConsumer
			gotServiceCacheReader = gotCacheReader
			gotServiceTxManager = gotTxManager
			gotServiceUserRepo = gotUserRepo
			gotServiceSnapshotter = gotSnapshotter
			gotServiceTrackedTokens = gotTrackedTokens
			return service, nil
		},
		lifecycleRun: func(_ context.Context, _ *slog.Logger, gotService lifecycle.Service) error {
			gotLifecycleService = gotService
			return nil
		},
	}

	if err := runWithConfig(ctx, validConfigForRunTests(), deps); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if listedChainID != 1 {
		t.Errorf("expected tracked tokens to be loaded for chain 1, got %d", listedChainID)
	}
	if gotSnapshotterRepos.txManager != txManager {
		t.Error("expected shared snapshotter to receive tx manager")
	}
	if gotSnapshotterRepos.userRepo != userRepo {
		t.Error("expected shared snapshotter to receive user repo")
	}
	if gotSnapshotterRepos.protocolRepo != protocolRepo {
		t.Error("expected shared snapshotter to receive protocol repo")
	}
	if gotSnapshotterRepos.tokenRepo != tokenRepo {
		t.Error("expected shared snapshotter to receive token repo")
	}
	if gotSnapshotterRepos.positionRepo != positionRepo {
		t.Error("expected shared snapshotter to receive position repo")
	}
	if gotServiceConfig.MaxMessages != 10 {
		t.Errorf("expected MaxMessages 10, got %d", gotServiceConfig.MaxMessages)
	}
	if gotServiceConfig.ChainID != 1 {
		t.Errorf("expected ChainID 1, got %d", gotServiceConfig.ChainID)
	}
	if gotServiceConfig.Logger == nil {
		t.Error("expected non-nil logger in service config")
	}
	if gotServiceConsumer != consumer {
		t.Error("expected service to receive constructed consumer")
	}
	if gotServiceCacheReader != cacheReader {
		t.Error("expected service to receive constructed cache reader")
	}
	if gotServiceTxManager != txManager {
		t.Error("expected service to receive constructed tx manager")
	}
	if gotServiceUserRepo != userRepo {
		t.Error("expected service to receive constructed user repo")
	}
	if gotServiceSnapshotter != snapshotter {
		t.Error("expected service to receive shared snapshotter")
	}
	if len(gotServiceTrackedTokens) != 1 || gotServiceTrackedTokens[0] != trackedTokens[0] {
		t.Fatalf("expected service to receive startup-loaded tracked tokens, got %+v", gotServiceTrackedTokens)
	}
	if gotLifecycleService != service {
		t.Error("expected lifecycle.Run to receive constructed service")
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func validConfigForRunTests() cliConfig {
	return cliConfig{
		queueURL:    "https://sqs.us-east-1.amazonaws.com/123/queue",
		redisAddr:   "localhost:6379",
		dbURL:       "postgres://localhost/testdb",
		alchemyURL:  "wss://eth.example/ws",
		s3Bucket:    "raw-blocks",
		deployEnv:   "staging",
		maxMessages: 10,
		chainID:     1,
	}
}

type fakeSQSConsumer struct{}

func (fakeSQSConsumer) ReceiveMessages(context.Context, int) ([]outbound.SQSMessage, error) {
	return nil, nil
}

func (fakeSQSConsumer) DeleteMessage(context.Context, string) error {
	return nil
}

func (fakeSQSConsumer) Close() error {
	return nil
}

type fakeBlockCacheReader struct{}

func (fakeBlockCacheReader) GetBlock(context.Context, int64, int64, int) (json.RawMessage, error) {
	return nil, nil
}

func (fakeBlockCacheReader) GetReceipts(context.Context, int64, int64, int) (json.RawMessage, error) {
	return nil, nil
}

func (fakeBlockCacheReader) GetTraces(context.Context, int64, int64, int) (json.RawMessage, error) {
	return nil, nil
}

func (fakeBlockCacheReader) GetBlobs(context.Context, int64, int64, int) (json.RawMessage, error) {
	return nil, nil
}

func (fakeBlockCacheReader) Close() error {
	return nil
}

type fakeTxManager struct{}

func (fakeTxManager) WithTransaction(_ context.Context, fn func(tx pgx.Tx) error) error {
	return fn(nil)
}

type fakeUserRepository struct{}

func (fakeUserRepository) GetOrCreateUser(context.Context, pgx.Tx, entity.User) (int64, error) {
	return 0, nil
}

func (fakeUserRepository) EnsureUser(context.Context, pgx.Tx, entity.User) (int64, bool, error) {
	return 0, false, nil
}

func (fakeUserRepository) UpsertUserProtocolMetadata(context.Context, []*entity.UserProtocolMetadata) error {
	return nil
}

type fakeProtocolRepository struct{}

func (fakeProtocolRepository) GetOrCreateProtocol(context.Context, pgx.Tx, int64, common.Address, string, string, int64) (int64, error) {
	return 0, nil
}

func (fakeProtocolRepository) UpsertReserveData(context.Context, pgx.Tx, []*entity.SparkLendReserveData) error {
	return nil
}

type fakeTokenRepository struct{}

func (fakeTokenRepository) GetOrCreateToken(context.Context, pgx.Tx, int64, common.Address, string, int, int64) (int64, error) {
	return 0, nil
}

type fakePositionRepository struct{}

func (fakePositionRepository) SaveBorrower(context.Context, pgx.Tx, int64, int64, int64, int64, int, string, string, string, []byte) error {
	return nil
}

func (fakePositionRepository) SaveBorrowerCollateral(context.Context, pgx.Tx, int64, int64, int64, int64, int, string, string, string, []byte, bool) error {
	return nil
}

func (fakePositionRepository) SaveBorrowerCollaterals(context.Context, pgx.Tx, []outbound.CollateralRecord) error {
	return nil
}

type fakeReceiptTokenRepository struct {
	tokens []outbound.TrackedReceiptToken
	err    error
	listFn func(chainID int64)
}

func (f fakeReceiptTokenRepository) ListTrackedReceiptTokens(_ context.Context, chainID int64) ([]outbound.TrackedReceiptToken, error) {
	if f.listFn != nil {
		f.listFn(chainID)
	}
	if f.err != nil {
		return nil, f.err
	}
	return f.tokens, nil
}

type fakeLifecycleService struct{}

func (fakeLifecycleService) Start(context.Context) error { return nil }

func (fakeLifecycleService) Stop() error { return nil }

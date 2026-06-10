// Package maple_graphql_indexer orchestrates one Maple GraphQL snapshot
// cycle: pools, active loans (+collateral), Sky strategies, and Syrup
// globals are fetched from the Maple API and persisted into the maple_*
// tables, all stamped with a single synced_at timestamp.
package maple_graphql_indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mainnetChainID is the only chain the Maple GraphQL API serves.
const mainnetChainID = 1

// ServiceConfig holds configuration for the Maple GraphQL indexer service.
type ServiceConfig struct {
	// ChainID must be 1: the Maple GraphQL API is Ethereum-mainnet-scoped,
	// and accepting any other value would mix mainnet data into another
	// chain's rows.
	ChainID int

	// Logger is the structured logger for the service.
	Logger *slog.Logger
}

// Service runs Maple GraphQL snapshot cycles.
type Service struct {
	config    ServiceConfig
	client    outbound.MapleGraphQLClient
	repo      outbound.MapleGraphQLRepository
	txManager outbound.TxManager
	telemetry *Telemetry
	logger    *slog.Logger

	// now is injectable for tests; defaults to time.Now.
	now func() time.Time
}

// NewService creates a new Maple GraphQL indexer service. telemetry may be
// nil (all telemetry methods are nil-receiver-safe).
func NewService(config ServiceConfig, client outbound.MapleGraphQLClient, repo outbound.MapleGraphQLRepository, txManager outbound.TxManager, telemetry *Telemetry) (*Service, error) {
	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if txManager == nil {
		return nil, fmt.Errorf("txManager cannot be nil")
	}
	if config.ChainID != mainnetChainID {
		return nil, fmt.Errorf("chainID must be %d (the Maple GraphQL API is mainnet-scoped), got %d", mainnetChainID, config.ChainID)
	}

	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Service{
		config:    config,
		client:    client,
		repo:      repo,
		txManager: txManager,
		telemetry: telemetry,
		logger:    logger.With("component", "maple-graphql-indexer"),
		now:       time.Now,
	}, nil
}

// Sync runs one snapshot cycle. Each phase has its own GraphQL query/queries
// and its own DB transaction; a failing phase does not stop later phases,
// but its error is joined into the returned error so the run is marked
// failed. Loans and Sky strategies depend on the pool registry ids, so they
// are skipped (and reported failed) when the pool phase fails.
func (s *Service) Sync(ctx context.Context) error {
	syncedAt := s.now().UTC().Truncate(time.Second)
	ctx, span := s.telemetry.StartCycleSpan(ctx, syncedAt)
	defer span.End()

	s.logger.Info("starting sync cycle", "syncedAt", syncedAt)

	var poolIDs map[string]int64
	poolsErr := s.runPhase(ctx, "pools", func(ctx context.Context) error {
		var err error
		poolIDs, err = s.syncPools(ctx, syncedAt)
		return err
	})

	var loansErr, strategiesErr error
	if poolsErr == nil {
		loansErr = s.runPhase(ctx, "loans", func(ctx context.Context) error {
			return s.syncLoans(ctx, syncedAt, poolIDs)
		})
		strategiesErr = s.runPhase(ctx, "sky_strategies", func(ctx context.Context) error {
			return s.syncSkyStrategies(ctx, syncedAt, poolIDs)
		})
	} else {
		loansErr = fmt.Errorf("skipping loans: pool sync failed: %w", poolsErr)
		strategiesErr = fmt.Errorf("skipping sky strategies: pool sync failed: %w", poolsErr)
	}

	globalsErr := s.runPhase(ctx, "syrup_globals", func(ctx context.Context) error {
		return s.syncSyrupGlobals(ctx, syncedAt)
	})

	err := errors.Join(poolsErr, loansErr, strategiesErr, globalsErr)
	s.telemetry.RecordCycle(ctx, err)
	SetSpanError(span, err, "sync cycle failed")
	if err != nil {
		s.logger.Error("sync cycle finished with errors", "syncedAt", syncedAt, "error", err)
	} else {
		s.logger.Info("sync cycle complete", "syncedAt", syncedAt)
	}
	return err
}

// runPhase wraps a phase with a span, duration metric, and error logging.
func (s *Service) runPhase(ctx context.Context, phase string, fn func(ctx context.Context) error) error {
	ctx, span := s.telemetry.StartPhaseSpan(ctx, phase)
	defer span.End()

	start := s.now()
	err := fn(ctx)
	s.telemetry.RecordPhase(ctx, phase, s.now().Sub(start), err)
	SetSpanError(span, err, phase+" phase failed")
	if err != nil {
		s.logger.Error("phase failed", "phase", phase, "error", err)
	}
	return err
}

// ---------------------------------------------------------------------------
// Phase 1: pools
// ---------------------------------------------------------------------------

// syncPools fetches all pools and persists the registry rows plus one state
// snapshot per pool in a single transaction. Returns the lowercase hex
// address -> maple_pool.id map for the dependent phases.
func (s *Service) syncPools(ctx context.Context, syncedAt time.Time) (map[string]int64, error) {
	pools, err := s.client.GetPools(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching pools: %w", err)
	}

	protocolID, err := s.repo.GetMapleProtocolID(ctx, int64(s.config.ChainID))
	if err != nil {
		return nil, fmt.Errorf("resolving maple protocol: %w", err)
	}

	poolEntities := make([]*entity.MaplePool, 0, len(pools))
	for _, p := range pools {
		poolEntity, err := entity.NewMaplePool(
			int64(s.config.ChainID), protocolID, p.Address.Bytes(), p.Name,
			p.AssetAddress.Bytes(), p.AssetSymbol, int16(p.AssetDecimals), p.IsSyrup,
		)
		if err != nil {
			return nil, fmt.Errorf("pool %s: %w", strings.ToLower(p.Address.Hex()), err)
		}
		poolEntities = append(poolEntities, poolEntity)
	}

	var poolIDs map[string]int64
	err = s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		var err error
		poolIDs, err = s.repo.UpsertPools(ctx, tx, poolEntities)
		if err != nil {
			return fmt.Errorf("upserting pools: %w", err)
		}

		states := make([]*entity.MaplePoolState, 0, len(pools))
		for _, p := range pools {
			key := strings.ToLower(p.Address.Hex())
			poolID, ok := poolIDs[key]
			if !ok {
				return fmt.Errorf("pool %s missing from upsert result", key)
			}
			state, err := entity.NewMaplePoolState(
				poolID, syncedAt, p.TVL, p.LiquidAssets, p.CollateralUSD, p.PrincipalOut,
				computeUtilization(p.PrincipalOut, p.LiquidAssets), p.MonthlyAPY, p.SpotAPY,
			)
			if err != nil {
				return fmt.Errorf("pool state %s: %w", key, err)
			}
			states = append(states, state)
		}

		if err := s.repo.SavePoolStates(ctx, tx, states); err != nil {
			return fmt.Errorf("saving pool states: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	s.telemetry.RecordRowsWritten(ctx, "maple_pool_state", len(pools))
	s.logger.Info("pools synced", "count", len(pools))
	return poolIDs, nil
}

// computeUtilization derives principal_out / (liquid_assets + principal_out),
// returning 0 for an empty pool.
func computeUtilization(principalOut, liquidAssets *big.Int) float64 {
	denominator := new(big.Int).Add(liquidAssets, principalOut)
	if denominator.Sign() == 0 {
		return 0
	}
	utilization, _ := new(big.Float).Quo(
		new(big.Float).SetInt(principalOut),
		new(big.Float).SetInt(denominator),
	).Float64()
	return utilization
}

// ---------------------------------------------------------------------------
// Phase 2: loans + collateral
// ---------------------------------------------------------------------------

// syncLoans fetches all active loans and persists borrowers, loan registry
// rows, loan states, and collateral snapshots in a single transaction, so a
// loan snapshot is all-or-nothing.
func (s *Service) syncLoans(ctx context.Context, syncedAt time.Time, poolIDs map[string]int64) error {
	loans, err := s.client.GetActiveLoans(ctx)
	if err != nil {
		return fmt.Errorf("fetching active loans: %w", err)
	}
	if len(loans) == 0 {
		s.logger.Warn("no active loans returned by the API")
		return nil
	}

	protocolID, err := s.repo.GetMapleProtocolID(ctx, int64(s.config.ChainID))
	if err != nil {
		return fmt.Errorf("resolving maple protocol: %w", err)
	}

	// A loan whose pool is missing from the map is a hard error — pools were
	// fetched seconds earlier in this same cycle.
	for _, l := range loans {
		key := strings.ToLower(l.PoolAddress.Hex())
		if _, ok := poolIDs[key]; !ok {
			return fmt.Errorf("loan %s references unknown pool %s", strings.ToLower(l.LoanID.Hex()), key)
		}
	}

	borrowers := distinctBorrowers(loans)

	collateralCount := 0
	err = s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		borrowerIDs, err := s.repo.GetOrCreateBorrowerUsers(ctx, tx, int64(s.config.ChainID), borrowers)
		if err != nil {
			return fmt.Errorf("resolving borrowers: %w", err)
		}

		loanEntities := make([]*entity.MapleLoan, 0, len(loans))
		for _, l := range loans {
			borrowerUserID, ok := borrowerIDs[l.Borrower]
			if !ok {
				return fmt.Errorf("borrower %s missing from upsert result", strings.ToLower(l.Borrower.Hex()))
			}
			loanEntity, err := entity.NewMapleLoan(
				int64(s.config.ChainID), protocolID, l.LoanID.Bytes(),
				poolIDs[strings.ToLower(l.PoolAddress.Hex())], borrowerUserID, toEntityLoanMeta(l.LoanMeta),
			)
			if err != nil {
				return fmt.Errorf("loan %s: %w", strings.ToLower(l.LoanID.Hex()), err)
			}
			loanEntities = append(loanEntities, loanEntity)
		}

		loanIDs, err := s.repo.UpsertLoans(ctx, tx, loanEntities)
		if err != nil {
			return fmt.Errorf("upserting loans: %w", err)
		}

		states := make([]*entity.MapleLoanState, 0, len(loans))
		collaterals := make([]*entity.MapleLoanCollateral, 0, len(loans))
		for _, l := range loans {
			key := strings.ToLower(l.LoanID.Hex())
			loanID, ok := loanIDs[key]
			if !ok {
				return fmt.Errorf("loan %s missing from upsert result", key)
			}

			state, err := entity.NewMapleLoanState(loanID, syncedAt, l.State, l.PrincipalOwed, l.AcmRatio)
			if err != nil {
				return fmt.Errorf("loan state %s: %w", key, err)
			}
			states = append(states, state)

			// Loans with null API collateral simply have no collateral row.
			if l.Collateral == nil {
				continue
			}
			collateral, err := entity.NewMapleLoanCollateral(
				loanID, syncedAt, l.Collateral.Asset, l.Collateral.AssetAmount,
				int16(l.Collateral.Decimals), l.Collateral.AssetValueUSD,
				l.Collateral.State, l.Collateral.Custodian, l.Collateral.LiquidationLevel,
			)
			if err != nil {
				return fmt.Errorf("loan collateral %s: %w", key, err)
			}
			collaterals = append(collaterals, collateral)
		}

		if err := s.repo.SaveLoanStates(ctx, tx, states); err != nil {
			return fmt.Errorf("saving loan states: %w", err)
		}
		if err := s.repo.SaveLoanCollaterals(ctx, tx, collaterals); err != nil {
			return fmt.Errorf("saving loan collaterals: %w", err)
		}
		collateralCount = len(collaterals)
		return nil
	})
	if err != nil {
		return err
	}

	s.telemetry.RecordRowsWritten(ctx, "maple_loan_state", len(loans))
	s.telemetry.RecordRowsWritten(ctx, "maple_loan_collateral", collateralCount)
	s.logger.Info("loans synced", "count", len(loans), "collaterals", collateralCount, "borrowers", len(borrowers))
	return nil
}

// distinctBorrowers extracts the unique borrower addresses, preserving first
// appearance order.
func distinctBorrowers(loans []outbound.MapleActiveLoan) []common.Address {
	seen := make(map[common.Address]struct{}, len(loans))
	borrowers := make([]common.Address, 0, len(loans))
	for _, l := range loans {
		if _, ok := seen[l.Borrower]; ok {
			continue
		}
		seen[l.Borrower] = struct{}{}
		borrowers = append(borrowers, l.Borrower)
	}
	return borrowers
}

// toEntityLoanMeta maps the client DTO meta to the entity meta.
func toEntityLoanMeta(meta *outbound.MapleLoanMeta) *entity.MapleLoanMeta {
	if meta == nil {
		return nil
	}
	return &entity.MapleLoanMeta{
		Type:          meta.Type,
		AssetSymbol:   meta.AssetSymbol,
		Dex:           meta.DexName,
		WalletAddress: meta.WalletAddress,
		WalletType:    meta.WalletType,
		Location:      meta.Location,
	}
}

// ---------------------------------------------------------------------------
// Phase 3: Sky strategies
// ---------------------------------------------------------------------------

// syncSkyStrategies fetches all Sky strategies and persists registry rows
// plus one state snapshot per strategy in a single transaction.
func (s *Service) syncSkyStrategies(ctx context.Context, syncedAt time.Time, poolIDs map[string]int64) error {
	strategies, err := s.client.GetSkyStrategies(ctx)
	if err != nil {
		return fmt.Errorf("fetching sky strategies: %w", err)
	}
	if len(strategies) == 0 {
		s.logger.Info("no sky strategies returned by the API")
		return nil
	}

	strategyEntities := make([]*entity.MapleSkyStrategy, 0, len(strategies))
	for _, st := range strategies {
		key := strings.ToLower(st.PoolAddress.Hex())
		poolID, ok := poolIDs[key]
		if !ok {
			return fmt.Errorf("sky strategy %s references unknown pool %s", strings.ToLower(st.Address.Hex()), key)
		}
		strategyEntity, err := entity.NewMapleSkyStrategy(int64(s.config.ChainID), st.Address.Bytes(), poolID, st.Version)
		if err != nil {
			return fmt.Errorf("sky strategy %s: %w", strings.ToLower(st.Address.Hex()), err)
		}
		strategyEntities = append(strategyEntities, strategyEntity)
	}

	err = s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		strategyIDs, err := s.repo.UpsertSkyStrategies(ctx, tx, strategyEntities)
		if err != nil {
			return fmt.Errorf("upserting sky strategies: %w", err)
		}

		states := make([]*entity.MapleSkyStrategyState, 0, len(strategies))
		for _, st := range strategies {
			key := strings.ToLower(st.Address.Hex())
			strategyID, ok := strategyIDs[key]
			if !ok {
				return fmt.Errorf("sky strategy %s missing from upsert result", key)
			}
			state, err := entity.NewMapleSkyStrategyState(
				strategyID, syncedAt, st.State, st.CurrentlyDeployed,
				st.DepositedAssets, st.WithdrawnAssets, st.StrategyFeeRate, st.TotalFeesCollected,
			)
			if err != nil {
				return fmt.Errorf("sky strategy state %s: %w", key, err)
			}
			states = append(states, state)
		}

		if err := s.repo.SaveSkyStrategyStates(ctx, tx, states); err != nil {
			return fmt.Errorf("saving sky strategy states: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	s.telemetry.RecordRowsWritten(ctx, "maple_sky_strategy_state", len(strategies))
	s.logger.Info("sky strategies synced", "count", len(strategies))
	return nil
}

// ---------------------------------------------------------------------------
// Phase 4: Syrup globals
// ---------------------------------------------------------------------------

// syncSyrupGlobals fetches the protocol-wide Syrup aggregates and persists
// the singleton snapshot.
func (s *Service) syncSyrupGlobals(ctx context.Context, syncedAt time.Time) error {
	globals, err := s.client.GetSyrupGlobals(ctx)
	if err != nil {
		return fmt.Errorf("fetching syrup globals: %w", err)
	}

	state, err := entity.NewMapleSyrupGlobalState(
		int64(s.config.ChainID), syncedAt, globals.TVL, globals.APY,
		globals.CollateralAPY, globals.PoolAPY, globals.DripsYieldBoost,
	)
	if err != nil {
		return fmt.Errorf("syrup global state: %w", err)
	}

	err = s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.repo.SaveSyrupGlobalState(ctx, tx, state); err != nil {
			return fmt.Errorf("saving syrup global state: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	s.telemetry.RecordRowsWritten(ctx, "maple_syrup_global_state", 1)
	s.logger.Info("syrup globals synced")
	return nil
}

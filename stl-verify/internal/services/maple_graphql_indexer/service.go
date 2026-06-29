// Package maple_graphql_indexer orchestrates one Maple GraphQL snapshot
// cycle: pools, active open-term loans (+collateral), live fixed-term loans,
// Sky strategies, and Syrup globals are fetched from the Maple API and
// persisted into the maple_* tables, all stamped with a single synced_at
// timestamp.
package maple_graphql_indexer

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity/maple"
	"github.com/archon-research/stl/stl-verify/internal/pkg/telemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mainnetChainID is the only chain the Maple GraphQL API serves.
const mainnetChainID = 1

// ServiceConfig holds configuration for the Maple GraphQL indexer service.
type ServiceConfig struct {
	// ChainID must be 1: the Maple GraphQL API is Ethereum-mainnet-scoped,
	// and accepting any other value would mix mainnet data into another
	// chain's rows.
	ChainID int64

	// Logger is the structured logger for the service.
	Logger *slog.Logger
}

// Service runs Maple GraphQL snapshot cycles.
type Service struct {
	config    ServiceConfig
	client    outbound.MapleGraphQLClient
	repo      outbound.MapleGraphQLRepository
	tokenRepo outbound.TokenRepository
	userRepo  outbound.UserRepository
	txManager outbound.TxManager
	telemetry *Telemetry
	logger    *slog.Logger

	// now is injectable for tests; defaults to time.Now.
	now func() time.Time
}

// NewService creates a new Maple GraphQL indexer service. telemetry may be
// nil (all telemetry methods are nil-receiver-safe). The shared token and user
// registries are upserted via tokenRepo/userRepo with a nil block (GraphQL data
// has no block context), so an existing on-chain block is preserved.
func NewService(config ServiceConfig, client outbound.MapleGraphQLClient, repo outbound.MapleGraphQLRepository, tokenRepo outbound.TokenRepository, userRepo outbound.UserRepository, txManager outbound.TxManager, telemetry *Telemetry) (*Service, error) {
	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	if repo == nil {
		return nil, fmt.Errorf("repo cannot be nil")
	}
	if tokenRepo == nil {
		return nil, fmt.Errorf("tokenRepo cannot be nil")
	}
	if userRepo == nil {
		return nil, fmt.Errorf("userRepo cannot be nil")
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
		tokenRepo: tokenRepo,
		userRepo:  userRepo,
		txManager: txManager,
		telemetry: telemetry,
		logger:    logger.With("component", "maple-graphql-indexer"),
		now:       time.Now,
	}, nil
}

// Sync runs one snapshot cycle stamped with the current time. Prefer SyncAt
// with a retry-stable timestamp when running under a retrying harness.
func (s *Service) Sync(ctx context.Context) error {
	return s.SyncAt(ctx, s.now())
}

// SyncAt runs one snapshot cycle stamped with syncedAt (normalized to UTC
// second precision). Each phase has its own GraphQL query/queries and its
// own DB transaction; a failing phase does not stop later phases, but its
// error is joined into the returned error so the run is marked failed.
// Loans and Sky strategies depend on the pool registry ids, so they are
// skipped (and reported failed) when the pool phase fails. A cancelled
// context short-circuits the remaining phases instead of attempting them
// with a dead context.
//
// Passing a timestamp that is stable across harness retries (e.g. the
// Temporal activity's workflow-recorded schedule time) makes retries
// idempotent: phases that already persisted re-insert at the same synced_at
// and build, and the processing-version trigger plus ON CONFLICT DO NOTHING
// dedupe them, so a retry caused by one failing phase does not multiply the
// healthy phases' snapshots.
func (s *Service) SyncAt(ctx context.Context, syncedAt time.Time) error {
	syncedAt = maple.NormalizeSyncedAt(syncedAt)
	ctx, span := s.telemetry.StartCycleSpan(ctx, syncedAt)
	defer span.End()

	s.logger.Info("starting sync cycle", "syncedAt", syncedAt)

	err := s.runPhases(ctx, syncedAt)
	s.telemetry.RecordCycle(ctx, err)
	telemetry.SetSpanError(span, err, "sync cycle failed")
	if err != nil {
		s.logger.Error("sync cycle finished with errors", "syncedAt", syncedAt, "error", err)
	} else {
		s.logger.Info("sync cycle complete", "syncedAt", syncedAt)
	}
	return err
}

// runPhases runs the five sync phases in order, joining their errors. A
// context cancelled during a phase aborts the cycle there: later phases
// would only fail against the dead context, adding error-metric noise for
// every routine shutdown.
func (s *Service) runPhases(ctx context.Context, syncedAt time.Time) error {
	var (
		poolIDs    map[common.Address]int64
		protocolID int64
	)
	poolsErr := s.runPhase(ctx, "pools", func(ctx context.Context) error {
		var err error
		protocolID, err = s.repo.GetMapleProtocolID(ctx, s.config.ChainID)
		if err != nil {
			return fmt.Errorf("resolving maple protocol: %w", err)
		}
		poolIDs, err = s.syncPools(ctx, syncedAt, protocolID)
		return err
	})
	if ctxErr := ctx.Err(); ctxErr != nil {
		return errors.Join(poolsErr, fmt.Errorf("aborting sync cycle after pools phase: %w", ctxErr))
	}

	var loansErr, ftlErr, strategiesErr error
	if poolsErr == nil {
		loansErr = s.runPhase(ctx, "loans", func(ctx context.Context) error {
			return s.syncLoans(ctx, syncedAt, poolIDs, protocolID)
		})
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Join(poolsErr, loansErr, fmt.Errorf("aborting sync cycle after loans phase: %w", ctxErr))
		}
		ftlErr = s.runPhase(ctx, "fixed_term_loans", func(ctx context.Context) error {
			return s.syncFixedTermLoans(ctx, syncedAt, poolIDs, protocolID)
		})
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Join(poolsErr, loansErr, ftlErr, fmt.Errorf("aborting sync cycle after fixed-term loans phase: %w", ctxErr))
		}
		strategiesErr = s.runPhase(ctx, "sky_strategies", func(ctx context.Context) error {
			return s.syncSkyStrategies(ctx, syncedAt, poolIDs)
		})
		if ctxErr := ctx.Err(); ctxErr != nil {
			return errors.Join(poolsErr, loansErr, ftlErr, strategiesErr, fmt.Errorf("aborting sync cycle after sky strategies phase: %w", ctxErr))
		}
	} else {
		// The pool error joins once below; the skip errors deliberately do
		// not wrap it again. Record the skipped phases so per-phase error
		// metrics see them.
		loansErr = errors.New("skipping loans: pool phase failed")
		ftlErr = errors.New("skipping fixed-term loans: pool phase failed")
		strategiesErr = errors.New("skipping sky strategies: pool phase failed")
		s.telemetry.RecordPhase(ctx, "loans", 0, loansErr)
		s.telemetry.RecordPhase(ctx, "fixed_term_loans", 0, ftlErr)
		s.telemetry.RecordPhase(ctx, "sky_strategies", 0, strategiesErr)
	}

	globalsErr := s.runPhase(ctx, "syrup_globals", func(ctx context.Context) error {
		return s.syncSyrupGlobals(ctx, syncedAt)
	})

	return errors.Join(poolsErr, loansErr, ftlErr, strategiesErr, globalsErr)
}

// runPhase wraps a phase with a span, duration metric, and error logging.
func (s *Service) runPhase(ctx context.Context, phase string, fn func(ctx context.Context) error) error {
	ctx, span := s.telemetry.StartPhaseSpan(ctx, phase)
	defer span.End()

	start := s.now()
	err := fn(ctx)
	s.telemetry.RecordPhase(ctx, phase, s.now().Sub(start), err)
	telemetry.SetSpanError(span, err, phase+" phase failed")
	if err != nil {
		s.logger.Error("phase failed", "phase", phase, "error", err)
	}
	return err
}

// ---------------------------------------------------------------------------
// Phase 1: pools
// ---------------------------------------------------------------------------

// syncPools fetches all pools and persists the registry rows plus one state
// snapshot per pool in a single transaction. Returns the
// address -> maple_pool.id map for the dependent phases.
func (s *Service) syncPools(ctx context.Context, syncedAt time.Time, protocolID int64) (map[common.Address]int64, error) {
	pools, err := s.client.GetPools(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching pools: %w", err)
	}
	// Maple has had ~21 pools for years; zero pools means a broken upstream
	// response (e.g. data:null), never a valid snapshot. Refuse it so the run
	// is marked failed instead of silently writing nothing forever.
	if len(pools) == 0 {
		return nil, fmt.Errorf("API returned 0 pools; refusing to treat an empty pool set as a valid snapshot")
	}
	if err := requireUniqueIDs("pool", len(pools), func(i int) common.Address { return pools[i].Address }); err != nil {
		return nil, err
	}

	for _, p := range pools {
		if p.TVL == nil {
			s.telemetry.RecordNullDowngrade(ctx, "pool_tvl")
		}
		if p.CollateralUSD == nil {
			s.telemetry.RecordNullDowngrade(ctx, "pool_collateral_value_usd")
		}
		if p.MonthlyAPY == nil {
			s.telemetry.RecordNullDowngrade(ctx, "pool_monthly_apy")
		}
		if p.SpotAPY == nil {
			s.telemetry.RecordNullDowngrade(ctx, "pool_spot_apy")
		}
	}

	assets, err := distinctAssetTokens(s.config.ChainID, pools)
	if err != nil {
		return nil, err
	}

	var poolIDs map[common.Address]int64
	err = s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		assetTokenIDs, err := s.tokenRepo.GetOrCreateTokens(ctx, tx, assets)
		if err != nil {
			return fmt.Errorf("resolving asset tokens: %w", err)
		}

		poolEntities := make([]*maple.Pool, 0, len(pools))
		for _, p := range pools {
			assetTokenID, ok := assetTokenIDs[p.AssetAddress]
			if !ok {
				return fmt.Errorf("pool %s: asset token %s missing from upsert result", lowerHex(p.Address), lowerHex(p.AssetAddress))
			}
			poolEntity, err := maple.NewPool(
				s.config.ChainID, protocolID, p.Address.Bytes(), p.Name,
				assetTokenID, p.IsSyrup,
			)
			if err != nil {
				return fmt.Errorf("pool %s: %w", lowerHex(p.Address), err)
			}
			poolEntities = append(poolEntities, poolEntity)
		}

		poolIDs, err = s.repo.RecordPools(ctx, tx, syncedAt, poolEntities)
		if err != nil {
			return fmt.Errorf("recording pools: %w", err)
		}

		states := make([]*maple.PoolState, 0, len(pools))
		for _, p := range pools {
			poolID, ok := poolIDs[p.Address]
			if !ok {
				return fmt.Errorf("pool %s missing from record result", lowerHex(p.Address))
			}
			state, err := maple.NewPoolState(maple.PoolStateParams{
				PoolID:             poolID,
				SyncedAt:           syncedAt,
				TVL:                p.TVL,
				LiquidAssets:       p.LiquidAssets,
				CollateralValueUSD: p.CollateralUSD,
				PrincipalOut:       p.PrincipalOut,
				MonthlyAPY:         p.MonthlyAPY,
				SpotAPY:            p.SpotAPY,
			})
			if err != nil {
				return fmt.Errorf("pool state %s: %w", lowerHex(p.Address), err)
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

// distinctAssetTokens extracts the unique pool asset tokens as token-registry
// upsert inputs, validating each asset's metadata and failing when two pools
// report the same asset address with conflicting symbol or decimals (an
// inconsistency the token table cannot represent and must not silently
// first-write-wins). CreatedAtBlock is left nil: GraphQL data has no block
// context, and a nil block preserves any existing on-chain block.
func distinctAssetTokens(chainID int64, pools []outbound.MaplePool) ([]outbound.TokenInput, error) {
	seen := make(map[common.Address]outbound.TokenInput, len(pools))
	assets := make([]outbound.TokenInput, 0, len(pools))
	for _, p := range pools {
		if p.AssetSymbol == "" {
			return nil, fmt.Errorf("pool %s: asset %s: symbol must not be empty", lowerHex(p.Address), lowerHex(p.AssetAddress))
		}
		// SMALLINT range guard: an API bug returning e.g. 65542 must not be
		// stored as a wrapped, smaller decimals value.
		decimals, err := toInt16(p.AssetDecimals)
		if err != nil {
			return nil, fmt.Errorf("pool %s: asset decimals: %w", lowerHex(p.Address), err)
		}
		asset := outbound.TokenInput{ChainID: chainID, Address: p.AssetAddress, Symbol: p.AssetSymbol, Decimals: int(decimals)}
		if prev, ok := seen[p.AssetAddress]; ok {
			if prev.Symbol != asset.Symbol || prev.Decimals != asset.Decimals {
				return nil, fmt.Errorf("asset %s reported with conflicting metadata: %s/%d vs %s/%d",
					lowerHex(p.AssetAddress), prev.Symbol, prev.Decimals, asset.Symbol, asset.Decimals)
			}
			continue
		}
		seen[p.AssetAddress] = asset
		assets = append(assets, asset)
	}
	return assets, nil
}

// toInt16 converts an int to int16, rejecting values that would silently
// wrap (an API bug returning e.g. 65542 must not become decimals 6).
func toInt16(v int) (int16, error) {
	if v < 0 || v > math.MaxInt16 {
		return 0, fmt.Errorf("value %d out of int16 range", v)
	}
	return int16(v), nil
}

// lowerHex renders an address in the lowercase 0x-prefixed hex form used in
// this package's logs and error messages.
func lowerHex(a common.Address) string {
	return strings.ToLower(a.Hex())
}

// requireUniqueIDs rejects API result sets containing the same entity twice.
// The GraphQL queries cannot be ordered (orderBy is invalid on these
// collections), so skip-based pagination has no stable order; a duplicate
// would otherwise be silently dropped by ON CONFLICT DO NOTHING — possibly
// with a different payload than the row that won.
func requireUniqueIDs(kind string, n int, id func(i int) common.Address) error {
	seen := make(map[common.Address]struct{}, n)
	for i := range n {
		addr := id(i)
		if _, ok := seen[addr]; ok {
			return fmt.Errorf("API returned duplicate %s %s (unstable pagination?)", kind, lowerHex(addr))
		}
		seen[addr] = struct{}{}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Phase 2: loans + collateral
// ---------------------------------------------------------------------------

// syncLoans fetches all active loans and persists borrowers, loan registry
// rows, loan states, and collateral snapshots in a single transaction, so a
// loan snapshot is all-or-nothing.
func (s *Service) syncLoans(ctx context.Context, syncedAt time.Time, poolIDs map[common.Address]int64, protocolID int64) error {
	loans, err := s.client.GetActiveLoans(ctx)
	if err != nil {
		return fmt.Errorf("fetching active loans: %w", err)
	}
	if len(loans) == 0 {
		// Emit explicit zeros so a dropped-to-zero loan book is distinguishable
		// from a broken metric pipeline (metric absence). A genuine zero is
		// unexpected for Maple (~61 active loans today), so it also warns.
		s.telemetry.RecordRowsWritten(ctx, "maple_loan_state", 0)
		s.telemetry.RecordRowsWritten(ctx, "maple_loan_collateral", 0)
		s.logger.Warn("no active loans returned by the API")
		return nil
	}
	if err := requireUniqueIDs("loan", len(loans), func(i int) common.Address { return loans[i].LoanID }); err != nil {
		return err
	}

	// A loan whose pool is missing from the map is a hard error — pools were
	// fetched seconds earlier in this same cycle.
	for _, l := range loans {
		if _, ok := poolIDs[l.PoolAddress]; !ok {
			return fmt.Errorf("loan %s references unknown pool %s", lowerHex(l.LoanID), lowerHex(l.PoolAddress))
		}
		if l.AcmRatio == nil {
			s.telemetry.RecordNullDowngrade(ctx, "loan_acm_ratio")
		}
		if l.Collateral != nil {
			if l.Collateral.AssetAmount == nil {
				s.telemetry.RecordNullDowngrade(ctx, "collateral_asset_amount")
			}
			if l.Collateral.AssetValueUSD == nil {
				s.telemetry.RecordNullDowngrade(ctx, "collateral_asset_value_usd")
			}
			if l.Collateral.LiquidationLevel == nil {
				s.telemetry.RecordNullDowngrade(ctx, "collateral_liquidation_level")
			}
		}
	}

	borrowers := distinctAddresses(loans, func(l outbound.MapleActiveLoan) common.Address { return l.Borrower })

	collateralCount := 0
	err = s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		// FirstSeenBlock left nil: GraphQL data has no block context, and a nil
		// block preserves any existing on-chain first-seen block.
		borrowerUsers := make([]entity.User, len(borrowers))
		for i, addr := range borrowers {
			borrowerUsers[i] = entity.User{ChainID: s.config.ChainID, Address: addr}
		}
		borrowerIDs, err := s.userRepo.GetOrCreateUsers(ctx, tx, borrowerUsers)
		if err != nil {
			return fmt.Errorf("resolving borrowers: %w", err)
		}

		loanEntities, err := s.buildLoanEntities(loans, poolIDs, borrowerIDs, protocolID)
		if err != nil {
			return err
		}

		loanIDs, err := s.repo.RecordLoans(ctx, tx, syncedAt, loanEntities)
		if err != nil {
			return fmt.Errorf("recording loans: %w", err)
		}

		states, collaterals, err := buildLoanSnapshots(loans, loanIDs, syncedAt)
		if err != nil {
			return err
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

// buildLoanEntities maps API loans to registry entities with resolved pool
// and borrower ids.
func (s *Service) buildLoanEntities(loans []outbound.MapleActiveLoan, poolIDs map[common.Address]int64, borrowerIDs map[common.Address]int64, protocolID int64) ([]*maple.Loan, error) {
	loanEntities := make([]*maple.Loan, 0, len(loans))
	for _, l := range loans {
		borrowerUserID, ok := borrowerIDs[l.Borrower]
		if !ok {
			return nil, fmt.Errorf("borrower %s missing from upsert result", lowerHex(l.Borrower))
		}
		loanEntity, err := maple.NewLoan(
			s.config.ChainID, protocolID, l.LoanID.Bytes(),
			poolIDs[l.PoolAddress], borrowerUserID, toEntityLoanMeta(l.LoanMeta),
		)
		if err != nil {
			return nil, fmt.Errorf("loan %s: %w", lowerHex(l.LoanID), err)
		}
		loanEntities = append(loanEntities, loanEntity)
	}
	return loanEntities, nil
}

// buildLoanSnapshots maps API loans to state and collateral snapshot
// entities. Loans with null API collateral simply have no collateral row.
func buildLoanSnapshots(loans []outbound.MapleActiveLoan, loanIDs map[common.Address]int64, syncedAt time.Time) ([]*maple.LoanState, []*maple.LoanCollateral, error) {
	states := make([]*maple.LoanState, 0, len(loans))
	collaterals := make([]*maple.LoanCollateral, 0, len(loans))
	for _, l := range loans {
		loanID, ok := loanIDs[l.LoanID]
		if !ok {
			return nil, nil, fmt.Errorf("loan %s missing from record result", lowerHex(l.LoanID))
		}

		state, err := maple.NewLoanState(loanID, syncedAt, l.State, l.PrincipalOwed, l.AcmRatio)
		if err != nil {
			return nil, nil, fmt.Errorf("loan state %s: %w", lowerHex(l.LoanID), err)
		}
		states = append(states, state)

		if l.Collateral == nil {
			continue
		}
		collateralDecimals, err := toInt16(l.Collateral.Decimals)
		if err != nil {
			return nil, nil, fmt.Errorf("loan collateral %s: decimals: %w", lowerHex(l.LoanID), err)
		}
		collateral, err := maple.NewLoanCollateral(maple.LoanCollateralParams{
			LoanID:           loanID,
			SyncedAt:         syncedAt,
			AssetSymbol:      l.Collateral.Asset,
			AssetAmount:      l.Collateral.AssetAmount,
			AssetDecimals:    collateralDecimals,
			AssetValueUSD:    l.Collateral.AssetValueUSD,
			State:            l.Collateral.State,
			Custodian:        l.Collateral.Custodian,
			LiquidationLevel: l.Collateral.LiquidationLevel,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("loan collateral %s: %w", lowerHex(l.LoanID), err)
		}
		collaterals = append(collaterals, collateral)
	}
	return states, collaterals, nil
}

// distinctAddresses returns the unique addresses produced by key over items,
// preserving first-appearance order.
func distinctAddresses[T any](items []T, key func(T) common.Address) []common.Address {
	seen := make(map[common.Address]struct{}, len(items))
	out := make([]common.Address, 0, len(items))
	for _, it := range items {
		addr := key(it)
		if _, ok := seen[addr]; ok {
			continue
		}
		seen[addr] = struct{}{}
		out = append(out, addr)
	}
	return out
}

// toEntityLoanMeta maps the client DTO meta to the entity meta.
func toEntityLoanMeta(meta *outbound.MapleLoanMeta) *maple.LoanMeta {
	if meta == nil {
		return nil
	}
	return &maple.LoanMeta{
		Type:          meta.Type,
		AssetSymbol:   meta.AssetSymbol,
		DexName:       meta.DexName,
		WalletAddress: meta.WalletAddress,
		WalletType:    meta.WalletType,
		Location:      meta.Location,
	}
}

// ---------------------------------------------------------------------------
// Phase 2b: fixed-term loans
// ---------------------------------------------------------------------------

// syncFixedTermLoans fetches all live fixed-term loans and persists borrowers,
// asset tokens, registry rows and state snapshots in a single transaction, so a
// loan snapshot is all-or-nothing. Unlike the OTL loans phase, an empty result
// is the expected steady state today (the FTL product is dormant, not retired):
// it info-logs, writes zero rows, and succeeds. A nonzero result is the signal
// the book woke up.
func (s *Service) syncFixedTermLoans(ctx context.Context, syncedAt time.Time, poolIDs map[common.Address]int64, protocolID int64) error {
	loans, err := s.client.GetActiveFixedTermLoans(ctx)
	if err != nil {
		return fmt.Errorf("fetching fixed-term loans: %w", err)
	}
	if len(loans) == 0 {
		// Emit an explicit zero so the dormant book is distinguishable from a
		// broken metric pipeline (metric absence). Zero is expected here, so
		// unlike the OTL phase this is info, not warn.
		s.telemetry.RecordRowsWritten(ctx, "maple_ftl_loan_state", 0)
		s.logger.Info("no live fixed-term loans returned by the API (expected while the FTL product is dormant)")
		return nil
	}
	if err := requireUniqueIDs("fixed-term loan", len(loans), func(i int) common.Address { return loans[i].LoanID }); err != nil {
		return err
	}

	for _, l := range loans {
		if _, ok := poolIDs[l.PoolAddress]; !ok {
			return fmt.Errorf("fixed-term loan %s references unknown pool %s", lowerHex(l.LoanID), lowerHex(l.PoolAddress))
		}
		if l.AcmRatio == nil {
			s.telemetry.RecordNullDowngrade(ctx, "ftl_acm_ratio")
		}
		if l.StateDetail == "" {
			s.telemetry.RecordNullDowngrade(ctx, "ftl_state_detail")
		}
	}

	assets, err := distinctFTLAssetTokens(s.config.ChainID, loans)
	if err != nil {
		return err
	}
	borrowers := distinctAddresses(loans, func(l outbound.MapleFixedTermLoan) common.Address { return l.Borrower })

	err = s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		assetTokenIDs, err := s.tokenRepo.GetOrCreateTokens(ctx, tx, assets)
		if err != nil {
			return fmt.Errorf("resolving fixed-term loan asset tokens: %w", err)
		}

		borrowerUsers := make([]entity.User, len(borrowers))
		for i, addr := range borrowers {
			borrowerUsers[i] = entity.User{ChainID: s.config.ChainID, Address: addr}
		}
		borrowerIDs, err := s.userRepo.GetOrCreateUsers(ctx, tx, borrowerUsers)
		if err != nil {
			return fmt.Errorf("resolving fixed-term loan borrowers: %w", err)
		}

		loanEntities, err := s.buildFTLLoanEntities(loans, poolIDs, borrowerIDs, assetTokenIDs, protocolID)
		if err != nil {
			return err
		}

		loanIDs, err := s.repo.UpsertFixedTermLoans(ctx, tx, loanEntities)
		if err != nil {
			return fmt.Errorf("upserting fixed-term loans: %w", err)
		}

		states, err := buildFTLLoanStates(loans, loanIDs, syncedAt)
		if err != nil {
			return err
		}
		if err := s.repo.SaveFixedTermLoanStates(ctx, tx, states); err != nil {
			return fmt.Errorf("saving fixed-term loan states: %w", err)
		}
		return nil
	})
	if err != nil {
		return err
	}

	s.telemetry.RecordRowsWritten(ctx, "maple_ftl_loan_state", len(loans))
	s.logger.Info("fixed-term loans synced", "count", len(loans), "borrowers", len(borrowers))
	return nil
}

// buildFTLLoanEntities maps API fixed-term loans to registry entities with
// resolved pool, borrower, and asset-token ids.
func (s *Service) buildFTLLoanEntities(loans []outbound.MapleFixedTermLoan, poolIDs, borrowerIDs, assetTokenIDs map[common.Address]int64, protocolID int64) ([]*maple.FTLLoan, error) {
	loanEntities := make([]*maple.FTLLoan, 0, len(loans))
	for _, l := range loans {
		borrowerUserID, ok := borrowerIDs[l.Borrower]
		if !ok {
			return nil, fmt.Errorf("fixed-term loan %s: borrower %s missing from upsert result", lowerHex(l.LoanID), lowerHex(l.Borrower))
		}
		collateralTokenID, ok := assetTokenIDs[l.Collateral.Address]
		if !ok {
			return nil, fmt.Errorf("fixed-term loan %s: collateral token %s missing from upsert result", lowerHex(l.LoanID), lowerHex(l.Collateral.Address))
		}
		fundsTokenID, ok := assetTokenIDs[l.Funds.Address]
		if !ok {
			return nil, fmt.Errorf("fixed-term loan %s: funds token %s missing from upsert result", lowerHex(l.LoanID), lowerHex(l.Funds.Address))
		}
		// poolIDs membership is also checked up front in syncFixedTermLoans;
		// resolve with the ok guard here too so a future refactor can never
		// silently write maple_pool_id = 0.
		poolID, ok := poolIDs[l.PoolAddress]
		if !ok {
			return nil, fmt.Errorf("fixed-term loan %s references unknown pool %s", lowerHex(l.LoanID), lowerHex(l.PoolAddress))
		}
		loanEntity, err := maple.NewFTLLoan(
			s.config.ChainID, protocolID, l.LoanID.Bytes(),
			poolID, borrowerUserID, collateralTokenID, fundsTokenID,
		)
		if err != nil {
			return nil, fmt.Errorf("fixed-term loan %s: %w", lowerHex(l.LoanID), err)
		}
		loanEntities = append(loanEntities, loanEntity)
	}
	return loanEntities, nil
}

// buildFTLLoanStates maps API fixed-term loans to state snapshot entities,
// converting the API epoch-second sentinel 0 to a nil timestamp (SQL NULL).
func buildFTLLoanStates(loans []outbound.MapleFixedTermLoan, loanIDs map[common.Address]int64, syncedAt time.Time) ([]*maple.FTLLoanState, error) {
	states := make([]*maple.FTLLoanState, 0, len(loans))
	for _, l := range loans {
		loanID, ok := loanIDs[l.LoanID]
		if !ok {
			return nil, fmt.Errorf("fixed-term loan %s missing from upsert result", lowerHex(l.LoanID))
		}
		state, err := maple.NewFTLLoanState(maple.FTLLoanStateParams{
			LoanID:              loanID,
			SyncedAt:            syncedAt,
			State:               l.State,
			StateDetail:         l.StateDetail,
			PrincipalOwed:       l.PrincipalOwed,
			InterestRate:        l.InterestRate,
			InterestPaid:        l.InterestPaid,
			PaymentsRemaining:   l.PaymentsRemaining,
			PaymentIntervalDays: l.PaymentIntervalDays,
			TermDays:            l.TermDays,
			MaturityDate:        epochToTime(l.MaturityDate),
			NextPaymentDue:      epochToTime(l.NextPaymentDue),
			CollateralAmount:    l.CollateralAmount,
			CollateralRequired:  l.CollateralRequired,
			CollateralRatio:     l.CollateralRatio,
			DrawdownAmount:      l.DrawdownAmount,
			ClaimableAmount:     l.ClaimableAmount,
			AcmRatio:            l.AcmRatio,
			IsImpaired:          l.IsImpaired,
		})
		if err != nil {
			return nil, fmt.Errorf("fixed-term loan state %s: %w", lowerHex(l.LoanID), err)
		}
		states = append(states, state)
	}
	return states, nil
}

// epochToTime converts an API epoch-second timestamp to a *time.Time, mapping
// the sentinel 0 (pre-funding / none due) to nil so it persists as SQL NULL
// rather than 1970-01-01.
func epochToTime(secs int64) *time.Time {
	if secs == 0 {
		return nil
	}
	t := time.Unix(secs, 0).UTC()
	return &t
}

// distinctFTLAssetTokens extracts the unique collateral and funds tokens across
// all loans as token-registry upsert inputs, validating each asset's metadata
// and failing when the same address is reported with conflicting symbol or
// decimals. CreatedAtBlock is left nil: GraphQL data has no block context.
func distinctFTLAssetTokens(chainID int64, loans []outbound.MapleFixedTermLoan) ([]outbound.TokenInput, error) {
	seen := make(map[common.Address]outbound.TokenInput, len(loans)*2)
	assets := make([]outbound.TokenInput, 0, len(loans)*2)
	add := func(loanID common.Address, a outbound.MapleAssetToken) error {
		if a.Symbol == "" {
			return fmt.Errorf("fixed-term loan %s: asset %s: symbol must not be empty", lowerHex(loanID), lowerHex(a.Address))
		}
		decimals, err := toInt16(a.Decimals)
		if err != nil {
			return fmt.Errorf("fixed-term loan %s: asset %s decimals: %w", lowerHex(loanID), lowerHex(a.Address), err)
		}
		asset := outbound.TokenInput{ChainID: chainID, Address: a.Address, Symbol: a.Symbol, Decimals: int(decimals)}
		if prev, ok := seen[a.Address]; ok {
			if prev.Symbol != asset.Symbol || prev.Decimals != asset.Decimals {
				return fmt.Errorf("asset %s reported with conflicting metadata: %s/%d vs %s/%d",
					lowerHex(a.Address), prev.Symbol, prev.Decimals, asset.Symbol, asset.Decimals)
			}
			return nil
		}
		seen[a.Address] = asset
		assets = append(assets, asset)
		return nil
	}
	for _, l := range loans {
		if err := add(l.LoanID, l.Collateral); err != nil {
			return nil, err
		}
		if err := add(l.LoanID, l.Funds); err != nil {
			return nil, err
		}
	}
	return assets, nil
}

// ---------------------------------------------------------------------------
// Phase 3: Sky strategies
// ---------------------------------------------------------------------------

// syncSkyStrategies fetches all Sky strategies and persists registry rows
// plus one state snapshot per strategy in a single transaction.
func (s *Service) syncSkyStrategies(ctx context.Context, syncedAt time.Time, poolIDs map[common.Address]int64) error {
	strategies, err := s.client.GetSkyStrategies(ctx)
	if err != nil {
		return fmt.Errorf("fetching sky strategies: %w", err)
	}
	if len(strategies) == 0 {
		// Emit an explicit zero so an empty strategy set is distinguishable
		// from a broken metric pipeline (metric absence).
		s.telemetry.RecordRowsWritten(ctx, "maple_sky_strategy_state", 0)
		s.logger.Warn("no sky strategies returned by the API")
		return nil
	}
	if err := requireUniqueIDs("sky strategy", len(strategies), func(i int) common.Address { return strategies[i].Address }); err != nil {
		return err
	}

	strategyEntities := make([]*maple.SkyStrategy, 0, len(strategies))
	for _, st := range strategies {
		if st.StrategyFeeRate == nil {
			s.telemetry.RecordNullDowngrade(ctx, "strategy_fee_rate")
		}
		if st.TotalFeesCollected == nil {
			s.telemetry.RecordNullDowngrade(ctx, "strategy_total_fees_collected")
		}
		poolID, ok := poolIDs[st.PoolAddress]
		if !ok {
			return fmt.Errorf("sky strategy %s references unknown pool %s", lowerHex(st.Address), lowerHex(st.PoolAddress))
		}
		strategyEntity, err := maple.NewSkyStrategy(s.config.ChainID, st.Address.Bytes(), poolID, st.Version)
		if err != nil {
			return fmt.Errorf("sky strategy %s: %w", lowerHex(st.Address), err)
		}
		strategyEntities = append(strategyEntities, strategyEntity)
	}

	err = s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		strategyIDs, err := s.repo.RecordSkyStrategies(ctx, tx, syncedAt, strategyEntities)
		if err != nil {
			return fmt.Errorf("recording sky strategies: %w", err)
		}

		states := make([]*maple.SkyStrategyState, 0, len(strategies))
		for _, st := range strategies {
			strategyID, ok := strategyIDs[st.Address]
			if !ok {
				return fmt.Errorf("sky strategy %s missing from record result", lowerHex(st.Address))
			}
			state, err := maple.NewSkyStrategyState(maple.SkyStrategyStateParams{
				SkyStrategyID:      strategyID,
				SyncedAt:           syncedAt,
				State:              st.State,
				CurrentlyDeployed:  st.CurrentlyDeployed,
				DepositedAssets:    st.DepositedAssets,
				WithdrawnAssets:    st.WithdrawnAssets,
				StrategyFeeRate:    st.StrategyFeeRate,
				TotalFeesCollected: st.TotalFeesCollected,
			})
			if err != nil {
				return fmt.Errorf("sky strategy state %s: %w", lowerHex(st.Address), err)
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
	if globals.DripsYieldBoost == nil {
		s.telemetry.RecordNullDowngrade(ctx, "syrup_drips_yield_boost")
	}

	state, err := maple.NewSyrupGlobalState(
		s.config.ChainID, syncedAt, globals.TVL, globals.APY,
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

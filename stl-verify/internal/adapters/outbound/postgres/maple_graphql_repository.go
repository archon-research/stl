package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity/maple"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that MapleGraphQLRepository implements the port.
var _ outbound.MapleGraphQLRepository = (*MapleGraphQLRepository)(nil)

// MapleGraphQLRepository is a PostgreSQL implementation of the
// outbound.MapleGraphQLRepository port.
type MapleGraphQLRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	buildID   buildregistry.BuildID
	batchSize int
}

// NewMapleGraphQLRepository creates a new PostgreSQL Maple GraphQL repository.
// If batchSize is <= 0, a default batch size of 1000 is used.
func NewMapleGraphQLRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID, batchSize int) (*MapleGraphQLRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &MapleGraphQLRepository{
		pool:      pool,
		logger:    logger,
		buildID:   buildID,
		batchSize: batchSize,
	}, nil
}

// GetMapleProtocolID resolves the seeded maple protocol row id for a chain.
// The row is seeded by the create-maple-tables migration, so absence is a
// hard configuration error.
func (r *MapleGraphQLRepository) GetMapleProtocolID(ctx context.Context, chainID int64) (int64, error) {
	var id int64
	err := r.pool.QueryRow(ctx,
		`SELECT id FROM protocol WHERE chain_id = $1 AND name = 'maple'`,
		chainID,
	).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, fmt.Errorf("maple protocol row not found for chain %d (migration not applied?)", chainID)
	}
	if err != nil {
		return 0, fmt.Errorf("querying maple protocol: %w", err)
	}
	return id, nil
}

// GetOrCreateBorrowerUsers bulk-upserts borrower addresses into "user" and
// returns address -> user id.
//
// Unlike UserRepository.GetOrCreateUser, this inserts NULL first_seen_block
// (GraphQL data has no block context) and never modifies first_seen_block on
// conflict — that method's LEAST() merge would clobber existing users'
// first_seen_block down to our zero value.
func (r *MapleGraphQLRepository) GetOrCreateBorrowerUsers(ctx context.Context, tx pgx.Tx, chainID int64, borrowers []common.Address) (map[common.Address]int64, error) {
	if len(borrowers) == 0 {
		return make(map[common.Address]int64), nil
	}

	// Sort + dedupe for a stable row-lock acquisition order across
	// concurrent writers.
	sorted := make([]common.Address, len(borrowers))
	copy(sorted, borrowers)
	slices.SortFunc(sorted, func(a, b common.Address) int { return a.Cmp(b) })
	sorted = slices.Compact(sorted)

	batch := &pgx.Batch{}
	for _, addr := range sorted {
		// The no-op DO UPDATE (instead of DO NOTHING) makes RETURNING yield
		// the existing row's id on conflict; DO NOTHING returns no row.
		batch.Queue(
			`INSERT INTO "user" (chain_id, address, created_at, updated_at, metadata)
			 VALUES ($1, $2, NOW(), NOW(), '{}'::jsonb)
			 ON CONFLICT (chain_id, address) DO UPDATE SET id = "user".id
			 RETURNING id`,
			chainID, addr.Bytes(),
		)
	}

	return collectBatchIDs(ctx, tx, batch, sorted, "borrower user",
		func(addr common.Address) common.Address { return addr })
}

// UpsertPools upserts pool registry rows and returns
// address -> maple_pool.id. On conflict, refreshes name, asset details, and
// is_syrup.
func (r *MapleGraphQLRepository) UpsertPools(ctx context.Context, tx pgx.Tx, pools []*maple.Pool) (map[common.Address]int64, error) {
	if len(pools) == 0 {
		return make(map[common.Address]int64), nil
	}

	sorted := sortedByBytesKey(pools, func(p *maple.Pool) []byte { return p.Address })

	batch := &pgx.Batch{}
	for _, p := range sorted {
		batch.Queue(
			`INSERT INTO maple_pool (chain_id, protocol_id, address, name, asset_address, asset_symbol, asset_decimals, is_syrup)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			 ON CONFLICT (chain_id, address) DO UPDATE SET
			     name = EXCLUDED.name,
			     asset_address = EXCLUDED.asset_address,
			     asset_symbol = EXCLUDED.asset_symbol,
			     asset_decimals = EXCLUDED.asset_decimals,
			     is_syrup = EXCLUDED.is_syrup
			 RETURNING id`,
			p.ChainID, p.ProtocolID, p.Address, p.Name, p.AssetAddress, p.AssetSymbol, p.AssetDecimals, p.IsSyrup,
		)
	}

	return collectBatchIDs(ctx, tx, batch, sorted, "maple pool",
		func(p *maple.Pool) common.Address { return common.BytesToAddress(p.Address) })
}

// SavePoolStates inserts pool state snapshots. The BEFORE INSERT trigger
// assigns processing_version; ON CONFLICT DO NOTHING dedupes same-build
// retries (ADR-0002).
func (r *MapleGraphQLRepository) SavePoolStates(ctx context.Context, tx pgx.Tx, states []*maple.PoolState) error {
	if len(states) == 0 {
		return nil
	}

	// Sort a copy by natural key for stable advisory-lock acquisition order
	// (the caller's slice is not mutated).
	sorted := sortedCopy(states, func(a, b *maple.PoolState) int {
		return cmp.Or(
			cmp.Compare(a.PoolID, b.PoolID),
			a.SyncedAt.Compare(b.SyncedAt),
		)
	})

	for chunk := range slices.Chunk(sorted, r.batchSize) {
		if err := r.savePoolStateBatch(ctx, tx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (r *MapleGraphQLRepository) savePoolStateBatch(ctx context.Context, tx pgx.Tx, states []*maple.PoolState) error {
	const cols = 10
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_pool_state (maple_pool_id, synced_at, tvl, liquid_assets, collateral_value_usd, principal_out, utilization, monthly_apy, spot_apy, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		liquidAssets, err := bigIntToNumeric(s.LiquidAssets)
		if err != nil {
			return fmt.Errorf("converting liquid_assets for pool %d: %w", s.PoolID, err)
		}
		principalOut, err := bigIntToNumeric(s.PrincipalOut)
		if err != nil {
			return fmt.Errorf("converting principal_out for pool %d: %w", s.PoolID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.PoolID, s.SyncedAt, optionalNumeric(s.TVL), liquidAssets,
			optionalNumeric(s.CollateralValueUSD), principalOut, s.Utilization,
			optionalNumeric(s.MonthlyAPY), optionalNumeric(s.SpotAPY), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_pool_id, synced_at, processing_version) DO NOTHING`)

	tag, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("saving maple pool states: %w", err)
	}
	r.warnDedupedRows("maple_pool_state", tag, len(states))
	return nil
}

// UpsertLoans upserts loan registry rows and returns loan
// address -> maple_loan.id. On conflict, refreshes the pool reference and the
// loanMeta columns (a loan's metadata can change between snapshots).
// borrower_user_id is deliberately not refreshed: a loan contract's borrower
// is immutable, so the value from first insert stays authoritative — and the
// stored value is compared against the incoming one so a violation of that
// assumption fails the call instead of vanishing.
func (r *MapleGraphQLRepository) UpsertLoans(ctx context.Context, tx pgx.Tx, loans []*maple.Loan) (map[common.Address]int64, error) {
	if len(loans) == 0 {
		return make(map[common.Address]int64), nil
	}

	sorted := sortedByBytesKey(loans, func(l *maple.Loan) []byte { return l.LoanAddress })

	batch := &pgx.Batch{}
	for _, l := range sorted {
		var metaType, metaAssetSymbol, metaDex, metaWalletAddress, metaWalletType, metaLocation *string
		if l.LoanMeta != nil {
			metaType = nullIfEmpty(l.LoanMeta.Type)
			metaAssetSymbol = nullIfEmpty(l.LoanMeta.AssetSymbol)
			metaDex = nullIfEmpty(l.LoanMeta.DexName)
			metaWalletAddress = nullIfEmpty(l.LoanMeta.WalletAddress)
			metaWalletType = nullIfEmpty(l.LoanMeta.WalletType)
			metaLocation = nullIfEmpty(l.LoanMeta.Location)
		}

		batch.Queue(
			`INSERT INTO maple_loan (chain_id, protocol_id, loan_address, loan_type, maple_pool_id, borrower_user_id,
			                         loan_meta_type, loan_meta_asset_symbol, loan_meta_dex, loan_meta_wallet_address,
			                         loan_meta_wallet_type, loan_meta_location)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
			 ON CONFLICT (chain_id, loan_address) DO UPDATE SET
			     maple_pool_id = EXCLUDED.maple_pool_id,
			     loan_meta_type = EXCLUDED.loan_meta_type,
			     loan_meta_asset_symbol = EXCLUDED.loan_meta_asset_symbol,
			     loan_meta_dex = EXCLUDED.loan_meta_dex,
			     loan_meta_wallet_address = EXCLUDED.loan_meta_wallet_address,
			     loan_meta_wallet_type = EXCLUDED.loan_meta_wallet_type,
			     loan_meta_location = EXCLUDED.loan_meta_location
			 RETURNING id, borrower_user_id`,
			l.ChainID, l.ProtocolID, l.LoanAddress, l.LoanType, l.PoolID, l.BorrowerUserID,
			metaType, metaAssetSymbol, metaDex, metaWalletAddress, metaWalletType, metaLocation,
		)
	}

	// The scan enforces borrower immutability: the upsert never refreshes
	// borrower_user_id, so a stored value differing from the API's resolved
	// borrower means the "a loan contract's borrower is immutable"
	// assumption broke upstream — fail loudly instead of silently keeping
	// the stale association.
	return collectBatchRows(ctx, tx, batch, sorted, "maple loan",
		func(row pgx.Row, l *maple.Loan) (common.Address, int64, error) {
			addr := common.BytesToAddress(l.LoanAddress)
			var id, storedBorrower int64
			if err := row.Scan(&id, &storedBorrower); err != nil {
				return common.Address{}, 0, fmt.Errorf("upserting maple loan %s: %w", addr, err)
			}
			if storedBorrower != l.BorrowerUserID {
				return common.Address{}, 0, fmt.Errorf("maple loan %s borrower changed: stored user id %d, API resolved user id %d (loan borrowers are immutable; refusing the snapshot)", addr, storedBorrower, l.BorrowerUserID)
			}
			return addr, id, nil
		})
}

// SaveLoanStates inserts loan state snapshots (same trigger/conflict
// semantics as SavePoolStates).
func (r *MapleGraphQLRepository) SaveLoanStates(ctx context.Context, tx pgx.Tx, states []*maple.LoanState) error {
	if len(states) == 0 {
		return nil
	}

	sorted := sortedCopy(states, func(a, b *maple.LoanState) int {
		return cmp.Or(
			cmp.Compare(a.LoanID, b.LoanID),
			a.SyncedAt.Compare(b.SyncedAt),
		)
	})

	for chunk := range slices.Chunk(sorted, r.batchSize) {
		if err := r.saveLoanStateBatch(ctx, tx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (r *MapleGraphQLRepository) saveLoanStateBatch(ctx context.Context, tx pgx.Tx, states []*maple.LoanState) error {
	const cols = 6
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, acm_ratio, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		principalOwed, err := bigIntToNumeric(s.PrincipalOwed)
		if err != nil {
			return fmt.Errorf("converting principal_owed for loan %d: %w", s.LoanID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.LoanID, s.SyncedAt, s.State, principalOwed, optionalNumeric(s.AcmRatio), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_loan_id, synced_at, processing_version) DO NOTHING`)

	tag, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("saving maple loan states: %w", err)
	}
	r.warnDedupedRows("maple_loan_state", tag, len(states))
	return nil
}

// SaveLoanCollaterals inserts loan collateral snapshots. Loans with null API
// collateral have no row; callers pass only non-nil collaterals.
func (r *MapleGraphQLRepository) SaveLoanCollaterals(ctx context.Context, tx pgx.Tx, collaterals []*maple.LoanCollateral) error {
	if len(collaterals) == 0 {
		return nil
	}

	sorted := sortedCopy(collaterals, func(a, b *maple.LoanCollateral) int {
		return cmp.Or(
			cmp.Compare(a.LoanID, b.LoanID),
			a.SyncedAt.Compare(b.SyncedAt),
		)
	})

	for chunk := range slices.Chunk(sorted, r.batchSize) {
		if err := r.saveLoanCollateralBatch(ctx, tx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (r *MapleGraphQLRepository) saveLoanCollateralBatch(ctx context.Context, tx pgx.Tx, collaterals []*maple.LoanCollateral) error {
	const cols = 10
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_loan_collateral (maple_loan_id, synced_at, asset_symbol, asset_amount, asset_decimals, asset_value_usd, state, custodian, liquidation_level, build_id) VALUES `)

	args := make([]any, 0, len(collaterals)*cols)
	for i, c := range collaterals {
		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, c.LoanID, c.SyncedAt, c.AssetSymbol, optionalNumeric(c.AssetAmount), c.AssetDecimals,
			optionalNumeric(c.AssetValueUSD), nullIfEmpty(c.State), nullIfEmpty(c.Custodian), optionalNumeric(c.LiquidationLevel), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_loan_id, synced_at, processing_version) DO NOTHING`)

	tag, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("saving maple loan collaterals: %w", err)
	}
	r.warnDedupedRows("maple_loan_collateral", tag, len(collaterals))
	return nil
}

// UpsertSkyStrategies upserts strategy registry rows and returns strategy
// address -> maple_sky_strategy.id. On conflict, refreshes the pool
// reference and version.
func (r *MapleGraphQLRepository) UpsertSkyStrategies(ctx context.Context, tx pgx.Tx, strategies []*maple.SkyStrategy) (map[common.Address]int64, error) {
	if len(strategies) == 0 {
		return make(map[common.Address]int64), nil
	}

	sorted := sortedByBytesKey(strategies, func(s *maple.SkyStrategy) []byte { return s.StrategyAddress })

	batch := &pgx.Batch{}
	for _, s := range sorted {
		batch.Queue(
			`INSERT INTO maple_sky_strategy (chain_id, strategy_address, maple_pool_id, version)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (chain_id, strategy_address) DO UPDATE SET
			     maple_pool_id = EXCLUDED.maple_pool_id,
			     version = EXCLUDED.version
			 RETURNING id`,
			s.ChainID, s.StrategyAddress, s.PoolID, s.Version,
		)
	}

	return collectBatchIDs(ctx, tx, batch, sorted, "maple sky strategy",
		func(s *maple.SkyStrategy) common.Address { return common.BytesToAddress(s.StrategyAddress) })
}

// SaveSkyStrategyStates inserts strategy state snapshots (same
// trigger/conflict semantics as SavePoolStates).
func (r *MapleGraphQLRepository) SaveSkyStrategyStates(ctx context.Context, tx pgx.Tx, states []*maple.SkyStrategyState) error {
	if len(states) == 0 {
		return nil
	}

	sorted := sortedCopy(states, func(a, b *maple.SkyStrategyState) int {
		return cmp.Or(
			cmp.Compare(a.SkyStrategyID, b.SkyStrategyID),
			a.SyncedAt.Compare(b.SyncedAt),
		)
	})

	for chunk := range slices.Chunk(sorted, r.batchSize) {
		if err := r.saveSkyStrategyStateBatch(ctx, tx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func (r *MapleGraphQLRepository) saveSkyStrategyStateBatch(ctx context.Context, tx pgx.Tx, states []*maple.SkyStrategyState) error {
	const cols = 9
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_sky_strategy_state (maple_sky_strategy_id, synced_at, state, currently_deployed, deposited_assets, withdrawn_assets, strategy_fee_rate, total_fees_collected, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		currentlyDeployed, err := bigIntToNumeric(s.CurrentlyDeployed)
		if err != nil {
			return fmt.Errorf("converting currently_deployed for strategy %d: %w", s.SkyStrategyID, err)
		}
		depositedAssets, err := bigIntToNumeric(s.DepositedAssets)
		if err != nil {
			return fmt.Errorf("converting deposited_assets for strategy %d: %w", s.SkyStrategyID, err)
		}
		withdrawnAssets, err := bigIntToNumeric(s.WithdrawnAssets)
		if err != nil {
			return fmt.Errorf("converting withdrawn_assets for strategy %d: %w", s.SkyStrategyID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.SkyStrategyID, s.SyncedAt, s.State, currentlyDeployed, depositedAssets,
			withdrawnAssets, optionalNumeric(s.StrategyFeeRate), optionalNumeric(s.TotalFeesCollected), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_sky_strategy_id, synced_at, processing_version) DO NOTHING`)

	tag, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("saving maple sky strategy states: %w", err)
	}
	r.warnDedupedRows("maple_sky_strategy_state", tag, len(states))
	return nil
}

// SaveSyrupGlobalState inserts the protocol-wide Syrup aggregate snapshot
// (same trigger/conflict semantics as SavePoolStates).
func (r *MapleGraphQLRepository) SaveSyrupGlobalState(ctx context.Context, tx pgx.Tx, state *maple.SyrupGlobalState) error {
	if state == nil {
		return fmt.Errorf("syrup global state cannot be nil")
	}

	tvl, err := bigIntToNumeric(state.TVL)
	if err != nil {
		return fmt.Errorf("converting tvl: %w", err)
	}
	apy, err := bigIntToNumeric(state.APY)
	if err != nil {
		return fmt.Errorf("converting apy: %w", err)
	}
	collateralAPY, err := bigIntToNumeric(state.CollateralAPY)
	if err != nil {
		return fmt.Errorf("converting collateral_apy: %w", err)
	}
	poolAPY, err := bigIntToNumeric(state.PoolAPY)
	if err != nil {
		return fmt.Errorf("converting pool_apy: %w", err)
	}

	tag, err := tx.Exec(ctx,
		`INSERT INTO maple_syrup_global_state (chain_id, synced_at, tvl, apy, collateral_apy, pool_apy, drips_yield_boost, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (chain_id, synced_at, processing_version) DO NOTHING`,
		state.ChainID, state.SyncedAt, tvl, apy, collateralAPY, poolAPY,
		optionalNumeric(state.DripsYieldBoost), int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving maple syrup global state: %w", err)
	}
	r.warnDedupedRows("maple_syrup_global_state", tag, 1)
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// warnDedupedRows makes ON CONFLICT DO NOTHING dedup visible. A full dedup
// (zero rows inserted) is the signature of a Temporal activity retry
// re-running an already-persisted phase at the same synced_at and build (the
// trigger reuses the processing_version and the insert dedupes), so it logs
// at warn. A partial dedup within a single batch is never a retry artifact —
// some rows collided while siblings did not (clock regression, a duplicate
// that slipped the service guards, a trigger bug assigning a colliding
// version) — so it logs at error.
func (r *MapleGraphQLRepository) warnDedupedRows(table string, tag pgconn.CommandTag, expected int) {
	inserted := tag.RowsAffected()
	switch {
	case inserted == int64(expected):
	case inserted == 0:
		r.logger.Warn("state insert fully deduplicated by ON CONFLICT DO NOTHING (expected on activity retries)",
			"table", table,
			"expected", expected,
			"inserted", inserted,
		)
	default:
		r.logger.Error("state insert PARTIALLY deduplicated by ON CONFLICT DO NOTHING (never a retry artifact; investigate)",
			"table", table,
			"expected", expected,
			"inserted", inserted,
		)
	}
}

package postgres

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"slices"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
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

// UpsertPools upserts pool registry rows and returns lowercase hex
// address -> maple_pool.id. On conflict, refreshes name, asset details, and
// is_syrup.
func (r *MapleGraphQLRepository) UpsertPools(ctx context.Context, tx pgx.Tx, pools []*entity.MaplePool) (map[string]int64, error) {
	if len(pools) == 0 {
		return make(map[string]int64), nil
	}

	sorted := sortedByBytesKey(pools, func(p *entity.MaplePool) []byte { return p.Address })

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
		func(p *entity.MaplePool) string { return addressKey(p.Address) })
}

// SavePoolStates inserts pool state snapshots. The BEFORE INSERT trigger
// assigns processing_version; ON CONFLICT DO NOTHING dedupes same-build
// retries (ADR-0002).
func (r *MapleGraphQLRepository) SavePoolStates(ctx context.Context, tx pgx.Tx, states []*entity.MaplePoolState) error {
	if len(states) == 0 {
		return nil
	}

	// Sort a copy by natural key for stable advisory-lock acquisition order
	// (the caller's slice is not mutated).
	sorted := sortedCopy(states, func(a, b *entity.MaplePoolState) int {
		return cmp.Or(
			cmp.Compare(a.MaplePoolID, b.MaplePoolID),
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

func (r *MapleGraphQLRepository) savePoolStateBatch(ctx context.Context, tx pgx.Tx, states []*entity.MaplePoolState) error {
	const cols = 10
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_pool_state (maple_pool_id, synced_at, tvl, liquid_assets, collateral_value_usd, principal_out, utilization, monthly_apy, spot_apy, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		tvl, err := bigIntToNumeric(s.TVL)
		if err != nil {
			return fmt.Errorf("converting tvl for pool %d: %w", s.MaplePoolID, err)
		}
		liquidAssets, err := bigIntToNumeric(s.LiquidAssets)
		if err != nil {
			return fmt.Errorf("converting liquid_assets for pool %d: %w", s.MaplePoolID, err)
		}
		collateralValueUSD, err := bigIntToNumeric(s.CollateralValueUSD)
		if err != nil {
			return fmt.Errorf("converting collateral_value_usd for pool %d: %w", s.MaplePoolID, err)
		}
		principalOut, err := bigIntToNumeric(s.PrincipalOut)
		if err != nil {
			return fmt.Errorf("converting principal_out for pool %d: %w", s.MaplePoolID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.MaplePoolID, s.SyncedAt, tvl, liquidAssets, collateralValueUSD,
			principalOut, s.Utilization, optionalNumeric(s.MonthlyAPY), optionalNumeric(s.SpotAPY), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_pool_id, synced_at, processing_version) DO NOTHING`)

	if _, err := tx.Exec(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("saving maple pool states: %w", err)
	}
	return nil
}

// UpsertLoans upserts loan registry rows and returns lowercase hex loan
// address -> maple_loan.id. On conflict, refreshes the pool reference and the
// loanMeta columns (a loan's metadata can change between snapshots).
// borrower_user_id is deliberately not refreshed: a loan contract's borrower
// is immutable, so the value from first insert stays authoritative.
func (r *MapleGraphQLRepository) UpsertLoans(ctx context.Context, tx pgx.Tx, loans []*entity.MapleLoan) (map[string]int64, error) {
	if len(loans) == 0 {
		return make(map[string]int64), nil
	}

	sorted := sortedByBytesKey(loans, func(l *entity.MapleLoan) []byte { return l.LoanAddress })

	batch := &pgx.Batch{}
	for _, l := range sorted {
		var metaType, metaAssetSymbol, metaDex, metaWalletAddress, metaWalletType, metaLocation *string
		if l.LoanMeta != nil {
			metaType = nullIfEmpty(l.LoanMeta.Type)
			metaAssetSymbol = nullIfEmpty(l.LoanMeta.AssetSymbol)
			metaDex = nullIfEmpty(l.LoanMeta.Dex)
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
			 RETURNING id`,
			l.ChainID, l.ProtocolID, l.LoanAddress, l.LoanType, l.MaplePoolID, l.BorrowerUserID,
			metaType, metaAssetSymbol, metaDex, metaWalletAddress, metaWalletType, metaLocation,
		)
	}

	return collectBatchIDs(ctx, tx, batch, sorted, "maple loan",
		func(l *entity.MapleLoan) string { return addressKey(l.LoanAddress) })
}

// SaveLoanStates inserts loan state snapshots (same trigger/conflict
// semantics as SavePoolStates).
func (r *MapleGraphQLRepository) SaveLoanStates(ctx context.Context, tx pgx.Tx, states []*entity.MapleLoanState) error {
	if len(states) == 0 {
		return nil
	}

	sorted := sortedCopy(states, func(a, b *entity.MapleLoanState) int {
		return cmp.Or(
			cmp.Compare(a.MapleLoanID, b.MapleLoanID),
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

func (r *MapleGraphQLRepository) saveLoanStateBatch(ctx context.Context, tx pgx.Tx, states []*entity.MapleLoanState) error {
	const cols = 6
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, acm_ratio, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		principalOwed, err := bigIntToNumeric(s.PrincipalOwed)
		if err != nil {
			return fmt.Errorf("converting principal_owed for loan %d: %w", s.MapleLoanID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.MapleLoanID, s.SyncedAt, s.State, principalOwed, optionalNumeric(s.AcmRatio), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_loan_id, synced_at, processing_version) DO NOTHING`)

	if _, err := tx.Exec(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("saving maple loan states: %w", err)
	}
	return nil
}

// SaveLoanCollaterals inserts loan collateral snapshots. Loans with null API
// collateral have no row; callers pass only non-nil collaterals.
func (r *MapleGraphQLRepository) SaveLoanCollaterals(ctx context.Context, tx pgx.Tx, collaterals []*entity.MapleLoanCollateral) error {
	if len(collaterals) == 0 {
		return nil
	}

	sorted := sortedCopy(collaterals, func(a, b *entity.MapleLoanCollateral) int {
		return cmp.Or(
			cmp.Compare(a.MapleLoanID, b.MapleLoanID),
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

func (r *MapleGraphQLRepository) saveLoanCollateralBatch(ctx context.Context, tx pgx.Tx, collaterals []*entity.MapleLoanCollateral) error {
	const cols = 10
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_loan_collateral (maple_loan_id, synced_at, asset_symbol, asset_amount, asset_decimals, asset_value_usd, state, custodian, liquidation_level, build_id) VALUES `)

	args := make([]any, 0, len(collaterals)*cols)
	for i, c := range collaterals {
		assetAmount, err := bigIntToNumeric(c.AssetAmount)
		if err != nil {
			return fmt.Errorf("converting asset_amount for loan %d: %w", c.MapleLoanID, err)
		}
		assetValueUSD, err := bigIntToNumeric(c.AssetValueUSD)
		if err != nil {
			return fmt.Errorf("converting asset_value_usd for loan %d: %w", c.MapleLoanID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, c.MapleLoanID, c.SyncedAt, c.AssetSymbol, assetAmount, c.AssetDecimals,
			assetValueUSD, nullIfEmpty(c.State), nullIfEmpty(c.Custodian), optionalNumeric(c.LiquidationLevel), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_loan_id, synced_at, processing_version) DO NOTHING`)

	if _, err := tx.Exec(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("saving maple loan collaterals: %w", err)
	}
	return nil
}

// UpsertSkyStrategies upserts strategy registry rows and returns lowercase
// hex strategy address -> maple_sky_strategy.id. On conflict, refreshes the
// pool reference and version.
func (r *MapleGraphQLRepository) UpsertSkyStrategies(ctx context.Context, tx pgx.Tx, strategies []*entity.MapleSkyStrategy) (map[string]int64, error) {
	if len(strategies) == 0 {
		return make(map[string]int64), nil
	}

	sorted := sortedByBytesKey(strategies, func(s *entity.MapleSkyStrategy) []byte { return s.StrategyAddress })

	batch := &pgx.Batch{}
	for _, s := range sorted {
		batch.Queue(
			`INSERT INTO maple_sky_strategy (chain_id, strategy_address, maple_pool_id, version)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (chain_id, strategy_address) DO UPDATE SET
			     maple_pool_id = EXCLUDED.maple_pool_id,
			     version = EXCLUDED.version
			 RETURNING id`,
			s.ChainID, s.StrategyAddress, s.MaplePoolID, s.Version,
		)
	}

	return collectBatchIDs(ctx, tx, batch, sorted, "maple sky strategy",
		func(s *entity.MapleSkyStrategy) string { return addressKey(s.StrategyAddress) })
}

// SaveSkyStrategyStates inserts strategy state snapshots (same
// trigger/conflict semantics as SavePoolStates).
func (r *MapleGraphQLRepository) SaveSkyStrategyStates(ctx context.Context, tx pgx.Tx, states []*entity.MapleSkyStrategyState) error {
	if len(states) == 0 {
		return nil
	}

	sorted := sortedCopy(states, func(a, b *entity.MapleSkyStrategyState) int {
		return cmp.Or(
			cmp.Compare(a.MapleSkyStrategyID, b.MapleSkyStrategyID),
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

func (r *MapleGraphQLRepository) saveSkyStrategyStateBatch(ctx context.Context, tx pgx.Tx, states []*entity.MapleSkyStrategyState) error {
	const cols = 9
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_sky_strategy_state (maple_sky_strategy_id, synced_at, state, currently_deployed, deposited_assets, withdrawn_assets, strategy_fee_rate, total_fees_collected, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		currentlyDeployed, err := bigIntToNumeric(s.CurrentlyDeployed)
		if err != nil {
			return fmt.Errorf("converting currently_deployed for strategy %d: %w", s.MapleSkyStrategyID, err)
		}
		depositedAssets, err := bigIntToNumeric(s.DepositedAssets)
		if err != nil {
			return fmt.Errorf("converting deposited_assets for strategy %d: %w", s.MapleSkyStrategyID, err)
		}
		withdrawnAssets, err := bigIntToNumeric(s.WithdrawnAssets)
		if err != nil {
			return fmt.Errorf("converting withdrawn_assets for strategy %d: %w", s.MapleSkyStrategyID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.MapleSkyStrategyID, s.SyncedAt, s.State, currentlyDeployed, depositedAssets,
			withdrawnAssets, optionalNumeric(s.StrategyFeeRate), optionalNumeric(s.TotalFeesCollected), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_sky_strategy_id, synced_at, processing_version) DO NOTHING`)

	if _, err := tx.Exec(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("saving maple sky strategy states: %w", err)
	}
	return nil
}

// SaveSyrupGlobalState inserts the protocol-wide Syrup aggregate snapshot
// (same trigger/conflict semantics as SavePoolStates).
func (r *MapleGraphQLRepository) SaveSyrupGlobalState(ctx context.Context, tx pgx.Tx, state *entity.MapleSyrupGlobalState) error {
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

	_, err = tx.Exec(ctx,
		`INSERT INTO maple_syrup_global_state (chain_id, synced_at, tvl, apy, collateral_apy, pool_apy, drips_yield_boost, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (chain_id, synced_at, processing_version) DO NOTHING`,
		state.ChainID, state.SyncedAt, tvl, apy, collateralAPY, poolAPY,
		optionalNumeric(state.DripsYieldBoost), int(r.buildID),
	)
	if err != nil {
		return fmt.Errorf("saving maple syrup global state: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// addressKey renders a raw address as the lowercase 0x-prefixed hex string
// used as registry map key (matches strings.ToLower(common.Address.Hex())).
func addressKey(address []byte) string {
	return "0x" + hex.EncodeToString(address)
}

// optionalNumeric converts an optional *big.Int to a nullable NUMERIC arg.
func optionalNumeric(b *big.Int) *string {
	if b == nil {
		return nil
	}
	s := b.String()
	return &s
}

// nullIfEmpty maps empty strings to SQL NULL.
func nullIfEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// sortedCopy returns a sorted copy of items, leaving the caller's slice
// untouched. Sorting before insert gives concurrent writers a stable
// row/advisory-lock acquisition order (ADR-0002).
func sortedCopy[T any](items []T, cmpFn func(a, b T) int) []T {
	sorted := make([]T, len(items))
	copy(sorted, items)
	slices.SortFunc(sorted, cmpFn)
	return sorted
}

// sortedByBytesKey returns a copy of items sorted by a bytes key.
func sortedByBytesKey[T any](items []T, key func(T) []byte) []T {
	return sortedCopy(items, func(a, b T) int { return bytes.Compare(key(a), key(b)) })
}

// writeValuesPlaceholders appends "($n, $n+1, ...)" for row i with the given
// column count, comma-separated from the previous row.
func writeValuesPlaceholders(sb *strings.Builder, row, cols int) {
	if row > 0 {
		sb.WriteString(", ")
	}
	sb.WriteString("(")
	for c := range cols {
		if c > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(sb, "$%d", row*cols+c+1)
	}
	sb.WriteString(")")
}

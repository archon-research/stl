package postgres

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
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

// RecordPools registers pool identity rows and records their editorial
// attributes (name, is_syrup) in the maple_pool_meta satellite, returning
// address -> maple_pool.id. The hub holds identity only: protocol_id and
// asset_token_id are immutable, so the hub insert refreshes nothing on conflict
// (the no-op DO UPDATE keeps RETURNING yielding the stored row) and the scan
// fails the run if either differs from the incoming one. An editorial change
// appends a satellite row via appendMeta when the editorial hashdiff differs
// from the pool's latest one.
func (r *MapleGraphQLRepository) RecordPools(ctx context.Context, tx pgx.Tx, syncedAt time.Time, pools []*maple.Pool) (map[common.Address]int64, error) {
	if len(pools) == 0 {
		return make(map[common.Address]int64), nil
	}

	sorted := sortedByBytesKey(pools, func(p *maple.Pool) []byte { return p.Address })

	batch := &pgx.Batch{}
	for _, p := range sorted {
		batch.Queue(
			`INSERT INTO maple_pool (chain_id, protocol_id, address, asset_token_id)
			 VALUES ($1, $2, $3, $4)
			 ON CONFLICT (chain_id, address) DO UPDATE SET id = maple_pool.id
			 RETURNING id, protocol_id, asset_token_id`,
			p.ChainID, p.ProtocolID, p.Address, p.AssetTokenID,
		)
	}

	ids, err := collectBatchRows(ctx, tx, batch, sorted, "maple pool",
		func(row pgx.Row, p *maple.Pool) (common.Address, int64, error) {
			addr := common.BytesToAddress(p.Address)
			var id, storedProtocolID, storedAssetTokenID int64
			if err := row.Scan(&id, &storedProtocolID, &storedAssetTokenID); err != nil {
				return common.Address{}, 0, fmt.Errorf("recording maple pool %s: %w", addr, err)
			}
			var mismatches []string
			if storedProtocolID != p.ProtocolID {
				mismatches = append(mismatches, fmt.Sprintf("protocol_id (stored %d, incoming %d)", storedProtocolID, p.ProtocolID))
			}
			if storedAssetTokenID != p.AssetTokenID {
				mismatches = append(mismatches, fmt.Sprintf("asset_token_id (stored %d, incoming %d)", storedAssetTokenID, p.AssetTokenID))
			}
			if err := registryMismatchError("maple pool", addr, mismatches); err != nil {
				return common.Address{}, 0, err
			}
			return addr, id, nil
		})
	if err != nil {
		return nil, err
	}

	rows := make([]metaRow, 0, len(sorted))
	for _, p := range sorted {
		rows = append(rows, metaRow{
			id:        ids[common.BytesToAddress(p.Address)],
			editorial: []any{p.Name, p.IsSyrup},
		})
	}
	if err := r.appendMeta(ctx, tx, "maple_pool_meta", "maple_pool_id",
		[]string{"name", "is_syrup"}, syncedAt, rows); err != nil {
		return nil, fmt.Errorf("recording maple pool meta: %w", err)
	}
	return ids, nil
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

	var inserted int64
	for chunk := range slices.Chunk(sorted, r.batchSize) {
		n, err := r.savePoolStateBatch(ctx, tx, chunk)
		if err != nil {
			return err
		}
		inserted += n
	}
	return r.checkDedupedRows("maple_pool_state", inserted, len(states))
}

func (r *MapleGraphQLRepository) savePoolStateBatch(ctx context.Context, tx pgx.Tx, states []*maple.PoolState) (int64, error) {
	const cols = 10
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_pool_state (maple_pool_id, synced_at, tvl, liquid_assets, collateral_value_usd, principal_out, utilization, monthly_apy, spot_apy, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		liquidAssets, err := bigIntToNumeric(s.LiquidAssets)
		if err != nil {
			return 0, fmt.Errorf("converting liquid_assets for pool %d: %w", s.PoolID, err)
		}
		principalOut, err := bigIntToNumeric(s.PrincipalOut)
		if err != nil {
			return 0, fmt.Errorf("converting principal_out for pool %d: %w", s.PoolID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.PoolID, s.SyncedAt, optionalNumeric(s.TVL), liquidAssets,
			optionalNumeric(s.CollateralValueUSD), principalOut, s.Utilization,
			optionalNumeric(s.MonthlyAPY), optionalNumeric(s.SpotAPY), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_pool_id, synced_at, processing_version) DO NOTHING`)

	return r.execInsert(ctx, tx, "maple_pool_state", sb.String(), args)
}

// loanMetaCols holds a loan's six nullable loanMeta columns as satellite column args.
type loanMetaCols struct {
	typ, assetSymbol, dex, walletAddress, walletType, location *string
}

// loanMetaColsOf maps a loan's metadata to its column args, treating a nil
// LoanMeta and every empty field alike as SQL NULL.
func loanMetaColsOf(l *maple.Loan) loanMetaCols {
	if l.LoanMeta == nil {
		return loanMetaCols{}
	}
	return loanMetaCols{
		typ:           nullIfEmpty(l.LoanMeta.Type),
		assetSymbol:   nullIfEmpty(l.LoanMeta.AssetSymbol),
		dex:           nullIfEmpty(l.LoanMeta.DexName),
		walletAddress: nullIfEmpty(l.LoanMeta.WalletAddress),
		walletType:    nullIfEmpty(l.LoanMeta.WalletType),
		location:      nullIfEmpty(l.LoanMeta.Location),
	}
}

// RecordLoans registers loan identity rows and records their editorial
// attributes (loan_type and the six loanMeta columns) in the maple_loan_meta
// satellite, returning loan address -> maple_loan.id. The hub holds identity
// only: maple_pool_id and borrower_user_id are immutable, so the hub insert
// refreshes nothing on conflict and the scan fails the run if either differs
// from the incoming one. A loanMeta change appends a satellite row via
// appendMeta (nullable columns compared NULL-safely via the hashdiff null
// sentinel).
func (r *MapleGraphQLRepository) RecordLoans(ctx context.Context, tx pgx.Tx, syncedAt time.Time, loans []*maple.Loan) (map[common.Address]int64, error) {
	if len(loans) == 0 {
		return make(map[common.Address]int64), nil
	}

	sorted := sortedByBytesKey(loans, func(l *maple.Loan) []byte { return l.LoanAddress })

	batch := &pgx.Batch{}
	for _, l := range sorted {
		batch.Queue(
			`INSERT INTO maple_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id)
			 VALUES ($1, $2, $3, $4, $5)
			 ON CONFLICT (chain_id, loan_address) DO UPDATE SET id = maple_loan.id
			 RETURNING id, maple_pool_id, borrower_user_id`,
			l.ChainID, l.ProtocolID, l.LoanAddress, l.PoolID, l.BorrowerUserID,
		)
	}

	ids, err := collectBatchRows(ctx, tx, batch, sorted, "maple loan",
		func(row pgx.Row, l *maple.Loan) (common.Address, int64, error) {
			addr := common.BytesToAddress(l.LoanAddress)
			var id, storedPoolID, storedBorrower int64
			if err := row.Scan(&id, &storedPoolID, &storedBorrower); err != nil {
				return common.Address{}, 0, fmt.Errorf("recording maple loan %s: %w", addr, err)
			}
			var mismatches []string
			if storedPoolID != l.PoolID {
				mismatches = append(mismatches, fmt.Sprintf("maple_pool_id (stored %d, incoming %d)", storedPoolID, l.PoolID))
			}
			if storedBorrower != l.BorrowerUserID {
				mismatches = append(mismatches, fmt.Sprintf("borrower_user_id (stored %d, incoming %d)", storedBorrower, l.BorrowerUserID))
			}
			if err := registryMismatchError("maple loan", addr, mismatches); err != nil {
				return common.Address{}, 0, err
			}
			return addr, id, nil
		})
	if err != nil {
		return nil, err
	}

	rows := make([]metaRow, 0, len(sorted))
	for _, l := range sorted {
		m := loanMetaColsOf(l)
		rows = append(rows, metaRow{
			id:        ids[common.BytesToAddress(l.LoanAddress)],
			editorial: []any{l.LoanType, m.typ, m.assetSymbol, m.dex, m.walletAddress, m.walletType, m.location},
		})
	}
	if err := r.appendMeta(ctx, tx, "maple_loan_meta", "maple_loan_id",
		[]string{"loan_type", "loan_meta_type", "loan_meta_asset_symbol", "loan_meta_dex",
			"loan_meta_wallet_address", "loan_meta_wallet_type", "loan_meta_location"},
		syncedAt, rows); err != nil {
		return nil, fmt.Errorf("recording maple loan meta: %w", err)
	}
	return ids, nil
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

	var inserted int64
	for chunk := range slices.Chunk(sorted, r.batchSize) {
		n, err := r.saveLoanStateBatch(ctx, tx, chunk)
		if err != nil {
			return err
		}
		inserted += n
	}
	return r.checkDedupedRows("maple_loan_state", inserted, len(states))
}

func (r *MapleGraphQLRepository) saveLoanStateBatch(ctx context.Context, tx pgx.Tx, states []*maple.LoanState) (int64, error) {
	const cols = 6
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_loan_state (maple_loan_id, synced_at, state, principal_owed, acm_ratio, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		principalOwed, err := bigIntToNumeric(s.PrincipalOwed)
		if err != nil {
			return 0, fmt.Errorf("converting principal_owed for loan %d: %w", s.LoanID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.LoanID, s.SyncedAt, s.State, principalOwed, optionalNumeric(s.AcmRatio), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_loan_id, synced_at, processing_version) DO NOTHING`)

	return r.execInsert(ctx, tx, "maple_loan_state", sb.String(), args)
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

	var inserted int64
	for chunk := range slices.Chunk(sorted, r.batchSize) {
		n, err := r.saveLoanCollateralBatch(ctx, tx, chunk)
		if err != nil {
			return err
		}
		inserted += n
	}
	return r.checkDedupedRows("maple_loan_collateral", inserted, len(collaterals))
}

func (r *MapleGraphQLRepository) saveLoanCollateralBatch(ctx context.Context, tx pgx.Tx, collaterals []*maple.LoanCollateral) (int64, error) {
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

	return r.execInsert(ctx, tx, "maple_loan_collateral", sb.String(), args)
}

// RecordFixedTermLoans registers fixed-term loan identity rows and returns loan
// address -> maple_ftl_loan.id. maple_pool_id, borrower_user_id,
// collateral_token_id and funds_token_id are immutable per loan (fundingPool
// and the underlying assets are fixed at origination), so the hub insert
// refreshes nothing on conflict (the no-op DO UPDATE keeps RETURNING yielding
// the stored row) and the scan fails the run if any stored value differs from
// the incoming one. The FTL registry has no editorial attributes, so unlike
// RecordPools/RecordLoans it appends no satellite row. Refinance-mutable terms
// live in maple_ftl_loan_state, not here.
func (r *MapleGraphQLRepository) RecordFixedTermLoans(ctx context.Context, tx pgx.Tx, loans []*maple.FTLLoan) (map[common.Address]int64, error) {
	if len(loans) == 0 {
		return make(map[common.Address]int64), nil
	}

	sorted := sortedByBytesKey(loans, func(l *maple.FTLLoan) []byte { return l.LoanAddress })

	batch := &pgx.Batch{}
	for _, l := range sorted {
		batch.Queue(
			`INSERT INTO maple_ftl_loan (chain_id, protocol_id, loan_address, maple_pool_id, borrower_user_id,
			                             collateral_token_id, funds_token_id)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 ON CONFLICT (chain_id, loan_address) DO UPDATE SET id = maple_ftl_loan.id
			 RETURNING id, maple_pool_id, borrower_user_id, collateral_token_id, funds_token_id`,
			l.ChainID, l.ProtocolID, l.LoanAddress, l.PoolID, l.BorrowerUserID,
			l.CollateralTokenID, l.FundsTokenID,
		)
	}

	return collectBatchRows(ctx, tx, batch, sorted, "maple ftl loan",
		func(row pgx.Row, l *maple.FTLLoan) (common.Address, int64, error) {
			addr := common.BytesToAddress(l.LoanAddress)
			var id, storedPoolID, storedBorrower, storedCollateralToken, storedFundsToken int64
			if err := row.Scan(&id, &storedPoolID, &storedBorrower, &storedCollateralToken, &storedFundsToken); err != nil {
				return common.Address{}, 0, fmt.Errorf("recording maple ftl loan %s: %w", addr, err)
			}
			var mismatches []string
			for _, c := range []struct {
				field            string
				stored, incoming int64
			}{
				{"maple_pool_id", storedPoolID, l.PoolID},
				{"borrower_user_id", storedBorrower, l.BorrowerUserID},
				{"collateral_token_id", storedCollateralToken, l.CollateralTokenID},
				{"funds_token_id", storedFundsToken, l.FundsTokenID},
			} {
				if c.stored != c.incoming {
					mismatches = append(mismatches, fmt.Sprintf("%s (stored %d, incoming %d)", c.field, c.stored, c.incoming))
				}
			}
			if err := registryMismatchError("maple ftl loan", addr, mismatches); err != nil {
				return common.Address{}, 0, err
			}
			return addr, id, nil
		})
}

// SaveFixedTermLoanStates inserts fixed-term loan state snapshots (same
// trigger/conflict semantics as SavePoolStates).
func (r *MapleGraphQLRepository) SaveFixedTermLoanStates(ctx context.Context, tx pgx.Tx, states []*maple.FTLLoanState) error {
	if len(states) == 0 {
		return nil
	}

	sorted := sortedCopy(states, func(a, b *maple.FTLLoanState) int {
		return cmp.Or(
			cmp.Compare(a.LoanID, b.LoanID),
			a.SyncedAt.Compare(b.SyncedAt),
		)
	})

	var inserted int64
	for chunk := range slices.Chunk(sorted, r.batchSize) {
		n, err := r.saveFixedTermLoanStateBatch(ctx, tx, chunk)
		if err != nil {
			return err
		}
		inserted += n
	}
	return r.checkDedupedRows("maple_ftl_loan_state", inserted, len(states))
}

func (r *MapleGraphQLRepository) saveFixedTermLoanStateBatch(ctx context.Context, tx pgx.Tx, states []*maple.FTLLoanState) (int64, error) {
	const cols = 20
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_ftl_loan_state (maple_ftl_loan_id, synced_at, state, state_detail, principal_owed, interest_rate, interest_paid, payments_remaining, payment_interval_days, term_days, maturity_date, next_payment_due, collateral_amount, collateral_required, collateral_ratio, drawdown_amount, claimable_amount, acm_ratio, is_impaired, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		principalOwed, err := bigIntToNumeric(s.PrincipalOwed)
		if err != nil {
			return 0, fmt.Errorf("converting principal_owed for ftl loan %d: %w", s.LoanID, err)
		}
		interestRate, err := bigIntToNumeric(s.InterestRate)
		if err != nil {
			return 0, fmt.Errorf("converting interest_rate for ftl loan %d: %w", s.LoanID, err)
		}
		interestPaid, err := bigIntToNumeric(s.InterestPaid)
		if err != nil {
			return 0, fmt.Errorf("converting interest_paid for ftl loan %d: %w", s.LoanID, err)
		}
		collateralAmount, err := bigIntToNumeric(s.CollateralAmount)
		if err != nil {
			return 0, fmt.Errorf("converting collateral_amount for ftl loan %d: %w", s.LoanID, err)
		}
		collateralRequired, err := bigIntToNumeric(s.CollateralRequired)
		if err != nil {
			return 0, fmt.Errorf("converting collateral_required for ftl loan %d: %w", s.LoanID, err)
		}
		collateralRatio, err := bigIntToNumeric(s.CollateralRatio)
		if err != nil {
			return 0, fmt.Errorf("converting collateral_ratio for ftl loan %d: %w", s.LoanID, err)
		}
		drawdownAmount, err := bigIntToNumeric(s.DrawdownAmount)
		if err != nil {
			return 0, fmt.Errorf("converting drawdown_amount for ftl loan %d: %w", s.LoanID, err)
		}
		claimableAmount, err := bigIntToNumeric(s.ClaimableAmount)
		if err != nil {
			return 0, fmt.Errorf("converting claimable_amount for ftl loan %d: %w", s.LoanID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.LoanID, s.SyncedAt, s.State, nullIfEmpty(s.StateDetail),
			principalOwed, interestRate, interestPaid,
			s.PaymentsRemaining, s.PaymentIntervalDays, s.TermDays,
			s.MaturityDate, s.NextPaymentDue,
			collateralAmount, collateralRequired, collateralRatio,
			drawdownAmount, claimableAmount, optionalNumeric(s.AcmRatio), s.IsImpaired, int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_ftl_loan_id, synced_at, processing_version) DO NOTHING`)

	return r.execInsert(ctx, tx, "maple_ftl_loan_state", sb.String(), args)
}

// RecordSkyStrategies registers strategy identity rows and records their
// editorial attribute (version) in the maple_sky_strategy_meta satellite,
// returning strategy address -> maple_sky_strategy.id. The hub holds identity
// only: maple_pool_id is immutable, so the hub insert refreshes nothing on
// conflict and the scan fails the run if it differs from the incoming one. A
// version change (e.g. a Governor-enabled proxy upgrade) appends a satellite
// row via appendMeta.
func (r *MapleGraphQLRepository) RecordSkyStrategies(ctx context.Context, tx pgx.Tx, syncedAt time.Time, strategies []*maple.SkyStrategy) (map[common.Address]int64, error) {
	if len(strategies) == 0 {
		return make(map[common.Address]int64), nil
	}

	sorted := sortedByBytesKey(strategies, func(s *maple.SkyStrategy) []byte { return s.StrategyAddress })

	batch := &pgx.Batch{}
	for _, s := range sorted {
		batch.Queue(
			`INSERT INTO maple_sky_strategy (chain_id, strategy_address, maple_pool_id)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (chain_id, strategy_address) DO UPDATE SET id = maple_sky_strategy.id
			 RETURNING id, maple_pool_id`,
			s.ChainID, s.StrategyAddress, s.PoolID,
		)
	}

	ids, err := collectBatchRows(ctx, tx, batch, sorted, "maple sky strategy",
		func(row pgx.Row, s *maple.SkyStrategy) (common.Address, int64, error) {
			addr := common.BytesToAddress(s.StrategyAddress)
			var id, storedPoolID int64
			if err := row.Scan(&id, &storedPoolID); err != nil {
				return common.Address{}, 0, fmt.Errorf("recording maple sky strategy %s: %w", addr, err)
			}
			var mismatches []string
			if storedPoolID != s.PoolID {
				mismatches = append(mismatches, fmt.Sprintf("maple_pool_id (stored %d, incoming %d)", storedPoolID, s.PoolID))
			}
			if err := registryMismatchError("maple sky strategy", addr, mismatches); err != nil {
				return common.Address{}, 0, err
			}
			return addr, id, nil
		})
	if err != nil {
		return nil, err
	}

	rows := make([]metaRow, 0, len(sorted))
	for _, s := range sorted {
		rows = append(rows, metaRow{
			id:        ids[common.BytesToAddress(s.StrategyAddress)],
			editorial: []any{s.Version},
		})
	}
	if err := r.appendMeta(ctx, tx, "maple_sky_strategy_meta", "maple_sky_strategy_id",
		[]string{"version"}, syncedAt, rows); err != nil {
		return nil, fmt.Errorf("recording maple sky strategy meta: %w", err)
	}
	return ids, nil
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

	var inserted int64
	for chunk := range slices.Chunk(sorted, r.batchSize) {
		n, err := r.saveSkyStrategyStateBatch(ctx, tx, chunk)
		if err != nil {
			return err
		}
		inserted += n
	}
	return r.checkDedupedRows("maple_sky_strategy_state", inserted, len(states))
}

func (r *MapleGraphQLRepository) saveSkyStrategyStateBatch(ctx context.Context, tx pgx.Tx, states []*maple.SkyStrategyState) (int64, error) {
	const cols = 9
	var sb strings.Builder
	sb.WriteString(`INSERT INTO maple_sky_strategy_state (maple_sky_strategy_id, synced_at, state, currently_deployed, deposited_assets, withdrawn_assets, strategy_fee_rate, total_fees_collected, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		currentlyDeployed, err := bigIntToNumeric(s.CurrentlyDeployed)
		if err != nil {
			return 0, fmt.Errorf("converting currently_deployed for strategy %d: %w", s.SkyStrategyID, err)
		}
		depositedAssets, err := bigIntToNumeric(s.DepositedAssets)
		if err != nil {
			return 0, fmt.Errorf("converting deposited_assets for strategy %d: %w", s.SkyStrategyID, err)
		}
		withdrawnAssets, err := bigIntToNumeric(s.WithdrawnAssets)
		if err != nil {
			return 0, fmt.Errorf("converting withdrawn_assets for strategy %d: %w", s.SkyStrategyID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.SkyStrategyID, s.SyncedAt, s.State, currentlyDeployed, depositedAssets,
			withdrawnAssets, optionalNumeric(s.StrategyFeeRate), optionalNumeric(s.TotalFeesCollected), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (maple_sky_strategy_id, synced_at, processing_version) DO NOTHING`)

	return r.execInsert(ctx, tx, "maple_sky_strategy_state", sb.String(), args)
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

	inserted, err := r.execInsert(ctx, tx, "maple_syrup_global_state",
		`INSERT INTO maple_syrup_global_state (chain_id, synced_at, tvl, apy, collateral_apy, pool_apy, drips_yield_boost, build_id)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		 ON CONFLICT (chain_id, synced_at, processing_version) DO NOTHING`,
		[]any{state.ChainID, state.SyncedAt, tvl, apy, collateralAPY, poolAPY,
			optionalNumeric(state.DripsYieldBoost), int(r.buildID)},
	)
	if err != nil {
		return err
	}
	return r.checkDedupedRows("maple_syrup_global_state", inserted, 1)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// execInsert executes a state-insert statement, wraps its error with the
// table name, and returns the number of rows actually inserted. Save methods
// must sum the counts across their chunks and run checkDedupedRows once on
// the whole logical save — checking per chunk would misread a partial dedup
// whose collided and fresh rows land in different chunks as one full dedup
// plus one clean insert.
func (r *MapleGraphQLRepository) execInsert(ctx context.Context, tx pgx.Tx, table, sql string, args []any) (int64, error) {
	tag, err := tx.Exec(ctx, sql, args...)
	if err != nil {
		return 0, fmt.Errorf("saving %s: %w", table, err)
	}
	return tag.RowsAffected(), nil
}

// checkDedupedRows makes ON CONFLICT DO NOTHING dedup visible. A full dedup
// (zero rows inserted) is the signature of a Temporal activity retry
// re-running an already-persisted phase at the same synced_at and build (the
// trigger reuses the processing_version and the insert dedupes), so it logs
// at warn and succeeds. A partial dedup means some rows collided while
// siblings did not — a clock regression, a duplicate that slipped the
// service guards, a trigger bug assigning a colliding version, or upstream
// data changing between retry attempts at the same synced_at. Committing
// would silently drop the collided rows, so it fails the save and the
// caller's transaction rolls the snapshot back. In the changed-upstream-data
// case every retry of that tick keeps failing (the committed rows collide
// again each attempt) until the next scheduled tick's fresh synced_at —
// loud by design. Single-row saves (expected == 1) can only ever hit the
// full-dedup warn path.
func (r *MapleGraphQLRepository) checkDedupedRows(table string, inserted int64, expected int) error {
	switch {
	case inserted == int64(expected):
		return nil
	case inserted == 0:
		r.logger.Warn("state insert fully deduplicated by ON CONFLICT DO NOTHING (expected on activity retries)",
			"table", table,
			"expected", expected,
			"inserted", inserted,
		)
		return nil
	default:
		return fmt.Errorf("state insert into %s partially deduplicated by ON CONFLICT DO NOTHING (%d of %d rows inserted); failing the save so the caller rolls back instead of silently dropping the collided rows", table, inserted, expected)
	}
}

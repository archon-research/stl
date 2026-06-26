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

// UpsertPools upserts pool registry rows and returns
// address -> maple_pool.id. name, asset_token_id, and is_syrup are immutable
// per pool, so the upsert refreshes nothing on conflict (the no-op DO UPDATE
// keeps RETURNING yielding the stored row) and the scan fails the run if any
// stored value differs from the incoming one.
func (r *MapleGraphQLRepository) UpsertPools(ctx context.Context, tx pgx.Tx, pools []*maple.Pool) (map[common.Address]int64, error) {
	if len(pools) == 0 {
		return make(map[common.Address]int64), nil
	}

	sorted := sortedByBytesKey(pools, func(p *maple.Pool) []byte { return p.Address })

	batch := &pgx.Batch{}
	for _, p := range sorted {
		batch.Queue(
			`INSERT INTO maple_pool (chain_id, protocol_id, address, name, asset_token_id, is_syrup)
			 VALUES ($1, $2, $3, $4, $5, $6)
			 ON CONFLICT (chain_id, address) DO UPDATE SET id = maple_pool.id
			 RETURNING id, name, asset_token_id, is_syrup`,
			p.ChainID, p.ProtocolID, p.Address, p.Name, p.AssetTokenID, p.IsSyrup,
		)
	}

	return collectBatchRows(ctx, tx, batch, sorted, "maple pool",
		func(row pgx.Row, p *maple.Pool) (common.Address, int64, error) {
			addr := common.BytesToAddress(p.Address)
			var id, storedAssetTokenID int64
			var storedName *string // name is a nullable column; a stored NULL is itself a mismatch
			var storedIsSyrup bool
			if err := row.Scan(&id, &storedName, &storedAssetTokenID, &storedIsSyrup); err != nil {
				return common.Address{}, 0, fmt.Errorf("upserting maple pool %s: %w", addr, err)
			}
			var mismatches []string
			if storedName == nil || *storedName != p.Name {
				mismatches = append(mismatches, fmt.Sprintf("name (stored %s, incoming %q)", strOrNull(storedName), p.Name))
			}
			if storedAssetTokenID != p.AssetTokenID {
				mismatches = append(mismatches, fmt.Sprintf("asset_token_id (stored %d, incoming %d)", storedAssetTokenID, p.AssetTokenID))
			}
			if storedIsSyrup != p.IsSyrup {
				mismatches = append(mismatches, fmt.Sprintf("is_syrup (stored %t, incoming %t)", storedIsSyrup, p.IsSyrup))
			}
			if err := registryMismatchError("maple pool", addr, mismatches); err != nil {
				return common.Address{}, 0, err
			}
			return addr, id, nil
		})
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

// loanMetaCols holds a loan's six nullable loanMeta columns as upsert args.
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

// loanMetaEqual reports NULL-safe equality of every loanMeta column.
func loanMetaEqual(a, b loanMetaCols) bool {
	return equalStringPtr(a.typ, b.typ) &&
		equalStringPtr(a.assetSymbol, b.assetSymbol) &&
		equalStringPtr(a.dex, b.dex) &&
		equalStringPtr(a.walletAddress, b.walletAddress) &&
		equalStringPtr(a.walletType, b.walletType) &&
		equalStringPtr(a.location, b.location)
}

const advisoryLockLoanSQL = `SELECT pg_advisory_xact_lock(hashtextextended(format('maple_loan|%s|%s', $1::int, encode($2::bytea, 'hex')), 0))`

// loanLatestAndCycleSQL returns exactly one row (anchored on (SELECT 1)); each
// LEFT JOIN LATERAL leaves its columns NULL when no matching version exists.
// `latest` is the absolute latest row (immutable-field guard); `cyc` is the
// version current at $3 = syncedAt (loanMeta dedup).
const loanLatestAndCycleSQL = `
	SELECT latest.maple_pool_id, latest.borrower_user_id,
	       cyc.id, cyc.loan_meta_type, cyc.loan_meta_asset_symbol, cyc.loan_meta_dex,
	       cyc.loan_meta_wallet_address, cyc.loan_meta_wallet_type, cyc.loan_meta_location
	  FROM (SELECT 1) anchor
	  LEFT JOIN LATERAL (
	      SELECT maple_pool_id, borrower_user_id
	        FROM maple_loan
	       WHERE chain_id = $1 AND loan_address = $2
	       ORDER BY synced_at DESC, id DESC LIMIT 1
	  ) latest ON true
	  LEFT JOIN LATERAL (
	      SELECT id, loan_meta_type, loan_meta_asset_symbol, loan_meta_dex,
	             loan_meta_wallet_address, loan_meta_wallet_type, loan_meta_location
	        FROM maple_loan
	       WHERE chain_id = $1 AND loan_address = $2 AND synced_at <= $3
	       ORDER BY synced_at DESC, id DESC LIMIT 1
	  ) cyc ON true`

const insertLoanSQL = `
	INSERT INTO maple_loan (chain_id, protocol_id, loan_address, loan_type, maple_pool_id, borrower_user_id,
	                        loan_meta_type, loan_meta_asset_symbol, loan_meta_dex, loan_meta_wallet_address,
	                        loan_meta_wallet_type, loan_meta_location, synced_at)
	 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	 RETURNING id`

// RecordLoans records loan registry rows and returns loan address ->
// maple_loan.id of the row matching THIS cycle's metadata.
//
// maple_loan is an append-only registry. maple_pool_id and borrower_user_id are
// strictly immutable per loan (on-chain facts) — a change versus the latest
// stored row fails the run. The six loanMeta columns are off-chain editorial
// metadata Maple enriches after origination (a stored NULL loan_meta_type later
// resolves to a value such as "intercompany"). Rather than mutate the stored row
// — which would erase the metadata in effect when earlier state snapshots were
// taken, breaking the reproducibility of downstream loan-risk calculations — any
// loanMeta difference appends a NEW row, leaving prior rows intact. State
// snapshots FK the row current at their sync cycle, so a join reproduces the
// metadata that was live then. The latest version per loan is the row with the
// greatest (synced_at, id), where synced_at is the cycle timestamp passed in.
//
// Each loan is checked against two reference rows, on purpose:
//   - Immutable fields are validated against the absolute latest row — they
//     never change, so any version is a valid witness.
//   - The loanMeta equality decision is made against the version current at
//     syncedAt (the greatest synced_at <= syncedAt), NOT the absolute latest.
//     Comparing against the latest would, when a newer version already exists,
//     re-append a duplicate every time an older cycle is replayed with metadata
//     that already matches the version live at that cycle. Deduping at the cycle
//     boundary keeps replay/backfill idempotent.
//
// The append decision is made from a prior read, which ON CONFLICT cannot guard,
// so each loan takes a per-loan pg_advisory_xact_lock on its natural key
// (ADR-0002 §3). Locks and reads are issued in one batched round-trip in sorted
// address order, so locks are acquired in a consistent order (deadlock-free) and
// the lock+read phase costs one round-trip instead of two per loan; only loans
// that actually changed get an insert, batched in a second round-trip.
func (r *MapleGraphQLRepository) RecordLoans(ctx context.Context, tx pgx.Tx, loans []*maple.Loan, syncedAt time.Time) (map[common.Address]int64, error) {
	if len(loans) == 0 {
		return make(map[common.Address]int64), nil
	}

	sorted := sortedByBytesKey(loans, func(l *maple.Loan) []byte { return l.LoanAddress })

	reads, err := r.lockAndReadLoans(ctx, tx, sorted, syncedAt)
	if err != nil {
		return nil, err
	}

	result := make(map[common.Address]int64, len(sorted))
	var toInsert []*maple.Loan
	for i, l := range sorted {
		addr := common.BytesToAddress(l.LoanAddress)
		rd := reads[i]

		if rd.latestPool == nil {
			toInsert = append(toInsert, l) // brand-new loan
			continue
		}

		var mismatches []string
		if *rd.latestPool != l.PoolID {
			mismatches = append(mismatches, fmt.Sprintf("maple_pool_id (stored %d, incoming %d)", *rd.latestPool, l.PoolID))
		}
		if *rd.latestBorrower != l.BorrowerUserID {
			mismatches = append(mismatches, fmt.Sprintf("borrower_user_id (stored %d, incoming %d)", *rd.latestBorrower, l.BorrowerUserID))
		}
		if err := registryMismatchError("maple loan", addr, mismatches); err != nil {
			return nil, err
		}

		if rd.cycleID != nil && loanMetaEqual(rd.cycle, loanMetaColsOf(l)) {
			result[addr] = *rd.cycleID // unchanged: reuse the version current at syncedAt
			continue
		}
		toInsert = append(toInsert, l) // changed metadata, or a cycle older than every stored version
	}

	if err := r.insertLoanVersions(ctx, tx, toInsert, syncedAt, result); err != nil {
		return nil, err
	}
	return result, nil
}

// loanRead is one loan's pre-decision state: its latest immutable fields (nil
// when the loan has no rows yet) and the loanMeta of the version current at
// syncedAt (cycleID nil when syncedAt predates every stored version).
type loanRead struct {
	latestPool, latestBorrower *int64
	cycleID                    *int64
	cycle                      loanMetaCols
}

// lockAndReadLoans takes every loan's advisory lock and reads its latest row
// plus the version current at syncedAt, in a single batched round-trip. Locks
// are queued in the callers' sorted address order, so they are acquired in a
// consistent order across concurrent writers (deadlock-free).
func (r *MapleGraphQLRepository) lockAndReadLoans(ctx context.Context, tx pgx.Tx, sorted []*maple.Loan, syncedAt time.Time) ([]loanRead, error) {
	batch := &pgx.Batch{}
	for _, l := range sorted {
		batch.Queue(advisoryLockLoanSQL, l.ChainID, l.LoanAddress)
		batch.Queue(loanLatestAndCycleSQL, l.ChainID, l.LoanAddress, syncedAt)
	}
	br := tx.SendBatch(ctx, batch)

	reads := make([]loanRead, len(sorted))
	var firstErr error
	for i, l := range sorted {
		addr := common.BytesToAddress(l.LoanAddress)
		if _, err := br.Exec(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("locking maple loan %s: %w", addr, err)
		}
		var rd loanRead
		if err := br.QueryRow().Scan(
			&rd.latestPool, &rd.latestBorrower,
			&rd.cycleID, &rd.cycle.typ, &rd.cycle.assetSymbol, &rd.cycle.dex,
			&rd.cycle.walletAddress, &rd.cycle.walletType, &rd.cycle.location,
		); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("reading maple loan %s: %w", addr, err)
		}
		reads[i] = rd
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("closing maple loan read batch: %w", err)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return reads, nil
}

// insertLoanVersions appends the changed/new loans in one batched round-trip,
// recording each new id in result.
func (r *MapleGraphQLRepository) insertLoanVersions(ctx context.Context, tx pgx.Tx, loans []*maple.Loan, syncedAt time.Time, result map[common.Address]int64) error {
	if len(loans) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, l := range loans {
		m := loanMetaColsOf(l)
		batch.Queue(insertLoanSQL,
			l.ChainID, l.ProtocolID, l.LoanAddress, l.LoanType, l.PoolID, l.BorrowerUserID,
			m.typ, m.assetSymbol, m.dex, m.walletAddress, m.walletType, m.location, syncedAt)
	}
	br := tx.SendBatch(ctx, batch)

	var firstErr error
	for _, l := range loans {
		addr := common.BytesToAddress(l.LoanAddress)
		var id int64
		if err := br.QueryRow().Scan(&id); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("inserting maple loan %s: %w", addr, err)
			}
			continue
		}
		result[addr] = id
	}
	if err := br.Close(); err != nil && firstErr == nil {
		firstErr = fmt.Errorf("closing maple loan insert batch: %w", err)
	}
	return firstErr
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

// UpsertSkyStrategies upserts strategy registry rows and returns strategy
// address -> maple_sky_strategy.id. maple_pool_id and version are immutable
// per strategy, so the upsert refreshes nothing on conflict (the no-op DO
// UPDATE keeps RETURNING yielding the stored row) and the scan fails the run
// if any stored value differs from the incoming one. version has a documented
// live mutation path (Governor-enabled proxy upgrade) but is empirically
// unchanged to date; a real upgrade would trip this guard, which is the
// intended first-observed-mismatch signal.
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
			 ON CONFLICT (chain_id, strategy_address) DO UPDATE SET id = maple_sky_strategy.id
			 RETURNING id, maple_pool_id, version`,
			s.ChainID, s.StrategyAddress, s.PoolID, s.Version,
		)
	}

	return collectBatchRows(ctx, tx, batch, sorted, "maple sky strategy",
		func(row pgx.Row, s *maple.SkyStrategy) (common.Address, int64, error) {
			addr := common.BytesToAddress(s.StrategyAddress)
			var id, storedPoolID int64
			var storedVersion *int // version is a nullable column; a stored NULL is itself a mismatch
			if err := row.Scan(&id, &storedPoolID, &storedVersion); err != nil {
				return common.Address{}, 0, fmt.Errorf("upserting maple sky strategy %s: %w", addr, err)
			}
			var mismatches []string
			if storedPoolID != s.PoolID {
				mismatches = append(mismatches, fmt.Sprintf("maple_pool_id (stored %d, incoming %d)", storedPoolID, s.PoolID))
			}
			if storedVersion == nil || *storedVersion != s.Version {
				mismatches = append(mismatches, fmt.Sprintf("version (stored %s, incoming %d)", intOrNull(storedVersion), s.Version))
			}
			if err := registryMismatchError("maple sky strategy", addr, mismatches); err != nil {
				return common.Address{}, 0, err
			}
			return addr, id, nil
		})
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

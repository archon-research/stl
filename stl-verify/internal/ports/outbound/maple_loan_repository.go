package outbound

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity/maple"
)

// MapleGraphQLRepository defines the persistence interface for Maple GraphQL
// indexer data. All write methods run within an external transaction so the
// service controls snapshot atomicity per sync phase; GetMapleProtocolID is a
// standalone registry read. Borrower users and pool asset tokens are upserted
// through the shared UserRepository/TokenRepository, not this port.
//
// Registry upserts (pools, loans, strategies) return address -> database-id
// maps and resolve ids even when the row already exists
// (ON CONFLICT DO UPDATE ... RETURNING id).
//
// State saves use ON CONFLICT DO NOTHING on the
// (natural key, processing_version) primary key: the BEFORE INSERT trigger
// reuses the processing_version for same-build retries (deduplicated by the
// conflict clause) and assigns MAX+1 for a new build_id (insert succeeds).
// A save where every row dedupes succeeds (the retry path); a save where
// only some rows dedupe returns an error so the caller's transaction rolls
// back instead of silently dropping the collided rows.
// Implementations must sort batches by natural key before inserting so
// concurrent writers acquire the per-row advisory locks in a stable order
// (ADR-0002).
type MapleGraphQLRepository interface {
	// GetMapleProtocolID resolves the seeded maple protocol row id for a chain.
	GetMapleProtocolID(ctx context.Context, chainID int64) (int64, error)

	// RecordPools registers pool identity rows (asset_token_id already resolved
	// by the service via TokenRepository.GetOrCreateTokens) and records their
	// editorial attributes (name, is_syrup) in the maple_pool_meta satellite at
	// syncedAt, returning address -> maple_pool.id. protocol_id and
	// asset_token_id are immutable identity: nothing is refreshed on conflict
	// and implementations must fail when either differs from the incoming one.
	// Editorial changes append a satellite row instead of failing.
	RecordPools(ctx context.Context, tx pgx.Tx, syncedAt time.Time, pools []*maple.Pool) (map[common.Address]int64, error)

	// SavePoolStates inserts pool state snapshots.
	SavePoolStates(ctx context.Context, tx pgx.Tx, states []*maple.PoolState) error

	// RecordLoans registers loan identity rows (maple_pool_id and
	// borrower_user_id already resolved by the service) and records their
	// editorial attributes (loan_type and the loanMeta columns) in the
	// maple_loan_meta satellite at syncedAt, returning loan address ->
	// maple_loan.id. maple_pool_id and borrower_user_id are immutable identity:
	// nothing is refreshed on conflict and implementations must fail when
	// either differs from the incoming one. loanMeta changes append a satellite
	// row instead of failing.
	RecordLoans(ctx context.Context, tx pgx.Tx, syncedAt time.Time, loans []*maple.Loan) (map[common.Address]int64, error)

	// SaveLoanStates inserts loan state snapshots.
	SaveLoanStates(ctx context.Context, tx pgx.Tx, states []*maple.LoanState) error

	// SaveLoanCollaterals inserts loan collateral snapshots. Loans with null
	// API collateral have no row; callers pass only non-nil collaterals.
	SaveLoanCollaterals(ctx context.Context, tx pgx.Tx, collaterals []*maple.LoanCollateral) error

	// RecordSkyStrategies registers strategy identity rows and records their
	// editorial attribute (version) in the maple_sky_strategy_meta satellite at
	// syncedAt, returning strategy address -> maple_sky_strategy.id.
	// maple_pool_id is immutable identity: nothing is refreshed on conflict and
	// implementations must fail when it differs from the incoming one. A version
	// change appends a satellite row instead of failing.
	RecordSkyStrategies(ctx context.Context, tx pgx.Tx, syncedAt time.Time, strategies []*maple.SkyStrategy) (map[common.Address]int64, error)

	// SaveSkyStrategyStates inserts strategy state snapshots.
	SaveSkyStrategyStates(ctx context.Context, tx pgx.Tx, states []*maple.SkyStrategyState) error

	// SaveSyrupGlobalState inserts the protocol-wide Syrup aggregate snapshot.
	SaveSyrupGlobalState(ctx context.Context, tx pgx.Tx, state *maple.SyrupGlobalState) error
}

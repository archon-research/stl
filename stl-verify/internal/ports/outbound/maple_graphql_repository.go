package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MapleGraphQLRepository defines the persistence interface for Maple GraphQL
// indexer data. All write methods run within an external transaction so the
// service controls snapshot atomicity per sync phase; GetMapleProtocolID is a
// standalone registry read.
//
// Registry upserts (pools, loans, strategies, borrower users) return
// address -> database-id maps and resolve ids even when the row already
// exists (ON CONFLICT DO UPDATE ... RETURNING id).
//
// State saves use ON CONFLICT DO NOTHING on the
// (natural key, processing_version) primary key: the BEFORE INSERT trigger
// reuses the processing_version for same-build retries (deduplicated by the
// conflict clause) and assigns MAX+1 for a new build_id (insert succeeds).
// Implementations must sort batches by natural key before inserting so
// concurrent writers acquire the per-row advisory locks in a stable order
// (ADR-0002).
type MapleGraphQLRepository interface {
	// GetMapleProtocolID resolves the seeded maple protocol row id for a chain.
	GetMapleProtocolID(ctx context.Context, chainID int64) (int64, error)

	// GetOrCreateBorrowerUsers bulk-upserts borrower addresses into "user" and
	// returns address -> user id. Inserts NULL first_seen_block (GraphQL data
	// has no block) and never modifies first_seen_block on conflict, so users
	// first seen by on-chain indexers keep their earliest block.
	GetOrCreateBorrowerUsers(ctx context.Context, tx pgx.Tx, chainID int64, borrowers []common.Address) (map[common.Address]int64, error)

	// UpsertPools upserts pool registry rows and returns
	// address -> maple_pool.id. On conflict, refreshes name, asset_address,
	// asset_symbol, asset_decimals, and is_syrup.
	UpsertPools(ctx context.Context, tx pgx.Tx, pools []*entity.MaplePool) (map[common.Address]int64, error)

	// SavePoolStates inserts pool state snapshots.
	SavePoolStates(ctx context.Context, tx pgx.Tx, states []*entity.MaplePoolState) error

	// UpsertLoans upserts loan registry rows (maple_pool_id and
	// borrower_user_id already resolved by the service) and returns loan
	// address -> maple_loan.id. On conflict, refreshes maple_pool_id and the
	// loanMeta columns (a loan can be reassigned and gain/lose meta between
	// snapshots); borrower_user_id is deliberately never refreshed (a loan
	// contract's borrower is immutable), and implementations must fail when
	// the stored borrower differs from the incoming one.
	UpsertLoans(ctx context.Context, tx pgx.Tx, loans []*entity.MapleLoan) (map[common.Address]int64, error)

	// SaveLoanStates inserts loan state snapshots.
	SaveLoanStates(ctx context.Context, tx pgx.Tx, states []*entity.MapleLoanState) error

	// SaveLoanCollaterals inserts loan collateral snapshots. Loans with null
	// API collateral have no row; callers pass only non-nil collaterals.
	SaveLoanCollaterals(ctx context.Context, tx pgx.Tx, collaterals []*entity.MapleLoanCollateral) error

	// UpsertSkyStrategies upserts strategy registry rows and returns strategy
	// address -> maple_sky_strategy.id. On conflict, refreshes maple_pool_id
	// and version.
	UpsertSkyStrategies(ctx context.Context, tx pgx.Tx, strategies []*entity.MapleSkyStrategy) (map[common.Address]int64, error)

	// SaveSkyStrategyStates inserts strategy state snapshots.
	SaveSkyStrategyStates(ctx context.Context, tx pgx.Tx, states []*entity.MapleSkyStrategyState) error

	// SaveSyrupGlobalState inserts the protocol-wide Syrup aggregate snapshot.
	SaveSyrupGlobalState(ctx context.Context, tx pgx.Tx, state *entity.MapleSyrupGlobalState) error
}

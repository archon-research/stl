package outbound

import (
	"context"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity/maple"
)

// MapleAssetToken identifies a pool's underlying ERC-20 asset as reported by
// the Maple GraphQL API (poolV2.asset).
type MapleAssetToken struct {
	Address  common.Address
	Symbol   string
	Decimals int16
}

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

	// GetOrCreateAssetTokens bulk-upserts pool asset tokens into token and
	// returns address -> token id. Inserts NULL created_at_block (GraphQL data
	// has no block) and never modifies existing rows on conflict, so tokens
	// seeded by migrations or on-chain indexers keep their created_at_block,
	// symbol, and decimals. (TokenRepository.GetOrCreateTokens is unsuitable
	// here: its LEAST() merge would clobber created_at_block down to our zero
	// value.)
	GetOrCreateAssetTokens(ctx context.Context, tx pgx.Tx, chainID int64, assets []MapleAssetToken) (map[common.Address]int64, error)

	// UpsertPools upserts pool registry rows (asset_token_id already resolved
	// by the service via GetOrCreateAssetTokens) and returns
	// address -> maple_pool.id. On conflict, refreshes name, asset_token_id,
	// and is_syrup.
	UpsertPools(ctx context.Context, tx pgx.Tx, pools []*maple.Pool) (map[common.Address]int64, error)

	// SavePoolStates inserts pool state snapshots.
	SavePoolStates(ctx context.Context, tx pgx.Tx, states []*maple.PoolState) error

	// UpsertLoans upserts loan registry rows (maple_pool_id and
	// borrower_user_id already resolved by the service) and returns loan
	// address -> maple_loan.id. On conflict, refreshes maple_pool_id and the
	// loanMeta columns (a loan can be reassigned and gain/lose meta between
	// snapshots); borrower_user_id is deliberately never refreshed (a loan
	// contract's borrower is immutable), and implementations must fail when
	// the stored borrower differs from the incoming one.
	UpsertLoans(ctx context.Context, tx pgx.Tx, loans []*maple.Loan) (map[common.Address]int64, error)

	// SaveLoanStates inserts loan state snapshots.
	SaveLoanStates(ctx context.Context, tx pgx.Tx, states []*maple.LoanState) error

	// SaveLoanCollaterals inserts loan collateral snapshots. Loans with null
	// API collateral have no row; callers pass only non-nil collaterals.
	SaveLoanCollaterals(ctx context.Context, tx pgx.Tx, collaterals []*maple.LoanCollateral) error

	// UpsertSkyStrategies upserts strategy registry rows and returns strategy
	// address -> maple_sky_strategy.id. On conflict, refreshes maple_pool_id
	// and version.
	UpsertSkyStrategies(ctx context.Context, tx pgx.Tx, strategies []*maple.SkyStrategy) (map[common.Address]int64, error)

	// SaveSkyStrategyStates inserts strategy state snapshots.
	SaveSkyStrategyStates(ctx context.Context, tx pgx.Tx, states []*maple.SkyStrategyState) error

	// SaveSyrupGlobalState inserts the protocol-wide Syrup aggregate snapshot.
	SaveSyrupGlobalState(ctx context.Context, tx pgx.Tx, state *maple.SyrupGlobalState) error
}

package postgres

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var _ outbound.FluidVaultRepository = (*FluidVaultRepository)(nil)

// FluidVaultRepository is a PostgreSQL implementation of the
// outbound.FluidVaultRepository port.
type FluidVaultRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	buildID   buildregistry.BuildID
	batchSize int
}

// NewFluidVaultRepository creates a new PostgreSQL Fluid vault repository.
// If batchSize is <= 0, a default batch size of 1000 is used.
func NewFluidVaultRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID, batchSize int) (*FluidVaultRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &FluidVaultRepository{
		pool:      pool,
		logger:    logger,
		buildID:   buildID,
		batchSize: batchSize,
	}, nil
}

// UpsertVaults upserts vault registry rows and returns address -> fluid_vault.id.
// vault_type, collateral_token_id, and debt_token_id are immutable per vault, so
// the upsert refreshes nothing on conflict (the no-op DO UPDATE keeps RETURNING
// yielding the stored row) and the scan fails the run if any stored value
// differs from the incoming one.
func (r *FluidVaultRepository) UpsertVaults(ctx context.Context, tx pgx.Tx, vaults []*entity.FluidVault) (map[common.Address]int64, error) {
	if len(vaults) == 0 {
		return make(map[common.Address]int64), nil
	}

	// Dedup is address-only and the result map is keyed by address, so a
	// mixed-chain batch would silently drop a colliding address. Reject it first.
	if err := requireSingleChain(vaults, func(v *entity.FluidVault) int64 { return v.ChainID }, "fluid vaults"); err != nil {
		return nil, err
	}

	sorted := sortedByBytesKey(vaults, func(v *entity.FluidVault) []byte { return v.Address })

	batch := &pgx.Batch{}
	for _, v := range sorted {
		batch.Queue(
			`INSERT INTO fluid_vault (chain_id, protocol_id, address, vault_type, collateral_token_id, debt_token_id, created_at_block)
			 VALUES ($1, $2, $3, $4, $5, $6, $7)
			 ON CONFLICT (chain_id, address) DO UPDATE SET id = fluid_vault.id
			 RETURNING id, vault_type, collateral_token_id, debt_token_id`,
			v.ChainID, v.ProtocolID, v.Address, v.VaultType, v.CollateralTokenID, v.DebtTokenID, v.CreatedAtBlock,
		)
	}

	return collectBatchRows(ctx, tx, batch, sorted, "fluid vault",
		func(row pgx.Row, v *entity.FluidVault) (common.Address, int64, error) {
			addr := common.BytesToAddress(v.Address)
			var id, storedCollTokenID, storedDebtTokenID int64
			var storedVaultType string
			if err := row.Scan(&id, &storedVaultType, &storedCollTokenID, &storedDebtTokenID); err != nil {
				return common.Address{}, 0, fmt.Errorf("upserting fluid vault %s: %w", addr, err)
			}
			var mismatches []string
			if storedVaultType != v.VaultType {
				mismatches = append(mismatches, fmt.Sprintf("vault_type (stored %q, incoming %q)", storedVaultType, v.VaultType))
			}
			if storedCollTokenID != v.CollateralTokenID {
				mismatches = append(mismatches, fmt.Sprintf("collateral_token_id (stored %d, incoming %d)", storedCollTokenID, v.CollateralTokenID))
			}
			if storedDebtTokenID != v.DebtTokenID {
				mismatches = append(mismatches, fmt.Sprintf("debt_token_id (stored %d, incoming %d)", storedDebtTokenID, v.DebtTokenID))
			}
			if err := registryMismatchError("fluid vault", addr, mismatches); err != nil {
				return common.Address{}, 0, err
			}
			return addr, id, nil
		})
}

// GetAllVaults retrieves all known vaults for a chain, keyed by contract address.
func (r *FluidVaultRepository) GetAllVaults(ctx context.Context, chainID int64) (map[common.Address]*entity.FluidVault, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, protocol_id, address, vault_type, collateral_token_id, debt_token_id, created_at_block
		 FROM fluid_vault WHERE chain_id = $1`, chainID)
	if err != nil {
		return nil, fmt.Errorf("querying fluid vaults: %w", err)
	}
	defer rows.Close()

	vaults := make(map[common.Address]*entity.FluidVault)
	for rows.Next() {
		var v entity.FluidVault
		if err := rows.Scan(&v.ID, &v.ProtocolID, &v.Address, &v.VaultType, &v.CollateralTokenID, &v.DebtTokenID, &v.CreatedAtBlock); err != nil {
			return nil, fmt.Errorf("scanning fluid vault: %w", err)
		}
		v.ChainID = chainID
		vaults[common.BytesToAddress(v.Address)] = &v
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating fluid vaults: %w", err)
	}
	return vaults, nil
}

// SaveVaultStates appends per-vault aggregate snapshots. The BEFORE INSERT
// trigger assigns processing_version; ON CONFLICT DO NOTHING dedupes same-build
// retries (ADR-0002). The slice is sorted by natural key for a stable
// advisory-lock acquisition order (the caller's slice is not mutated).
func (r *FluidVaultRepository) SaveVaultStates(ctx context.Context, tx pgx.Tx, states []*entity.FluidVaultState) error {
	if len(states) == 0 {
		return nil
	}

	sorted := sortedCopy(states, func(a, b *entity.FluidVaultState) int {
		return cmp.Or(
			cmp.Compare(a.FluidVaultID, b.FluidVaultID),
			cmp.Compare(a.BlockNumber, b.BlockNumber),
			cmp.Compare(a.BlockVersion, b.BlockVersion),
			a.Timestamp.Compare(b.Timestamp),
		)
	})

	var inserted int64
	for chunk := range slices.Chunk(sorted, r.batchSize) {
		n, err := r.saveVaultStateBatch(ctx, tx, chunk)
		if err != nil {
			return err
		}
		inserted += n
	}
	return checkDedupedStateRows(r.logger, "fluid_vault_state", inserted, len(states))
}

func (r *FluidVaultRepository) saveVaultStateBatch(ctx context.Context, tx pgx.Tx, states []*entity.FluidVaultState) (int64, error) {
	const cols = 11
	var sb strings.Builder
	sb.WriteString(`INSERT INTO fluid_vault_state (fluid_vault_id, block_number, block_version, timestamp, total_collateral, total_debt, supply_exchange_price, borrow_exchange_price, supply_rate, borrow_rate, build_id) VALUES `)

	args := make([]any, 0, len(states)*cols)
	for i, s := range states {
		totalCollateral, err := bigIntToNumeric(s.TotalCollateral)
		if err != nil {
			return 0, fmt.Errorf("converting total_collateral for vault %d: %w", s.FluidVaultID, err)
		}
		totalDebt, err := bigIntToNumeric(s.TotalDebt)
		if err != nil {
			return 0, fmt.Errorf("converting total_debt for vault %d: %w", s.FluidVaultID, err)
		}

		writeValuesPlaceholders(&sb, i, cols)
		args = append(args, s.FluidVaultID, s.BlockNumber, s.BlockVersion, s.Timestamp,
			totalCollateral, totalDebt, optionalNumeric(s.SupplyExchangePrice), optionalNumeric(s.BorrowExchangePrice),
			optionalNumeric(s.SupplyRate), optionalNumeric(s.BorrowRate), int(r.buildID))
	}
	sb.WriteString(` ON CONFLICT (fluid_vault_id, block_number, block_version, timestamp, processing_version) DO NOTHING`)

	tag, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return 0, fmt.Errorf("saving fluid_vault_state: %w", err)
	}
	return tag.RowsAffected(), nil
}

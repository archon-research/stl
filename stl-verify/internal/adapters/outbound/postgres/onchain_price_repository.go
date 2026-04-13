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
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that OnchainPriceRepository implements outbound.OnchainPriceRepository.
var _ outbound.OnchainPriceRepository = (*OnchainPriceRepository)(nil)

// OnchainPriceRepository is a PostgreSQL implementation of the outbound.OnchainPriceRepository port.
type OnchainPriceRepository struct {
	pool      *pgxpool.Pool
	logger    *slog.Logger
	buildID   buildregistry.BuildID
	batchSize int
}

// NewOnchainPriceRepository creates a new PostgreSQL onchain price repository.
// If batchSize is <= 0, a default batch size of 1000 is used.
func NewOnchainPriceRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID, batchSize int) (*OnchainPriceRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	if batchSize <= 0 {
		batchSize = 1000
	}
	return &OnchainPriceRepository{
		pool:      pool,
		logger:    logger,
		buildID:   buildID,
		batchSize: batchSize,
	}, nil
}

// GetOracle retrieves an oracle by its name.
func (r *OnchainPriceRepository) GetOracle(ctx context.Context, name string) (*entity.Oracle, error) {
	var o entity.Oracle
	var addrBytes []byte
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, display_name, chain_id, address, oracle_type,
		       deployment_block, enabled, price_decimals, created_at, updated_at
		FROM oracle
		WHERE name = $1
	`, name).Scan(
		&o.ID, &o.Name, &o.DisplayName, &o.ChainID, &addrBytes, &o.OracleType,
		&o.DeploymentBlock, &o.Enabled, &o.PriceDecimals, &o.CreatedAt, &o.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("oracle not found: %s", name)
	}
	if err != nil {
		return nil, fmt.Errorf("querying oracle: %w", err)
	}
	copy(o.Address[:], addrBytes)
	return &o, nil
}

// GetEnabledAssets retrieves all enabled assets for a given oracle.
func (r *OnchainPriceRepository) GetEnabledAssets(ctx context.Context, oracleID int64) ([]*entity.OracleAsset, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency, created_at
		FROM oracle_asset
		WHERE oracle_id = $1 AND enabled = true
		ORDER BY id
	`, oracleID)
	if err != nil {
		return nil, fmt.Errorf("querying enabled oracle assets: %w", err)
	}
	defer rows.Close()

	var assets []*entity.OracleAsset
	for rows.Next() {
		var oa entity.OracleAsset
		var feedAddrBytes []byte
		var feedDecimals *int
		if err := rows.Scan(&oa.ID, &oa.OracleID, &oa.TokenID, &oa.Enabled,
			&feedAddrBytes, &feedDecimals, &oa.QuoteCurrency, &oa.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning oracle asset: %w", err)
		}
		if len(feedAddrBytes) > 0 {
			oa.FeedAddress = common.BytesToAddress(feedAddrBytes)
		}
		if feedDecimals != nil {
			oa.FeedDecimals = *feedDecimals
		}
		assets = append(assets, &oa)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating oracle assets: %w", err)
	}
	return assets, nil
}

// GetLatestPrices returns the most recent price per token for a given oracle.
// Used for change detection: only store prices that differ from the previous block.
func (r *OnchainPriceRepository) GetLatestPrices(ctx context.Context, oracleID int64) (map[int64]float64, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT ON (token_id) token_id, price_usd
		FROM onchain_token_price
		WHERE oracle_id = $1
		ORDER BY token_id, block_number DESC, block_version DESC
	`, oracleID)
	if err != nil {
		return nil, fmt.Errorf("querying latest onchain prices: %w", err)
	}
	defer rows.Close()

	prices := make(map[int64]float64)
	for rows.Next() {
		var tokenID int64
		var priceUSD float64
		if err := rows.Scan(&tokenID, &priceUSD); err != nil {
			return nil, fmt.Errorf("scanning latest price: %w", err)
		}
		prices[tokenID] = priceUSD
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating latest prices: %w", err)
	}
	return prices, nil
}

// GetLatestBlock returns the highest block number stored for a given oracle.
// Returns 0 if no blocks have been stored yet.
func (r *OnchainPriceRepository) GetLatestBlock(ctx context.Context, oracleID int64) (int64, error) {
	var blockNumber *int64
	err := r.pool.QueryRow(ctx, `
		SELECT MAX(block_number)
		FROM onchain_token_price
		WHERE oracle_id = $1
	`, oracleID).Scan(&blockNumber)
	if err != nil {
		return 0, fmt.Errorf("querying latest block: %w", err)
	}
	if blockNumber == nil {
		return 0, nil
	}
	return *blockNumber, nil
}

// GetTokenAddresses returns a map of token_id → on-chain address for enabled oracle assets.
func (r *OnchainPriceRepository) GetTokenAddresses(ctx context.Context, oracleID int64) (map[int64][]byte, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT oa.token_id, t.address
		FROM oracle_asset oa
		JOIN token t ON t.id = oa.token_id
		WHERE oa.oracle_id = $1 AND oa.enabled = true
		ORDER BY oa.id
	`, oracleID)
	if err != nil {
		return nil, fmt.Errorf("querying token addresses: %w", err)
	}
	defer rows.Close()

	addrs := make(map[int64][]byte)
	for rows.Next() {
		var tokenID int64
		var address []byte
		if err := rows.Scan(&tokenID, &address); err != nil {
			return nil, fmt.Errorf("scanning token address: %w", err)
		}
		addrs[tokenID] = address
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating token addresses: %w", err)
	}
	return addrs, nil
}

// UpsertPrices inserts onchain price records in batches.
// Uses ON CONFLICT DO NOTHING to handle duplicates.
func (r *OnchainPriceRepository) UpsertPrices(ctx context.Context, prices []*entity.OnchainTokenPrice) error {
	if len(prices) == 0 {
		return nil
	}

	// Sort by natural key to ensure consistent lock acquisition order and prevent deadlocks.
	slices.SortFunc(prices, func(a, b *entity.OnchainTokenPrice) int {
		return cmp.Or(
			cmp.Compare(a.TokenID, b.TokenID),
			cmp.Compare(a.OracleID, b.OracleID),
			cmp.Compare(a.BlockNumber, b.BlockNumber),
			cmp.Compare(a.BlockVersion, b.BlockVersion),
			a.Timestamp.Compare(b.Timestamp),
		)
	})

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	for i := 0; i < len(prices); i += r.batchSize {
		end := min(i+r.batchSize, len(prices))
		batch := prices[i:end]

		if err := r.upsertPriceBatch(ctx, tx, batch); err != nil {
			return err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}
	return nil
}

func (r *OnchainPriceRepository) upsertPriceBatch(ctx context.Context, tx pgx.Tx, prices []*entity.OnchainTokenPrice) error {
	if len(prices) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString(`
		INSERT INTO onchain_token_price (token_id, oracle_id, block_number, block_version, timestamp, price_usd, build_id)
		VALUES `)

	args := make([]any, 0, len(prices)*7)
	for i, price := range prices {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * 7
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7))

		args = append(args, price.TokenID, price.OracleID, price.BlockNumber, price.BlockVersion, price.Timestamp, price.PriceUSD, int(r.buildID))
	}

	sb.WriteString(` ON CONFLICT (token_id, oracle_id, block_number, block_version, processing_version, timestamp) DO NOTHING`)

	_, err := tx.Exec(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("upserting onchain price batch: %w", err)
	}
	return nil
}

// GetEnabledOraclesByChain retrieves all enabled oracles for a given chain.
func (r *OnchainPriceRepository) GetEnabledOraclesByChain(ctx context.Context, chainID int64) ([]*entity.Oracle, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, name, display_name, chain_id, address, oracle_type,
		       deployment_block, enabled, price_decimals, created_at, updated_at
		FROM oracle
		WHERE enabled = true AND chain_id = $1
		ORDER BY id
	`, chainID)
	if err != nil {
		return nil, fmt.Errorf("querying enabled oracles by chain: %w", err)
	}
	defer rows.Close()

	var oracles []*entity.Oracle
	for rows.Next() {
		var o entity.Oracle
		var addrBytes []byte
		if err := rows.Scan(
			&o.ID, &o.Name, &o.DisplayName, &o.ChainID, &addrBytes, &o.OracleType,
			&o.DeploymentBlock, &o.Enabled, &o.PriceDecimals, &o.CreatedAt, &o.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scanning oracle: %w", err)
		}
		o.Address = common.BytesToAddress(addrBytes)
		oracles = append(oracles, &o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating oracles: %w", err)
	}
	return oracles, nil
}

// GetOracleByAddress retrieves an oracle by chain_id and onchain address.
func (r *OnchainPriceRepository) GetOracleByAddress(ctx context.Context, chainID int, address []byte) (*entity.Oracle, error) {
	var o entity.Oracle
	var addrBytes []byte
	err := r.pool.QueryRow(ctx, `
		SELECT id, name, display_name, chain_id, address, oracle_type,
		       deployment_block, enabled, price_decimals, created_at, updated_at
		FROM oracle
		WHERE chain_id = $1 AND address = $2
	`, chainID, address).Scan(
		&o.ID, &o.Name, &o.DisplayName, &o.ChainID, &addrBytes, &o.OracleType,
		&o.DeploymentBlock, &o.Enabled, &o.PriceDecimals, &o.CreatedAt, &o.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("querying oracle by address: %w", err)
	}
	copy(o.Address[:], addrBytes)
	return &o, nil
}

// InsertOracle inserts a new oracle and returns it with the generated ID.
func (r *OnchainPriceRepository) InsertOracle(ctx context.Context, oracle *entity.Oracle) (*entity.Oracle, error) {
	if oracle.OracleType == "" {
		return nil, fmt.Errorf("inserting oracle: oracle_type is required")
	}
	err := r.pool.QueryRow(ctx, `
		INSERT INTO oracle (name, display_name, chain_id, address, oracle_type, deployment_block, enabled, price_decimals)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING id, created_at, updated_at
	`, oracle.Name, oracle.DisplayName, oracle.ChainID, oracle.Address.Bytes(),
		oracle.OracleType, oracle.DeploymentBlock, oracle.Enabled, oracle.PriceDecimals,
	).Scan(&oracle.ID, &oracle.CreatedAt, &oracle.UpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("inserting oracle: %w", err)
	}
	return oracle, nil
}

// GetAllActiveProtocolOracles retrieves all active protocol-oracle bindings.
// Returns only the latest binding per protocol (by from_block DESC).
func (r *OnchainPriceRepository) GetAllActiveProtocolOracles(ctx context.Context) ([]*entity.ProtocolOracle, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT DISTINCT ON (protocol_id) id, protocol_id, oracle_id, from_block, created_at
		FROM protocol_oracle
		ORDER BY protocol_id, from_block DESC
	`)
	if err != nil {
		return nil, fmt.Errorf("querying active protocol oracles: %w", err)
	}
	defer rows.Close()

	var bindings []*entity.ProtocolOracle
	for rows.Next() {
		var po entity.ProtocolOracle
		if err := rows.Scan(&po.ID, &po.ProtocolID, &po.OracleID, &po.FromBlock, &po.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning protocol oracle: %w", err)
		}
		bindings = append(bindings, &po)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating protocol oracles: %w", err)
	}
	return bindings, nil
}

// InsertProtocolOracleBinding inserts a new protocol-oracle binding.
func (r *OnchainPriceRepository) InsertProtocolOracleBinding(ctx context.Context, binding *entity.ProtocolOracle) (*entity.ProtocolOracle, error) {
	err := r.pool.QueryRow(ctx, `
		INSERT INTO protocol_oracle (protocol_id, oracle_id, from_block)
		VALUES ($1, $2, $3)
		RETURNING id, created_at
	`, binding.ProtocolID, binding.OracleID, binding.FromBlock,
	).Scan(&binding.ID, &binding.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("inserting protocol oracle binding: %w", err)
	}
	return binding, nil
}

// GetAllProtocolOracleBindings retrieves ALL protocol-oracle bindings ordered by protocol and from_block.
func (r *OnchainPriceRepository) GetAllProtocolOracleBindings(ctx context.Context) ([]*entity.ProtocolOracle, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, protocol_id, oracle_id, from_block, created_at
		FROM protocol_oracle
		ORDER BY protocol_id, from_block
	`)
	if err != nil {
		return nil, fmt.Errorf("querying protocol oracle bindings: %w", err)
	}
	defer rows.Close()

	var bindings []*entity.ProtocolOracle
	for rows.Next() {
		var po entity.ProtocolOracle
		if err := rows.Scan(&po.ID, &po.ProtocolID, &po.OracleID, &po.FromBlock, &po.CreatedAt); err != nil {
			return nil, fmt.Errorf("scanning protocol oracle binding: %w", err)
		}
		bindings = append(bindings, &po)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating protocol oracle bindings: %w", err)
	}
	return bindings, nil
}

// CopyOracleAssets copies all enabled oracle_asset rows from one oracle to another.
func (r *OnchainPriceRepository) CopyOracleAssets(ctx context.Context, fromOracleID, toOracleID int64) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency)
		SELECT $2, token_id, enabled, feed_address, feed_decimals, quote_currency
		FROM oracle_asset
		WHERE oracle_id = $1 AND enabled = true
		ON CONFLICT DO NOTHING
	`, fromOracleID, toOracleID)
	if err != nil {
		return fmt.Errorf("copying oracle assets from %d to %d: %w", fromOracleID, toOracleID, err)
	}
	return nil
}

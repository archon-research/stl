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
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that OnchainPriceRepository implements outbound.OnchainPriceRepository.
var _ outbound.OnchainPriceRepository = (*OnchainPriceRepository)(nil)

// OnchainPriceRepository is a PostgreSQL implementation of the outbound.OnchainPriceRepository port.
type OnchainPriceRepository struct {
	db        querier
	logger    *slog.Logger
	buildID   buildregistry.BuildID
	runID     buildregistry.RunID
	batchSize int
}

// NewOnchainPriceRepository creates a new PostgreSQL onchain price repository.
// If batchSize is <= 0, a default batch size of 1000 is used.
func NewOnchainPriceRepository(pool *pgxpool.Pool, logger *slog.Logger, buildID buildregistry.BuildID, runID buildregistry.RunID, batchSize int) (*OnchainPriceRepository, error) {
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
		db:        pool,
		logger:    logger,
		buildID:   buildID,
		runID:     runID,
		batchSize: batchSize,
	}, nil
}

// WithTx returns a copy of the repository whose statements run on tx: how a writer
// loads its reference data inside the transaction that records its run snapshot
// (buildregistry.Registry.OpenRun, ADR-0006 §2). The copy is valid until tx ends.
func (r *OnchainPriceRepository) WithTx(tx pgx.Tx) *OnchainPriceRepository {
	scoped := *r
	scoped.db = tx
	return &scoped
}

// GetOracle retrieves an oracle by its name.
func (r *OnchainPriceRepository) GetOracle(ctx context.Context, name string) (*entity.Oracle, error) {
	var o entity.Oracle
	var addrBytes []byte
	err := r.db.QueryRow(ctx, `
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

// Ordered by the natural key, not by id: a re-versioned row gets a fresh id, so id order would
// reshuffle the list on every mapping change.
var enabledAssetsSQL = fmt.Sprintf(`
	SELECT id, oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency, created_at
	FROM %s oa
	WHERE oracle_id = $1 AND enabled = true
	ORDER BY oracle_id, token_id, feed_key
`, OracleAssetAsOf("$2::timestamptz"))

func (r *OnchainPriceRepository) GetEnabledAssets(ctx context.Context, oracleID int64, referenceEffectiveAt time.Time) ([]*entity.OracleAsset, error) {
	rows, err := r.db.Query(ctx, enabledAssetsSQL, oracleID, referenceEffectiveAt)
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
	rows, err := r.db.Query(ctx, `
		SELECT DISTINCT ON (token_id) token_id, price_usd
		FROM onchain_token_price
		WHERE oracle_id = $1
		ORDER BY token_id, block_number DESC, block_version DESC, processing_version DESC
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
	err := r.db.QueryRow(ctx, `
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

// Same effective_at as GetEnabledAssets, or a unit would carry an asset it never resolved an
// address for.
var tokenInfosSQL = fmt.Sprintf(`
	SELECT oa.token_id, t.address, t.decimals
	FROM %s oa
	JOIN token t ON t.id = oa.token_id
	WHERE oa.oracle_id = $1 AND oa.enabled = true
	ORDER BY oa.oracle_id, oa.token_id, oa.feed_key
`, OracleAssetAsOf("$2::timestamptz"))

func (r *OnchainPriceRepository) GetTokenInfos(ctx context.Context, oracleID int64, referenceEffectiveAt time.Time) (map[int64]outbound.TokenInfo, error) {
	rows, err := r.db.Query(ctx, tokenInfosSQL, oracleID, referenceEffectiveAt)
	if err != nil {
		return nil, fmt.Errorf("querying token infos: %w", err)
	}
	defer rows.Close()

	infos := make(map[int64]outbound.TokenInfo)
	for rows.Next() {
		var tokenID int64
		var info outbound.TokenInfo
		if err := rows.Scan(&tokenID, &info.Address, &info.Decimals); err != nil {
			return nil, fmt.Errorf("scanning token info: %w", err)
		}
		infos[tokenID] = info
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterating token infos: %w", err)
	}
	return infos, nil
}

// UpsertPrices inserts onchain price records in batches.
// Uses ON CONFLICT DO NOTHING to handle duplicates.
func (r *OnchainPriceRepository) UpsertPrices(ctx context.Context, prices []*entity.OnchainTokenPrice) error {
	if len(prices) == 0 {
		return nil
	}

	// Sort by natural key once, before chunking; same rationale as
	// PriceRepository.UpsertPrices. See ADR-0002 §3.
	slices.SortFunc(prices, func(a, b *entity.OnchainTokenPrice) int {
		return cmp.Or(
			cmp.Compare(a.TokenID, b.TokenID),
			cmp.Compare(a.OracleID, b.OracleID),
			cmp.Compare(a.BlockNumber, b.BlockNumber),
			cmp.Compare(a.BlockVersion, b.BlockVersion),
			a.Timestamp.Compare(b.Timestamp),
		)
	})

	tx, err := r.db.Begin(ctx)
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
		INSERT INTO onchain_token_price (token_id, oracle_id, block_number, block_version, timestamp, price_usd, build_id, run_id)
		VALUES `)

	const cols = 8
	args := make([]any, 0, len(prices)*cols)
	for i, price := range prices {
		if i > 0 {
			sb.WriteString(", ")
		}
		baseIdx := i * cols
		sb.WriteString(fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			baseIdx+1, baseIdx+2, baseIdx+3, baseIdx+4, baseIdx+5, baseIdx+6, baseIdx+7, baseIdx+8))

		args = append(args, price.TokenID, price.OracleID, price.BlockNumber, price.BlockVersion, price.Timestamp, price.PriceUSD, int(r.buildID), int64(r.runID))
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
	rows, err := r.db.Query(ctx, `
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
	err := r.db.QueryRow(ctx, `
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
	err := r.db.QueryRow(ctx, `
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

// InsertProtocolOracleBinding inserts a new protocol-oracle binding.
func (r *OnchainPriceRepository) InsertProtocolOracleBinding(ctx context.Context, binding *entity.ProtocolOracle) (*entity.ProtocolOracle, error) {
	err := r.db.QueryRow(ctx, `
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
	rows, err := r.db.Query(ctx, `
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

// valid_from is the run's recorded instant, not the wall clock, so a replay produces identically
// dated rows; change_reason renders it in UTC so the text never depends on the session TimeZone.
// Zero rows copied is legitimate but logged, because the target is then registered with no assets
// and the symptom surfaces much later in a different process.
// change_reason is rendered in Go and bound as a parameter: the text is a description for a human
// reader, not a value SQL needs to compute, and formatting it here keeps the query free of a
// session-TimeZone-dependent to_char.
var copyOracleAssetsSQL = fmt.Sprintf(`
	INSERT INTO oracle_asset (oracle_id, token_id, enabled, feed_address, feed_decimals, quote_currency, valid_from, change_reason, run_id)
	SELECT $2, token_id, enabled, feed_address, feed_decimals, quote_currency, $3::timestamptz, $4, $5
	FROM %s oa
	WHERE oracle_id = $1 AND enabled = true
	ON CONFLICT DO NOTHING
`, OracleAssetAsOf("$3::timestamptz"))

// The arbiter skips a source key the target already holds at processing_version 0, which is benign
// only while that existing row carries the same mapping. A differing one leaves the target
// partially mapped, and reporting success would hide it.
var unmappedSourceAssetsSQL = fmt.Sprintf(`
	SELECT count(*)
	FROM %[1]s src
	WHERE src.oracle_id = $1 AND src.enabled
	  AND NOT EXISTS (
		SELECT 1
		FROM %[1]s tgt
		WHERE tgt.oracle_id = $2
		  AND tgt.token_id = src.token_id
		  AND tgt.feed_key = src.feed_key
		  AND tgt.enabled
		  AND tgt.feed_decimals IS NOT DISTINCT FROM src.feed_decimals
		  AND tgt.quote_currency IS NOT DISTINCT FROM src.quote_currency
	  )
`, OracleAssetAsOf("$3::timestamptz"))

// The INSERT and its verification share one transaction. The arbiter skips only the conflicting
// keys, so a target holding a conflicting version still takes every other source mapping: on
// autocommit the caller got an error while that subset stayed behind, registering the target as
// partially mapped. Appending cannot undo it either — the table forbids DELETE, so the residue
// could only be superseded by a further disabling version. Rolling back leaves the target as it
// was, which is the only outcome the error message honestly describes.
func (r *OnchainPriceRepository) CopyOracleAssets(ctx context.Context, fromOracleID, toOracleID int64, referenceEffectiveAt time.Time) error {
	changeReason := fmt.Sprintf("copied from oracle %d as of %s",
		fromOracleID, referenceEffectiveAt.UTC().Format(time.RFC3339))

	tx, err := r.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("beginning transaction: %w", err)
	}
	defer rollback(ctx, tx, r.logger)

	tag, err := tx.Exec(ctx, copyOracleAssetsSQL, fromOracleID, toOracleID, referenceEffectiveAt, changeReason, int64(r.runID))
	if err != nil {
		return fmt.Errorf("copying oracle assets from %d to %d: %w", fromOracleID, toOracleID, err)
	}

	// Reads the rows the INSERT just wrote: the count is what the commit would make visible.
	var unmapped int
	if err := tx.QueryRow(ctx, unmappedSourceAssetsSQL,
		fromOracleID, toOracleID, referenceEffectiveAt).Scan(&unmapped); err != nil {
		return fmt.Errorf("verifying copied oracle assets from %d to %d: %w", fromOracleID, toOracleID, err)
	}
	if unmapped > 0 {
		return fmt.Errorf("copying oracle assets from %d to %d: %d of the source's enabled mappings are absent or differ on the target; it already carries conflicting versions",
			fromOracleID, toOracleID, unmapped)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing the oracle asset copy from %d to %d: %w", fromOracleID, toOracleID, err)
	}

	r.logger.Info("copied oracle assets",
		"from_oracle_id", fromOracleID,
		"to_oracle_id", toOracleID,
		"rows", tag.RowsAffected(),
		"reference_effective_at", referenceEffectiveAt.UTC().Format(time.RFC3339Nano))
	return nil
}

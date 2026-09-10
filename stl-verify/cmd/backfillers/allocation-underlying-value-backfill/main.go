// Package main backfills allocation_position.underlying_value for rows written
// before the 2026-07-06 deploy that started populating it on ingest.
//
// Four kinds of holding are resolved: direct erc20 holdings (underlying_value
// duplicates balance, underlying_token_id self-referencing per the column's
// documented invariant; gated on the axis-synome token_type registry, not
// merely "no receipt_token match" -- see token_type_registry.go), aToken
// holdings (1:1 by construction), erc4626-like receipt tokens, whose
// conversion ratio genuinely moves over time, and everything else (Curve LP
// shares, NAV/RWA shares, pre-cutover uni_v3 rows), which is left NULL as
// not computable in this phase. The erc4626 case is read from a real archive
// RPC -- convertToAssets at the row's own pinned block_number, batched per
// block via multicall (see erc4626_archive.go) -- falling back to a
// price-ratio derivation from onchain_token_price only for a row the archive
// could not answer (see classify.go).
//
// Sweeps are in scope alongside in/out transfers: validateTransferParties
// requires both transfer parties only for a transfer-driven row, and requires
// them absent for a sweep, so a pre-cutover sweep is already a valid entity
// with nothing to recover first (pre-cutover in/out rows need the transfer-party
// log re-decode first and are left for that half of VEC-759).
//
// Writes go through the same AllocationRepository.SavePositions the live tracker
// uses, so the append-only invariant holds for free: a new build_id makes the
// assign_processing_version_allocation_position trigger see no exact-duplicate row
// and assign a fresh processing_version rather than colliding with the original.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/big"
	"os"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
	slog.Info("completed successfully")
}

type cliConfig struct {
	dbURL       string
	after       time.Time
	before      time.Time
	primeID     int64
	limit       int
	dryRun      bool
	maxPriceLag int64
}

func parseFlags(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("allocation-underlying-value-backfill", flag.ContinueOnError)
	dbURL := fs.String("db", "", "PostgreSQL connection URL (required)")
	before := fs.String("before", "2026-07-06T14:00:00Z", "Backfill rows created strictly before this RFC3339 instant")
	after := fs.String("after", "", "Resume cursor: only rows created at or after this RFC3339 instant (empty = from the beginning)")
	primeID := fs.Int64("prime-id", 0, "Restrict to one prime.id (0 = all primes)")
	limit := fs.Int("limit", 100, "Max candidate rows to process this run")
	dryRun := fs.Bool("dry-run", true, "Log what would be written without saving")
	maxPriceLag := fs.Int64("max-price-block-lag", 7200, "Reject an erc4626 conversion whose newest at-or-before price is more than this many blocks older than the row (~1 day on mainnet)")
	if err := fs.Parse(args); err != nil {
		return cliConfig{}, err
	}
	if *dbURL == "" {
		return cliConfig{}, fmt.Errorf("--db is required")
	}
	beforeAt, err := time.Parse(time.RFC3339, *before)
	if err != nil {
		return cliConfig{}, fmt.Errorf("--before: %w", err)
	}
	var afterAt time.Time
	if *after != "" {
		afterAt, err = time.Parse(time.RFC3339, *after)
		if err != nil {
			return cliConfig{}, fmt.Errorf("--after: %w", err)
		}
	}
	return cliConfig{dbURL: *dbURL, after: afterAt, before: beforeAt, primeID: *primeID, limit: *limit, dryRun: *dryRun, maxPriceLag: *maxPriceLag}, nil
}

// candidateRow is a row missing underlying_value, plus the classification
// inputs needed to resolve it: direct/aToken/erc4626 bucketing, and the
// registry and price-history fields the erc4626 and price-ratio paths need.
type candidateRow struct {
	chainID              int64
	tokenAddress         common.Address
	tokenDecimals        int32
	primeID              int64
	proxyAddress         common.Address
	balance              *big.Int // raw units, upscaled from the DB's human-normalized text via humanToRaw
	balanceHuman         string
	scaledBalance        *big.Int // raw units; nil when the original row's scaled_balance is NULL
	blockNumber          int64
	blockVersion         int32
	txHash               string
	logIndex             int32
	txAmount             *big.Int
	direction            string
	fromAddress          *common.Address
	toAddress            *common.Address
	createdAt            time.Time
	isReceiptToken       bool
	underlyingAddress    *common.Address
	underlyingDecimals   *int32 // NULL for a direct holding (self-referencing case)
	underlyingIsOneToOne bool   // true for aTokens; false for erc4626-like (skip)
	// erc4626 conversion, derived from onchain_token_price at (or at-or-before)
	// this row's own block rather than from a live convertToAssets call. Both
	// are nil when the price history does not reach this block.
	erc4626UnderlyingHuman  *string
	sharePriceBlockLag      *int64
	underlyingPriceBlockLag *int64
	protocolName            string
}

func run(ctx context.Context, args []string) error {
	cfg, err := parseFlags(args)
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, cfg.dbURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()

	deps, err := wireDependencies(ctx, pool)
	if err != nil {
		return err
	}

	candidates, err := fetchCandidates(ctx, pool, cfg)
	if err != nil {
		return fmt.Errorf("fetch candidates: %w", err)
	}

	classified, err := resolveAndClassify(ctx, candidates, cfg)
	if err != nil {
		return err
	}
	// Logged only once classification has actually succeeded: this count and
	// cursor are what a batched caller advances -after by, and a value logged
	// before a possible abort above would name a batch that was never written.
	logCandidatesFetched(candidates, cfg.limit)

	if cfg.dryRun {
		logDryRunPreview(classified)
		return nil
	}

	return persist(ctx, deps, classified)
}

// runnerDeps bundles the write-path collaborators run() needs once
// classification is done, so wiring them up is one error-checked step instead
// of several inline ones.
type runnerDeps struct {
	txm       *postgres.TxManager
	allocRepo *postgres.AllocationRepository
}

func wireDependencies(ctx context.Context, pool *pgxpool.Pool) (runnerDeps, error) {
	registry, err := buildregistry.New(ctx, pool)
	if err != nil {
		return runnerDeps{}, fmt.Errorf("register build: %w", err)
	}

	var runID buildregistry.RunID
	if _, err := registry.OpenRun(ctx, time.Now().UTC(), func(_ pgx.Tx, id buildregistry.RunID) error {
		runID = id
		return nil
	}); err != nil {
		return runnerDeps{}, fmt.Errorf("open writer run: %w", err)
	}

	txm, err := postgres.NewTxManager(pool, nil)
	if err != nil {
		return runnerDeps{}, fmt.Errorf("tx manager: %w", err)
	}
	tokenRepo, err := postgres.NewTokenRepository(pool, nil, 100, runID)
	if err != nil {
		return runnerDeps{}, fmt.Errorf("token repository: %w", err)
	}
	allocRepo := postgres.NewAllocationRepository(pool, txm, tokenRepo, nil, registry.BuildID(), runID)

	return runnerDeps{txm: txm, allocRepo: allocRepo}, nil
}

// logCandidatesFetched reports the cursor a batched caller advances by. It is
// the created_at of the last candidate FETCHED, not the last one written:
// rows this run classified as permanently skippable (erc4626, or a receipt
// token the registry cannot resolve an underlying for) are never going to be
// written, so a caller that advanced only past written rows would re-fetch
// them forever and the LIMIT window would stop making progress long before
// the gap was closed.
//
// -after has only created_at resolution, so a full batch sharing one
// created_at value (more distinct rows at that instant than -limit) cannot be
// advanced past by any -after value: the next run re-fetches the same batch
// forever. That's flagged here, not fixed here -- fixing it needs a cursor
// with a secondary key, which is a CLI contract change.
func logCandidatesFetched(candidates []candidateRow, limit int) {
	if len(candidates) == 0 {
		slog.Info("candidates fetched", "count", 0)
		return
	}
	maxCreatedAt := candidates[len(candidates)-1].createdAt
	slog.Info("candidates fetched",
		"count", len(candidates),
		"max_created_at", maxCreatedAt.UTC().Format(time.RFC3339Nano),
	)
	if len(candidates) == limit && candidates[0].createdAt.Equal(maxCreatedAt) {
		slog.Warn("this batch's rows all share one created_at and filled -limit; "+
			"-after cannot resolve finer than created_at, so if any are permanently "+
			"skippable the next run may re-fetch this same batch instead of progressing",
			"created_at", maxCreatedAt.UTC().Format(time.RFC3339Nano), "limit", limit)
	}
}

// resolveAndClassify reads the real erc4626 conversions this batch's rows
// need from the archive, then classifies every candidate (falling back to the
// price-ratio derivation only where the archive could not answer).
func resolveAndClassify(ctx context.Context, candidates []candidateRow, cfg cliConfig) ([]positionSource, error) {
	tokenTypes, err := loadTokenTypeRegistry()
	if err != nil {
		return nil, fmt.Errorf("loading token type registry: %w", err)
	}

	archiveResolver, err := newERC4626ArchiveResolver()
	if err != nil {
		return nil, fmt.Errorf("initializing erc4626 archive resolver: %w", err)
	}
	archiveResults, err := archiveResolver.resolve(ctx, candidates, slog.Default())
	if err != nil {
		return nil, fmt.Errorf("resolving erc4626 conversions via archive: %w", err)
	}

	classified, stats, err := classifyCandidates(candidates, archiveResults, tokenTypes, cfg)
	if err != nil {
		return nil, err
	}
	logClassification(stats, len(classified))
	return classified, nil
}

func logDryRunPreview(classified []positionSource) {
	slog.Info("dry run: not writing. Pass -dry-run=false to persist.")
	const maxLogged = 10
	for i, ps := range classified {
		if i >= maxLogged {
			slog.Info("... additional rows omitted from dry-run log", "remaining", len(classified)-maxLogged)
			break
		}
		p := ps.position
		slog.Info("would backfill",
			"chain_id", p.ChainID, "token", p.TokenAddress.Hex(),
			"block_number", p.BlockNumber, "underlying_value", p.Underlying.Value.String(),
			"underlying_asset", p.Underlying.AssetAddress.Hex(), "source", ps.source)
	}
}

func persist(ctx context.Context, deps runnerDeps, classified []positionSource) error {
	positions := make([]*entity.AllocationPosition, len(classified))
	for i, ps := range classified {
		positions[i] = ps.position
	}

	if err := deps.txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		return deps.allocRepo.SavePositions(ctx, tx, positions)
	}); err != nil {
		return fmt.Errorf("save positions: %w", err)
	}

	// "submitted", not "confirmed inserted": ON CONFLICT DO NOTHING can no-op
	// an individual row (e.g. a concurrent run already wrote it), and
	// SavePositions returns no per-row count to distinguish that from a real
	// insert.
	slog.Info("backfilled", "rows_submitted", len(positions))
	return nil
}

// toEntity rebuilds the original event as a new AllocationPosition carrying the
// derived underlying valuation. Every other field, including ScaledBalance, is
// copied from the row that already exists in history — this is a correction,
// not a new event.
func toEntity(c candidateRow, underlyingAsset common.Address, underlyingDecimals int32, underlyingRaw *big.Int) *entity.AllocationPosition {
	return &entity.AllocationPosition{
		ChainID:       c.chainID,
		TokenAddress:  c.tokenAddress,
		PrimeID:       c.primeID,
		ProxyAddress:  c.proxyAddress,
		Balance:       c.balance,
		ScaledBalance: c.scaledBalance,
		// Load-bearing. The repository writes balance as
		// toNumeric(pos.Balance, pos.TokenDecimals), so leaving this at its zero
		// value descales by 10^0 and stores the RAW integer in a column that
		// holds human-normalized values -- balance inflated by 10^18 for an
		// 18-decimal token. underlying_value escaped it only because
		// Underlying.AssetDecimals is set, which is what made the bug survive a
		// row-count-only check of the write.
		TokenDecimals:  int(c.tokenDecimals),
		BlockNumber:    c.blockNumber,
		BlockVersion:   int(c.blockVersion),
		TxHash:         c.txHash,
		LogIndex:       int(c.logIndex),
		TxAmount:       c.txAmount,
		Direction:      c.direction,
		FromAddress:    c.fromAddress,
		ToAddress:      c.toAddress,
		CreatedAtBlock: c.blockNumber,
		CreatedAt:      c.createdAt, // block timestamp, unchanged from the original row
		Underlying: &entity.UnderlyingValuation{
			Value:         underlyingRaw,
			AssetAddress:  underlyingAsset,
			AssetDecimals: int(underlyingDecimals),
		},
	}
}

// candidateQuery finds flow rows missing underlying_value, joined with enough
// registry and price-history data to classify each one downstream: a
// receipt_token row with no match is a direct holding (subject to the
// axis-synome token_type check in classify.go); a match under an Aave-family
// protocol is an aToken; anything else with a match is erc4626-like.
//
// The price-ratio fallback's onchain_token_price reads are gated exactly like
// the balance-pricing SQL gates them (allocation_position_repository.py,
// _ALLOCATION_ACTIVITY_BUCKETS_SQL's token_context CTE): a price only counts
// if its oracle is the one protocol_oracle binds to the row's own protocol,
// and that (oracle, token) mapping is enabled in oracle_asset. Without this a
// price from an unrelated, possibly-disabled oracle silently entered the
// ratio.
const candidateQuery = `
	WITH enabled_oracle_assets AS MATERIALIZED (
		SELECT DISTINCT ON (oracle_id, token_id, feed_key) oracle_id, token_id, enabled
		FROM oracle_asset
		WHERE valid_from <= now()
		ORDER BY oracle_id, token_id, feed_key, valid_from DESC, processing_version DESC
	)
	SELECT
		ap.chain_id, t.address, t.decimals,
		ap.prime_id, ap.proxy_address,
		ap.balance::text, ap.scaled_balance::text, ap.block_number, ap.block_version,
		encode(ap.tx_hash, 'hex'), ap.log_index, ap.tx_amount::text, ap.direction,
		ap.from_address, ap.to_address, ap.created_at,
		rt.receipt_token_address IS NOT NULL AS is_receipt_token,
		ut.address, ut.decimals,
		p.name,
		-- erc4626 conversion derived from price history instead of a live
		-- convertToAssets call. A vault share's on-chain USD price divided
		-- by its underlying's, both read at or before this row's own block,
		-- IS the redemption ratio at that block -- so the underlying amount
		-- is balance * share_price / underlying_price. Both sides come from
		-- onchain_token_price (our own chain-derived table), so this stays
		-- inside the "chain RPC or cached block payload" rule. Used only as a
		-- fallback when a real archive convertToAssets read can't reach this
		-- row's own block.
		(ap.balance * shp.price_usd / NULLIF(undp.price_usd, 0))::text,
		(ap.block_number - shp.block_number),
		(ap.block_number - undp.block_number)
	FROM allocation_position ap
	JOIN token t ON t.id = ap.token_id
	LEFT JOIN receipt_token rt ON rt.chain_id = ap.chain_id AND rt.receipt_token_address = t.address
	LEFT JOIN token ut ON ut.id = rt.underlying_token_id
	LEFT JOIN protocol p ON p.id = rt.protocol_id
	LEFT JOIN LATERAL (
		SELECT o.price_usd, o.block_number
		FROM onchain_token_price o
		JOIN protocol_oracle po ON po.oracle_id = o.oracle_id AND po.protocol_id = rt.protocol_id
		JOIN enabled_oracle_assets oa ON oa.oracle_id = o.oracle_id AND oa.token_id = o.token_id AND oa.enabled
		WHERE o.token_id = ap.token_id AND o.block_number <= ap.block_number
		ORDER BY o.block_number DESC, o.block_version DESC, o.processing_version DESC, o.oracle_id DESC
		LIMIT 1
	) shp ON TRUE
	LEFT JOIN LATERAL (
		SELECT o.price_usd, o.block_number
		FROM onchain_token_price o
		JOIN protocol_oracle po ON po.oracle_id = o.oracle_id AND po.protocol_id = rt.protocol_id
		JOIN enabled_oracle_assets oa ON oa.oracle_id = o.oracle_id AND oa.token_id = o.token_id AND oa.enabled
		WHERE o.token_id = rt.underlying_token_id AND o.block_number <= ap.block_number
		ORDER BY o.block_number DESC, o.block_version DESC, o.processing_version DESC, o.oracle_id DESC
		LIMIT 1
	) undp ON TRUE
	WHERE ap.direction IN ('in', 'out', 'sweep')
	  AND ap.underlying_value IS NULL
	  -- Only rows that can pass AllocationPosition.Validate() as they stand.
	  -- validateTransferParties requires both transfer parties on an in/out
	  -- row and requires them ABSENT on a sweep, so a pre-cutover sweep is
	  -- already a valid entity while a pre-cutover in/out row is not: its
	  -- from_address/to_address were never populated either (they did not
	  -- start being written until 2026-08-20, a separate and later cutover).
	  -- Those rows need the transfer-party log re-decode first; they are left
	  -- for that half of VEC-759 rather than failed on here.
	  AND (ap.direction = 'sweep'
	       OR (ap.from_address IS NOT NULL AND ap.to_address IS NOT NULL))
	  AND ap.created_at < $1
	  AND ($4::timestamptz IS NULL OR ap.created_at >= $4)
	  AND ($2 = 0 OR ap.prime_id = $2)
	  -- Skip rows a previous run already corrected. Without this the read is
	  -- not idempotent: append-only means the ORIGINAL row keeps its NULL
	  -- underlying_value forever, so a second run re-selects every row it
	  -- already fixed and stacks another processing_version on top. That also
	  -- makes the job resumable, so a large gap can be walked in -limit sized
	  -- batches instead of held in one transaction.
	  AND NOT EXISTS (
	      SELECT 1 FROM allocation_position c
	      WHERE c.chain_id       = ap.chain_id
	        AND c.token_id       = ap.token_id
	        AND c.prime_id       = ap.prime_id
	        AND c.proxy_address  = ap.proxy_address
	        AND c.block_number   = ap.block_number
	        AND c.block_version  = ap.block_version
	        AND c.tx_hash        = ap.tx_hash
	        AND c.log_index      = ap.log_index
	        AND c.direction      = ap.direction
	        AND c.processing_version > ap.processing_version
	  )
	-- block_number/log_index only break ties for a deterministic scan order;
	-- the resume cursor itself is created_at alone (see the -after doc and
	-- logCandidatesFetched's saturation warning for that cursor's limit).
	ORDER BY ap.created_at, ap.block_number, ap.log_index
	LIMIT $3`

func fetchCandidates(ctx context.Context, pool *pgxpool.Pool, cfg cliConfig) ([]candidateRow, error) {
	var afterArg any
	if !cfg.after.IsZero() {
		afterArg = cfg.after
	}
	rows, err := pool.Query(ctx, candidateQuery, cfg.before, cfg.primeID, cfg.limit, afterArg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []candidateRow
	for rows.Next() {
		c, err := scanCandidateRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// scanCandidateRow decodes one candidateQuery row and derives the fields that
// need conversion from their wire representation: addresses from raw bytes,
// balances from decimal text back to raw on-chain units, and the Aave-family
// classification from the resolved protocol name.
func scanCandidateRow(rows pgx.Rows) (candidateRow, error) {
	var (
		c                       candidateRow
		tokenAddr, proxyAddr    []byte
		fromAddr, toAddr        []byte
		underlyingAddr          []byte
		underlyingDecimals      *int32
		protocolName            *string
		balanceStr, txAmountStr string
		scaledBalanceStr        *string
		txHashHex               string
	)
	if err := rows.Scan(
		&c.chainID, &tokenAddr, &c.tokenDecimals,
		&c.primeID, &proxyAddr,
		&balanceStr, &scaledBalanceStr, &c.blockNumber, &c.blockVersion,
		&txHashHex, &c.logIndex, &txAmountStr, &c.direction,
		&fromAddr, &toAddr, &c.createdAt,
		&c.isReceiptToken,
		&underlyingAddr, &underlyingDecimals,
		&protocolName,
		&c.erc4626UnderlyingHuman, &c.sharePriceBlockLag, &c.underlyingPriceBlockLag,
	); err != nil {
		return candidateRow{}, err
	}

	c.tokenAddress = common.BytesToAddress(tokenAddr)
	c.proxyAddress = common.BytesToAddress(proxyAddr)
	c.txHash = "0x" + txHashHex
	if fromAddr != nil {
		addr := common.BytesToAddress(fromAddr)
		c.fromAddress = &addr
	}
	if toAddr != nil {
		addr := common.BytesToAddress(toAddr)
		c.toAddress = &addr
	}
	if underlyingAddr != nil {
		addr := common.BytesToAddress(underlyingAddr)
		c.underlyingAddress = &addr
	}
	c.underlyingDecimals = underlyingDecimals
	if protocolName != nil {
		c.protocolName = *protocolName
		// aTokens are always 1:1 with their underlying by construction
		// (Aave/SparkLend-family protocols); everything else with a
		// receipt-token match is erc4626-like and needs a real historical
		// convertToAssets read (see erc4626_archive.go).
		c.underlyingIsOneToOne = isAaveFamily(*protocolName)
	}

	balance, err := humanToRaw(balanceStr, c.tokenDecimals)
	if err != nil {
		return candidateRow{}, fmt.Errorf("balance for block %d: %w", c.blockNumber, err)
	}
	c.balance = balance
	c.balanceHuman = balanceStr

	if scaledBalanceStr != nil {
		scaledBalance, err := humanToRaw(*scaledBalanceStr, c.tokenDecimals)
		if err != nil {
			return candidateRow{}, fmt.Errorf("scaled_balance for block %d: %w", c.blockNumber, err)
		}
		c.scaledBalance = scaledBalance
	}

	txAmount, err := humanToRaw(txAmountStr, c.tokenDecimals)
	if err != nil {
		return candidateRow{}, fmt.Errorf("tx_amount for block %d: %w", c.blockNumber, err)
	}
	c.txAmount = txAmount

	return c, nil
}

// isAaveFamily reports whether a protocol issues aTokens, which are 1:1 with
// their underlying by construction and so need no conversion read.
//
// Aave registers one protocol row per market -- "Aave V3 Lido", "Aave V3 Base",
// "Aave V3 RWA" and so on -- so this matches the family prefix rather than an
// exact list, which would silently classify an unlisted market as erc4626 and
// skip rows that need no on-chain call at all.
func isAaveFamily(protocolName string) bool {
	return protocolName == "SparkLend" ||
		strings.HasPrefix(protocolName, "Aave V2") ||
		strings.HasPrefix(protocolName, "Aave V3")
}

// humanToRaw reverses the decimals-normalization applied at write time,
// exactly (via big.Rat, never float64) so the backfilled raw amount matches
// what the original on-chain read would have produced.
func humanToRaw(human string, decimals int32) (*big.Int, error) {
	r, ok := new(big.Rat).SetString(human)
	if !ok {
		return nil, fmt.Errorf("invalid decimal %q", human)
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	r.Mul(r, new(big.Rat).SetInt(scale))
	if !r.IsInt() {
		return nil, fmt.Errorf("%q at %d decimals is not an exact integer raw amount", human, decimals)
	}
	return r.Num(), nil
}

// Package main prototypes backfilling allocation_position.underlying_value for rows
// written before the 2026-07-06 deploy that started populating it on ingest.
//
// Scope: only the deterministic subset that needs no on-chain call — direct erc20
// holdings (underlying_value duplicates balance, underlying_token_id self-referencing
// per the column's documented invariant) and aToken holdings (1:1 by construction).
// erc4626-like receipt tokens are skipped and logged: their conversion ratio genuinely
// varies over time, so a correct backfill needs a historical convertToAssets call at
// each row's pinned block_number, which needs real archive RPC access this prototype
// does not have.
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
	dbURL   string
	before  time.Time
	primeID int64
	limit   int
	dryRun  bool
}

func parseFlags(args []string) (cliConfig, error) {
	fs := flag.NewFlagSet("allocation-underlying-value-backfill", flag.ContinueOnError)
	dbURL := fs.String("db", "", "PostgreSQL connection URL (required)")
	before := fs.String("before", "2026-07-06T14:00:00Z", "Backfill rows created strictly before this RFC3339 instant")
	primeID := fs.Int64("prime-id", 0, "Restrict to one prime.id (0 = all primes)")
	limit := fs.Int("limit", 100, "Max candidate rows to process this run")
	dryRun := fs.Bool("dry-run", true, "Log what would be written without saving")
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
	return cliConfig{dbURL: *dbURL, before: beforeAt, primeID: *primeID, limit: *limit, dryRun: *dryRun}, nil
}

// candidateRow is a row missing underlying_value, plus the classification
// inputs needed to decide whether it's in the deterministic (no-RPC) subset.
type candidateRow struct {
	chainID              int64
	tokenAddress         common.Address
	tokenDecimals        int32
	primeID              int64
	proxyAddress         common.Address
	balance              *big.Int // raw units, descaled by tokenDecimals at read time
	balanceHuman         string
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
	underlyingAddress    *common.Address // NULL for a direct holding (self-referencing case)
	underlyingIsOneToOne bool            // true for aTokens; false for erc4626-like (skip)
	protocolName         string
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

	registry, err := buildregistry.NewWithIdentity(ctx, pool, buildregistry.Identity{
		Service:   "allocation-underlying-value-backfill",
		GitHash:   "prototype",
		BuildTime: time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("register build: %w", err)
	}

	var runID buildregistry.RunID
	if _, err := registry.OpenRun(ctx, time.Now().UTC(), func(_ pgx.Tx, id buildregistry.RunID) error {
		runID = id
		return nil
	}); err != nil {
		return fmt.Errorf("open writer run: %w", err)
	}

	txm, err := postgres.NewTxManager(pool, nil)
	if err != nil {
		return fmt.Errorf("tx manager: %w", err)
	}
	tokenRepo, err := postgres.NewTokenRepository(pool, nil, 100, runID)
	if err != nil {
		return fmt.Errorf("token repository: %w", err)
	}
	allocRepo := postgres.NewAllocationRepository(pool, txm, tokenRepo, nil, registry.BuildID(), runID)

	candidates, err := fetchCandidates(ctx, pool, cfg)
	if err != nil {
		return fmt.Errorf("fetch candidates: %w", err)
	}
	slog.Info("candidates fetched", "count", len(candidates))

	positions := make([]*entity.AllocationPosition, 0, len(candidates))
	var skippedERC4626 int
	for _, c := range candidates {
		if !c.isReceiptToken {
			// Direct holding: underlying_value duplicates balance, denominated
			// in the token itself (the invariant "underlying_token_id IS NULL
			// iff underlying_value IS NULL" forces a self-reference here, not NULL).
			positions = append(positions, toEntity(c, c.tokenAddress, c.tokenDecimals, c.balance))
			continue
		}
		if c.underlyingIsOneToOne {
			// aToken: 1:1 by construction, so the raw underlying amount equals
			// the raw balance; only the denominating asset differs.
			positions = append(positions, toEntity(c, *c.underlyingAddress, c.tokenDecimals, c.balance))
			continue
		}
		skippedERC4626++
	}

	slog.Info("classified",
		"deterministic_no_rpc_needed", len(positions),
		"skipped_erc4626_needs_historical_rpc", skippedERC4626,
	)

	if cfg.dryRun {
		slog.Info("dry run: not writing. Pass -dry-run=false to persist.")
		for i, p := range positions {
			if i >= 10 {
				slog.Info("... additional rows omitted from dry-run log", "remaining", len(positions)-10)
				break
			}
			slog.Info("would backfill",
				"chain_id", p.ChainID, "token", p.TokenAddress.Hex(),
				"block_number", p.BlockNumber, "underlying_value", p.Underlying.Value.String(),
				"underlying_asset", p.Underlying.AssetAddress.Hex())
		}
		return nil
	}

	if err := txm.WithTransaction(ctx, func(tx pgx.Tx) error {
		return allocRepo.SavePositions(ctx, tx, positions)
	}); err != nil {
		return fmt.Errorf("save positions: %w", err)
	}

	slog.Info("backfilled", "rows_written", len(positions))
	return nil
}

// toEntity rebuilds the original event as a new AllocationPosition carrying the
// derived underlying valuation. Every other field is copied from the row that
// already exists in history — this is a correction, not a new event.
func toEntity(c candidateRow, underlyingAsset common.Address, underlyingDecimals int32, underlyingRaw *big.Int) *entity.AllocationPosition {
	return &entity.AllocationPosition{
		ChainID:        c.chainID,
		TokenAddress:   c.tokenAddress,
		PrimeID:        c.primeID,
		ProxyAddress:   c.proxyAddress,
		Balance:        c.balance,
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

// fetchCandidates finds flow rows missing underlying_value and classifies each
// as a direct holding, an aToken (1:1), or an erc4626-like receipt token that
// needs a real historical on-chain call this prototype cannot make.
//
// Classification mirrors the categorization used to size this backfill: a
// receipt_token row with no match is a direct holding; a match under an
// Aave-family protocol is an aToken; anything else with a match is erc4626-like.
func fetchCandidates(ctx context.Context, pool *pgxpool.Pool, cfg cliConfig) ([]candidateRow, error) {
	query := `
		SELECT
			ap.chain_id, t.address, t.decimals,
			ap.prime_id, ap.proxy_address,
			ap.balance::text, ap.block_number, ap.block_version,
			encode(ap.tx_hash, 'hex'), ap.log_index, ap.tx_amount::text, ap.direction,
			ap.from_address, ap.to_address, ap.created_at,
			rt.receipt_token_address IS NOT NULL AS is_receipt_token,
			ut.address, ut.decimals,
			p.name
		FROM allocation_position ap
		JOIN token t ON t.id = ap.token_id
		LEFT JOIN receipt_token rt ON rt.chain_id = ap.chain_id AND rt.receipt_token_address = t.address
		LEFT JOIN token ut ON ut.id = rt.underlying_token_id
		LEFT JOIN protocol p ON p.id = rt.protocol_id
		WHERE ap.direction IN ('in', 'out')
		  AND ap.underlying_value IS NULL
		  AND ap.created_at < $1
		  AND ($2 = 0 OR ap.prime_id = $2)
		ORDER BY ap.created_at
		LIMIT $3`

	rows, err := pool.Query(ctx, query, cfg.before, cfg.primeID, cfg.limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []candidateRow
	for rows.Next() {
		var (
			c                       candidateRow
			tokenAddr, proxyAddr    []byte
			fromAddr, toAddr        []byte
			underlyingAddr          []byte
			underlyingDecimals      *int32
			protocolName            *string
			balanceStr, txAmountStr string
			txHashHex               string
		)
		if err := rows.Scan(
			&c.chainID, &tokenAddr, &c.tokenDecimals,
			&c.primeID, &proxyAddr,
			&balanceStr, &c.blockNumber, &c.blockVersion,
			&txHashHex, &c.logIndex, &txAmountStr, &c.direction,
			&fromAddr, &toAddr, &c.createdAt,
			&c.isReceiptToken,
			&underlyingAddr, &underlyingDecimals,
			&protocolName,
		); err != nil {
			return nil, err
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
		if protocolName != nil {
			c.protocolName = *protocolName
			// aTokens are always 1:1 with their underlying by construction
			// (Aave/SparkLend-family protocols); everything else with a
			// receipt-token match is erc4626-like and needs a real
			// historical convertToAssets call this prototype skips.
			c.underlyingIsOneToOne = isAaveFamily(*protocolName)
		}

		balance, err := humanToRaw(balanceStr, c.tokenDecimals)
		if err != nil {
			return nil, fmt.Errorf("balance for block %d: %w", c.blockNumber, err)
		}
		c.balance = balance
		c.balanceHuman = balanceStr

		txAmount, err := humanToRaw(txAmountStr, c.tokenDecimals)
		if err != nil {
			return nil, fmt.Errorf("tx_amount for block %d: %w", c.blockNumber, err)
		}
		c.txAmount = txAmount

		out = append(out, c)
	}
	return out, rows.Err()
}

func isAaveFamily(protocolName string) bool {
	switch protocolName {
	case "Aave V2", "Aave V3", "SparkLend":
		return true
	default:
		return false
	}
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

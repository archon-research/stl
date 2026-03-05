// Package vault_debt provides a polling service that reads each prime agent's
// vault debt from the Sky/MakerDAO Vat contract and writes snapshots to Postgres.
//
// Flow:
//  1. At Start, load prime agents from the database via PrimeDebtRepository.GetPrimes.
//  2. Fetch the latest block number via BlockQuerier.
//  3. Batch-resolve all vault ilks in a single multicall via VatCaller.ResolveIlks.
//  4. On every tick, fetch latest block, batch-read all debts via VatCaller.ReadDebts.
//  5. Compute exact debt: art * rate / 1e27 (wad-scaled big.Int).
//  6. Write all snapshots (including block number) via PrimeDebtRepository.SaveDebtSnapshots.
package vault_debt

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultPollInterval = 15 * time.Minute

// resolvedPrime is a Prime with its ilk resolved at startup.
type resolvedPrime struct {
	entity.Prime
	ilk [32]byte
}

// Config holds configuration for VaultDebtService.
type Config struct {
	// PollInterval controls how often debt is read from the chain.
	// Must be positive. Defaults to 15 minutes.
	PollInterval time.Duration

	// Logger is the structured logger.
	Logger *slog.Logger
}

func configDefaults() Config {
	return Config{
		PollInterval: defaultPollInterval,
		Logger:       slog.Default(),
	}
}

// VaultDebtService polls each prime agent's vault debt on a fixed interval
// and writes append-only snapshots to Postgres.
//
// Start blocks until the context is cancelled. lifecycle.Run handles signal
// trapping and context cancellation.
type VaultDebtService struct {
	config       Config
	caller       outbound.VatCaller
	repo         outbound.PrimeDebtRepository
	blockQuerier outbound.BlockQuerier
	logger       *slog.Logger
}

// NewVaultDebtService creates a new VaultDebtService.
func NewVaultDebtService(
	config Config,
	caller outbound.VatCaller,
	repo outbound.PrimeDebtRepository,
	blockQuerier outbound.BlockQuerier,
) (*VaultDebtService, error) {
	if caller == nil {
		return nil, fmt.Errorf("vat caller is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("prime debt repository is required")
	}
	if blockQuerier == nil {
		return nil, fmt.Errorf("block querier is required")
	}

	defaults := configDefaults()
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.PollInterval < 0 {
		return nil, fmt.Errorf("poll interval must be positive, got %s", config.PollInterval)
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &VaultDebtService{
		config:       config,
		caller:       caller,
		repo:         repo,
		blockQuerier: blockQuerier,
		logger:       config.Logger.With("component", "vault-debt-service"),
	}, nil
}

// Start loads primes, resolves ilks, then runs the blocking poll loop.
// It returns when ctx is cancelled (clean shutdown) or if initial setup fails.
func (s *VaultDebtService) Start(ctx context.Context) error {
	primes, err := s.repo.GetPrimes(ctx)
	if err != nil {
		return fmt.Errorf("load primes: %w", err)
	}
	if len(primes) == 0 {
		return fmt.Errorf("no primes found in database")
	}

	blockNum, err := s.latestBlock(ctx)
	if err != nil {
		return fmt.Errorf("get latest block for ilk resolution: %w", err)
	}

	resolved, err := s.resolveIlks(ctx, primes, blockNum)
	if err != nil {
		return fmt.Errorf("resolve vault ilks: %w", err)
	}

	s.logger.Info("vault debt service started",
		"primes", len(resolved),
		"pollInterval", s.config.PollInterval,
		"block", blockNum,
	)

	// Sync immediately on start before waiting for the first tick.
	if err := s.syncAll(ctx, resolved); err != nil {
		s.logger.Warn("initial debt sync failed", "error", err)
	}

	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.logger.Info("vault debt service stopped")
			return nil
		case <-ticker.C:
			if err := s.syncAll(ctx, resolved); err != nil {
				s.logger.Warn("debt sync failed", "error", err)
			}
		}
	}
}

// Stop is a no-op — shutdown is driven by context cancellation via lifecycle.Run.
func (s *VaultDebtService) Stop() error {
	return nil
}

// latestBlock fetches the latest block number from the node.
func (s *VaultDebtService) latestBlock(ctx context.Context) (int64, error) {
	bn, err := s.blockQuerier.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("block number: %w", err)
	}
	return int64(bn), nil
}

// resolveIlks batch-reads vault.ilk() for all primes in a single multicall.
func (s *VaultDebtService) resolveIlks(ctx context.Context, primes []entity.Prime, blockNum int64) ([]resolvedPrime, error) {
	vaults := make([]common.Address, len(primes))
	for i, p := range primes {
		vaults[i] = p.VaultAddress
	}

	ilkMap, err := s.caller.ResolveIlks(ctx, vaults, big.NewInt(blockNum))
	if err != nil {
		return nil, err
	}

	resolved := make([]resolvedPrime, 0, len(primes))
	for _, prime := range primes {
		ilk, ok := ilkMap[prime.VaultAddress]
		if !ok {
			return nil, fmt.Errorf("ilk not resolved for %s (%s)", prime.Name, prime.VaultAddress)
		}

		s.logger.Info("resolved vault ilk",
			"prime", prime.Name,
			"vault", prime.VaultAddress,
			"ilk", ilkToString(ilk),
		)

		resolved = append(resolved, resolvedPrime{
			Prime: prime,
			ilk:   ilk,
		})
	}

	return resolved, nil
}

// syncAll fetches the latest block, batch-reads on-chain debt for all primes,
// and writes snapshots to Postgres.
func (s *VaultDebtService) syncAll(ctx context.Context, primes []resolvedPrime) error {
	start := time.Now()
	syncedAt := start.UTC()

	blockNum, err := s.latestBlock(ctx)
	if err != nil {
		return fmt.Errorf("get latest block: %w", err)
	}

	// Build queries.
	queries := make([]outbound.DebtQuery, len(primes))
	for i, p := range primes {
		queries[i] = outbound.DebtQuery{
			Ilk:          p.ilk,
			VaultAddress: p.VaultAddress,
		}
	}

	// Single multicall for all rate + art reads.
	results, err := s.caller.ReadDebts(ctx, queries, big.NewInt(blockNum))
	if err != nil {
		return fmt.Errorf("read debts: %w", err)
	}

	// Build snapshots from results.
	snapshots := make([]*entity.PrimeDebt, 0, len(primes))
	for i, prime := range primes {
		if i >= len(results) {
			s.logger.Warn("missing result for prime", "prime", prime.Name)
			continue
		}

		r := results[i]
		if r.Err != nil {
			s.logger.Warn("failed to read vault debt",
				"prime", prime.Name,
				"vault", prime.VaultAddress,
				"error", r.Err,
			)
			continue
		}

		debtWad := blockchain.ComputeDebtWad(r.Art, r.Rate)

		s.logger.Debug("read vault debt",
			"prime", prime.Name,
			"ilk", ilkToString(prime.ilk),
			"debtWad", debtWad,
			"block", blockNum,
		)

		snapshots = append(snapshots, &entity.PrimeDebt{
			PrimeID:      prime.ID,
			PrimeName:    prime.Name,
			VaultAddress: prime.VaultAddress,
			IlkName:      ilkToString(prime.ilk),
			DebtWad:      debtWad,
			BlockNumber:  blockNum,
			SyncedAt:     syncedAt,
		})
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("all vault reads failed, skipping db write")
	}

	if err := s.repo.SaveDebtSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("save debt snapshots: %w", err)
	}

	s.logger.Info("debt sync complete",
		"primes", len(snapshots),
		"block", blockNum,
		"duration", time.Since(start),
	)

	return nil
}

// ilkToString converts a bytes32 ilk to a human-readable string by
// trimming trailing null bytes (e.g. "ETH-A\x00\x00..." -> "ETH-A").
func ilkToString(ilk [32]byte) string {
	end := len(ilk)
	for end > 0 && ilk[end-1] == 0 {
		end--
	}
	return string(ilk[:end])
}

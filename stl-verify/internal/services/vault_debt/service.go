// Package vault_debt provides a polling service that reads each prime agent's
// vault debt from the Sky/MakerDAO Vat contract and writes snapshots to Postgres.
//
// Flow:
//  1. At Start, load prime agents from the database via PrimeDebtRepository.GetPrimes.
//  2. For each prime, call vault.ilk() once to resolve and cache the ilk identifier.
//  3. On every tick, call vat.ilks(ilk) for rate and vat.urns(ilk, vault) for art.
//  4. Compute exact debt: art * rate / 1e27 using big.Rat (full 18-decimal wad precision).
//  5. Write all snapshots atomically via PrimeDebtRepository.SaveDebtSnapshots.
package vault_debt

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/ethereum"
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
	// Defaults to 15 minutes.
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
type VaultDebtService struct {
	config Config
	caller *ethereum.VatCaller
	repo   outbound.PrimeDebtRepository
	primes []resolvedPrime
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	logger *slog.Logger
}

// NewVaultDebtService creates a new VaultDebtService.
func NewVaultDebtService(
	config Config,
	caller *ethereum.VatCaller,
	repo outbound.PrimeDebtRepository,
) (*VaultDebtService, error) {
	if caller == nil {
		return nil, fmt.Errorf("vat caller is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("prime debt repository is required")
	}

	defaults := configDefaults()
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &VaultDebtService{
		config: config,
		caller: caller,
		repo:   repo,
		logger: config.Logger.With("component", "vault-debt-service"),
	}, nil
}

// Start loads primes from the database, resolves their ilks, then starts the polling loop.
func (s *VaultDebtService) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	primes, err := s.repo.GetPrimes(ctx)
	if err != nil {
		return fmt.Errorf("load primes: %w", err)
	}
	if len(primes) == 0 {
		return fmt.Errorf("no primes found in database")
	}

	if err := s.resolveIlks(ctx, primes); err != nil {
		return fmt.Errorf("resolve vault ilks: %w", err)
	}

	s.wg.Add(1)
	go s.run()

	s.logger.Info("vault debt service started",
		"primes", len(s.primes),
		"pollInterval", s.config.PollInterval,
	)
	return nil
}

// Stop cancels the polling loop and waits for it to exit cleanly.
func (s *VaultDebtService) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("vault debt service stopped")
	return nil
}

// resolveIlks calls vault.ilk() on each prime's vault and caches the result.
// Called once at Start so ilk reads do not add latency to every poll tick.
func (s *VaultDebtService) resolveIlks(ctx context.Context, primes []entity.Prime) error {
	resolved := make([]resolvedPrime, 0, len(primes))

	for _, prime := range primes {
		addr := common.HexToAddress(prime.VaultAddress)
		ilk, err := s.caller.GetIlk(ctx, addr)
		if err != nil {
			return fmt.Errorf("get ilk for %s (%s): %w", prime.Name, prime.VaultAddress, err)
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

	s.primes = resolved
	return nil
}

// run is the main polling loop.
func (s *VaultDebtService) run() {
	defer s.wg.Done()

	// Sync immediately on start before waiting for the first tick.
	if err := s.syncAll(); err != nil {
		s.logger.Warn("initial debt sync failed", "error", err)
	}

	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			if err := s.syncAll(); err != nil {
				s.logger.Warn("debt sync failed", "error", err)
			}
		}
	}
}

// syncAll reads on-chain debt for every prime and writes snapshots to Postgres.
func (s *VaultDebtService) syncAll() error {
	start := time.Now()
	syncedAt := start.UTC()

	snapshots := make([]*entity.PrimeDebt, 0, len(s.primes))

	for _, prime := range s.primes {
		snapshot, err := s.readDebt(s.ctx, prime, syncedAt)
		if err != nil {
			// A single failing vault should not block the others.
			s.logger.Warn("failed to read vault debt",
				"prime", prime.Name,
				"vault", prime.VaultAddress,
				"error", err,
			)
			continue
		}

		s.logger.Debug("read vault debt",
			"prime", snapshot.PrimeName,
			"ilk", snapshot.IlkName,
			"debtWad", snapshot.DebtWad,
		)

		snapshots = append(snapshots, snapshot)
	}

	if len(snapshots) == 0 {
		return fmt.Errorf("all vault reads failed, skipping db write")
	}

	if err := s.repo.SaveDebtSnapshots(s.ctx, snapshots); err != nil {
		return fmt.Errorf("save debt snapshots: %w", err)
	}

	s.logger.Info("debt sync complete",
		"primes", len(snapshots),
		"duration", time.Since(start),
	)

	return nil
}

// readDebt fetches rate and art from the Vat and computes exact debt for one prime.
func (s *VaultDebtService) readDebt(ctx context.Context, prime resolvedPrime, syncedAt time.Time) (*entity.PrimeDebt, error) {
	addr := common.HexToAddress(prime.VaultAddress)

	rate, err := s.caller.GetRate(ctx, prime.ilk)
	if err != nil {
		return nil, fmt.Errorf("get rate: %w", err)
	}

	art, err := s.caller.GetNormalizedDebt(ctx, prime.ilk, addr)
	if err != nil {
		return nil, fmt.Errorf("get normalized debt: %w", err)
	}

	return &entity.PrimeDebt{
		PrimeID:      prime.ID,
		PrimeName:    prime.Name,
		VaultAddress: prime.VaultAddress,
		IlkName:      ilkToString(prime.ilk),
		DebtWad:      ethereum.ComputeDebtWad(art, rate),
		SyncedAt:     syncedAt,
	}, nil
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

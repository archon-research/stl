// Package prime_debt reads each prime agent's vault debt from the Sky/MakerDAO
// Vat contract and writes append-only snapshots to Postgres.
//
// Flow:
//  1. At Start, load prime agents from the database via PrimeDebtRepository.GetPrimes.
//  2. Resolve all vault ilks in a single multicall via VatCaller.ResolveIlks.
//  3. Consume block events from SQS via sqsutil.RunLoop.
//  4. Every SweepEveryNBlocks, batch-read all debts via VatCaller.ReadDebts.
//  5. Compute exact debt: art * rate / 1e27 (wad-scaled big.Int).
//  6. Write all snapshots (including block number) via PrimeDebtRepository.SaveDebtSnapshots.
package prime_debt

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/rpcerr"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const defaultSweepEveryNBlocks = 75

// resolvedPrime is a Prime with its ilk resolved at startup.
type resolvedPrime struct {
	entity.Prime
	ilk [32]byte
}

// Config holds configuration for VaultDebtService.
type Config struct {
	// SweepEveryNBlocks controls how often debt is read from the chain.
	// Defaults to 75.
	SweepEveryNBlocks int

	// ChainID is the expected chain ID for incoming block events.
	ChainID int64

	// MaxMessages is the max number of SQS messages per poll.
	MaxMessages int

	// PollInterval controls how often SQS is polled for new messages.
	PollInterval time.Duration

	// Logger is the structured logger.
	Logger *slog.Logger
}

func configDefaults() Config {
	return Config{
		SweepEveryNBlocks: defaultSweepEveryNBlocks,
		MaxMessages:       10,
		PollInterval:      100 * time.Millisecond,
		Logger:            slog.Default(),
	}
}

// VaultDebtService consumes block events from SQS and periodically reads
// each prime agent's vault debt from the Vat contract.
type VaultDebtService struct {
	config           Config
	caller           outbound.VatCaller
	repo             outbound.PrimeDebtRepository
	sqsConsumer      outbound.SQSConsumer
	blockQuerier     entity.BlockQuerier
	logger           *slog.Logger
	resolved         []resolvedPrime
	blocksSinceSweep int
	wg               sync.WaitGroup
	ctx              context.Context
	cancel           context.CancelFunc
}

// NewVaultDebtService creates a new VaultDebtService.
func NewVaultDebtService(
	config Config,
	caller outbound.VatCaller,
	repo outbound.PrimeDebtRepository,
	sqsConsumer outbound.SQSConsumer,
	blockQuerier entity.BlockQuerier,
) (*VaultDebtService, error) {
	if caller == nil {
		return nil, fmt.Errorf("vat caller is required")
	}
	if repo == nil {
		return nil, fmt.Errorf("prime debt repository is required")
	}
	if sqsConsumer == nil {
		return nil, fmt.Errorf("sqs consumer is required")
	}
	if blockQuerier == nil {
		return nil, fmt.Errorf("block querier is required")
	}

	defaults := configDefaults()
	if config.SweepEveryNBlocks == 0 {
		config.SweepEveryNBlocks = defaults.SweepEveryNBlocks
	}
	if config.MaxMessages == 0 {
		config.MaxMessages = defaults.MaxMessages
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.ChainID == 0 {
		return nil, fmt.Errorf("chain ID is required")
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}

	return &VaultDebtService{
		config:       config,
		caller:       caller,
		repo:         repo,
		sqsConsumer:  sqsConsumer,
		blockQuerier: blockQuerier,
		logger:       config.Logger.With("component", "vault-debt-service"),
	}, nil
}

// Start loads primes, resolves ilks, then starts the SQS processing loop.
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

	s.resolved = resolved
	s.blocksSinceSweep = s.config.SweepEveryNBlocks - 1 // first block triggers immediate read
	s.ctx, s.cancel = context.WithCancel(ctx)

	s.wg.Go(func() {
		sqsutil.RunLoop(s.ctx, sqsutil.Config{
			Consumer:     s.sqsConsumer,
			MaxMessages:  s.config.MaxMessages,
			PollInterval: s.config.PollInterval,
			Logger:       s.logger,
			ChainID:      s.config.ChainID,
		}, s.processBlock)
	})

	s.logger.Info("vault debt service started",
		"primes", len(resolved),
		"sweepEveryNBlocks", s.config.SweepEveryNBlocks,
		"chainID", s.config.ChainID,
		"block", blockNum,
	)

	return nil
}

// Stop cancels the SQS processing loop and waits for the goroutine to exit.
func (s *VaultDebtService) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("vault debt service stopped")
	return nil
}

// processBlock is called for every block event from SQS.
// It only reads debt every SweepEveryNBlocks.
func (s *VaultDebtService) processBlock(
	ctx context.Context,
	event outbound.BlockEvent,
) error {
	s.blocksSinceSweep++
	if s.blocksSinceSweep < s.config.SweepEveryNBlocks {
		return nil
	}

	if err := s.syncAll(ctx, event.BlockNumber, event.Version); err != nil {
		return err
	}

	s.blocksSinceSweep = 0
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
		return nil, fmt.Errorf("resolve ilks at block %d: %w", blockNum, err)
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

// syncAll batch-reads on-chain debt for all primes at the given block
// and writes snapshots to Postgres.
func (s *VaultDebtService) syncAll(ctx context.Context, blockNumber int64, blockVersion int) error {
	start := time.Now()
	syncedAt := start.UTC()

	// Build queries.
	queries := make([]entity.DebtQuery, len(s.resolved))
	for i, p := range s.resolved {
		queries[i] = entity.DebtQuery{
			Ilk:          p.ilk,
			VaultAddress: p.VaultAddress,
		}
	}

	// Single multicall for all rate + art reads.
	results, err := s.caller.ReadDebts(ctx, queries, big.NewInt(blockNumber))
	if err != nil {
		return fmt.Errorf("read debts at block %d: %w", blockNumber, err)
	}

	// Build snapshots from results.
	snapshots := make([]*entity.PrimeDebt, 0, len(s.resolved))
	for i, prime := range s.resolved {
		if i >= len(results) {
			s.logger.Warn("missing result for prime", "prime", prime.Name)
			continue
		}

		r := results[i]
		if r.Err != nil {
			// VEC-188: classify the per-prime error. Reverts are the
			// contract's "no data" signal and legitimately skip. Transport
			// errors are transient and must surface so the SQS message NACKs.
			if !rpcerr.IsEVMRevert(r.Err) {
				return fmt.Errorf("prime %s: transport error reading debt: %w", prime.Name, r.Err)
			}
			s.logger.Warn("vault debt call reverted (contract-level); skipping",
				"prime", prime.Name,
				"vault", prime.VaultAddress,
				"error", r.Err,
			)
			continue
		}

		debtWad := entity.ComputeDebtWad(r.Art, r.Rate)

		s.logger.Debug("read vault debt",
			"prime", prime.Name,
			"ilk", ilkToString(prime.ilk),
			"debtWad", debtWad,
			"block", blockNumber,
		)

		snapshots = append(snapshots, &entity.PrimeDebt{
			PrimeID:      prime.ID,
			IlkName:      ilkToString(prime.ilk),
			DebtWad:      debtWad,
			BlockNumber:  blockNumber,
			BlockVersion: blockVersion,
			SyncedAt:     syncedAt,
		})
	}

	// Asymmetry vs the per-prime classification above: a single prime
	// reverting is treated as legitimate "no debt this block" (skip +
	// continue). But if EVERY prime reverts we treat it as a hard error
	// rather than ACK silently. With only 2 primes today, simultaneous
	// legitimate reverts are implausible — an all-empty result is more
	// likely a misconfiguration (wrong vault addresses) than a real
	// "no data" answer. Prefer the loud DLQ over a silent ACK. Revisit
	// this when a 3rd+ prime is added and the all-revert case becomes
	// plausible. See VEC-188 PR review discussion.
	if len(snapshots) == 0 {
		return fmt.Errorf("all vault reads failed, skipping db write")
	}

	for i, snap := range snapshots {
		if err := snap.Validate(); err != nil {
			return fmt.Errorf("debt snapshot %d: %w", i, err)
		}
	}

	if err := s.repo.SaveDebtSnapshots(ctx, snapshots); err != nil {
		return fmt.Errorf("save debt snapshots: %w", err)
	}

	s.logger.Info("debt sync complete",
		"primes", len(snapshots),
		"block", blockNumber,
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

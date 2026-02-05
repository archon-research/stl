// Package data_validator provides validation of block chain data stored by the watcher.
// It verifies reorg events against Etherscan's canonical chain and validates chain integrity.
package data_validator

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ServiceConfig holds configuration for the data validator service.
type ServiceConfig struct {
	// FromBlock is the start of the block range to validate (0 = min block in DB).
	FromBlock int64

	// ToBlock is the end of the block range to validate (0 = max block in DB).
	ToBlock int64

	// SpotCheckCount is the number of random blocks to verify against Etherscan.
	SpotCheckCount int

	// ValidateReorgs enables reorg event validation against Etherscan.
	ValidateReorgs bool

	// ValidateChainIntegrity enables parent-hash chain validation.
	ValidateChainIntegrity bool

	// Logger is the structured logger.
	Logger *slog.Logger
}

// DefaultConfig returns the default service configuration.
func DefaultConfig() ServiceConfig {
	return ServiceConfig{
		FromBlock:              0,
		ToBlock:                0,
		SpotCheckCount:         10,
		ValidateReorgs:         true,
		ValidateChainIntegrity: true,
		Logger:                 slog.Default(),
	}
}

// Service performs data validation against an authoritative source.
type Service struct {
	config         ServiceConfig
	blockStateRepo outbound.BlockStateRepository
	blockVerifier  outbound.BlockVerifier
	logger         *slog.Logger
}

// NewService creates a new data validator service.
func NewService(
	config ServiceConfig,
	blockStateRepo outbound.BlockStateRepository,
	blockVerifier outbound.BlockVerifier,
) (*Service, error) {
	if blockStateRepo == nil {
		return nil, fmt.Errorf("blockStateRepo is required")
	}
	if blockVerifier == nil {
		return nil, fmt.Errorf("blockVerifier is required")
	}

	if config.Logger == nil {
		config.Logger = slog.Default()
	}

	return &Service{
		config:         config,
		blockStateRepo: blockStateRepo,
		blockVerifier:  blockVerifier,
		logger:         config.Logger.With("component", "data-validator"),
	}, nil
}

// Validate runs all configured validations and returns a report.
func (s *Service) Validate(ctx context.Context) (*Report, error) {
	fromBlock, toBlock, err := s.resolveBlockRange(ctx)
	if err != nil {
		return nil, fmt.Errorf("resolving block range: %w", err)
	}

	s.logger.Info("starting validation",
		"from_block", fromBlock,
		"to_block", toBlock,
		"verifier", s.blockVerifier.Name(),
	)

	report := NewReport(fromBlock, toBlock)

	// Run chain integrity check
	if s.config.ValidateChainIntegrity {
		result := s.validateChainIntegrity(ctx, fromBlock, toBlock)
		report.AddCheck(result)
	}

	// Run reorg validation
	if s.config.ValidateReorgs {
		results, err := s.validateReorgs(ctx, fromBlock, toBlock)
		if err != nil {
			report.AddCheck(CheckResult{
				Name:    "Reorg Validation",
				Status:  StatusError,
				Message: err.Error(),
			})
		} else {
			for _, result := range results {
				report.AddCheck(result)
			}
		}
	}

	// Run spot checks
	if s.config.SpotCheckCount > 0 {
		results := s.runSpotChecks(ctx, fromBlock, toBlock)
		for _, result := range results {
			report.AddCheck(result)
		}
	}

	report.Finalize()

	s.logger.Info("validation complete",
		"passed", report.Passed,
		"failed", report.Failed,
		"errors", report.Errors,
		"duration", report.Duration,
	)

	return report, nil
}

// resolveBlockRange determines the actual block range to validate.
func (s *Service) resolveBlockRange(ctx context.Context) (int64, int64, error) {
	fromBlock := s.config.FromBlock
	toBlock := s.config.ToBlock

	if fromBlock == 0 {
		minBlock, err := s.blockStateRepo.GetMinBlockNumber(ctx)
		if err != nil {
			return 0, 0, fmt.Errorf("getting min block: %w", err)
		}
		fromBlock = minBlock
	}

	if toBlock == 0 {
		maxBlock, err := s.blockStateRepo.GetMaxBlockNumber(ctx)
		if err != nil {
			return 0, 0, fmt.Errorf("getting max block: %w", err)
		}
		toBlock = maxBlock
	}

	if fromBlock > toBlock {
		return 0, 0, fmt.Errorf("from_block (%d) > to_block (%d)", fromBlock, toBlock)
	}

	return fromBlock, toBlock, nil
}

// validateChainIntegrity verifies parent-hash linkage using the repository method.
func (s *Service) validateChainIntegrity(ctx context.Context, fromBlock, toBlock int64) CheckResult {
	start := time.Now()
	s.logger.Info("validating chain integrity", "from", fromBlock, "to", toBlock)

	err := s.blockStateRepo.VerifyChainIntegrity(ctx, fromBlock, toBlock)
	duration := time.Since(start)

	if err != nil {
		return CheckResult{
			Name:     "Chain Integrity",
			Status:   StatusFailed,
			Message:  err.Error(),
			Duration: duration,
		}
	}

	return CheckResult{
		Name:     "Chain Integrity",
		Status:   StatusPassed,
		Message:  "Parent-hash chain valid",
		Duration: duration,
	}
}

// validateReorgs validates each reorg event against Etherscan.
func (s *Service) validateReorgs(ctx context.Context, fromBlock, toBlock int64) ([]CheckResult, error) {
	s.logger.Info("fetching reorg events", "from", fromBlock, "to", toBlock)

	events, err := s.blockStateRepo.GetReorgEventsByBlockRange(ctx, fromBlock, toBlock)
	if err != nil {
		return nil, fmt.Errorf("fetching reorg events: %w", err)
	}

	if len(events) == 0 {
		s.logger.Info("no reorg events in range")
		return []CheckResult{{
			Name:    "Reorg Validation",
			Status:  StatusPassed,
			Message: "No reorg events in range",
		}}, nil
	}

	s.logger.Info("validating reorg events", "count", len(events))

	results := make([]CheckResult, 0, len(events))
	for _, event := range events {
		result := s.validateSingleReorg(ctx, event)
		results = append(results, result)
	}

	return results, nil
}

// validateSingleReorg validates a single reorg event against Etherscan.
func (s *Service) validateSingleReorg(ctx context.Context, event outbound.ReorgEvent) CheckResult {
	start := time.Now()
	name := fmt.Sprintf("Reorg %d at block %d", event.ID, event.BlockNumber)

	s.logger.Debug("validating reorg event",
		"id", event.ID,
		"block", event.BlockNumber,
		"old_hash", event.OldHash,
		"new_hash", event.NewHash,
	)

	// Fetch the canonical block from Etherscan
	canonicalBlock, err := s.blockVerifier.GetBlockByNumber(ctx, event.BlockNumber)
	duration := time.Since(start)

	if err != nil {
		return CheckResult{
			Name:     name,
			Status:   StatusError,
			Message:  fmt.Sprintf("Failed to fetch block: %v", err),
			Duration: duration,
			Details: map[string]any{
				"reorg_id":     event.ID,
				"block_number": event.BlockNumber,
			},
		}
	}

	if canonicalBlock == nil {
		return CheckResult{
			Name:     name,
			Status:   StatusError,
			Message:  "Block not found on Etherscan",
			Duration: duration,
			Details: map[string]any{
				"reorg_id":     event.ID,
				"block_number": event.BlockNumber,
			},
		}
	}

	// The new_hash from our reorg event should match Etherscan's canonical hash
	if !hashesMatch(event.NewHash, canonicalBlock.Hash) {
		return CheckResult{
			Name:   name,
			Status: StatusFailed,
			Message: fmt.Sprintf("Hash mismatch\n"+
				"Expected: %s\n"+
				"Actual:   %s", event.NewHash, canonicalBlock.Hash),
			Duration: duration,
			Details: map[string]any{
				"reorg_id":       event.ID,
				"block_number":   event.BlockNumber,
				"expected_hash":  event.NewHash,
				"canonical_hash": canonicalBlock.Hash,
			},
		}
	}

	return CheckResult{
		Name:     name,
		Status:   StatusPassed,
		Message:  "new_hash matches canonical chain",
		Duration: duration,
		Details: map[string]any{
			"reorg_id":     event.ID,
			"block_number": event.BlockNumber,
		},
	}
}

// runSpotChecks performs random block hash verification.
func (s *Service) runSpotChecks(ctx context.Context, fromBlock, toBlock int64) []CheckResult {
	blockRange := toBlock - fromBlock + 1
	count := s.config.SpotCheckCount
	if int64(count) > blockRange {
		count = int(blockRange)
	}

	s.logger.Info("running spot checks", "count", count)

	// Select random blocks
	selectedBlocks := selectRandomBlocks(fromBlock, toBlock, count)

	results := make([]CheckResult, 0, count)
	for _, blockNum := range selectedBlocks {
		result := s.spotCheckBlock(ctx, blockNum)
		results = append(results, result)
	}

	return results
}

// spotCheckBlock verifies a single block's hash against Etherscan.
func (s *Service) spotCheckBlock(ctx context.Context, blockNum int64) CheckResult {
	start := time.Now()
	name := fmt.Sprintf("Spot check block %d", blockNum)

	// Get local block
	localBlock, err := s.blockStateRepo.GetBlockByNumber(ctx, blockNum)
	if err != nil {
		return CheckResult{
			Name:     name,
			Status:   StatusError,
			Message:  fmt.Sprintf("Failed to fetch local block: %v", err),
			Duration: time.Since(start),
		}
	}

	if localBlock == nil {
		return CheckResult{
			Name:     name,
			Status:   StatusError,
			Message:  "Block not found in local database",
			Duration: time.Since(start),
		}
	}

	// Get canonical block from Etherscan
	canonicalBlock, err := s.blockVerifier.GetBlockByNumber(ctx, blockNum)
	duration := time.Since(start)

	if err != nil {
		return CheckResult{
			Name:     name,
			Status:   StatusError,
			Message:  fmt.Sprintf("Failed to fetch from %s: %v", s.blockVerifier.Name(), err),
			Duration: duration,
		}
	}

	if canonicalBlock == nil {
		return CheckResult{
			Name:     name,
			Status:   StatusError,
			Message:  fmt.Sprintf("Block not found on %s", s.blockVerifier.Name()),
			Duration: duration,
		}
	}

	// Compare hashes
	if !hashesMatch(localBlock.Hash, canonicalBlock.Hash) {
		return CheckResult{
			Name:   name,
			Status: StatusFailed,
			Message: fmt.Sprintf("Hash mismatch\n"+
				"Local:     %s\n"+
				"Canonical: %s", localBlock.Hash, canonicalBlock.Hash),
			Duration: duration,
			Details: map[string]any{
				"block_number":   blockNum,
				"local_hash":     localBlock.Hash,
				"canonical_hash": canonicalBlock.Hash,
			},
		}
	}

	return CheckResult{
		Name:     name,
		Status:   StatusPassed,
		Message:  "Hash verified",
		Duration: duration,
	}
}

// selectRandomBlocks selects n random block numbers from the given range.
func selectRandomBlocks(fromBlock, toBlock int64, n int) []int64 {
	blockRange := toBlock - fromBlock + 1
	if int64(n) >= blockRange {
		// Return all blocks in range
		blocks := make([]int64, blockRange)
		for i := int64(0); i < blockRange; i++ {
			blocks[i] = fromBlock + i
		}
		return blocks
	}

	// Use a map to avoid duplicates
	selected := make(map[int64]bool, n)
	for len(selected) < n {
		block := fromBlock + rand.Int63n(blockRange)
		selected[block] = true
	}

	blocks := make([]int64, 0, n)
	for block := range selected {
		blocks = append(blocks, block)
	}
	return blocks
}

// hashesMatch compares two block hashes (case-insensitive, handles 0x prefix).
func hashesMatch(hash1, hash2 string) bool {
	h1 := strings.ToLower(strings.TrimPrefix(hash1, "0x"))
	h2 := strings.ToLower(strings.TrimPrefix(hash2, "0x"))
	return h1 == h2
}

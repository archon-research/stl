// Package data_validator provides validation of block chain data stored by the watcher.
// It verifies reorg events against a canonical chain source (provided via the
// BlockVerifier port) and validates chain integrity. The source is chain-specific
// and selected by the caller; the service itself is chain-agnostic.
package data_validator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"strconv"
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

	// SpotCheckCount is the number of random blocks to verify against the canonical chain source.
	SpotCheckCount int

	// ValidateReorgs enables reorg event validation against the canonical chain source.
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

	// Run chain integrity checks
	if s.config.ValidateChainIntegrity {
		report.AddCheck(s.validateChainIntegrity(ctx, fromBlock, toBlock))
		report.AddCheck(s.validateNoOrphanOnlyHeights(ctx, fromBlock, toBlock))
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
	s.reportCheckOutcomes(report)

	s.logger.Info("validation complete",
		"passed", report.Passed,
		"failed", report.Failed,
		"errors", report.Errors,
		"skipped", report.Skipped,
		"duration", report.Duration,
	)

	return report, nil
}

// reportCheckOutcomes logs every check that did not pass. The report object
// never leaves the process — the runner keeps counts, the alert fires on the
// exit code — so the log is the only route by which a check's message and
// details (the heights the runbook tells the operator to read) reach anyone.
func (s *Service) reportCheckOutcomes(report *Report) {
	for _, check := range report.Checks {
		attrs := []any{"check", check.Name, "message", check.Message}
		if len(check.Details) > 0 {
			attrs = append(attrs, "details", check.Details)
		}
		switch check.Status {
		case StatusError:
			s.logger.Error("validation check errored", attrs...)
		case StatusFailed:
			s.logger.Warn("validation check failed", attrs...)
		case StatusSkipped:
			s.logger.Info("validation check skipped", attrs...)
		}
	}
}

// resolveBlockRange determines the actual block range to validate.
func (s *Service) resolveBlockRange(ctx context.Context) (int64, int64, error) {
	// GetMin/MaxBlockNumber return 0 for both an empty table and a genesis-only
	// table, so emptiness can't be inferred from them. Check existence directly:
	// a nil last block means nothing has been ingested yet, which is a hard
	// failure for a validation run.
	lastBlock, err := s.blockStateRepo.GetLastBlock(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("getting last block: %w", err)
	}
	if lastBlock == nil {
		return 0, 0, fmt.Errorf("no blocks found in database to validate (chain may not be ingested yet)")
	}

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

// validateChainIntegrity verifies the stored chain under two bounds. Up to the
// backfill watermark every height must be present and linked. Above it a
// missing height is the gap filler's live work — an out-of-order arrival is a
// hole for seconds, and backfill_watermark_lag covers it — but a broken link or
// a duplicated height never repairs itself: it pins the watermark, so the
// bounded check would never reach it and detection would fall back to the lag
// alert 1000 blocks later (ARCT-379).
func (s *Service) validateChainIntegrity(ctx context.Context, fromBlock, toBlock int64) CheckResult {
	const name = "Chain Integrity"
	start := time.Now()

	watermark, err := s.blockStateRepo.GetBackfillWatermark(ctx)
	if err != nil {
		return CheckResult{
			Name:     name,
			Status:   StatusError,
			Message:  fmt.Sprintf("Failed to read backfill watermark: %v", err),
			Duration: time.Since(start),
		}
	}

	verifyTo := toBlock
	if watermark > 0 {
		verifyTo = min(toBlock, watermark)
	}
	if verifyTo < fromBlock {
		return s.verifyParentLinksOnly(ctx, name, start, fromBlock, toBlock, watermark)
	}

	s.logger.Info("validating chain integrity",
		"from", fromBlock, "to", verifyTo, "parent_links_to", toBlock, "watermark", watermark)

	if err := s.verifyChainOver(ctx, fromBlock, verifyTo, toBlock); err != nil {
		return CheckResult{
			Name:     name,
			Status:   StatusFailed,
			Message:  err.Error(),
			Duration: time.Since(start),
		}
	}

	return CheckResult{
		Name:     name,
		Status:   StatusPassed,
		Message:  chainValidMessage(watermark, verifyTo, toBlock),
		Duration: time.Since(start),
	}
}

// verifyParentLinksOnly is the check that survives a watermark below the range
// start. The break that pins the watermark there is the one a Skipped result
// would hide until 30-day retention drops the rows and hides it for good.
func (s *Service) verifyParentLinksOnly(ctx context.Context, name string, start time.Time, fromBlock, toBlock, watermark int64) CheckResult {
	skipped := fmt.Sprintf("the strict missing-height check was skipped because backfill watermark %d is below the range start %d", watermark, fromBlock)

	if err := s.blockStateRepo.VerifyParentLinks(ctx, fromBlock, toBlock); err != nil {
		return CheckResult{
			Name:     name,
			Status:   StatusFailed,
			Message:  fmt.Sprintf("%s (%s)", err.Error(), skipped),
			Duration: time.Since(start),
		}
	}
	return CheckResult{
		Name:     name,
		Status:   StatusPassed,
		Message:  fmt.Sprintf("Parent links valid through block %d; %s", toBlock, skipped),
		Duration: time.Since(start),
	}
}

// verifyChainOver runs the strict check up to verifyTo and the parent-link
// check above it. verifyTo is the lower bound of the second range, not the
// height after it: a late arrival at verifyTo+1 is saved on its successor's
// word alone, without checking its own predecessor
// (live_data's classifyOutOfOrderArrival), so that pair is exactly the one a
// break hides in.
func (s *Service) verifyChainOver(ctx context.Context, fromBlock, verifyTo, toBlock int64) error {
	if err := s.blockStateRepo.VerifyChainIntegrity(ctx, fromBlock, verifyTo); err != nil {
		return err
	}
	if toBlock <= verifyTo {
		return nil
	}
	return s.blockStateRepo.VerifyParentLinks(ctx, verifyTo, toBlock)
}

// chainValidMessage states what was verified under which bound, so a passed
// check cannot be read as "the whole range is whole".
func chainValidMessage(watermark, verifyTo, toBlock int64) string {
	through := fmt.Sprintf("block %d", verifyTo)
	if verifyTo == watermark {
		through = fmt.Sprintf("backfill watermark %d", watermark)
	}
	if toBlock <= verifyTo {
		return fmt.Sprintf("Parent-hash chain valid through %s", through)
	}
	return fmt.Sprintf("Parent-hash chain valid through %s; parent links valid through block %d", through, toBlock)
}

// orphanOnlyHeightsListed caps how many heights both the message and Details
// name — reportCheckOutcomes puts Details in one slog record, and Loki drops a
// line over 256 KB. The reported count stays exact.
const orphanOnlyHeightsListed = 100

// validateNoOrphanOnlyHeights reports heights whose only stored block is
// orphaned. It scans the full range and names every one, where chain integrity
// stops at the watermark and at the first hole — and a fresh occurrence sits
// above the watermark, since the reorg that caused it rewound the watermark.
func (s *Service) validateNoOrphanOnlyHeights(ctx context.Context, fromBlock, toBlock int64) CheckResult {
	const name = "Orphan-only heights"
	start := time.Now()
	s.logger.Info("checking for orphan-only heights", "from", fromBlock, "to", toBlock)

	heights, err := s.blockStateRepo.FindOrphanOnlyHeights(ctx, fromBlock, toBlock)
	duration := time.Since(start)

	if err != nil {
		return CheckResult{
			Name:     name,
			Status:   StatusError,
			Message:  fmt.Sprintf("Failed to query orphan-only heights: %v", err),
			Duration: duration,
		}
	}

	if len(heights) == 0 {
		return CheckResult{
			Name:     name,
			Status:   StatusPassed,
			Message:  "Every height has a canonical block",
			Duration: duration,
		}
	}

	return CheckResult{
		Name:     name,
		Status:   StatusFailed,
		Message:  fmt.Sprintf("%d height(s) have only an orphaned block: %s", len(heights), formatHeights(heights, orphanOnlyHeightsListed)),
		Duration: duration,
		Details: map[string]any{
			"orphan_only_heights":      heights[:min(len(heights), orphanOnlyHeightsListed)],
			"orphan_only_height_count": len(heights),
		},
	}
}

// formatHeights renders block numbers as a comma-separated list, naming at most
// limit of them and summarising the remainder.
func formatHeights(heights []int64, limit int) string {
	listed, suffix := heights, ""
	if len(heights) > limit {
		listed, suffix = heights[:limit], fmt.Sprintf(" (+%d more)", len(heights)-limit)
	}

	parts := make([]string, len(listed))
	for i, height := range listed {
		parts[i] = strconv.FormatInt(height, 10)
	}
	return strings.Join(parts, ", ") + suffix
}

// validateReorgs validates each reorg event against the canonical chain source.
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
	for i, event := range events {
		if i > 0 && i%10 == 0 {
			s.logger.Info("reorg validation progress", "completed", i, "total", len(events))
		}
		result := s.validateSingleReorg(ctx, event)
		results = append(results, result)
	}

	return results, nil
}

// validateSingleReorg validates a single reorg event against the canonical chain source.
func (s *Service) validateSingleReorg(ctx context.Context, event outbound.ReorgEvent) CheckResult {
	start := time.Now()
	name := fmt.Sprintf("Reorg %d at block %d", event.ID, event.BlockNumber)

	s.logger.Debug("validating reorg event",
		"id", event.ID,
		"block", event.BlockNumber,
		"old_hash", event.OldHash,
		"new_hash", event.NewHash,
	)

	// Fetch the canonical block from the verifier source
	canonicalBlock, err := s.blockVerifier.GetBlockByNumber(ctx, event.BlockNumber)
	duration := time.Since(start)

	if err != nil {
		return CheckResult{
			Name:     name,
			Status:   canonicalCheckStatus(err),
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
			Message:  fmt.Sprintf("Block not found on %s", s.blockVerifier.Name()),
			Duration: duration,
			Details: map[string]any{
				"reorg_id":     event.ID,
				"block_number": event.BlockNumber,
			},
		}
	}

	// The new_hash from our reorg event should match the canonical chain source's hash
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
	for i, blockNum := range selectedBlocks {
		if i > 0 && i%10 == 0 {
			s.logger.Info("spot check progress", "completed", i, "total", count)
		}
		result := s.spotCheckBlock(ctx, blockNum)
		results = append(results, result)
	}

	return results
}

// spotCheckBlock verifies a single block's hash against the canonical chain source.
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

	// Get canonical block from the verifier source
	canonicalBlock, err := s.blockVerifier.GetBlockByNumber(ctx, blockNum)
	duration := time.Since(start)

	if err != nil {
		return CheckResult{
			Name:     name,
			Status:   canonicalCheckStatus(err),
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
		for i := range blockRange {
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

// canonicalCheckStatus chooses the status for a failed canonical-source fetch.
// A transient outage (rate-limit, timeout, 5xx) is inconclusive, not a data
// discrepancy, so it is skipped rather than failing the whole run. A permanent
// error (bad key, parse failure) stays a hard error.
func canonicalCheckStatus(err error) string {
	if errors.Is(err, outbound.ErrCanonicalSourceUnavailable) {
		return StatusSkipped
	}
	return StatusError
}

// hashesMatch compares two block hashes (case-insensitive, handles 0x prefix).
func hashesMatch(hash1, hash2 string) bool {
	h1 := strings.ToLower(strings.TrimPrefix(hash1, "0x"))
	h2 := strings.ToLower(strings.TrimPrefix(hash2, "0x"))
	return h1 == h2
}

package data_validator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// mockBlockStateRepository implements outbound.BlockStateRepository for testing.
type mockBlockStateRepository struct {
	minBlockNumber      int64
	maxBlockNumber      int64
	lastBlock           *outbound.BlockState
	blocks              map[int64]*outbound.BlockState
	reorgEvents         []outbound.ReorgEvent
	chainIntegrityError error
	orphanOnlyHeights   []int64
	orphanOnlyErr       error
	backfillWatermark   int64
	backfillCursorErr   error

	// chainIntegrityViolationAt and parentLinkViolationAt report a violation
	// only when that height falls inside the range the corresponding check is
	// asked for; the recorded ranges are what prove the service asked for the
	// right bounds, and asked at all.
	chainIntegrityViolationAt int64
	parentLinkViolationAt     int64
	verifiedRanges            []outbound.BlockRange
	parentLinkRanges          []outbound.BlockRange

	// cursorReads names every cursor and max-block read in the order they were
	// made; a watermark read after the max block is the race the ordering
	// avoids.
	cursorReads []string
}

func (m *mockBlockStateRepository) SaveBlock(ctx context.Context, state outbound.BlockState) (int, error) {
	return 0, nil
}

func (m *mockBlockStateRepository) GetLastBlock(ctx context.Context) (*outbound.BlockState, error) {
	if m.lastBlock != nil {
		return m.lastBlock, nil
	}
	// Treat any seeded data as a non-empty DB so existing tests need no edits;
	// only a repo with no blocks at all reports nil (genuinely empty).
	if m.maxBlockNumber > 0 || len(m.blocks) > 0 {
		return &outbound.BlockState{Number: m.maxBlockNumber}, nil
	}
	return nil, nil
}

func (m *mockBlockStateRepository) GetBlockByNumber(ctx context.Context, number int64) (*outbound.BlockState, error) {
	if block, ok := m.blocks[number]; ok {
		return block, nil
	}
	return nil, nil
}

func (m *mockBlockStateRepository) GetBlockByHash(ctx context.Context, hash string) (*outbound.BlockState, error) {
	return nil, nil
}

func (m *mockBlockStateRepository) GetBlockVersionCount(ctx context.Context, number int64) (int, error) {
	return 0, nil
}

func (m *mockBlockStateRepository) GetRecentBlocks(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	return nil, nil
}

func (m *mockBlockStateRepository) GetLowestCanonicalAbove(ctx context.Context, number, maxNumber int64) (*outbound.BlockState, error) {
	return nil, nil
}

func (m *mockBlockStateRepository) MarkBlockOrphaned(ctx context.Context, hash string) error {
	return nil
}

func (m *mockBlockStateRepository) ClearBlocksOrphaned(ctx context.Context, anchorHash string, hashes []string) error {
	return nil
}

func (m *mockBlockStateRepository) HandleReorgAtomic(ctx context.Context, commonAncestor int64, event outbound.ReorgEvent, newBlock outbound.BlockState) (int, error) {
	return 0, nil
}

func (m *mockBlockStateRepository) GetMinBlockNumber(ctx context.Context) (int64, error) {
	return m.minBlockNumber, nil
}

func (m *mockBlockStateRepository) GetMaxBlockNumber(ctx context.Context) (int64, error) {
	m.cursorReads = append(m.cursorReads, "GetMaxBlockNumber")
	return m.maxBlockNumber, nil
}

func (m *mockBlockStateRepository) GetBackfillWatermark(ctx context.Context) (int64, error) {
	m.cursorReads = append(m.cursorReads, "GetBackfillWatermark")
	return m.backfillWatermark, nil
}

func (m *mockBlockStateRepository) GetBackfillCursor(ctx context.Context) (outbound.BackfillCursor, error) {
	m.cursorReads = append(m.cursorReads, "GetBackfillCursor")
	if m.backfillCursorErr != nil {
		return outbound.BackfillCursor{}, m.backfillCursorErr
	}
	return outbound.BackfillCursor{Watermark: m.backfillWatermark}, nil
}

func (m *mockBlockStateRepository) RewindBackfillWatermark(ctx context.Context, to int64) (int64, bool, error) {
	return 0, false, nil
}

func (m *mockBlockStateRepository) AdvanceBackfillWatermark(ctx context.Context, expected outbound.BackfillCursor, watermark int64) (bool, error) {
	return true, nil
}

func (m *mockBlockStateRepository) FindGaps(ctx context.Context, minBlock, maxBlock int64) ([]outbound.BlockRange, error) {
	return nil, nil
}

func (m *mockBlockStateRepository) VerifyChainIntegrity(ctx context.Context, fromBlock, toBlock int64) error {
	m.verifiedRanges = append(m.verifiedRanges, outbound.BlockRange{From: fromBlock, To: toBlock})
	if m.chainIntegrityViolationAt > 0 &&
		m.chainIntegrityViolationAt >= fromBlock && m.chainIntegrityViolationAt <= toBlock {
		return fmt.Errorf("chain integrity violation: canonical block(s) %d to %d missing between blocks %d and %d",
			m.chainIntegrityViolationAt, m.chainIntegrityViolationAt,
			m.chainIntegrityViolationAt-1, m.chainIntegrityViolationAt+1)
	}
	return m.chainIntegrityError
}

func (m *mockBlockStateRepository) VerifyParentLinks(ctx context.Context, fromBlock, toBlock int64) error {
	m.parentLinkRanges = append(m.parentLinkRanges, outbound.BlockRange{From: fromBlock, To: toBlock})
	if m.parentLinkViolationAt > 0 &&
		m.parentLinkViolationAt >= fromBlock && m.parentLinkViolationAt <= toBlock {
		return fmt.Errorf("chain integrity violation at block %d: parent_hash 0xa does not match hash 0xb of block %d",
			m.parentLinkViolationAt, m.parentLinkViolationAt-1)
	}
	return nil
}

func (m *mockBlockStateRepository) FindOrphanOnlyHeights(ctx context.Context, fromBlock, toBlock int64) ([]int64, error) {
	return m.orphanOnlyHeights, m.orphanOnlyErr
}

func (m *mockBlockStateRepository) MarkPublishComplete(ctx context.Context, hash string) error {
	return nil
}

func (m *mockBlockStateRepository) GetMinUnpublishedBlock(ctx context.Context) (int64, bool, error) {
	return 0, false, nil
}

func (m *mockBlockStateRepository) GetBlocksWithIncompletePublish(ctx context.Context, limit int) ([]outbound.BlockState, error) {
	return nil, nil
}

func (m *mockBlockStateRepository) GetReorgEventsByBlockRange(ctx context.Context, fromBlock, toBlock int64) ([]outbound.ReorgEvent, error) {
	var filtered []outbound.ReorgEvent
	for _, e := range m.reorgEvents {
		if e.BlockNumber >= fromBlock && e.BlockNumber <= toBlock {
			filtered = append(filtered, e)
		}
	}
	return filtered, nil
}

// mockBlockVerifier implements outbound.BlockVerifier for testing.
type mockBlockVerifier struct {
	blocks map[int64]*outbound.CanonicalBlock
	name   string
	// getBlockByNumber, when set, overrides the map lookup so a test can inject
	// a per-call error (e.g. a transient canonical-source failure).
	getBlockByNumber func(ctx context.Context, number int64) (*outbound.CanonicalBlock, error)
}

func (m *mockBlockVerifier) Name() string {
	if m.name == "" {
		return "mock"
	}
	return m.name
}

func (m *mockBlockVerifier) GetBlockByNumber(ctx context.Context, number int64) (*outbound.CanonicalBlock, error) {
	if m.getBlockByNumber != nil {
		return m.getBlockByNumber(ctx, number)
	}
	if block, ok := m.blocks[number]; ok {
		return block, nil
	}
	return nil, nil
}

func (m *mockBlockVerifier) GetBlockByHash(ctx context.Context, hash string) (*outbound.CanonicalBlock, error) {
	return nil, nil
}

func (m *mockBlockVerifier) GetLatestBlockNumber(ctx context.Context) (int64, error) {
	return 0, nil
}

func TestNewService(t *testing.T) {
	repo := &mockBlockStateRepository{}
	verifier := &mockBlockVerifier{}

	tests := []struct {
		name        string
		repo        outbound.BlockStateRepository
		verifier    outbound.BlockVerifier
		wantErr     bool
		errContains string
	}{
		{
			name:     "valid config",
			repo:     repo,
			verifier: verifier,
			wantErr:  false,
		},
		{
			name:        "nil repo",
			repo:        nil,
			verifier:    verifier,
			wantErr:     true,
			errContains: "blockStateRepo",
		},
		{
			name:        "nil verifier",
			repo:        repo,
			verifier:    nil,
			wantErr:     true,
			errContains: "blockVerifier",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewService(DefaultConfig(), tt.repo, tt.verifier)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewService() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr && tt.errContains != "" && err != nil {
				if !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error %q should contain %q", err.Error(), tt.errContains)
				}
			}
		})
	}
}

func TestService_ValidateChainIntegrity(t *testing.T) {
	tests := []struct {
		name         string
		integrityErr error
		wantStatus   string
	}{
		{
			name:         "chain valid",
			integrityErr: nil,
			wantStatus:   StatusPassed,
		},
		{
			name:         "chain invalid",
			integrityErr: errors.New("integrity violation at block 100"),
			wantStatus:   StatusFailed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &mockBlockStateRepository{
				minBlockNumber:      1,
				maxBlockNumber:      100,
				chainIntegrityError: tt.integrityErr,
			}
			verifier := &mockBlockVerifier{}

			config := DefaultConfig()
			config.ValidateChainIntegrity = true
			config.ValidateReorgs = false
			config.SpotCheckCount = 0

			svc, err := NewService(config, repo, verifier)
			if err != nil {
				t.Fatalf("NewService() error = %v", err)
			}

			ctx := context.Background()
			report, err := svc.Validate(ctx)
			if err != nil {
				t.Fatalf("Validate() error = %v", err)
			}

			got := findCheck(t, report, "Chain Integrity")
			if got.Status != tt.wantStatus {
				t.Errorf("got status %q, want %q", got.Status, tt.wantStatus)
			}
		})
	}
}

func TestService_ValidateReorgs(t *testing.T) {
	tests := []struct {
		name        string
		localReorgs []outbound.ReorgEvent
		canonical   map[int64]*outbound.CanonicalBlock
		wantPassed  int
		wantFailed  int
	}{
		{
			name:        "no reorgs",
			localReorgs: nil,
			canonical:   nil,
			wantPassed:  1, // "No reorg events in range" message
			wantFailed:  0,
		},
		{
			name: "reorg hash matches",
			localReorgs: []outbound.ReorgEvent{
				{
					ID:          1,
					BlockNumber: 100,
					OldHash:     "0xold",
					NewHash:     "0xabc123",
				},
			},
			canonical: map[int64]*outbound.CanonicalBlock{
				100: {Number: 100, Hash: "0xabc123"},
			},
			wantPassed: 1,
			wantFailed: 0,
		},
		{
			name: "reorg hash mismatch",
			localReorgs: []outbound.ReorgEvent{
				{
					ID:          1,
					BlockNumber: 100,
					OldHash:     "0xold",
					NewHash:     "0xabc123",
				},
			},
			canonical: map[int64]*outbound.CanonicalBlock{
				100: {Number: 100, Hash: "0xdifferent"},
			},
			wantPassed: 0,
			wantFailed: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &mockBlockStateRepository{
				minBlockNumber: 1,
				maxBlockNumber: 200,
				reorgEvents:    tt.localReorgs,
			}
			verifier := &mockBlockVerifier{
				blocks: tt.canonical,
			}

			config := DefaultConfig()
			config.ValidateChainIntegrity = false
			config.ValidateReorgs = true
			config.SpotCheckCount = 0

			svc, err := NewService(config, repo, verifier)
			if err != nil {
				t.Fatalf("NewService() error = %v", err)
			}

			ctx := context.Background()
			report, err := svc.Validate(ctx)
			if err != nil {
				t.Fatalf("Validate() error = %v", err)
			}

			if report.Passed != tt.wantPassed {
				t.Errorf("got %d passed, want %d", report.Passed, tt.wantPassed)
			}
			if report.Failed != tt.wantFailed {
				t.Errorf("got %d failed, want %d", report.Failed, tt.wantFailed)
			}
		})
	}
}

func TestService_SpotChecks(t *testing.T) {
	localBlocks := map[int64]*outbound.BlockState{
		100: {Number: 100, Hash: "0xabc123", BlockTimestamp: time.Now().Unix()},
		101: {Number: 101, Hash: "0xdef456", BlockTimestamp: time.Now().Unix()},
		102: {Number: 102, Hash: "0xghi789", BlockTimestamp: time.Now().Unix()},
	}

	canonicalBlocks := map[int64]*outbound.CanonicalBlock{
		100: {Number: 100, Hash: "0xabc123"},
		101: {Number: 101, Hash: "0xdef456"},
		102: {Number: 102, Hash: "0xghi789"},
	}

	repo := &mockBlockStateRepository{
		minBlockNumber: 100,
		maxBlockNumber: 102,
		blocks:         localBlocks,
	}
	verifier := &mockBlockVerifier{
		blocks: canonicalBlocks,
	}

	config := DefaultConfig()
	config.ValidateChainIntegrity = false
	config.ValidateReorgs = false
	config.SpotCheckCount = 3

	svc, err := NewService(config, repo, verifier)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	ctx := context.Background()
	report, err := svc.Validate(ctx)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if report.Passed != 3 {
		t.Errorf("got %d passed, want 3", report.Passed)
	}
	if report.Failed != 0 {
		t.Errorf("got %d failed, want 0", report.Failed)
	}
}

func TestService_SpotChecks_Mismatch(t *testing.T) {
	localBlocks := map[int64]*outbound.BlockState{
		100: {Number: 100, Hash: "0xlocal", BlockTimestamp: time.Now().Unix()},
	}

	canonicalBlocks := map[int64]*outbound.CanonicalBlock{
		100: {Number: 100, Hash: "0xcanonical"},
	}

	repo := &mockBlockStateRepository{
		minBlockNumber: 100,
		maxBlockNumber: 100,
		blocks:         localBlocks,
	}
	verifier := &mockBlockVerifier{
		blocks: canonicalBlocks,
	}

	config := DefaultConfig()
	config.ValidateChainIntegrity = false
	config.ValidateReorgs = false
	config.SpotCheckCount = 1

	svc, err := NewService(config, repo, verifier)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	ctx := context.Background()
	report, err := svc.Validate(ctx)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if report.Passed != 0 {
		t.Errorf("got %d passed, want 0", report.Passed)
	}
	if report.Failed != 1 {
		t.Errorf("got %d failed, want 1", report.Failed)
	}
}

func TestService_SpotCheck_TransientCanonicalError_SkipsCheckAndRunSucceeds(t *testing.T) {
	repo := &mockBlockStateRepository{
		minBlockNumber: 100,
		maxBlockNumber: 100,
		blocks:         map[int64]*outbound.BlockState{100: {Number: 100, Hash: "0xabc"}},
	}
	verifier := &mockBlockVerifier{
		name: "etherscan",
		getBlockByNumber: func(_ context.Context, _ int64) (*outbound.CanonicalBlock, error) {
			return nil, fmt.Errorf("fetching block: %w", outbound.ErrCanonicalSourceUnavailable)
		},
	}

	config := DefaultConfig()
	config.ValidateChainIntegrity = false
	config.ValidateReorgs = false
	config.SpotCheckCount = 1

	svc, err := NewService(config, repo, verifier)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	report, err := svc.Validate(context.Background())
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if report.Errors != 0 {
		t.Errorf("Errors = %d, want 0 (transient must be skipped)", report.Errors)
	}
	if report.Skipped != 1 {
		t.Errorf("Skipped = %d, want 1", report.Skipped)
	}
	if !report.Success() {
		t.Error("Success() = false, want true (transient throttle must not fail the run)")
	}
}

func TestService_SpotCheck_PermanentCanonicalError_RecordsErrorAndRunFails(t *testing.T) {
	repo := &mockBlockStateRepository{
		minBlockNumber: 100,
		maxBlockNumber: 100,
		blocks:         map[int64]*outbound.BlockState{100: {Number: 100, Hash: "0xabc"}},
	}
	verifier := &mockBlockVerifier{
		name: "etherscan",
		getBlockByNumber: func(_ context.Context, _ int64) (*outbound.CanonicalBlock, error) {
			return nil, fmt.Errorf("API error: invalid api key")
		},
	}

	config := DefaultConfig()
	config.ValidateChainIntegrity = false
	config.ValidateReorgs = false
	config.SpotCheckCount = 1

	svc, err := NewService(config, repo, verifier)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	report, err := svc.Validate(context.Background())
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if report.Errors == 0 {
		t.Error("Errors = 0, want > 0 (a permanent canonical error is a real problem)")
	}
	if report.Success() {
		t.Error("Success() = true, want false")
	}
}

func TestService_Reorg_TransientCanonicalError_Skips(t *testing.T) {
	repo := &mockBlockStateRepository{
		minBlockNumber: 1,
		maxBlockNumber: 200,
		reorgEvents: []outbound.ReorgEvent{
			{ID: 1, BlockNumber: 100, OldHash: "0xold", NewHash: "0xnew"},
		},
	}
	verifier := &mockBlockVerifier{
		name: "etherscan",
		getBlockByNumber: func(_ context.Context, _ int64) (*outbound.CanonicalBlock, error) {
			return nil, fmt.Errorf("fetching block: %w", outbound.ErrCanonicalSourceUnavailable)
		},
	}

	config := DefaultConfig()
	config.ValidateChainIntegrity = false
	config.ValidateReorgs = true
	config.SpotCheckCount = 0

	svc, err := NewService(config, repo, verifier)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	report, err := svc.Validate(context.Background())
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if report.Errors != 0 {
		t.Errorf("Errors = %d, want 0", report.Errors)
	}
	if report.Skipped != 1 {
		t.Errorf("Skipped = %d, want 1", report.Skipped)
	}
	if !report.Success() {
		t.Error("Success() = false, want true")
	}
}

func TestHashesMatch(t *testing.T) {
	tests := []struct {
		hash1 string
		hash2 string
		want  bool
	}{
		{"0xabc123", "0xabc123", true},
		{"0xABC123", "0xabc123", true},
		{"abc123", "0xabc123", true},
		{"0xabc123", "abc123", true},
		{"0xabc123", "0xdef456", false},
		{"", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.hash1+"_"+tt.hash2, func(t *testing.T) {
			if got := hashesMatch(tt.hash1, tt.hash2); got != tt.want {
				t.Errorf("hashesMatch(%q, %q) = %v, want %v", tt.hash1, tt.hash2, got, tt.want)
			}
		})
	}
}

func TestSelectRandomBlocks(t *testing.T) {
	tests := []struct {
		from  int64
		to    int64
		n     int
		wantN int
	}{
		{1, 10, 5, 5},
		{1, 3, 10, 3}, // More requested than available
		{100, 100, 1, 1},
	}

	for _, tt := range tests {
		blocks := selectRandomBlocks(tt.from, tt.to, tt.n)
		if len(blocks) != tt.wantN {
			t.Errorf("selectRandomBlocks(%d, %d, %d) returned %d blocks, want %d",
				tt.from, tt.to, tt.n, len(blocks), tt.wantN)
		}

		// Verify all blocks are in range
		for _, b := range blocks {
			if b < tt.from || b > tt.to {
				t.Errorf("block %d out of range [%d, %d]", b, tt.from, tt.to)
			}
		}

		// Verify no duplicates
		seen := make(map[int64]bool)
		for _, b := range blocks {
			if seen[b] {
				t.Errorf("duplicate block %d", b)
			}
			seen[b] = true
		}
	}
}

func TestService_Validate_EmptyDatabaseErrors(t *testing.T) {
	repo := &mockBlockStateRepository{
		minBlockNumber: 0,
		maxBlockNumber: 0,
	}
	verifier := &mockBlockVerifier{}

	svc, err := NewService(DefaultConfig(), repo, verifier)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	ctx := context.Background()
	_, err = svc.Validate(ctx)
	if err == nil {
		t.Fatal("Validate() expected a non-nil error for empty database, got nil")
	}
	if !strings.Contains(err.Error(), "no blocks found") {
		t.Errorf("error %q should contain %q", err.Error(), "no blocks found")
	}
}

func TestService_Validate_GenesisOnlyNotTreatedAsEmpty(t *testing.T) {
	// A DB whose only canonical block is genesis (block 0) reports max/min == 0,
	// the same as an empty DB, but it is NOT empty and must be validated, not
	// rejected with "no blocks found".
	repo := &mockBlockStateRepository{
		minBlockNumber: 0,
		maxBlockNumber: 0,
		lastBlock:      &outbound.BlockState{Number: 0, Hash: "0xgenesis"},
		blocks:         map[int64]*outbound.BlockState{0: {Number: 0, Hash: "0xgenesis"}},
	}
	verifier := &mockBlockVerifier{
		blocks: map[int64]*outbound.CanonicalBlock{0: {Number: 0, Hash: "0xgenesis"}},
	}

	config := DefaultConfig()
	config.ValidateChainIntegrity = false
	config.ValidateReorgs = false
	config.SpotCheckCount = 1

	svc, err := NewService(config, repo, verifier)
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	report, err := svc.Validate(context.Background())
	if err != nil {
		t.Fatalf("genesis-only DB must not error, got: %v", err)
	}
	if report.Passed != 1 {
		t.Errorf("got %d passed, want 1 (block 0 spot-checked)", report.Passed)
	}
}

func TestReport_Success(t *testing.T) {
	tests := []struct {
		name   string
		passed int
		failed int
		errors int
		want   bool
	}{
		{"all passed", 5, 0, 0, true},
		{"some failed", 3, 2, 0, false},
		{"some errors", 3, 0, 2, false},
		{"failed and errors", 1, 1, 1, false},
		{"empty", 0, 0, 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			report := &Report{
				Passed: tt.passed,
				Failed: tt.failed,
				Errors: tt.errors,
			}
			if got := report.Success(); got != tt.want {
				t.Errorf("Success() = %v, want %v", got, tt.want)
			}
		})
	}
}

// findCheck returns the named check from a report, failing the test if absent.
func findCheck(t *testing.T, report *Report, name string) CheckResult {
	t.Helper()
	for _, check := range report.Checks {
		if check.Name == name {
			return check
		}
	}
	t.Fatalf("check %q not found in report checks %+v", name, report.Checks)
	return CheckResult{}
}

// TestService_OrphanOnlyHeights covers the ARCT-379 hole: a height whose only
// row is orphaned is invisible to VerifyChainIntegrity (it compares consecutive
// canonical rows only), so it needs its own check to reach the cronjob's
// failure exit and the VectorCronjobRunFailing alert.
func TestService_OrphanOnlyHeights(t *testing.T) {
	tests := []struct {
		name       string
		heights    []int64
		queryErr   error
		wantStatus string
	}{
		{name: "no orphan-only heights", heights: nil, wantStatus: StatusPassed},
		{name: "orphan-only height reported", heights: []int64{25395651}, wantStatus: StatusFailed},
		{name: "query failure is an error, not a pass", queryErr: errors.New("connection reset"), wantStatus: StatusError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &mockBlockStateRepository{
				minBlockNumber:    1,
				maxBlockNumber:    30000000,
				orphanOnlyHeights: tt.heights,
				orphanOnlyErr:     tt.queryErr,
			}

			config := DefaultConfig()
			config.ValidateChainIntegrity = true
			config.ValidateReorgs = false
			config.SpotCheckCount = 0

			svc, err := NewService(config, repo, &mockBlockVerifier{})
			if err != nil {
				t.Fatalf("NewService() error = %v", err)
			}

			report, err := svc.Validate(context.Background())
			if err != nil {
				t.Fatalf("Validate() error = %v", err)
			}

			got := findCheck(t, report, "Orphan-only heights")
			if got.Status != tt.wantStatus {
				t.Errorf("got status %q, want %q", got.Status, tt.wantStatus)
			}
			if tt.wantStatus == StatusFailed && !strings.Contains(got.Message, "25395651") {
				t.Errorf("message %q should name the offending height", got.Message)
			}
		})
	}
}

// TestService_OrphanOnlyHeights_ReportsExactCountAndTruncatesMessage: a reorg
// storm must be reported at its real size, not at the length of the list the
// message can carry.
func TestService_OrphanOnlyHeights_ReportsExactCountAndTruncatesMessage(t *testing.T) {
	heights := make([]int64, 150)
	for i := range heights {
		heights[i] = int64(25000000 + i)
	}

	repo := &mockBlockStateRepository{
		minBlockNumber:    1,
		maxBlockNumber:    30000000,
		orphanOnlyHeights: heights,
	}

	config := DefaultConfig()
	config.ValidateChainIntegrity = true
	config.ValidateReorgs = false
	config.SpotCheckCount = 0

	svc, err := NewService(config, repo, &mockBlockVerifier{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	report, err := svc.Validate(context.Background())
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	got := findCheck(t, report, "Orphan-only heights")
	if got.Status != StatusFailed {
		t.Fatalf("got status %q, want %q", got.Status, StatusFailed)
	}
	if !strings.Contains(got.Message, "150 height(s)") {
		t.Errorf("message %q should report the exact count", got.Message)
	}
	if !strings.Contains(got.Message, "25000099") {
		t.Errorf("message %q should list the first 100 heights", got.Message)
	}
	if strings.Contains(got.Message, "25000100") {
		t.Errorf("message %q should list no more than the first 100 heights", got.Message)
	}
	if !strings.Contains(got.Message, "(+50 more)") {
		t.Errorf("message %q should summarise the heights it does not list", got.Message)
	}

	details, ok := got.Details["orphan_only_heights"].([]int64)
	if !ok {
		t.Fatalf("Details[orphan_only_heights] = %T, want []int64", got.Details["orphan_only_heights"])
	}
	if len(details) != orphanOnlyHeightsListed {
		t.Errorf("Details carries %d heights, want the %d it is capped at", len(details), orphanOnlyHeightsListed)
	}
	if got.Details["orphan_only_height_count"] != len(heights) {
		t.Errorf("Details[orphan_only_height_count] = %v, want %d", got.Details["orphan_only_height_count"], len(heights))
	}
}

// TestService_ChainIntegrity_BoundedByWatermark: a missing height above the
// backfill watermark is the gap filler's live domain, where an out-of-order
// arrival is a hole for seconds, and verifying it would fail an hourly run on
// something backfill_watermark_lag already covers. A broken parent link up
// there never repairs itself, pins the watermark, and must still be reported.
func TestService_ChainIntegrity_BoundedByWatermark(t *testing.T) {
	tests := []struct {
		name            string
		fromBlock       int64
		watermark       int64
		violationAt     int64
		parentViolation int64
		wantStatus      string
		wantVerified    []outbound.BlockRange
		wantParentLinks []outbound.BlockRange
		wantMsgContains string
	}{
		{
			name:            "gap above the watermark is not verified",
			watermark:       500,
			violationAt:     800,
			wantStatus:      StatusPassed,
			wantVerified:    []outbound.BlockRange{{From: 1, To: 500}},
			wantParentLinks: []outbound.BlockRange{{From: 500, To: 1000}},
			wantMsgContains: "watermark 500",
		},
		{
			name:            "parent break above the watermark fails",
			watermark:       500,
			parentViolation: 501,
			wantStatus:      StatusFailed,
			wantVerified:    []outbound.BlockRange{{From: 1, To: 500}},
			wantParentLinks: []outbound.BlockRange{{From: 500, To: 1000}},
			wantMsgContains: "at block 501",
		},
		{
			name:         "hole below the watermark fails",
			watermark:    500,
			violationAt:  300,
			wantStatus:   StatusFailed,
			wantVerified: []outbound.BlockRange{{From: 1, To: 500}},
		},
		{
			name:         "unset watermark verifies the whole range",
			watermark:    0,
			violationAt:  800,
			wantStatus:   StatusFailed,
			wantVerified: []outbound.BlockRange{{From: 1, To: 1000}},
		},
		{
			name:            "watermark below the range start still verifies the parent links",
			fromBlock:       900,
			watermark:       500,
			wantStatus:      StatusPassed,
			wantParentLinks: []outbound.BlockRange{{From: 900, To: 1000}},
			wantMsgContains: "watermark 500 is below the range start 900",
		},
		{
			name:            "a broken link is reported even when the strict check is skipped",
			fromBlock:       900,
			watermark:       500,
			parentViolation: 950,
			wantStatus:      StatusFailed,
			wantParentLinks: []outbound.BlockRange{{From: 900, To: 1000}},
			wantMsgContains: "at block 950",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &mockBlockStateRepository{
				minBlockNumber:            1,
				maxBlockNumber:            1000,
				backfillWatermark:         tt.watermark,
				chainIntegrityViolationAt: tt.violationAt,
				parentLinkViolationAt:     tt.parentViolation,
			}

			config := DefaultConfig()
			config.FromBlock = tt.fromBlock
			config.ValidateChainIntegrity = true
			config.ValidateReorgs = false
			config.SpotCheckCount = 0

			svc, err := NewService(config, repo, &mockBlockVerifier{})
			if err != nil {
				t.Fatalf("NewService() error = %v", err)
			}

			report, err := svc.Validate(context.Background())
			if err != nil {
				t.Fatalf("Validate() error = %v", err)
			}

			got := findCheck(t, report, "Chain Integrity")
			if got.Status != tt.wantStatus {
				t.Errorf("got status %q, want %q (message %q)", got.Status, tt.wantStatus, got.Message)
			}
			assertRanges(t, "VerifyChainIntegrity", repo.verifiedRanges, tt.wantVerified)
			assertRanges(t, "VerifyParentLinks", repo.parentLinkRanges, tt.wantParentLinks)
			if tt.wantMsgContains != "" && !strings.Contains(got.Message, tt.wantMsgContains) {
				t.Errorf("message %q should contain %q", got.Message, tt.wantMsgContains)
			}
		})
	}
}

// assertRanges compares the ranges a check was called with, count included, so
// a call the service should not have made at all is visible.
func assertRanges(t *testing.T, name string, got, want []outbound.BlockRange) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s called %d time(s) with %v, want %d call(s) with %v", name, len(got), got, len(want), want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s call %d = [%d, %d], want [%d, %d]", name, i+1, got[i].From, got[i].To, want[i].From, want[i].To)
		}
	}
}

// TestService_Validate_LogsEveryNonPassedCheck: the report object never leaves
// the process — the runner keeps counts and the alert fires on the exit code —
// so a check's message and details reach an operator only through the log.
func TestService_Validate_LogsEveryNonPassedCheck(t *testing.T) {
	repo := &mockBlockStateRepository{
		minBlockNumber:    1,
		maxBlockNumber:    1000,
		backfillWatermark: 1000,
		orphanOnlyHeights: []int64{4, 7},
	}
	repo.chainIntegrityError = errors.New("chain integrity violation at block 42")

	logs := &testutil.SlogRecorder{}
	config := DefaultConfig()
	config.Logger = slog.New(logs)
	config.ValidateChainIntegrity = true
	config.ValidateReorgs = false
	config.SpotCheckCount = 0

	svc, err := NewService(config, repo, &mockBlockVerifier{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if _, err := svc.Validate(context.Background()); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	for _, want := range []string{"chain integrity violation at block 42", "have only an orphaned block: 4, 7"} {
		if !logs.ContainsAttr(want) {
			t.Errorf("no log record carries %q", want)
		}
	}
	if got := logs.CountWarn("validation check failed"); got != 2 {
		t.Errorf("failed-check warnings = %d, want 2", got)
	}
}

// TestService_ChainIntegrity_WatermarkAboveTheData: FindGaps scans only above
// the watermark, so heights between the last canonical block and a watermark
// above it are never scanned and never filled — and the min(toBlock, watermark)
// clamp reported that state as a chain valid through its last block (ARCT-379).
func TestService_ChainIntegrity_WatermarkAboveTheData(t *testing.T) {
	tests := []struct {
		name            string
		toBlock         int64
		watermark       int64
		wantStatus      string
		wantMsgContains []string
	}{
		{
			name:            "a watermark above the last canonical block fails",
			watermark:       2000,
			wantStatus:      StatusFailed,
			wantMsgContains: []string{"watermark 2000 is above the last canonical block 1000", "1001..2000"},
		},
		{
			name:       "a requested range ending below the watermark is not an anomaly",
			toBlock:    300,
			watermark:  500,
			wantStatus: StatusPassed,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &mockBlockStateRepository{
				minBlockNumber:    1,
				maxBlockNumber:    1000,
				backfillWatermark: tt.watermark,
			}

			config := DefaultConfig()
			config.ToBlock = tt.toBlock
			config.ValidateChainIntegrity = true
			config.ValidateReorgs = false
			config.SpotCheckCount = 0

			svc, err := NewService(config, repo, &mockBlockVerifier{})
			if err != nil {
				t.Fatalf("NewService() error = %v", err)
			}

			report, err := svc.Validate(context.Background())
			if err != nil {
				t.Fatalf("Validate() error = %v", err)
			}

			got := findCheck(t, report, "Chain Integrity")
			if got.Status != tt.wantStatus {
				t.Errorf("got status %q, want %q (message %q)", got.Status, tt.wantStatus, got.Message)
			}
			for _, want := range tt.wantMsgContains {
				if !strings.Contains(got.Message, want) {
					t.Errorf("message %q should contain %q", got.Message, want)
				}
			}
		})
	}
}

// TestService_Validate_ReadsTheCursorBeforeTheLastCanonicalBlock: the gap
// filler advances the watermark every few seconds, so a watermark read after
// the last canonical block can exceed it with nothing wrong. Read before it,
// and read once, the excess can only be a cursor above the data.
func TestService_Validate_ReadsTheCursorBeforeTheLastCanonicalBlock(t *testing.T) {
	repo := &mockBlockStateRepository{
		minBlockNumber:    1,
		maxBlockNumber:    1000,
		backfillWatermark: 1000,
	}

	config := DefaultConfig()
	config.ValidateChainIntegrity = true
	config.ValidateReorgs = false
	config.SpotCheckCount = 0

	svc, err := NewService(config, repo, &mockBlockVerifier{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	if _, err := svc.Validate(context.Background()); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	want := []string{"GetBackfillCursor", "GetMaxBlockNumber"}
	if !slices.Equal(repo.cursorReads, want) {
		t.Errorf("cursor reads = %v, want %v", repo.cursorReads, want)
	}
}

// TestService_Validate_FailsWhenTheCursorIsUnreadable: the watermark decides
// which bound every height in the range is checked under, so a run that cannot
// read it stops instead of reporting the chain under a silently-zero cursor.
func TestService_Validate_FailsWhenTheCursorIsUnreadable(t *testing.T) {
	repo := &mockBlockStateRepository{
		minBlockNumber:    1,
		maxBlockNumber:    1000,
		backfillCursorErr: errors.New("connection refused"),
	}

	svc, err := NewService(DefaultConfig(), repo, &mockBlockVerifier{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	if _, err := svc.Validate(context.Background()); err == nil {
		t.Fatal("Validate() error = nil, want the cursor read failure")
	} else if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error %q should carry the cursor read failure", err)
	}
}

// TestService_Validate_LogsEveryCheckDuration: the report never leaves the
// process, so a check that passes leaves no other trace — how long each one
// took is visible only if the finish line carries it.
func TestService_Validate_LogsEveryCheckDuration(t *testing.T) {
	repo := &mockBlockStateRepository{
		minBlockNumber:    1,
		maxBlockNumber:    1000,
		backfillWatermark: 1000,
	}

	logs := &testutil.SlogRecorder{}
	config := DefaultConfig()
	config.Logger = slog.New(logs)
	config.ValidateChainIntegrity = true
	config.ValidateReorgs = false
	config.SpotCheckCount = 0

	svc, err := NewService(config, repo, &mockBlockVerifier{})
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	report, err := svc.Validate(context.Background())
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	if got := logs.CountInfo("validation check finished"); got != len(report.Checks) {
		t.Fatalf("finish records = %d, want one per check (%d)", got, len(report.Checks))
	}
	for _, check := range report.Checks {
		attrs := finishRecordAttrs(t, logs, check.Name)
		if attrs["status"] != check.Status {
			t.Errorf("%s: status attr = %v, want %q", check.Name, attrs["status"], check.Status)
		}
		if _, ok := attrs["duration_ms"]; !ok {
			t.Errorf("%s: finish record carries no duration_ms", check.Name)
		}
	}
}

// finishRecordAttrs returns the attributes of the finish record for one check.
func finishRecordAttrs(t *testing.T, logs *testutil.SlogRecorder, name string) map[string]any {
	t.Helper()
	for _, record := range logs.Records {
		if record.Message != "validation check finished" {
			continue
		}
		attrs := map[string]any{}
		record.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.Any()
			return true
		})
		if attrs["check"] == name {
			return attrs
		}
	}
	t.Fatalf("no finish record for check %q", name)
	return nil
}

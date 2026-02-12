package data_validator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// mockBlockStateRepository implements outbound.BlockStateRepository for testing.
type mockBlockStateRepository struct {
	minBlockNumber      int64
	maxBlockNumber      int64
	blocks              map[int64]*outbound.BlockState
	reorgEvents         []outbound.ReorgEvent
	chainIntegrityError error
}

func (m *mockBlockStateRepository) SaveBlock(ctx context.Context, state outbound.BlockState) (int, error) {
	return 0, nil
}

func (m *mockBlockStateRepository) GetLastBlock(ctx context.Context) (*outbound.BlockState, error) {
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

func (m *mockBlockStateRepository) MarkBlockOrphaned(ctx context.Context, hash string) error {
	return nil
}

func (m *mockBlockStateRepository) HandleReorgAtomic(ctx context.Context, commonAncestor int64, event outbound.ReorgEvent, newBlock outbound.BlockState) (int, error) {
	return 0, nil
}

func (m *mockBlockStateRepository) GetMinBlockNumber(ctx context.Context) (int64, error) {
	return m.minBlockNumber, nil
}

func (m *mockBlockStateRepository) GetMaxBlockNumber(ctx context.Context) (int64, error) {
	return m.maxBlockNumber, nil
}

func (m *mockBlockStateRepository) GetBackfillWatermark(ctx context.Context) (int64, error) {
	return 0, nil
}

func (m *mockBlockStateRepository) SetBackfillWatermark(ctx context.Context, watermark int64) error {
	return nil
}

func (m *mockBlockStateRepository) FindGaps(ctx context.Context, minBlock, maxBlock int64) ([]outbound.BlockRange, error) {
	return nil, nil
}

func (m *mockBlockStateRepository) VerifyChainIntegrity(ctx context.Context, fromBlock, toBlock int64) error {
	return m.chainIntegrityError
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
}

func (m *mockBlockVerifier) Name() string {
	if m.name == "" {
		return "mock"
	}
	return m.name
}

func (m *mockBlockVerifier) GetBlockByNumber(ctx context.Context, number int64) (*outbound.CanonicalBlock, error) {
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

			if len(report.Checks) != 1 {
				t.Fatalf("expected 1 check, got %d", len(report.Checks))
			}

			if report.Checks[0].Status != tt.wantStatus {
				t.Errorf("got status %q, want %q", report.Checks[0].Status, tt.wantStatus)
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
		100: {Number: 100, Hash: "0xabc123"},
		101: {Number: 101, Hash: "0xdef456"},
		102: {Number: 102, Hash: "0xghi789"},
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
		100: {Number: 100, Hash: "0xlocal"},
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

func TestReport_FormatText(t *testing.T) {
	report := NewReport(21000000, 21100000)
	report.AddCheck(CheckResult{
		Name:     "Chain Integrity",
		Status:   StatusPassed,
		Message:  "Parent-hash chain valid",
		Duration: 1200 * time.Millisecond,
	})
	report.AddCheck(CheckResult{
		Name:     "Reorg 142 at block 21045678",
		Status:   StatusFailed,
		Message:  "Hash mismatch\nExpected: 0x1234\nActual: 0x5678",
		Duration: 800 * time.Millisecond,
	})
	report.Finalize()

	text := report.FormatText()

	if !strings.Contains(text, "DATA VALIDATION REPORT") {
		t.Error("missing header")
	}
	if !strings.Contains(text, "21,000,000") {
		t.Error("missing formatted from block")
	}
	if !strings.Contains(text, "[PASSED]") {
		t.Error("missing PASSED status")
	}
	if !strings.Contains(text, "[FAILED]") {
		t.Error("missing FAILED status")
	}
	if !strings.Contains(text, "1 passed, 1 failed") {
		t.Error("missing summary")
	}
}

func TestReport_FormatJSON(t *testing.T) {
	report := NewReport(100, 200)
	report.AddCheck(CheckResult{
		Name:   "Test Check",
		Status: StatusPassed,
	})
	report.Finalize()

	jsonStr, err := report.FormatJSON()
	if err != nil {
		t.Fatalf("FormatJSON() error = %v", err)
	}

	if !strings.Contains(jsonStr, `"from_block": 100`) {
		t.Error("missing from_block in JSON")
	}
	if !strings.Contains(jsonStr, `"status": "passed"`) {
		t.Error("missing status in JSON")
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

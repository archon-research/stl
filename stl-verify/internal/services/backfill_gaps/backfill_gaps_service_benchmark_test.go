package backfill_gaps

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// benchmarkBlockchainClient provides predictable block data for benchmarking.
type benchmarkBlockchainClient struct {
	mu     sync.RWMutex
	blocks map[int64]benchBlockData
}

type benchBlockData struct {
	header   outbound.BlockHeader
	receipts json.RawMessage
	traces   json.RawMessage
	blobs    json.RawMessage
}

func newBenchmarkClient(blockCount int64) *benchmarkBlockchainClient {
	client := &benchmarkBlockchainClient{
		blocks: make(map[int64]benchBlockData, blockCount),
	}
	for i := int64(1); i <= blockCount; i++ {
		client.addBlock(i, "")
	}
	return client
}

func (m *benchmarkBlockchainClient) addBlock(num int64, parentHash string) {
	hash := fmt.Sprintf("0x%064x", num)
	if parentHash == "" && num > 0 {
		parentHash = fmt.Sprintf("0x%064x", num-1)
	}

	header := outbound.BlockHeader{
		Number:     fmt.Sprintf("0x%x", num),
		Hash:       hash,
		ParentHash: parentHash,
		Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
	}

	m.blocks[num] = benchBlockData{
		header:   header,
		receipts: json.RawMessage(`[]`),
		traces:   json.RawMessage(`[]`),
		blobs:    json.RawMessage(`[]`),
	}
}

func (m *benchmarkBlockchainClient) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		data, _ := json.Marshal(bd.header)
		return data, nil
	}
	return nil, fmt.Errorf("block %d not found", blockNum)
}

func (m *benchmarkBlockchainClient) GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*outbound.BlockHeader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for _, bd := range m.blocks {
		if bd.header.Hash == hash {
			h := bd.header
			return &h, nil
		}
	}
	return nil, fmt.Errorf("block %s not found", hash)
}

func (m *benchmarkBlockchainClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.receipts, nil
	}
	return nil, fmt.Errorf("receipts for block %d not found", blockNum)
}

func (m *benchmarkBlockchainClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.traces, nil
	}
	return nil, fmt.Errorf("traces for block %d not found", blockNum)
}

func (m *benchmarkBlockchainClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.blobs, nil
	}
	return nil, fmt.Errorf("blobs for block %d not found", blockNum)
}

func (m *benchmarkBlockchainClient) GetCurrentBlockNumber(ctx context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var max int64
	for num := range m.blocks {
		if num > max {
			max = num
		}
	}
	return max, nil
}

func (m *benchmarkBlockchainClient) GetBlocksBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]outbound.BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make([]outbound.BlockData, len(blockNums))
	for i, num := range blockNums {
		result[i] = outbound.BlockData{BlockNumber: num}
		if bd, ok := m.blocks[num]; ok {
			blockJSON, _ := json.Marshal(bd.header)
			result[i].Block = blockJSON
			result[i].Receipts = bd.receipts
			result[i].Traces = bd.traces
			result[i].Blobs = bd.blobs
		}
	}
	return result, nil
}

// resettableBlockStateRepository wraps memory.BlockStateRepository and adds a Reset method.
type resettableBlockStateRepository struct {
	*memory.BlockStateRepository
	initialBlocks []outbound.BlockState
}

func newResettableBlockStateRepository(blockCount int64, gapBlocks []int64) *resettableBlockStateRepository {
	repo := memory.NewBlockStateRepository()

	// Build gap set for fast lookup
	gapSet := make(map[int64]bool, len(gapBlocks))
	for _, g := range gapBlocks {
		gapSet[g] = true
	}

	// Build initial blocks list
	initialBlocks := make([]outbound.BlockState, 0, int(blockCount)-len(gapBlocks))
	ctx := context.Background()

	for i := int64(1); i <= blockCount; i++ {
		if gapSet[i] {
			continue
		}
		block := outbound.BlockState{
			Number:     i,
			Hash:       fmt.Sprintf("0x%064x", i),
			ParentHash: fmt.Sprintf("0x%064x", i-1),
			ReceivedAt: time.Now().Unix(),
		}
		initialBlocks = append(initialBlocks, block)
		if err := repo.SaveBlock(ctx, block); err != nil {
			panic(fmt.Sprintf("failed to save block in test setup: %v", err))
		}
	}

	return &resettableBlockStateRepository{
		BlockStateRepository: repo,
		initialBlocks:        initialBlocks,
	}
}

// Reset restores the repository to its initial state (gaps present, no backfill).
func (r *resettableBlockStateRepository) Reset() {
	// Create a fresh repository
	r.BlockStateRepository = memory.NewBlockStateRepository()
	ctx := context.Background()
	for _, block := range r.initialBlocks {
		if err := r.SaveBlock(ctx, block); err != nil {
			panic(fmt.Sprintf("failed to save block in Reset: %v", err))
		}
	}
}

// BenchmarkBackfillService_RunOnce benchmarks the RunOnce method with various gap sizes.
// Uses Go 1.24+ b.Loop() for more accurate and predictable benchmarking.
func BenchmarkBackfillService_RunOnce(b *testing.B) {
	cases := []struct {
		name       string
		blockCount int64
		gapStart   int64
		gapEnd     int64
	}{
		{"SmallGap_10Blocks", 100, 45, 54},
		{"MediumGap_50Blocks", 200, 75, 124},
		{"LargeGap_100Blocks", 300, 100, 199},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			// Calculate gap blocks
			gapBlocks := make([]int64, 0, tc.gapEnd-tc.gapStart+1)
			for i := tc.gapStart; i <= tc.gapEnd; i++ {
				gapBlocks = append(gapBlocks, i)
			}

			// Setup outside the benchmark loop - b.Loop() handles timer automatically
			client := newBenchmarkClient(tc.blockCount)
			stateRepo := newResettableBlockStateRepository(tc.blockCount, gapBlocks)
			cache := memory.NewBlockCache()
			eventSink := memory.NewEventSink()

			config := BackfillConfig{
				ChainID:      1,
				BatchSize:    10,
				PollInterval: time.Hour,
				Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			}

			service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
			if err != nil {
				b.Fatalf("failed to create backfill service: %v", err)
			}

			ctx := context.Background()

			b.ReportAllocs()

			for b.Loop() {
				// Reset state to reintroduce gaps
				stateRepo.Reset()

				// Run the backfill
				if err := service.RunOnce(ctx); err != nil {
					b.Fatalf("RunOnce failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkBackfillService_FindAndFillGaps benchmarks gap detection and filling
// with varying numbers of gaps.
func BenchmarkBackfillService_FindAndFillGaps(b *testing.B) {
	cases := []struct {
		name     string
		gapCount int
	}{
		{"SingleGap", 1},
		{"FewGaps_5", 5},
		{"ManyGaps_20", 20},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			blockCount := int64(500)
			gapBlocks := make([]int64, 0)

			// Create evenly distributed gaps
			gapSize := int(blockCount) / (tc.gapCount + 1)
			for i := 0; i < tc.gapCount; i++ {
				gapStart := int64((i + 1) * gapSize)
				// Each gap is 5 blocks
				for j := int64(0); j < 5; j++ {
					gapBlocks = append(gapBlocks, gapStart+j)
				}
			}

			client := newBenchmarkClient(blockCount)
			stateRepo := newResettableBlockStateRepository(blockCount, gapBlocks)
			cache := memory.NewBlockCache()
			eventSink := memory.NewEventSink()

			config := BackfillConfig{
				ChainID:      1,
				BatchSize:    10,
				PollInterval: time.Hour,
				Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			}

			service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
			if err != nil {
				b.Fatalf("failed to create backfill service: %v", err)
			}

			ctx := context.Background()

			b.ReportAllocs()

			for b.Loop() {
				stateRepo.Reset()

				if err := service.RunOnce(ctx); err != nil {
					b.Fatalf("RunOnce failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkBackfillService_BatchSizes benchmarks different batch sizes for gap filling.
func BenchmarkBackfillService_BatchSizes(b *testing.B) {
	batchSizes := []int{1, 5, 10, 25, 50}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("BatchSize_%d", batchSize), func(b *testing.B) {
			blockCount := int64(200)
			// Gap of 50 blocks
			gapBlocks := make([]int64, 50)
			for i := int64(0); i < 50; i++ {
				gapBlocks[i] = 75 + i
			}

			client := newBenchmarkClient(blockCount)
			stateRepo := newResettableBlockStateRepository(blockCount, gapBlocks)
			cache := memory.NewBlockCache()
			eventSink := memory.NewEventSink()

			config := BackfillConfig{
				ChainID:      1,
				BatchSize:    batchSize,
				PollInterval: time.Hour,
				Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
			}

			service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
			if err != nil {
				b.Fatalf("failed to create backfill service: %v", err)
			}

			ctx := context.Background()

			b.ReportAllocs()

			for b.Loop() {
				stateRepo.Reset()

				if err := service.RunOnce(ctx); err != nil {
					b.Fatalf("RunOnce failed: %v", err)
				}
			}
		})
	}
}

// BenchmarkBackfillService_NoGaps benchmarks the overhead when there are no gaps.
// This measures the baseline cost of gap detection.
func BenchmarkBackfillService_NoGaps(b *testing.B) {
	blockCount := int64(1000)

	client := newBenchmarkClient(blockCount)
	stateRepo := newResettableBlockStateRepository(blockCount, nil) // No gaps
	cache := memory.NewBlockCache()
	eventSink := memory.NewEventSink()

	config := BackfillConfig{
		ChainID:      1,
		BatchSize:    10,
		PollInterval: time.Hour,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	service, err := NewBackfillService(config, client, stateRepo, cache, eventSink)
	if err != nil {
		b.Fatalf("failed to create backfill service: %v", err)
	}

	ctx := context.Background()

	b.ReportAllocs()

	for b.Loop() {
		if err := service.RunOnce(ctx); err != nil {
			b.Fatalf("RunOnce failed: %v", err)
		}
	}
}

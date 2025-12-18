package main

import (
	"context"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// TestTraceAllWithProfiling runs scanWithTraceAll with CPU and memory profiling
// Run with: go test -run TestTraceAllWithProfiling -v -timeout 10m
func TestTraceAllWithProfiling(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping profiling test in short mode")
	}

	dataDir := DefaultDataDir
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Skipf("Data directory %s does not exist, skipping test", dataDir)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use 100 blocks for meaningful profiling data
	startBlock := uint64(22152400)
	numBlocks := uint64(100)
	endBlock := startBlock + numBlocks - 1

	t.Log("=============================================================")
	t.Log("         TRACE ALL PROFILING")
	t.Log("=============================================================")
	t.Logf("Block range: %d to %d (%d blocks)", startBlock, endBlock, numBlocks)
	t.Log("")

	// Start CPU profiling
	cpuFile, err := os.Create("traceall_cpu.prof")
	if err != nil {
		t.Fatalf("Failed to create CPU profile: %v", err)
	}
	defer cpuFile.Close()

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		t.Fatalf("Failed to start CPU profile: %v", err)
	}

	config := ScanConfig{
		DataDir:          dataDir,
		SparkLendAddress: DefaultSparkLendPool,
		StartBlock:       startBlock,
		EndBlock:         endBlock,
		FailedOnly:       true,
		ScanAll:          false,
		TraceAll:         true,
		NumWorkers:       4, // 4 workers - good balance of parallelism
	}

	start := time.Now()
	result, err := ScanLiquidations(ctx, config)
	duration := time.Since(start)

	pprof.StopCPUProfile()

	if err != nil {
		t.Fatalf("ScanLiquidations failed: %v", err)
	}

	// Write memory profile
	memFile, err := os.Create("traceall_mem.prof")
	if err != nil {
		t.Fatalf("Failed to create memory profile: %v", err)
	}
	defer memFile.Close()

	runtime.GC() // Force GC for accurate memory stats
	if err := pprof.WriteHeapProfile(memFile); err != nil {
		t.Fatalf("Failed to write memory profile: %v", err)
	}

	// Results
	t.Logf("Results:")
	t.Logf("  Duration: %v", duration)
	t.Logf("  Transactions processed: %d", result.CandidateTxs)
	t.Logf("  Liquidations found: %d", len(result.Liquidations))
	if result.CandidateTxs > 0 {
		t.Logf("  Throughput: %.2f tx/sec", float64(result.CandidateTxs)/duration.Seconds())
		t.Logf("  Time per tx: %.2f ms", duration.Seconds()*1000/float64(result.CandidateTxs))
	}
	t.Log("")
	t.Log("Profiles saved:")
	t.Log("  CPU:    traceall_cpu.prof")
	t.Log("  Memory: traceall_mem.prof")
	t.Log("")
	t.Log("Analyze with:")
	t.Log("  go tool pprof -top traceall_cpu.prof")
	t.Log("  go tool pprof -top traceall_mem.prof")
	t.Log("  go tool pprof -http=:8080 traceall_cpu.prof")
}

// TestTraceAllBlockBreakdown measures time spent in each phase of block processing
// Run with: go test -run TestTraceAllBlockBreakdown -v -timeout 5m
func TestTraceAllBlockBreakdown(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping breakdown test in short mode")
	}

	dataDir := DefaultDataDir
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Skipf("Data directory %s does not exist, skipping test", dataDir)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Small range to analyze in detail
	startBlock := uint64(22152420)
	endBlock := uint64(22152430) // Just 11 blocks

	t.Log("=============================================================")
	t.Log("         TRACE ALL - BLOCK PROCESSING BREAKDOWN")
	t.Log("=============================================================")
	t.Logf("Block range: %d to %d", startBlock, endBlock)
	t.Log("")

	config := ScanConfig{
		DataDir:          dataDir,
		SparkLendAddress: DefaultSparkLendPool,
		StartBlock:       startBlock,
		EndBlock:         endBlock,
		FailedOnly:       true,
		ScanAll:          false,
		TraceAll:         true,
		NumWorkers:       4, // 4 workers - good balance of parallelism
	}

	// Time the full scan
	start := time.Now()
	result, err := ScanLiquidations(ctx, config)
	totalDuration := time.Since(start)

	if err != nil {
		t.Fatalf("ScanLiquidations failed: %v", err)
	}

	numBlocks := endBlock - startBlock + 1
	txCount := result.CandidateTxs

	t.Logf("Summary:")
	t.Logf("  Total time: %v", totalDuration)
	t.Logf("  Blocks: %d", numBlocks)
	t.Logf("  Transactions: %d", txCount)
	t.Logf("  Avg txs/block: %.1f", float64(txCount)/float64(numBlocks))
	t.Log("")
	t.Logf("Per-unit timing:")
	t.Logf("  Per block: %.2f ms", totalDuration.Seconds()*1000/float64(numBlocks))
	if txCount > 0 {
		t.Logf("  Per transaction: %.2f ms", totalDuration.Seconds()*1000/float64(txCount))
	}
	t.Log("")
	t.Logf("Throughput:")
	t.Logf("  Blocks/sec: %.2f", float64(numBlocks)/totalDuration.Seconds())
	if txCount > 0 {
		t.Logf("  Tx/sec: %.2f", float64(txCount)/totalDuration.Seconds())
	}
	t.Log("")
	t.Logf("Liquidations found: %d", len(result.Liquidations))
}

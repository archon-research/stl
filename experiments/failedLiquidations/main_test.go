package main

import (
	"context"
	"encoding/json"
	"os"
	"runtime"
	"runtime/pprof"
	"testing"
	"time"
)

// BenchmarkResult stores timing information for a benchmark run
type BenchmarkResult struct {
	Timestamp       string        `json:"timestamp"`
	Description     string        `json:"description"`
	NumWorkers      int           `json:"num_workers"`
	StartBlock      uint64        `json:"start_block"`
	EndBlock        uint64        `json:"end_block"`
	NumBlocks       uint64        `json:"num_blocks"`
	NumCandidateTxs int           `json:"num_candidate_txs"`
	NumLiquidations int           `json:"num_liquidations"`
	Duration        time.Duration `json:"duration_ns"`
	DurationStr     string        `json:"duration_str"`
	TxPerSecond     float64       `json:"tx_per_second"`
}

// saveBenchmarkResult appends a benchmark result to the results file
func saveBenchmarkResult(result BenchmarkResult) error {
	filename := "benchmark_results.json"

	var results []BenchmarkResult

	// Read existing results if file exists
	if data, err := os.ReadFile(filename); err == nil {
		_ = json.Unmarshal(data, &results)
	}

	results = append(results, result)

	data, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}

func TestParallelProcessingBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping benchmark test in short mode")
	}

	// Check if data directory exists
	dataDir := DefaultDataDir
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Skipf("Data directory %s does not exist, skipping test", dataDir)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test parameters
	// Use a block range known to have failed liquidations
	// Block 22152423 has known failed liquidations
	startBlock := uint64(22152400)
	endBlock := uint64(22152449) // 50 blocks

	// Number of workers = number of CPU cores
	numWorkers := runtime.NumCPU()
	t.Logf("Running benchmark with %d workers (CPU cores)", numWorkers)
	t.Logf("Block range: %d to %d (%d blocks)", startBlock, endBlock, endBlock-startBlock+1)

	// Build config
	config := ScanConfig{
		DataDir:          dataDir,
		SparkLendAddress: DefaultSparkLendPool,
		StartBlock:       startBlock,
		EndBlock:         endBlock,
		FailedOnly:       true,
		ScanAll:          false,
		TraceAll:         true, // Trace all transactions for consistent benchmarking
		NumWorkers:       numWorkers,
	}

	// Run the benchmark
	start := time.Now()

	result, err := ScanLiquidations(ctx, config)
	if err != nil {
		t.Fatalf("ScanLiquidations failed: %v", err)
	}

	duration := time.Since(start)

	var txPerSecond float64
	if result.CandidateTxs > 0 {
		txPerSecond = float64(result.CandidateTxs) / duration.Seconds()
	}

	t.Logf("Benchmark Results:")
	t.Logf("  Duration: %v", duration)
	t.Logf("  Blocks processed: %d", result.BlocksProcessed)
	t.Logf("  Transactions processed: %d", result.CandidateTxs)
	t.Logf("  Liquidations found: %d", len(result.Liquidations))
	t.Logf("  Throughput: %.2f tx/sec", txPerSecond)

	// Save results
	benchResult := BenchmarkResult{
		Timestamp:       time.Now().Format(time.RFC3339),
		Description:     "Baseline - Original parallel implementation",
		NumWorkers:      numWorkers,
		StartBlock:      startBlock,
		EndBlock:        endBlock,
		NumBlocks:       endBlock - startBlock + 1,
		NumCandidateTxs: result.CandidateTxs,
		NumLiquidations: len(result.Liquidations),
		Duration:        duration,
		DurationStr:     duration.String(),
		TxPerSecond:     txPerSecond,
	}

	if err := saveBenchmarkResult(benchResult); err != nil {
		t.Errorf("Failed to save benchmark result: %v", err)
	}

	t.Logf("Benchmark result saved to benchmark_results.json")
}

// TestSingleWorkerBenchmark tests with just 1 worker for comparison
func TestSingleWorkerBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping benchmark test in short mode")
	}

	// Check if data directory exists
	dataDir := DefaultDataDir
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Skipf("Data directory %s does not exist, skipping test", dataDir)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Block 22152423 has known failed liquidations
	startBlock := uint64(22152400)
	endBlock := uint64(22152449) // 50 blocks

	numWorkers := 1
	t.Logf("Running benchmark with %d worker", numWorkers)
	t.Logf("Block range: %d to %d (%d blocks)", startBlock, endBlock, endBlock-startBlock+1)

	config := ScanConfig{
		DataDir:          dataDir,
		SparkLendAddress: DefaultSparkLendPool,
		StartBlock:       startBlock,
		EndBlock:         endBlock,
		FailedOnly:       true,
		ScanAll:          false,
		TraceAll:         true,
		NumWorkers:       numWorkers,
	}

	start := time.Now()

	result, err := ScanLiquidations(ctx, config)
	if err != nil {
		t.Fatalf("ScanLiquidations failed: %v", err)
	}

	duration := time.Since(start)

	var txPerSecond float64
	if result.CandidateTxs > 0 {
		txPerSecond = float64(result.CandidateTxs) / duration.Seconds()
	}

	t.Logf("Benchmark Results:")
	t.Logf("  Duration: %v", duration)
	t.Logf("  Blocks processed: %d", result.BlocksProcessed)
	t.Logf("  Transactions processed: %d", result.CandidateTxs)
	t.Logf("  Liquidations found: %d", len(result.Liquidations))
	t.Logf("  Throughput: %.2f tx/sec", txPerSecond)

	benchResult := BenchmarkResult{
		Timestamp:       time.Now().Format(time.RFC3339),
		Description:     "Baseline - Single worker",
		NumWorkers:      numWorkers,
		StartBlock:      startBlock,
		EndBlock:        endBlock,
		NumBlocks:       endBlock - startBlock + 1,
		NumCandidateTxs: result.CandidateTxs,
		NumLiquidations: len(result.Liquidations),
		Duration:        duration,
		DurationStr:     duration.String(),
		TxPerSecond:     txPerSecond,
	}

	if err := saveBenchmarkResult(benchResult); err != nil {
		t.Errorf("Failed to save benchmark result: %v", err)
	}

	t.Logf("Benchmark result saved to benchmark_results.json")
}

// TestProfilingAnalysis runs the scanner with CPU profiling to identify bottlenecks
// Run with: go test -v -run TestProfilingAnalysis -timeout 10m
// Then analyze with: go tool pprof -http=:8080 cpu_profile.prof
func TestProfilingAnalysis(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping profiling test in short mode")
	}

	// Check if data directory exists
	dataDir := DefaultDataDir
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Skipf("Data directory %s does not exist, skipping test", dataDir)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use 1000 blocks with TracesToIdx for realistic profiling
	startBlock := uint64(22152000)
	endBlock := uint64(22152999) // 1000 blocks

	numWorkers := runtime.NumCPU()
	t.Logf("Running profiling test with %d workers", numWorkers)
	t.Logf("Block range: %d to %d (%d blocks)", startBlock, endBlock, endBlock-startBlock+1)

	// === CPU PROFILING ===
	cpuFile, err := os.Create("cpu_profile.prof")
	if err != nil {
		t.Fatalf("Failed to create CPU profile file: %v", err)
	}
	defer cpuFile.Close()

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		t.Fatalf("Failed to start CPU profile: %v", err)
	}
	t.Log("CPU profiling started...")

	// === BLOCK PROFILING (for channel/mutex contention) ===
	// Set block profile rate - 1 means capture all blocking events
	runtime.SetBlockProfileRate(1)
	t.Log("Block profiling enabled...")

	// === MUTEX PROFILING ===
	runtime.SetMutexProfileFraction(1)
	t.Log("Mutex profiling enabled...")

	config := ScanConfig{
		DataDir:          dataDir,
		SparkLendAddress: DefaultSparkLendPool,
		StartBlock:       startBlock,
		EndBlock:         endBlock,
		FailedOnly:       true,
		ScanAll:          false,
		TraceAll:         false, // Use TracesToIdx for realistic profiling
		NumWorkers:       numWorkers,
	}

	// Time different phases
	t.Log("=== PHASE TIMING ===")

	// Phase 1: Full scan (includes index + processing)
	fullStart := time.Now()
	result, err := ScanLiquidations(ctx, config)
	fullDuration := time.Since(fullStart)

	pprof.StopCPUProfile()
	t.Log("CPU profiling stopped.")

	// Disable profiling
	runtime.SetBlockProfileRate(0)
	runtime.SetMutexProfileFraction(0)

	if err != nil {
		t.Fatalf("ScanLiquidations failed: %v", err)
	}

	// === MEMORY PROFILING ===
	memFile, err := os.Create("mem_profile.prof")
	if err != nil {
		t.Fatalf("Failed to create memory profile file: %v", err)
	}
	defer memFile.Close()

	runtime.GC() // Force GC to get accurate memory stats
	if err := pprof.WriteHeapProfile(memFile); err != nil {
		t.Fatalf("Failed to write memory profile: %v", err)
	}
	t.Log("Memory profile written.")

	// === BLOCK PROFILE (channel/sync blocking) ===
	blockFile, err := os.Create("block_profile.prof")
	if err != nil {
		t.Fatalf("Failed to create block profile file: %v", err)
	}
	defer blockFile.Close()

	if err := pprof.Lookup("block").WriteTo(blockFile, 0); err != nil {
		t.Fatalf("Failed to write block profile: %v", err)
	}
	t.Log("Block profile written.")

	// === MUTEX PROFILE ===
	mutexFile, err := os.Create("mutex_profile.prof")
	if err != nil {
		t.Fatalf("Failed to create mutex profile file: %v", err)
	}
	defer mutexFile.Close()

	if err := pprof.Lookup("mutex").WriteTo(mutexFile, 0); err != nil {
		t.Fatalf("Failed to write mutex profile: %v", err)
	}
	t.Log("Mutex profile written.")

	// === GOROUTINE PROFILE ===
	goroutineFile, err := os.Create("goroutine_profile.prof")
	if err != nil {
		t.Fatalf("Failed to create goroutine profile file: %v", err)
	}
	defer goroutineFile.Close()

	if err := pprof.Lookup("goroutine").WriteTo(goroutineFile, 0); err != nil {
		t.Fatalf("Failed to write goroutine profile: %v", err)
	}
	t.Log("Goroutine profile written.")

	// === RESULTS ===
	t.Log("")
	t.Log("=== PROFILING RESULTS ===")
	t.Logf("Total Duration: %v", fullDuration)
	t.Logf("Blocks processed: %d", result.BlocksProcessed)
	t.Logf("Transactions processed: %d", result.CandidateTxs)
	t.Logf("Liquidations found: %d", len(result.Liquidations))

	if result.CandidateTxs > 0 {
		t.Logf("Throughput: %.2f tx/sec", float64(result.CandidateTxs)/fullDuration.Seconds())
		t.Logf("Avg time per tx: %.2f ms", fullDuration.Seconds()*1000/float64(result.CandidateTxs))
	}

	// Memory stats
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	t.Logf("Heap in use: %.2f MB", float64(memStats.HeapInuse)/1024/1024)
	t.Logf("Total allocated: %.2f MB", float64(memStats.TotalAlloc)/1024/1024)
	t.Logf("Num GC cycles: %d", memStats.NumGC)

	t.Log("")
	t.Log("=== PROFILE FILES CREATED ===")
	t.Log("CPU profile:       cpu_profile.prof")
	t.Log("Memory profile:    mem_profile.prof")
	t.Log("Block profile:     block_profile.prof   (channel/sync blocking)")
	t.Log("Mutex profile:     mutex_profile.prof   (mutex contention)")
	t.Log("Goroutine profile: goroutine_profile.prof")
	t.Log("")
	t.Log("To analyze (text output):")
	t.Log("  go tool pprof -top cpu_profile.prof")
	t.Log("  go tool pprof -top -cum cpu_profile.prof")
	t.Log("  go tool pprof -top block_profile.prof")
	t.Log("  go tool pprof -top mutex_profile.prof")
}

// TestIndexScanTiming specifically measures just the index scan portion
func TestIndexScanTiming(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping index scan timing test in short mode")
	}

	dataDir := DefaultDataDir
	if _, err := os.Stat(dataDir); os.IsNotExist(err) {
		t.Skipf("Data directory %s does not exist, skipping test", dataDir)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Test with a large block range to see index scan performance
	startBlock := uint64(17203646) // SparkLend deployment
	endBlock := uint64(18000000)   // ~800k blocks

	t.Logf("Testing index scan on blocks %d to %d (%d blocks)", startBlock, endBlock, endBlock-startBlock+1)

	// Use TracesToIdx mode (not TraceAll) to exercise the index
	config := ScanConfig{
		DataDir:          dataDir,
		SparkLendAddress: DefaultSparkLendPool,
		StartBlock:       startBlock,
		EndBlock:         endBlock,
		FailedOnly:       true,
		ScanAll:          false,
		TraceAll:         false, // Use TracesToIdx
		NumWorkers:       1,     // Single worker to isolate index scan time
	}

	start := time.Now()
	result, err := ScanLiquidations(ctx, config)
	duration := time.Since(start)

	if err != nil {
		t.Fatalf("ScanLiquidations failed: %v", err)
	}

	t.Log("")
	t.Log("=== INDEX SCAN TIMING RESULTS ===")
	t.Logf("Total Duration: %v", duration)
	t.Logf("Blocks in range: %d", endBlock-startBlock+1)
	t.Logf("Candidates found: %d", result.CandidateTxs)
	t.Logf("Unique blocks with candidates: %d", result.BlocksProcessed)
	t.Logf("Liquidations found: %d", len(result.Liquidations))

	if result.CandidateTxs > 0 {
		t.Logf("Time per candidate (index+process): %.2f ms", duration.Seconds()*1000/float64(result.CandidateTxs))
	}

	// Estimate: if processing is 0.7 tx/sec, how much was index vs processing?
	if result.CandidateTxs > 0 {
		estimatedProcessingTime := float64(result.CandidateTxs) / 0.7 // Based on observed 0.7 tx/sec
		actualSeconds := duration.Seconds()
		t.Log("")
		t.Logf("Estimated breakdown (assuming 0.7 tx/sec processing):")
		t.Logf("  Processing time: ~%.1f sec (%.1f%%)", estimatedProcessingTime, estimatedProcessingTime/actualSeconds*100)
		t.Logf("  Index scan time: ~%.1f sec (%.1f%%)", actualSeconds-estimatedProcessingTime, (actualSeconds-estimatedProcessingTime)/actualSeconds*100)
	}
}

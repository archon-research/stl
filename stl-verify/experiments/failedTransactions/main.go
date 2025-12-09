package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ethereum/go-ethereum/ethclient"
)

const (
	// SparkLend Pool contract address on Ethereum mainnet
	// This is the main entry point for supply, borrow, repay, withdraw, liquidationCall
	DefaultSparkLendPool = "0xC13e21B648A5Ee794902342038FF3aDAB66BE987"

	// Default RPC endpoint
	DefaultRPCEndpoint = "http://100.87.11.17:8545"

	// Default database path
	DefaultDBPath = "failed_transactions.db"

	// SparkLend deployment block on Ethereum mainnet (approximate)
	DefaultStartBlock = 17203646
)

func main() {
	// Command line flags
	rpcEndpoint := flag.String("rpc", DefaultRPCEndpoint, "Ethereum RPC endpoint")
	dbPath := flag.String("db", DefaultDBPath, "SQLite database path")
	sparkLendAddress := flag.String("contract", DefaultSparkLendPool, "SparkLend Pool contract address")
	startBlock := flag.Uint64("start", DefaultStartBlock, "Start block number")
	endBlock := flag.Uint64("end", 0, "End block number (0 for latest)")
	concurrency := flag.Int("concurrency", 500, "Number of concurrent workers for block processing")
	checkpointInterval := flag.Uint64("checkpoint", 1000, "Save sync state every N blocks")
	useTracing := flag.Bool("trace", false, "Use debug_traceTransaction for 100% accurate detection (slower, requires archive node)")
	flag.Parse()

	log.Printf("SparkLend Failed Transaction Scanner")
	log.Printf("=====================================")
	log.Printf("RPC Endpoint: %s", *rpcEndpoint)
	log.Printf("Database: %s", *dbPath)
	log.Printf("Contract: %s", *sparkLendAddress)
	log.Printf("Start Block: %d", *startBlock)
	if *endBlock > 0 {
		log.Printf("End Block: %d", *endBlock)
	} else {
		log.Printf("End Block: latest")
	}
	log.Printf("Concurrency: %d workers", *concurrency)
	log.Printf("Checkpoint interval: %d blocks", *checkpointInterval)
	if *useTracing {
		log.Printf("Detection mode: TRACING (100%% accurate, requires debug API)")
	} else {
		log.Printf("Detection mode: HEURISTIC (fast, may miss some indirect calls)")
	}
	log.Printf("=====================================")

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigChan
		log.Printf("Received signal %v, initiating graceful shutdown...", sig)
		cancel()
	}()

	// Connect to Ethereum client
	client, err := ethclient.Dial(*rpcEndpoint)
	if err != nil {
		log.Fatalf("Failed to connect to Ethereum client: %v", err)
	}
	defer client.Close()

	// Verify connection
	chainID, err := client.ChainID(ctx)
	if err != nil {
		log.Fatalf("Failed to get chain ID: %v", err)
	}
	log.Printf("Connected to chain ID: %d", chainID)

	// Initialize database
	db, err := NewDatabase(*dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer db.Close()

	// Get current stats
	count, err := db.GetFailedTransactionCount()
	if err != nil {
		log.Printf("Warning: failed to get transaction count: %v", err)
	} else {
		log.Printf("Existing failed transactions in database: %d", count)
	}

	// Check sync state
	syncState, err := db.GetSyncState()
	if err != nil {
		log.Printf("Warning: failed to get sync state: %v", err)
	} else if syncState != nil {
		log.Printf("Last synced block: %d", syncState.LastProcessedBlock)
	}

	// Create and start scanner
	scanner := NewScanner(client, *rpcEndpoint, db, *sparkLendAddress, *startBlock, *endBlock, *concurrency, *checkpointInterval, *useTracing)
	if err := scanner.Start(ctx); err != nil {
		if err == context.Canceled {
			log.Printf("Scan interrupted by user")
		} else {
			log.Fatalf("Scanner error: %v", err)
		}
	}

	// Final stats
	finalCount, err := db.GetFailedTransactionCount()
	if err != nil {
		log.Printf("Warning: failed to get final transaction count: %v", err)
	} else {
		log.Printf("Total failed transactions in database: %d", finalCount)
		if count > 0 {
			log.Printf("New failed transactions found: %d", finalCount-count)
		}
	}

	fmt.Println("\nScan complete!")
}

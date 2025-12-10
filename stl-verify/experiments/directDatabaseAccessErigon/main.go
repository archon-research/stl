package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/mdbx"
	"github.com/erigontech/erigon/db/rawdb"
)

func main() {
	// Default Erigon data directory
	dataDir := os.Getenv("ERIGON_DATA_DIR")
	if dataDir == "" {
		dataDir = "/mnt/data" // Common location, adjust as needed
	}

	chainDataPath := filepath.Join(dataDir, "chaindata")

	log.Printf("Opening Erigon database at: %s", chainDataPath)

	ctx := context.Background()

	// Open the MDBX database in read-only mode
	db, err := mdbx.New(kv.Label("chaindata"), nil).
		Path(chainDataPath).
		Readonly(true).
		Open(ctx)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Start a read-only transaction
	tx, err := db.BeginRo(ctx)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}
	defer tx.Rollback()

	// Get the current block number (head of the chain)
	headBlockNum := rawdb.ReadCurrentBlockNumber(tx)
	if headBlockNum == nil {
		log.Fatal("Failed to read current block number")
	}

	fmt.Printf("Current block number: %d\n", *headBlockNum)

	// Get the block hash for the head block
	headHash, err := rawdb.ReadCanonicalHash(tx, *headBlockNum)
	if err != nil {
		log.Fatalf("Failed to read canonical hash: %v", err)
	}
	fmt.Printf("Head block hash: %s\n", headHash.Hex())

	// Read the full block header
	header := rawdb.ReadHeader(tx, headHash, *headBlockNum)
	if header == nil {
		log.Fatal("Failed to read block header")
	}

	fmt.Printf("\nBlock Header Details:\n")
	fmt.Printf("  Number:      %d\n", header.Number.Uint64())
	fmt.Printf("  Hash:        %s\n", headHash.Hex())
	fmt.Printf("  Parent:      %s\n", header.ParentHash.Hex())
	fmt.Printf("  Timestamp:   %d\n", header.Time)
	fmt.Printf("  Gas Limit:   %d\n", header.GasLimit)
	fmt.Printf("  Gas Used:    %d\n", header.GasUsed)
	fmt.Printf("  Coinbase:    %s\n", header.Coinbase.Hex())
	fmt.Printf("  TxHash:      %s\n", header.TxHash.Hex())
	fmt.Printf("  ReceiptHash: %s\n", header.ReceiptHash.Hex())

	if header.BaseFee != nil {
		fmt.Printf("  BaseFee:     %s\n", header.BaseFee.String())
	}

	// Read the block body (transactions)
	body, _, _ := rawdb.ReadBody(tx, headHash, *headBlockNum)
	if body != nil {
		fmt.Printf("\nBlock Body:\n")
		fmt.Printf("  Transactions: %d\n", len(body.Transactions))
		fmt.Printf("  Uncles:       %d\n", len(body.Uncles))
		if body.Withdrawals != nil {
			fmt.Printf("  Withdrawals:  %d\n", len(body.Withdrawals))
		}
	}

	log.Println("\nDirect database access successful!")
}

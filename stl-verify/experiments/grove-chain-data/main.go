package main

import (
	"context"
	"fmt"
	"log"

	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {
	ctx := context.Background()

	// client, err := ethclient.Dial("http://100.87.11.17:8545")
	client, err := ethclient.Dial("https://ip-10-0-1-50.tail25f1d9.ts.net")
	if err != nil {
		log.Fatalf("Failed to connect to the Ethereum client: %v", err)
	}
	fmt.Println("Successfully connected to the Ethereum client")

	holdings, err := GetHoldings(client, ctx)
	if err != nil {
		log.Fatalf("Failed to get holdings: %v", err)
	}

	for _, holding := range holdings {
		fmt.Printf("Token: %s, Balance: %s\n", holding.ContractAddress, holding.Balance.String())
	}
}

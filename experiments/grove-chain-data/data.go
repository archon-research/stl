package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/archon-research/stl/stl-verify/experiments/grove-chain-data/resources"
	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

type Holding struct {
	resources.TokenData
	Balance *big.Int
}

func GetHoldings(client *ethclient.Client, ctx context.Context) ([]Holding, error) {
	chainID, err := client.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	var chainName string
	switch chainID.Int64() {
	case 1:
		chainName = "ethereum"
	case 43114:
		chainName = "avalanche"
	case 98865: // Plume Mainnet
		chainName = "plume"
	case 10143: // Monad Testnet (example ID, adjust as needed)
		chainName = "monad"
	default:
		return nil, fmt.Errorf("unsupported chain ID: %d", chainID)
	}

	tokens, ok := resources.GroveTokensData[chainName]
	if !ok {
		return []Holding{}, nil
	}

	var holdings []Holding
	for _, token := range tokens {
		balance, err := getTokenBalance(ctx, client, token.ContractAddress, token.WalletAddress)
		if err != nil {
			// Log error or continue? For now, let's return error or maybe 0 balance with error logged?
			// Returning error fails the whole batch. Let's wrap error or just fail.
			return nil, fmt.Errorf("failed to get balance for %s: %w", token.ContractAddress, err)
		}

		holdings = append(holdings, Holding{
			TokenData: token,
			Balance:   balance,
		})
	}

	return holdings, nil
}

func getTokenBalance(ctx context.Context, client *ethclient.Client, contractAddress, walletAddress string) (*big.Int, error) {
	// ERC20 balanceOf selector: 0x70a08231
	// balanceOf(address)

	if contractAddress == "0x0000000000000000000000000000000000000000" {
		// Native ETH/Token
		return client.BalanceAt(ctx, common.HexToAddress(walletAddress), nil)
	}

	contract := common.HexToAddress(contractAddress)
	wallet := common.HexToAddress(walletAddress)

	// Construct call data
	// selector (4 bytes) + address (32 bytes)
	data := make([]byte, 0, 36)
	data = append(data, common.Hex2Bytes("70a08231")...)
	data = append(data, common.LeftPadBytes(wallet.Bytes(), 32)...)

	msg := ethereum.CallMsg{
		To:   &contract,
		Data: data,
	}

	result, err := client.CallContract(ctx, msg, nil)
	if err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return big.NewInt(0), nil
	}

	return new(big.Int).SetBytes(result), nil
}

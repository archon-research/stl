package testutil

import (
	"math/big"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
)

// PackAssetPrices ABI-encodes prices as getAssetsPrices() return data.
func PackAssetPrices(t *testing.T, prices []*big.Int) []byte {
	t.Helper()
	oracleABI, err := abis.GetAaveOracleABI()
	if err != nil {
		t.Fatalf("loading oracle ABI: %v", err)
	}
	data, err := oracleABI.Methods["getAssetsPrices"].Outputs.Pack(prices)
	if err != nil {
		t.Fatalf("packing prices: %v", err)
	}
	return data
}

// PackAssetPrice ABI-encodes a single price as getAssetPrice() return data (uint256).
func PackAssetPrice(t *testing.T, price *big.Int) []byte {
	t.Helper()
	oracleABI, err := abis.GetAaveOracleABI()
	if err != nil {
		t.Fatalf("loading oracle ABI: %v", err)
	}
	data, err := oracleABI.Methods["getAssetPrice"].Outputs.Pack(price)
	if err != nil {
		t.Fatalf("packing price: %v", err)
	}
	return data
}

// PackLatestRoundData ABI-encodes latestRoundData() return data.
func PackLatestRoundData(t *testing.T, roundID *big.Int, answer *big.Int, startedAt *big.Int, updatedAt *big.Int, answeredInRound *big.Int) []byte {
	t.Helper()
	feedABI, err := abis.GetAggregatorV3ABI()
	if err != nil {
		t.Fatalf("loading AggregatorV3 ABI: %v", err)
	}
	data, err := feedABI.Methods["latestRoundData"].Outputs.Pack(roundID, answer, startedAt, updatedAt, answeredInRound)
	if err != nil {
		t.Fatalf("packing latestRoundData: %v", err)
	}
	return data
}

// PackLatestAnswer ABI-encodes latestAnswer() return data.
func PackLatestAnswer(t *testing.T, answer *big.Int) []byte {
	t.Helper()
	feedABI, err := abis.GetAggregatorV3ABI()
	if err != nil {
		t.Fatalf("loading AggregatorV3 ABI: %v", err)
	}
	data, err := feedABI.Methods["latestAnswer"].Outputs.Pack(answer)
	if err != nil {
		t.Fatalf("packing latestAnswer: %v", err)
	}
	return data
}

// MulticallResult matches the multicall3 aggregate3 output tuple.
type MulticallResult struct {
	Success    bool
	ReturnData []byte
}

// PackMulticallAggregate3 ABI-encodes results as aggregate3 return data.
func PackMulticallAggregate3(t *testing.T, results []MulticallResult) []byte {
	t.Helper()
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("loading multicall3 ABI: %v", err)
	}
	data, err := multicallABI.Methods["aggregate3"].Outputs.Pack(results)
	if err != nil {
		t.Fatalf("packing aggregate3: %v", err)
	}
	return data
}

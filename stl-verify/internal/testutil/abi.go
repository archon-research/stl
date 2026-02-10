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

//go:build integration && e2e

package aavelike

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/chainutil"
)

const reserveFactsTimeout = 2 * time.Minute

// TestRealReserveFacts_L2 reads registered L2 Aave V3 market reserves at
// spot-checked blocks over the real RPC and requires the registry to resolve the
// PoolDataProvider that actually served those values (ARCT-213). Each chain gets
// its current provider (USDC) and one earlier interval (WETH). Run it with
// `make e2e-real-reserve-facts`; it skips without ALCHEMY_API_KEY.
func TestRealReserveFacts_L2(t *testing.T) {
	apiKey := os.Getenv("ALCHEMY_API_KEY")
	if apiKey == "" {
		t.Skip("ALCHEMY_API_KEY not set")
	}

	tests := []struct {
		slug             string
		rpcBase          string
		block            int64
		poolDataProvider string
		reserve          string
		wantLTV          int64
		wantLiqThreshold int64
	}{
		{
			slug:             "aave_v3_arbitrum",
			rpcBase:          "https://arb-mainnet.g.alchemy.com/v2",
			block:            503000000,
			poolDataProvider: "0x243Aa95cAC2a25651eda86e80bEe66114413c43b",
			reserve:          "0xaf88d065e77c8cC2239327C5EDb3A432268e5831",
			wantLTV:          7500,
			wantLiqThreshold: 7800,
		},
		{
			// WETH inside the first of six provider intervals, so a wrong
			// older address or ActiveAtBlock fails here rather than during backfill.
			slug:             "aave_v3_arbitrum",
			rpcBase:          "https://arb-mainnet.g.alchemy.com/v2",
			block:            50000000,
			poolDataProvider: "0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654",
			reserve:          "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1",
			wantLTV:          8000,
			wantLiqThreshold: 8250,
		},
		{
			slug:             "aave_v3_optimism",
			rpcBase:          "https://opt-mainnet.g.alchemy.com/v2",
			block:            156535904,
			poolDataProvider: "0x243Aa95cAC2a25651eda86e80bEe66114413c43b",
			reserve:          "0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85",
			wantLTV:          7500,
			wantLiqThreshold: 7800,
		},
		{
			// WETH inside the third of the six Optimism intervals.
			slug:             "aave_v3_optimism",
			rpcBase:          "https://opt-mainnet.g.alchemy.com/v2",
			block:            125000000,
			poolDataProvider: "0x7deEB8aCE4220643D8edeC871a23807E4d006eE5",
			reserve:          "0x4200000000000000000000000000000000000006",
			wantLTV:          8000,
			wantLiqThreshold: 8250,
		},
		{
			slug:             "aave_v3_base",
			rpcBase:          "https://base-mainnet.g.alchemy.com/v2",
			block:            51041328,
			poolDataProvider: "0x0F43731EB8d45A581f4a36DD74F5f358bc90C73A",
			reserve:          "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
			wantLTV:          7500,
			wantLiqThreshold: 7800,
		},
		{
			// WETH inside the first of the five Base intervals.
			slug:             "aave_v3_base",
			rpcBase:          "https://base-mainnet.g.alchemy.com/v2",
			block:            10000000,
			poolDataProvider: "0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac",
			reserve:          "0x4200000000000000000000000000000000000006",
			wantLTV:          8000,
			wantLiqThreshold: 8300,
		},
	}

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("loading ERC20 ABI: %v", err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s/%d", tt.slug, tt.block), func(t *testing.T) {
			key, _, ok := blockchain.GetProtocolBySlug(tt.slug)
			if !ok {
				t.Fatalf("slug %q is not registered", tt.slug)
			}
			provider, ok := blockchain.GetPoolDataProviderForBlock(key.ChainID, key.PoolAddress, uint64(tt.block))
			if !ok {
				t.Fatalf("no PoolDataProvider registered for %s at block %d", tt.slug, tt.block)
			}
			if want := common.HexToAddress(tt.poolDataProvider); provider != want {
				t.Fatalf("PoolDataProvider at block %d = %s, want %s", tt.block, provider.Hex(), want.Hex())
			}

			ctx, cancel := context.WithTimeout(context.Background(), reserveFactsTimeout)
			defer cancel()

			ethClient, err := ethclient.DialContext(ctx, tt.rpcBase+"/"+apiKey)
			if err != nil {
				t.Fatalf("dialing %s: %s", tt.rpcBase, redactAPIKey(err, apiKey))
			}
			t.Cleanup(ethClient.Close)
			if err := chainutil.AssertChainID(ctx, ethClient, key.ChainID); err != nil {
				t.Fatalf("%s: %s", tt.rpcBase, redactAPIKey(err, apiKey))
			}

			multicaller, err := multicall.NewClient(ethClient, blockchain.Multicall3)
			if err != nil {
				t.Fatalf("creating multicall client: %v", err)
			}
			service, err := NewPositionReader(ethClient, multicaller, erc20ABI, logger).
				GetOrCreateBlockchainService(key.ChainID, key.PoolAddress)
			if err != nil {
				t.Fatalf("creating blockchain service: %v", err)
			}

			_, config, _, err := service.GetFullReserveData(ctx, common.HexToAddress(tt.reserve), tt.block, common.Hash{})
			if err != nil {
				t.Fatalf("GetFullReserveData(%s, %d): %s", tt.reserve, tt.block, redactAPIKey(err, apiKey))
			}
			if got := config.LTV.Int64(); got != tt.wantLTV {
				t.Errorf("ltv = %d, want %d", got, tt.wantLTV)
			}
			if got := config.LiquidationThreshold.Int64(); got != tt.wantLiqThreshold {
				t.Errorf("liquidationThreshold = %d, want %d", got, tt.wantLiqThreshold)
			}
		})
	}
}

// redactAPIKey keeps the API key out of test output: go-ethereum's transport
// errors embed the full request URL.
func redactAPIKey(err error, apiKey string) string {
	return strings.ReplaceAll(err.Error(), apiKey, "<redacted>")
}

// Chains whose historical state predates a migration the archive RPC does not
// serve: Arbitrum Nitro (block 22207817) and Optimism Bedrock (block 105235063).
// Rotations below these are verifiable only from the PoolDataProviderUpdated
// logs, which is how they were sourced.
var rpcStateFloor = map[int64]uint64{
	42161: 22207817,
	10:    105235063,
}

// TestRealPoolDataProviderRotations_L2 checks every PoolDataProviderHistory entry
// against the chain rather than against the registry: at each ActiveAtBlock the
// market's PoolAddressesProvider must already return that entry's address, and one
// block earlier it must not. A wrong address or a rotation block off in either
// direction therefore fails here instead of silently serving a backfill from the
// wrong provider. Run it with `make e2e-real-reserve-facts`.
func TestRealPoolDataProviderRotations_L2(t *testing.T) {
	apiKey := os.Getenv("ALCHEMY_API_KEY")
	if apiKey == "" {
		t.Skip("ALCHEMY_API_KEY not set")
	}

	providerABI, err := abis.ParseABI(`[{"inputs":[],"name":"getPoolDataProvider",` +
		`"outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"}]`)
	if err != nil {
		t.Fatalf("parsing PoolAddressesProvider ABI: %v", err)
	}
	callData, err := providerABI.Pack("getPoolDataProvider")
	if err != nil {
		t.Fatalf("packing getPoolDataProvider: %v", err)
	}

	markets := []struct {
		slug    string
		rpcBase string
	}{
		{"aave_v3_arbitrum", "https://arb-mainnet.g.alchemy.com/v2"},
		{"aave_v3_optimism", "https://opt-mainnet.g.alchemy.com/v2"},
		{"aave_v3_base", "https://base-mainnet.g.alchemy.com/v2"},
	}

	for _, market := range markets {
		key, config, ok := blockchain.GetProtocolBySlug(market.slug)
		if !ok {
			t.Fatalf("slug %q is not registered", market.slug)
		}

		ctx, cancel := context.WithTimeout(context.Background(), reserveFactsTimeout)
		defer cancel()

		ethClient, err := ethclient.DialContext(ctx, market.rpcBase+"/"+apiKey)
		if err != nil {
			t.Fatalf("dialing %s: %s", market.rpcBase, redactAPIKey(err, apiKey))
		}
		t.Cleanup(ethClient.Close)
		if err := chainutil.AssertChainID(ctx, ethClient, key.ChainID); err != nil {
			t.Fatalf("%s: %s", market.rpcBase, redactAPIKey(err, apiKey))
		}

		readProvider := func(t *testing.T, block uint64) common.Address {
			t.Helper()
			out, err := ethClient.CallContract(ctx, ethereum.CallMsg{
				To:   &config.PoolAddressesProvider.Address,
				Data: callData,
			}, new(big.Int).SetUint64(block))
			if err != nil {
				t.Fatalf("getPoolDataProvider at block %d: %s", block, redactAPIKey(err, apiKey))
			}
			return common.BytesToAddress(out)
		}

		for i, entry := range config.PoolDataProviderHistory {
			t.Run(fmt.Sprintf("%s/entry%d", market.slug, i), func(t *testing.T) {
				if floor := rpcStateFloor[key.ChainID]; entry.ActiveAtBlock <= floor {
					t.Skipf("block %d predates the archive floor %d for chain %d",
						entry.ActiveAtBlock, floor, key.ChainID)
				}

				if got := readProvider(t, entry.ActiveAtBlock); got != entry.Address {
					t.Errorf("provider at ActiveAtBlock %d = %s, want %s",
						entry.ActiveAtBlock, got.Hex(), entry.Address.Hex())
				}

				// One block earlier the rotation must not have happened yet.
				if before := entry.ActiveAtBlock - 1; before > rpcStateFloor[key.ChainID] &&
					before >= config.PoolAddressesProvider.ActiveAtBlock {
					if got := readProvider(t, before); got == entry.Address {
						t.Errorf("provider at block %d is already %s, so ActiveAtBlock %d is too late",
							before, got.Hex(), entry.ActiveAtBlock)
					}
				}
			})
		}
	}
}

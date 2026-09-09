//go:build integration && e2e

package aavelike

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

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

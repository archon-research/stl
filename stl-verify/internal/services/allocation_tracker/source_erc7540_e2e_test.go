//go:build e2e

package allocation_tracker

import (
	"context"
	"io"
	"log/slog"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/multicall"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpchttp"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// e2ePosition is one real Centrifuge ERC-7540 vault position (VEC-337).
// expectedShare is the known tranche token the vault must resolve to via
// share(); the source's result is cross-checked against a direct
// balanceOf(expectedShare, wallet) read at the same block.
type e2ePosition struct {
	name          string
	vault         common.Address
	expectedShare common.Address
	wallet        common.Address
}

// TestERC7540Source_E2E verifies the source against live chain state through
// Alchemy. Skipped unless ALCHEMY_API_KEY is set. Run with:
//
//	ALCHEMY_API_KEY=<key> go test -tags=e2e -v -run TestERC7540Source_E2E ./internal/services/allocation_tracker/
func TestERC7540Source_E2E(t *testing.T) {
	key := os.Getenv("ALCHEMY_API_KEY")
	if key == "" {
		t.Skip("ALCHEMY_API_KEY not set")
	}

	groveMainnet := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")
	sparkMainnet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	groveAvax := common.HexToAddress("0x7107dd8f56642327945294a18a4280c78e153644")

	chains := []struct {
		name      string
		url       string
		positions []e2ePosition
	}{
		{
			name: "mainnet",
			url:  "https://eth-mainnet.g.alchemy.com/v2/" + key,
			positions: []e2ePosition{
				{
					name:          "grove JAAA",
					vault:         common.HexToAddress("0x4880799ee5200fc58da299e965df644fbf46780b"),
					expectedShare: common.HexToAddress("0x5a0f93d040de44e78f251b03c43be9cf317dcf64"),
					wallet:        groveMainnet,
				},
				{
					name:          "grove JTRSY",
					vault:         common.HexToAddress("0xfe6920eb6c421f1179ca8c8d4170530cdbdfd77a"),
					expectedShare: common.HexToAddress("0x8c213ee79581ff4984583c6a801e5263418c4b86"),
					wallet:        groveMainnet,
				},
				{
					name:          "spark JTRSY",
					vault:         common.HexToAddress("0xfe6920eb6c421f1179ca8c8d4170530cdbdfd77a"),
					expectedShare: common.HexToAddress("0x8c213ee79581ff4984583c6a801e5263418c4b86"),
					wallet:        sparkMainnet,
				},
			},
		},
		{
			name: "avalanche-c",
			url:  "https://avax-mainnet.g.alchemy.com/v2/" + key,
			positions: []e2ePosition{
				{
					name:          "grove vault 0x1121f4e2",
					vault:         common.HexToAddress("0x1121f4e21ed8b9bc1bb9a2952cdd8639ac897784"),
					expectedShare: common.HexToAddress("0x58f93d6b1ef2f44ec379cb975657c132cbed3b6b"),
					wallet:        groveAvax,
				},
				{
					name:          "grove vault 0xfe6920eb",
					vault:         common.HexToAddress("0xfe6920eb6c421f1179ca8c8d4170530cdbdfd77a"),
					expectedShare: common.HexToAddress("0xa5d465251fbcc907f5dd6bb2145488dfc6a2627b"),
					wallet:        groveAvax,
				},
			},
		},
	}

	for _, chain := range chains {
		t.Run(chain.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			client, err := rpchttp.DialEthereum(ctx, chain.url)
			if err != nil {
				t.Fatalf("dial %s: %v", chain.name, err)
			}
			defer client.Close()

			mc, err := multicall.NewClient(client, blockchain.Multicall3)
			if err != nil {
				t.Fatalf("multicall client: %v", err)
			}

			// Pin a block so the source read and the cross-check see identical state.
			latest, err := client.BlockNumber(ctx)
			if err != nil {
				t.Fatalf("block number: %v", err)
			}
			block := int64(latest)
			t.Logf("%s @ block %d", chain.name, block)

			src, err := NewERC7540Source(mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
			if err != nil {
				t.Fatalf("new source: %v", err)
			}

			entries := make([]*TokenEntry, len(chain.positions))
			for i, p := range chain.positions {
				entries[i] = &TokenEntry{
					ContractAddress: p.vault,
					WalletAddress:   p.wallet,
					Chain:           chain.name,
					Protocol:        "centrifuge",
					TokenType:       "centrifuge",
				}
			}

			result, err := src.FetchBalances(ctx, entries, block)
			if err != nil {
				t.Fatalf("FetchBalances: %v", err)
			}

			resolvedShares := fetchVaultShares(ctx, t, src, mc, chain.positions, block)
			directBalances := fetchDirectShareBalances(ctx, t, mc, chain.positions, block)

			nonZero := 0
			for i, p := range chain.positions {
				if resolvedShares[i] != p.expectedShare {
					t.Errorf("%s: vault %s share() = %s, want %s",
						p.name, p.vault.Hex(), resolvedShares[i].Hex(), p.expectedShare.Hex())
				}

				got := result.Balances[entries[i].Key()]
				if got == nil {
					t.Errorf("%s: no balance returned", p.name)
					continue
				}
				if got.Balance.Cmp(directBalances[i]) != 0 {
					t.Errorf("%s: source balance %s != direct balanceOf(%s, %s) = %s",
						p.name, got.Balance, p.expectedShare.Hex(), p.wallet.Hex(), directBalances[i])
				}
				if got.Balance.Sign() > 0 {
					nonZero++
				}
				t.Logf("%-25s vault=%s share=%s balance=%s", p.name, p.vault.Hex(), resolvedShares[i].Hex(), got.Balance)
			}

			// The grove/spark wallets hold real positions; all-zero means we are
			// reading the wrong thing even if the cross-check trivially agrees.
			if nonZero == 0 {
				t.Errorf("all %s positions are zero; expected at least one live position", chain.name)
			}
		})
	}
}

// fetchVaultShares reads share() for each position's vault directly, so the
// test verifies resolution against the known tranche token addresses.
func fetchVaultShares(ctx context.Context, t *testing.T, src *ERC7540Source, mc outbound.Multicaller, positions []e2ePosition, block int64) []common.Address {
	t.Helper()

	data, err := src.vaultABI.Pack("share")
	if err != nil {
		t.Fatalf("pack share: %v", err)
	}
	calls := make([]outbound.Call, len(positions))
	for i, p := range positions {
		calls[i] = outbound.Call{Target: p.vault, AllowFailure: false, CallData: data}
	}

	results, err := mc.Execute(ctx, calls, big.NewInt(block))
	if err != nil {
		t.Fatalf("share multicall: %v", err)
	}

	shares := make([]common.Address, len(positions))
	for i, p := range positions {
		unpacked, err := src.vaultABI.Unpack("share", results[i].ReturnData)
		if err != nil || len(unpacked) == 0 {
			t.Fatalf("unpack share for vault %s: %v", p.vault.Hex(), err)
		}
		shares[i] = unpacked[0].(common.Address)
	}
	return shares
}

// fetchDirectShareBalances reads balanceOf(expectedShare, wallet) through the
// plain ERC20 ABI, independent of the source's code path.
func fetchDirectShareBalances(ctx context.Context, t *testing.T, mc outbound.Multicaller, positions []e2ePosition, block int64) []*big.Int {
	t.Helper()

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("erc20 abi: %v", err)
	}

	calls := make([]outbound.Call, len(positions))
	for i, p := range positions {
		data, err := erc20ABI.Pack("balanceOf", p.wallet)
		if err != nil {
			t.Fatalf("pack balanceOf: %v", err)
		}
		calls[i] = outbound.Call{Target: p.expectedShare, AllowFailure: false, CallData: data}
	}

	results, err := mc.Execute(ctx, calls, big.NewInt(block))
	if err != nil {
		t.Fatalf("balanceOf multicall: %v", err)
	}

	balances := make([]*big.Int, len(positions))
	for i := range positions {
		v := unpackUint256(erc20ABI, "balanceOf", results[i].ReturnData)
		if v == nil {
			t.Fatalf("unpack balanceOf for %s", positions[i].expectedShare.Hex())
		}
		balances[i] = v
	}
	return balances
}

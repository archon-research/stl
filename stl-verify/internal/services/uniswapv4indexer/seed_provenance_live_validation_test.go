//go:build livevalidation

package uniswapv4indexer

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// TestSeedProvenance re-derives the seeded registry from mainnet itself rather
// than from the seed. The migration test recomputes each PoolId from the seeded
// key, which proves the hash but not the key; this proves the key: every pool's
// PoolKey and deploy_block come from its own Initialize log, every token's
// decimals and symbol from the contract, and both contracts must have code.
func TestSeedProvenance(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	dbPool, _, cleanupDB := testutil.SetupTestDB(t, sharedDSN)
	defer cleanupDB()
	rows, err := postgres.NewUniswapV4Repository(dbPool, buildregistry.BuildID(1)).LoadPools(ctx, 1)
	if err != nil {
		t.Fatalf("LoadPools: %v", err)
	}
	pools := RegisteredPoolsFromRows(rows)
	if len(pools) == 0 {
		t.Fatal("no seeded pools on chain 1")
	}

	rpcClient, err := rpc.DialContext(ctx, alchemyURL(t))
	if err != nil {
		t.Fatalf("BLOCKED: rpc.Dial(alchemy): %v", err)
	}
	defer rpcClient.Close()
	eth := ethclient.NewClient(rpcClient)

	for _, addr := range []common.Address{pools[0].PoolManager, pools[0].StateView} {
		code, err := eth.CodeAt(ctx, addr, nil)
		if err != nil {
			t.Fatalf("eth_getCode %s: %v", addr, err)
		}
		if len(code) == 0 {
			t.Errorf("%s has no code on mainnet", addr)
		}
	}

	pmABI, err := poolManagerABIOnce()
	if err != nil {
		t.Fatal(err)
	}
	initialize := pmABI.Events["Initialize"]
	for _, pool := range pools {
		block := big.NewInt(pool.DeployBlock)
		logs, err := eth.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: block, ToBlock: block,
			Addresses: []common.Address{pool.PoolManager},
			Topics:    [][]common.Hash{{initialize.ID}, {pool.PoolIDHash}},
		})
		if err != nil {
			t.Fatalf("eth_getLogs Initialize for pool %s at %d: %v", pool.PoolIDHash, pool.DeployBlock, err)
		}
		if len(logs) != 1 {
			t.Errorf("pool %s: %d Initialize logs at deploy_block %d, want exactly 1 (deploy_block is not this pool's Initialize block)", pool.PoolIDHash, len(logs), pool.DeployBlock)
			continue
		}
		lg := logs[0]
		if len(lg.Topics) != 4 {
			t.Errorf("pool %s: Initialize log carries %d topics, want 4", pool.PoolIDHash, len(lg.Topics))
			continue
		}
		vals, err := initialize.Inputs.NonIndexed().Unpack(lg.Data)
		if err != nil {
			t.Errorf("pool %s: decoding Initialize data: %v", pool.PoolIDHash, err)
			continue
		}
		// Non-indexed order in v4-core's Initialize: fee, tickSpacing, hooks, sqrtPriceX96, tick.
		onChain := struct {
			currency0, currency1, hooks common.Address
			fee, tickSpacing            int64
		}{
			currency0:   common.BytesToAddress(lg.Topics[2].Bytes()),
			currency1:   common.BytesToAddress(lg.Topics[3].Bytes()),
			fee:         vals[0].(*big.Int).Int64(),
			tickSpacing: vals[1].(*big.Int).Int64(),
			hooks:       vals[2].(common.Address),
		}
		if onChain.currency0 != pool.Currency0 || onChain.currency1 != pool.Currency1 ||
			onChain.fee != int64(pool.Fee) || onChain.tickSpacing != int64(pool.TickSpacing) || onChain.hooks != pool.Hooks {
			t.Errorf("pool %s: seeded key (%s, %s, fee %d, tickSpacing %d, hooks %s) != Initialize log at block %d (%s, %s, fee %d, tickSpacing %d, hooks %s)",
				pool.PoolIDHash, pool.Currency0, pool.Currency1, pool.Fee, pool.TickSpacing, pool.Hooks, pool.DeployBlock,
				onChain.currency0, onChain.currency1, onChain.fee, onChain.tickSpacing, onChain.hooks)
		}
	}

	seen := map[common.Address]bool{}
	for _, pool := range pools {
		for _, c := range []common.Address{pool.Currency0, pool.Currency1} {
			if c == (common.Address{}) || seen[c] { // address(0) is native ETH: no contract to ask
				continue
			}
			seen[c] = true
			var seededSymbol string
			var seededDecimals int
			if err := dbPool.QueryRow(ctx, `SELECT symbol, decimals FROM token WHERE chain_id = 1 AND address = $1`, c.Bytes()).Scan(&seededSymbol, &seededDecimals); err != nil {
				t.Errorf("token %s: reading its seeded row: %v", c, err)
				continue
			}
			decimals, err := callUint8(ctx, eth, c, "313ce567")
			if err != nil {
				t.Errorf("token %s: decimals(): %v", c, err)
				continue
			}
			symbol, err := callString(ctx, eth, c, "95d89b41")
			if err != nil {
				t.Errorf("token %s: symbol(): %v", c, err)
				continue
			}
			if decimals != seededDecimals || symbol != seededSymbol {
				t.Errorf("token %s: seeded (%s, %d decimals) != contract (%s, %d decimals)", c, seededSymbol, seededDecimals, symbol, decimals)
			}
		}
	}
	t.Logf("re-derived from mainnet: %d pools (Initialize log at each deploy_block), %d tokens (decimals + symbol), PoolManager %s and StateView %s have code",
		len(pools), len(seen), pools[0].PoolManager, pools[0].StateView)
}

func callUint8(ctx context.Context, eth *ethclient.Client, to common.Address, selector string) (int, error) {
	out, err := eth.CallContract(ctx, ethereum.CallMsg{To: &to, Data: common.FromHex(selector)}, nil)
	if err != nil {
		return 0, err
	}
	if len(out) != 32 {
		return 0, fmt.Errorf("want a 32-byte word, got %d bytes", len(out))
	}
	return int(new(big.Int).SetBytes(out).Int64()), nil
}

// symbol() is a string on every seeded token; a bytes32 symbol (the MKR shape)
// is accepted too, so a future seed does not fail on a decode it should pass.
func callString(ctx context.Context, eth *ethclient.Client, to common.Address, selector string) (string, error) {
	out, err := eth.CallContract(ctx, ethereum.CallMsg{To: &to, Data: common.FromHex(selector)}, nil)
	if err != nil {
		return "", err
	}
	stringT, _ := abi.NewType("string", "", nil)
	if vals, err := (abi.Arguments{{Type: stringT}}).Unpack(out); err == nil && len(vals) == 1 {
		return vals[0].(string), nil
	}
	if len(out) == 32 {
		return strings.TrimRight(string(out), "\x00"), nil
	}
	return "", fmt.Errorf("symbol() returned %d bytes that decode as neither string nor bytes32", len(out))
}

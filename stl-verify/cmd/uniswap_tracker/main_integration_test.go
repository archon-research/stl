//go:build integration

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Integration test — full snapshot with mock RPC + TimescaleDB
// ---------------------------------------------------------------------------

func TestRunIntegration_FullSnapshot(t *testing.T) {
	ctx := context.Background()

	dbPool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	rpcServer := startMockRPC(t)
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("DATABASE_URL", dbURL)
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("LOG_LEVEL", "debug")

	err := run(ctx)
	if err != nil {
		t.Fatalf("run() failed: %v", err)
	}

	// Verify snapshot was saved
	var count int
	err = dbPool.QueryRow(ctx, "SELECT count(*) FROM uniswap_pool_snapshot").Scan(&count)
	if err != nil {
		t.Fatalf("query snapshot count: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 snapshot, got %d", count)
	}

	// Verify snapshot data
	var tvlUSD *string
	var fee int
	var price string
	err = dbPool.QueryRow(ctx,
		"SELECT tvl_usd, fee, price FROM uniswap_pool_snapshot WHERE pool_address = $1",
		common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d").Bytes(),
	).Scan(&tvlUSD, &fee, &price)
	if err != nil {
		t.Fatalf("query snapshot: %v", err)
	}
	if fee != 100 {
		t.Errorf("expected fee 100, got %d", fee)
	}
	if tvlUSD == nil {
		t.Error("expected tvl_usd to be set")
	}
}

// ---------------------------------------------------------------------------
// Mock Ethereum RPC with multicall3 support
// ---------------------------------------------------------------------------

const multicall3ABIJSON = `[{
	"name":"aggregate3",
	"type":"function",
	"inputs":[{"name":"calls","type":"tuple[]","components":[
		{"name":"target","type":"address"},
		{"name":"allowFailure","type":"bool"},
		{"name":"callData","type":"bytes"}
	]}],
	"outputs":[{"name":"returnData","type":"tuple[]","components":[
		{"name":"success","type":"bool"},
		{"name":"returnData","type":"bytes"}
	]}]
}]`

var mc3ABI abi.ABI

func init() {
	var err error
	mc3ABI, err = abi.JSON(strings.NewReader(multicall3ABIJSON))
	if err != nil {
		panic("parse multicall3 ABI: " + err.Error())
	}
}

var mockBlockHeader = `{
	"number": "0x17A0000",
	"hash": "0x1111111111111111111111111111111111111111111111111111111111111111",
	"parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
	"nonce": "0x0000000000000000",
	"sha3Uncles": "0x0000000000000000000000000000000000000000000000000000000000000000",
	"logsBloom": "0x` + strings.Repeat("00", 256) + `",
	"transactionsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
	"stateRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
	"receiptsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
	"miner": "0x0000000000000000000000000000000000000000",
	"difficulty": "0x0",
	"totalDifficulty": "0x0",
	"extraData": "0x",
	"size": "0x0",
	"gasLimit": "0x1c9c380",
	"gasUsed": "0x0",
	"timestamp": "0x67EA5000",
	"transactions": [],
	"uncles": [],
	"baseFeePerGas": "0x0",
	"mixHash": "0x0000000000000000000000000000000000000000000000000000000000000000"
}`

func startMockRPC(t *testing.T) *httptest.Server {
	t.Helper()

	poolABI, err := abis.GetUniswapV3PoolABI()
	if err != nil {
		t.Fatalf("load pool ABI: %v", err)
	}
	erc20ABI := parseERC20ABI(t)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req struct {
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
			ID     json.RawMessage   `json:"id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return
		}

		writeResult := func(result string) {
			resp := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":"%s"}`, string(req.ID), result)
			w.Write([]byte(resp))
		}

		writeResultRaw := func(result string) {
			resp := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, string(req.ID), result)
			w.Write([]byte(resp))
		}

		switch req.Method {
		case "eth_blockNumber":
			writeResult("0x17A0000")
		case "eth_getBlockByNumber":
			writeResultRaw(mockBlockHeader)
		case "eth_chainId":
			writeResult("0x1")
		case "net_version":
			writeResult("1")
		case "eth_call":
			writeResult(handleEthCall(t, req.Params, poolABI, erc20ABI))
		default:
			t.Errorf("unexpected RPC method: %s", req.Method)
			errResp := fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"method not found: %s"}}`, string(req.ID), req.Method)
			w.Write([]byte(errResp))
		}
	}))
}

func handleEthCall(t *testing.T, params []json.RawMessage, poolABI, erc20ABI *abi.ABI) string {
	t.Helper()

	if len(params) == 0 {
		return "0x"
	}

	var callObj struct {
		To    string `json:"to"`
		Data  string `json:"data"`
		Input string `json:"input"`
	}
	if err := json.Unmarshal(params[0], &callObj); err != nil {
		return "0x"
	}

	callData := callObj.Data
	if callData == "" {
		callData = callObj.Input
	}

	data := common.FromHex(callData)
	if len(data) < 4 {
		return "0x"
	}

	methodID := data[:4]
	aggregate3ID := mc3ABI.Methods["aggregate3"].ID

	if string(methodID) == string(aggregate3ID) {
		return handleMulticall3(t, data, poolABI, erc20ABI)
	}

	return dispatchCall(t, common.HexToAddress(callObj.To), data, poolABI, erc20ABI)
}

func handleMulticall3(t *testing.T, data []byte, poolABI, erc20ABI *abi.ABI) string {
	t.Helper()

	unpacked, err := mc3ABI.Methods["aggregate3"].Inputs.Unpack(data[4:])
	if err != nil {
		return "0x"
	}

	callsSlice := reflect.ValueOf(unpacked[0])
	numCalls := callsSlice.Len()

	results := make([]subResultData, numCalls)
	for i := range numCalls {
		call := callsSlice.Index(i)
		target := call.FieldByName("Target").Interface().(common.Address)
		cd := call.FieldByName("CallData").Interface().([]byte)

		ret := dispatchCall(t, target, cd, poolABI, erc20ABI)
		results[i] = subResultData{success: true, returnData: common.FromHex(ret)}
	}

	return "0x" + common.Bytes2Hex(encodeAggregate3Output(results))
}

type subResultData struct {
	success    bool
	returnData []byte
}

func encodeAggregate3Output(results []subResultData) []byte {
	n := len(results)
	buf := pad32(big.NewInt(32))
	buf = append(buf, pad32(big.NewInt(int64(n)))...)

	offsets := make([]int, n)
	currentOffset := n * 32
	for i, r := range results {
		offsets[i] = currentOffset
		paddedLen := ((len(r.returnData) + 31) / 32) * 32
		currentOffset += 32 + 32 + 32 + paddedLen
	}

	for _, off := range offsets {
		buf = append(buf, pad32(big.NewInt(int64(off)))...)
	}

	for _, r := range results {
		if r.success {
			buf = append(buf, pad32(big.NewInt(1))...)
		} else {
			buf = append(buf, pad32(big.NewInt(0))...)
		}
		buf = append(buf, pad32(big.NewInt(64))...)
		buf = append(buf, pad32(big.NewInt(int64(len(r.returnData))))...)
		padded := make([]byte, ((len(r.returnData)+31)/32)*32)
		copy(padded, r.returnData)
		buf = append(buf, padded...)
	}

	return buf
}

func pad32(v *big.Int) []byte {
	b := make([]byte, 32)
	vBytes := v.Bytes()
	copy(b[32-len(vBytes):], vBytes)
	return b
}

func dispatchCall(t *testing.T, target common.Address, data []byte, poolABI, erc20ABI *abi.ABI) string {
	t.Helper()

	methodID := data[:4]

	// Try pool methods
	for name, method := range poolABI.Methods {
		if string(method.ID) == string(methodID) {
			return handlePoolMethod(t, name, data[4:], poolABI)
		}
	}

	// Try ERC20 methods
	for name, method := range erc20ABI.Methods {
		if string(method.ID) == string(methodID) {
			return handleERC20Method(t, name, target, erc20ABI)
		}
	}

	return "0x"
}

func mustPack(t *testing.T, method string, outputs abi.Arguments, args ...any) []byte {
	t.Helper()
	result, err := outputs.Pack(args...)
	if err != nil {
		t.Fatalf("pack %s: %v", method, err)
	}
	return result
}

func handlePoolMethod(t *testing.T, method string, inputData []byte, poolABI *abi.ABI) string {
	t.Helper()

	switch method {
	case "slot0":
		// sqrtPriceX96 = 2^96 for exact 1:1 stablecoin price
		sqrtPrice := new(big.Int).Lsh(big.NewInt(1), 96)
		result := mustPack(t, "slot0", poolABI.Methods["slot0"].Outputs,
			sqrtPrice, big.NewInt(0), uint16(100), uint16(200), uint16(200), uint8(0), true,
		)
		return "0x" + common.Bytes2Hex(result)

	case "liquidity":
		liq := new(big.Int).SetUint64(250_012_631_439_399_290)
		return "0x" + common.Bytes2Hex(mustPack(t, "liquidity", poolABI.Methods["liquidity"].Outputs, liq))

	case "fee":
		return "0x" + common.Bytes2Hex(mustPack(t, "fee", poolABI.Methods["fee"].Outputs, big.NewInt(100)))

	case "token0":
		addr := common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a") // AUSD
		return "0x" + common.Bytes2Hex(mustPack(t, "token0", poolABI.Methods["token0"].Outputs, addr))

	case "token1":
		addr := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48") // USDC
		return "0x" + common.Bytes2Hex(mustPack(t, "token1", poolABI.Methods["token1"].Outputs, addr))

	case "observe":
		// Return tick cumulatives for TWAP: tick=0 → price=1.0
		result := mustPack(t, "observe", poolABI.Methods["observe"].Outputs,
			[]*big.Int{big.NewInt(0), big.NewInt(0)},
			[]*big.Int{new(big.Int).SetUint64(1000), new(big.Int).SetUint64(2000)},
		)
		return "0x" + common.Bytes2Hex(result)

	case "feeGrowthGlobal0X128":
		fg := new(big.Int).SetUint64(123456789012345678)
		return "0x" + common.Bytes2Hex(mustPack(t, "feeGrowthGlobal0X128", poolABI.Methods["feeGrowthGlobal0X128"].Outputs, fg))

	case "feeGrowthGlobal1X128":
		fg := new(big.Int).SetUint64(987654321098765432)
		return "0x" + common.Bytes2Hex(mustPack(t, "feeGrowthGlobal1X128", poolABI.Methods["feeGrowthGlobal1X128"].Outputs, fg))

	default:
		return "0x"
	}
}

func handleERC20Method(t *testing.T, method string, target common.Address, erc20ABI *abi.ABI) string {
	t.Helper()

	switch method {
	case "decimals":
		return "0x" + common.Bytes2Hex(mustPack(t, "decimals", erc20ABI.Methods["decimals"].Outputs, uint8(6)))

	case "symbol":
		var sym string
		switch target {
		case common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a"):
			sym = "AUSD"
		case common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"):
			sym = "USDC"
		default:
			sym = "UNKNOWN"
		}
		return "0x" + common.Bytes2Hex(mustPack(t, "symbol", erc20ABI.Methods["symbol"].Outputs, sym))

	case "balanceOf":
		// ~12M for each token in the pool
		balance := new(big.Int).Mul(big.NewInt(12_000_000), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
		return "0x" + common.Bytes2Hex(mustPack(t, "balanceOf", erc20ABI.Methods["balanceOf"].Outputs, balance))

	default:
		return "0x"
	}
}

// ---------------------------------------------------------------------------
// ABI helpers
// ---------------------------------------------------------------------------

func parseERC20ABI(t *testing.T) *abi.ABI {
	t.Helper()
	const erc20JSON = `[
		{"name":"decimals","inputs":[],"outputs":[{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"},
		{"name":"symbol","inputs":[],"outputs":[{"name":"","type":"string"}],"stateMutability":"view","type":"function"},
		{"name":"balanceOf","inputs":[{"name":"account","type":"address"}],"outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}
	]`
	parsed, err := abi.JSON(strings.NewReader(erc20JSON))
	if err != nil {
		t.Fatalf("parse ERC20 ABI: %v", err)
	}
	return &parsed
}

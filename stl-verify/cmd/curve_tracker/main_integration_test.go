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

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Integration tests for run()
// ---------------------------------------------------------------------------

func TestRunIntegration_BadDatabaseURL(t *testing.T) {
	rpcServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("DATABASE_URL", "postgres://invalid:invalid@localhost:1/nonexistent?connect_timeout=1")

	err := run(context.Background())
	if err == nil {
		t.Fatal("expected error for bad database URL")
	}
	if !strings.Contains(err.Error(), "database") && !strings.Contains(err.Error(), "connect") {
		t.Errorf("expected connection error, got: %v", err)
	}
}

func TestRunIntegration_MissingDatabaseURL(t *testing.T) {
	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("DATABASE_URL", "")

	err := run(context.Background())
	if err == nil {
		t.Fatal("expected error for missing DATABASE_URL")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL") {
		t.Errorf("expected DATABASE_URL error, got: %v", err)
	}
}

func TestRunIntegration_MissingAlchemyKey(t *testing.T) {
	t.Setenv("ALCHEMY_API_KEY", "")
	t.Setenv("DATABASE_URL", "postgres://localhost:5432/test")

	err := run(context.Background())
	if err == nil {
		t.Fatal("expected error for missing ALCHEMY_API_KEY")
	}
	if !strings.Contains(err.Error(), "ALCHEMY_API_KEY") {
		t.Errorf("expected ALCHEMY_API_KEY error, got: %v", err)
	}
}

func TestRunIntegration_FullSnapshot(t *testing.T) {
	ctx := context.Background()

	// Setup test DB with migrations
	dbPool, dbURL, dbCleanup := testutil.SetupTimescaleDB(t)
	defer dbCleanup()

	// Mock Curve API
	curveAPIServer := startMockCurveAPI(t)
	defer curveAPIServer.Close()

	// Mock Ethereum RPC with multicall support
	rpcServer := startMockRPC(t)
	defer rpcServer.Close()

	t.Setenv("ALCHEMY_API_KEY", "test-key")
	t.Setenv("ALCHEMY_HTTP_URL", rpcServer.URL)
	t.Setenv("DATABASE_URL", dbURL)
	t.Setenv("CHAIN_ID", "1")
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("CURVE_API_BASE_URL", curveAPIServer.URL)

	err := run(ctx)
	if err != nil {
		t.Fatalf("run() failed: %v", err)
	}

	// Verify snapshots were saved
	var count int
	err = dbPool.QueryRow(ctx, "SELECT count(*) FROM curve_pool_snapshot").Scan(&count)
	if err != nil {
		t.Fatalf("query snapshot count: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 snapshots (one per pool), got %d", count)
	}

	// Verify snapshot data
	var tvlUSD *string
	var nCoins int
	err = dbPool.QueryRow(ctx,
		"SELECT tvl_usd, n_coins FROM curve_pool_snapshot WHERE pool_address = $1",
		common.HexToAddress("0x00836fe54625be242bcfa286207795405ca4fd10").Bytes(),
	).Scan(&tvlUSD, &nCoins)
	if err != nil {
		t.Fatalf("query sUSDSUSDT snapshot: %v", err)
	}
	if nCoins != 2 {
		t.Errorf("expected 2 coins, got %d", nCoins)
	}
	if tvlUSD == nil {
		t.Error("expected tvl_usd to be set")
	}
}

// ---------------------------------------------------------------------------
// Mock Curve API
// ---------------------------------------------------------------------------

func startMockCurveAPI(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		switch {
		case strings.Contains(r.URL.Path, "getBaseApys"):
			json.NewEncoder(w).Encode(map[string]interface{}{
				"success": true,
				"data": map[string]interface{}{
					"baseApys": []map[string]interface{}{
						{
							"address":              "0x00836fe54625be242bcfa286207795405ca4fd10",
							"latestWeeklyApyPcent": 1.33,
						},
						{
							"address":              "0xa632d59b9b804a956bfaa9b48af3a1b74808fc1f",
							"latestWeeklyApyPcent": 0.10,
						},
						{
							"address":              "0xe79c1c7e24755574438a26d5e062ad2626c04662",
							"latestWeeklyApyPcent": 0.01,
						},
					},
				},
			})

		case strings.Contains(r.URL.Path, "getAllGauges"):
			json.NewEncoder(w).Encode(map[string]interface{}{})

		default:
			http.NotFound(w, r)
		}
	}))
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

func startMockRPC(t *testing.T) *httptest.Server {
	t.Helper()

	curveABI := parseCurveABI(t)
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

		switch req.Method {
		case "eth_blockNumber":
			writeResult("0x179A694")
		case "eth_chainId":
			writeResult("0x1")
		case "net_version":
			writeResult("1")
		case "eth_call":
			writeResult(handleEthCall(t, req.Params, curveABI, erc20ABI))
		default:
			writeResult("0x1")
		}
	}))
}

func handleEthCall(t *testing.T, params []json.RawMessage, curveABI, erc20ABI *abi.ABI) string {
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

	// go-ethereum may use "input" instead of "data"
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
		return handleMulticall3(t, data, curveABI, erc20ABI)
	}

	return dispatchCall(t, common.HexToAddress(callObj.To), data, curveABI, erc20ABI)
}

func handleMulticall3(t *testing.T, data []byte, curveABI, erc20ABI *abi.ABI) string {
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
		callData := call.FieldByName("CallData").Interface().([]byte)

		ret := dispatchCall(t, target, callData, curveABI, erc20ABI)
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

func dispatchCall(t *testing.T, target common.Address, data []byte, curveABI, erc20ABI *abi.ABI) string {
	t.Helper()

	if len(data) < 4 {
		return "0x"
	}

	methodID := data[:4]

	for name, method := range curveABI.Methods {
		if string(method.ID) == string(methodID) {
			return handleCurveMethod(t, name, data[4:], curveABI)
		}
	}

	for name, method := range erc20ABI.Methods {
		if string(method.ID) == string(methodID) {
			return handleERC20Method(t, name, target, erc20ABI)
		}
	}

	return "0x"
}

func handleCurveMethod(t *testing.T, method string, inputData []byte, curveABI *abi.ABI) string {
	t.Helper()

	switch method {
	case "N_COINS":
		result, _ := curveABI.Methods["N_COINS"].Outputs.Pack(big.NewInt(2))
		return "0x" + common.Bytes2Hex(result)

	case "get_balances":
		balances := []*big.Int{
			new(big.Int).Mul(big.NewInt(8_000_000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			new(big.Int).Mul(big.NewInt(41_000_000), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)),
		}
		result, _ := curveABI.Methods["get_balances"].Outputs.Pack(balances)
		return "0x" + common.Bytes2Hex(result)

	case "totalSupply":
		supply := new(big.Int).Mul(big.NewInt(48_666_473), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
		result, _ := curveABI.Methods["totalSupply"].Outputs.Pack(supply)
		return "0x" + common.Bytes2Hex(result)

	case "get_virtual_price":
		vp := new(big.Int).SetUint64(1_027_699_000_000_000_000)
		result, _ := curveABI.Methods["get_virtual_price"].Outputs.Pack(vp)
		return "0x" + common.Bytes2Hex(result)

	case "A":
		result, _ := curveABI.Methods["A"].Outputs.Pack(big.NewInt(20000))
		return "0x" + common.Bytes2Hex(result)

	case "fee":
		result, _ := curveABI.Methods["fee"].Outputs.Pack(big.NewInt(100000))
		return "0x" + common.Bytes2Hex(result)

	case "coins":
		args, err := curveABI.Methods["coins"].Inputs.Unpack(inputData)
		if err != nil {
			return "0x"
		}
		indexBig, ok := args[0].(*big.Int)
		if !ok {
			return "0x"
		}
		index := indexBig.Int64()
		var addr common.Address
		if index == 0 {
			addr = common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")
		} else {
			addr = common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7")
		}
		result, _ := curveABI.Methods["coins"].Outputs.Pack(addr)
		return "0x" + common.Bytes2Hex(result)

	case "price_oracle":
		price := new(big.Int).SetUint64(999_809_000_000_000_000)
		result, _ := curveABI.Methods["price_oracle"].Outputs.Pack(price)
		return "0x" + common.Bytes2Hex(result)

	default:
		return "0x"
	}
}

func handleERC20Method(t *testing.T, method string, target common.Address, erc20ABI *abi.ABI) string {
	t.Helper()

	switch method {
	case "decimals":
		var dec uint8
		switch target {
		case common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"):
			dec = 18
		case common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"):
			dec = 6
		case common.HexToAddress("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"):
			dec = 6
		case common.HexToAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"):
			dec = 18
		case common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"):
			dec = 6
		case common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a"):
			dec = 6
		default:
			dec = 18
		}
		result, _ := erc20ABI.Methods["decimals"].Outputs.Pack(dec)
		return "0x" + common.Bytes2Hex(result)

	case "symbol":
		var sym string
		switch target {
		case common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"):
			sym = "sUSDS"
		case common.HexToAddress("0xdAC17F958D2ee523a2206206994597C13D831ec7"):
			sym = "USDT"
		case common.HexToAddress("0x6c3ea9036406852006290770BEdFcAbA0e23A0e8"):
			sym = "PYUSD"
		case common.HexToAddress("0xdC035D45d973E3EC169d2276DDab16f1e407384F"):
			sym = "USDS"
		case common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"):
			sym = "USDC"
		case common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a"):
			sym = "AUSD"
		default:
			sym = "UNKNOWN"
		}
		result, _ := erc20ABI.Methods["symbol"].Outputs.Pack(sym)
		return "0x" + common.Bytes2Hex(result)

	default:
		return "0x"
	}
}

// ---------------------------------------------------------------------------
// ABI helpers
// ---------------------------------------------------------------------------

func parseCurveABI(t *testing.T) *abi.ABI {
	t.Helper()
	const curveJSON = `[
		{"name":"coins","inputs":[{"name":"i","type":"uint256"}],"outputs":[{"name":"","type":"address"}],"stateMutability":"view","type":"function"},
		{"name":"balances","inputs":[{"name":"i","type":"uint256"}],"outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
		{"name":"get_balances","inputs":[],"outputs":[{"name":"","type":"uint256[]"}],"stateMutability":"view","type":"function"},
		{"name":"N_COINS","inputs":[],"outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
		{"name":"get_virtual_price","inputs":[],"outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
		{"name":"totalSupply","inputs":[],"outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
		{"name":"A","inputs":[],"outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
		{"name":"fee","inputs":[],"outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"},
		{"name":"price_oracle","inputs":[{"name":"i","type":"uint256"}],"outputs":[{"name":"","type":"uint256"}],"stateMutability":"view","type":"function"}
	]`
	parsed, err := abi.JSON(strings.NewReader(curveJSON))
	if err != nil {
		t.Fatalf("parse curve ABI: %v", err)
	}
	return &parsed
}

func parseERC20ABI(t *testing.T) *abi.ABI {
	t.Helper()
	const erc20JSON = `[
		{"name":"decimals","inputs":[],"outputs":[{"name":"","type":"uint8"}],"stateMutability":"view","type":"function"},
		{"name":"symbol","inputs":[],"outputs":[{"name":"","type":"string"}],"stateMutability":"view","type":"function"}
	]`
	parsed, err := abi.JSON(strings.NewReader(erc20JSON))
	if err != nil {
		t.Fatalf("parse ERC20 ABI: %v", err)
	}
	return &parsed
}

package testutil

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
)

// JSONRPCRequest represents a JSON-RPC request.
type JSONRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      json.RawMessage `json:"id"`
}

// StartMockEthRPC creates a mock Ethereum node that handles multicall3 and
// block header requests. Each block returns unique prices so change detection
// doesn't filter them out.
//
// The mock auto-detects whether the caller uses batch getAssetsPrices (1 inner
// call) or individual getAssetPrice calls (N inner calls) and responds with
// the appropriate format.
func StartMockEthRPC(t *testing.T, numTokens int) *httptest.Server {
	t.Helper()

	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("load multicall3 ABI: %v", err)
	}
	oracleABI, err := abis.GetAaveOracleABI()
	if err != nil {
		t.Fatalf("load oracle ABI: %v", err)
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")

		var req JSONRPCRequest
		if err := json.Unmarshal(body, &req); err != nil {
			WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}

		switch req.Method {
		case "eth_call":
			blockNum := parseBlockFromEthCall(req.Params)
			numCalls := countMulticallInnerCalls(req.Params)

			type Result struct {
				Success    bool
				ReturnData []byte
			}

			var aggResult []byte
			if numCalls == 1 {
				// Batch mode: single getAssetsPrices call returning all prices
				prices := make([]*big.Int, numTokens)
				for i := range prices {
					prices[i] = new(big.Int).Mul(
						big.NewInt(1000+blockNum*10+int64(i)),
						new(big.Int).SetInt64(1e8),
					)
				}
				pricesData, _ := oracleABI.Methods["getAssetsPrices"].Outputs.Pack(prices)
				aggResult, _ = multicallABI.Methods["aggregate3"].Outputs.Pack([]Result{
					{Success: true, ReturnData: pricesData},
				})
			} else {
				// Individual mode: N getAssetPrice calls, one per token
				results := make([]Result, numCalls)
				for i := range results {
					price := new(big.Int).Mul(
						big.NewInt(1000+blockNum*10+int64(i)),
						new(big.Int).SetInt64(1e8),
					)
					priceData, _ := oracleABI.Methods["getAssetPrice"].Outputs.Pack(price)
					results[i] = Result{Success: true, ReturnData: priceData}
				}
				aggResult, _ = multicallABI.Methods["aggregate3"].Outputs.Pack(results)
			}

			resultHex := "0x" + hex.EncodeToString(aggResult)
			resultJSON, _ := json.Marshal(resultHex)
			WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))

		case "eth_getBlockByNumber":
			blockNum := parseBlockFromGetBlock(req.Params)
			writeBlockHeaderResponse(w, req.ID, blockNum)

		default:
			WriteRPCError(w, req.ID, -32601, "method not found: "+req.Method)
		}
	}))
}

// WriteRPCResult writes a JSON-RPC success response.
func WriteRPCResult(w http.ResponseWriter, id, result json.RawMessage) {
	_ = json.NewEncoder(w).Encode(map[string]json.RawMessage{
		"jsonrpc": json.RawMessage(`"2.0"`),
		"id":      id,
		"result":  result,
	})
}

// WriteRPCError writes a JSON-RPC error response.
func WriteRPCError(w http.ResponseWriter, id json.RawMessage, code int, message string) {
	errJSON, _ := json.Marshal(map[string]interface{}{"code": code, "message": message})
	_ = json.NewEncoder(w).Encode(map[string]json.RawMessage{
		"jsonrpc": json.RawMessage(`"2.0"`),
		"id":      id,
		"error":   json.RawMessage(errJSON),
	})
}

func writeBlockHeaderResponse(w http.ResponseWriter, id json.RawMessage, blockNum int64) {
	timestamp := 1700000000 + blockNum*12
	header := map[string]string{
		"parentHash":       fmt.Sprintf("0x%064x", blockNum-1),
		"sha3Uncles":       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		"miner":            "0x0000000000000000000000000000000000000000",
		"stateRoot":        "0x0000000000000000000000000000000000000000000000000000000000000000",
		"transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		"receiptsRoot":     "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		"logsBloom":        "0x" + strings.Repeat("0", 512),
		"difficulty":       "0x0",
		"number":           fmt.Sprintf("0x%x", blockNum),
		"gasLimit":         "0x1c9c380",
		"gasUsed":          "0x0",
		"timestamp":        fmt.Sprintf("0x%x", timestamp),
		"extraData":        "0x",
		"mixHash":          "0x0000000000000000000000000000000000000000000000000000000000000000",
		"nonce":            "0x0000000000000000",
		"baseFeePerGas":    "0x0",
	}
	headerJSON, _ := json.Marshal(header)
	WriteRPCResult(w, id, json.RawMessage(headerJSON))
}

// countMulticallInnerCalls extracts the number of inner calls from an eth_call
// request by reading the array length from the ABI-encoded aggregate3 input.
func countMulticallInnerCalls(params json.RawMessage) int {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return 1
	}
	var callObj map[string]interface{}
	if err := json.Unmarshal(p[0], &callObj); err != nil {
		return 1
	}
	// go-ethereum may use "data" or "input" for the calldata field
	dataHex, _ := callObj["data"].(string)
	if dataHex == "" {
		dataHex, _ = callObj["input"].(string)
	}
	if len(dataHex) < 10 {
		return 1
	}
	dataBytes, err := hex.DecodeString(strings.TrimPrefix(dataHex, "0x"))
	if err != nil {
		return 1
	}
	// Layout: 4-byte selector + 32-byte offset + 32-byte array length + ...
	// Read the array length at bytes 36-68.
	if len(dataBytes) < 68 {
		return 1
	}
	arrayLen := new(big.Int).SetBytes(dataBytes[36:68])
	n := int(arrayLen.Int64())
	if n < 1 {
		return 1
	}
	return n
}

func parseBlockFromEthCall(params json.RawMessage) int64 {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 2 {
		return 100
	}
	var blockHex string
	if err := json.Unmarshal(p[1], &blockHex); err != nil {
		return 100
	}
	return parseHexInt64(blockHex)
}

func parseBlockFromGetBlock(params json.RawMessage) int64 {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return 100
	}
	var blockHex string
	if err := json.Unmarshal(p[0], &blockHex); err != nil {
		return 100
	}
	return parseHexInt64(blockHex)
}

func parseHexInt64(s string) int64 {
	s = strings.TrimPrefix(s, "0x")
	n, _ := strconv.ParseInt(s, 16, 64)
	return n
}

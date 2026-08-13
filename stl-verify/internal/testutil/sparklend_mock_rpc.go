package testutil

import (
	"encoding/hex"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
)

// BuildSparkLendBorrowMockRPC returns a mock JSON-RPC server that handles the two
// RPC call patterns made by the aavelike position tracker when processing a Borrow
// event for a single reserve token:
//
//  1. Direct eth_call to UiPoolDataProvider.getUserReservesData, returning one
//     reserve with ScaledVariableDebt > 0 (a debt asset) and no collateral, so no
//     further collateral multicalls are needed.
//  2. eth_call to Multicall3.aggregate3 for ERC-20 metadata (decimals=18,
//     symbol="DAI", name="Dai Stablecoin") and for PoolDataProvider.getUserReserveData.
//
// It is shared by the sparklend worker and backfill integration tests so the
// Borrow-event RPC fixture lives in one place.
func BuildSparkLendBorrowMockRPC(t *testing.T, reserveAddress string) *httptest.Server {
	t.Helper()

	// SparkLend UiPoolDataProvider address: direct eth_call goes here.
	const uiPoolDataProvider = "0x56b7a1012765c285afac8b8f25c69bf10ccfe978"
	// Multicall3 address: aggregate3 calls go here.
	const multicall3Address = "0xca11bde05977b3631167028862be2a173976ca11"
	// SparkLend PoolDataProvider active at block 16776400.
	const poolDataProvider = "0xfc21d6d146e6086b8359705c8b28512a983db0cb"

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		t.Fatalf("load multicall3 ABI: %v", err)
	}
	sparklendABI, err := abis.GetSparklendUserReservesDataABI()
	if err != nil {
		t.Fatalf("load sparklend ABI: %v", err)
	}
	userReserveDataABI, err := abis.GetPoolDataProviderUserReserveDataABI()
	if err != nil {
		t.Fatalf("load pool data provider ABI: %v", err)
	}

	// Pre-pack ERC-20 metadata responses.
	decimalsData, err := erc20ABI.Methods["decimals"].Outputs.Pack(uint8(18))
	if err != nil {
		t.Fatalf("pack decimals: %v", err)
	}
	symbolData, err := erc20ABI.Methods["symbol"].Outputs.Pack("DAI")
	if err != nil {
		t.Fatalf("pack symbol: %v", err)
	}
	nameData, err := erc20ABI.Methods["name"].Outputs.Pack("Dai Stablecoin")
	if err != nil {
		t.Fatalf("pack name: %v", err)
	}

	type mcResult struct {
		Success    bool
		ReturnData []byte
	}

	// aggregate3 response: 3 results (decimals, symbol, name) for 1 token.
	aggResult, err := multicallABI.Methods["aggregate3"].Outputs.Pack([]mcResult{
		{Success: true, ReturnData: decimalsData},
		{Success: true, ReturnData: symbolData},
		{Success: true, ReturnData: nameData},
	})
	if err != nil {
		t.Fatalf("pack aggregate3: %v", err)
	}
	aggHex := "0x" + hex.EncodeToString(aggResult)

	// Pre-encode getUserReservesData response: the reserve with ScaledVariableDebt > 0
	// so extractUserPositionData identifies it as a debt asset.
	type sparklendReserve struct {
		UnderlyingAsset                common.Address
		ScaledATokenBalance            *big.Int
		UsageAsCollateralEnabledOnUser bool
		ScaledVariableDebt             *big.Int
	}
	borrowAmount := new(big.Int).Mul(big.NewInt(100), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	reserveAddr := common.HexToAddress(reserveAddress)
	getUserReservesResult, err := sparklendABI.Methods["getUserReservesData"].Outputs.Pack(
		[]sparklendReserve{
			{
				UnderlyingAsset:                reserveAddr,
				ScaledATokenBalance:            big.NewInt(0),
				UsageAsCollateralEnabledOnUser: false,
				ScaledVariableDebt:             borrowAmount,
			},
		},
		uint8(0), // eMode category
	)
	if err != nil {
		t.Fatalf("pack getUserReservesData: %v", err)
	}
	getUserReservesHex := "0x" + hex.EncodeToString(getUserReservesResult)

	// Pre-pack getUserReserveData result for the reserve (via PoolDataProvider multicall).
	zero := big.NewInt(0)
	getUserReserveDataResult, err := userReserveDataABI.Methods["getUserReserveData"].Outputs.Pack(
		zero,         // currentATokenBalance
		zero,         // currentStableDebt
		borrowAmount, // currentVariableDebt
		zero,         // principalStableDebt
		borrowAmount, // scaledVariableDebt
		zero,         // stableBorrowRate
		zero,         // liquidityRate
		zero,         // stableRateLastUpdated (uint40)
		false,        // usageAsCollateralEnabled
	)
	if err != nil {
		t.Fatalf("pack getUserReserveData: %v", err)
	}
	poolDataProviderAggResult, err := multicallABI.Methods["aggregate3"].Outputs.Pack([]mcResult{
		{Success: true, ReturnData: getUserReserveDataResult},
	})
	if err != nil {
		t.Fatalf("pack poolDataProvider aggregate3: %v", err)
	}
	poolProviderAggHex := "0x" + hex.EncodeToString(poolDataProviderAggResult)

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req rpcutil.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}

		if req.Method != "eth_call" {
			WriteRPCError(w, req.ID, -32601, "method not found: "+req.Method)
			return
		}

		target := ExtractCallTarget(req.Params)

		switch strings.ToLower(target) {
		case uiPoolDataProvider:
			// getUserReservesData: return the reserve with ScaledVariableDebt > 0.
			resultJSON, _ := json.Marshal(getUserReservesHex)
			WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))

		case multicall3Address:
			// Distinguish aggregate3 calls by inspecting the first inner call target.
			firstTarget := ExtractFirstAggregate3CallTarget(t, req.Params)
			switch firstTarget {
			case poolDataProvider:
				// getUserReserveData via PoolDataProvider.
				resultJSON, _ := json.Marshal(poolProviderAggHex)
				WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
			default:
				// ERC-20 metadata.
				resultJSON, _ := json.Marshal(aggHex)
				WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
			}

		default:
			// Unknown target: return an empty result rather than an error so the
			// process doesn't crash on unexpected calls.
			WriteRPCResult(w, req.ID, json.RawMessage(`"0x"`))
		}
	}))
}

// ExtractCallTarget reads the "to" field from an eth_call params array.
// Returns an empty string if parsing fails.
func ExtractCallTarget(params json.RawMessage) string {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return ""
	}
	var callObj map[string]string
	if err := json.Unmarshal(p[0], &callObj); err != nil {
		return ""
	}
	return strings.ToLower(callObj["to"])
}

// ExtractFirstAggregate3CallTarget inspects a Multicall3 aggregate3 calldata payload
// and returns the lowercase hex address of the first Call3 entry's target field.
// This lets a mock RPC distinguish between different aggregate3 usages (e.g. ERC-20
// metadata vs. PoolDataProvider getUserReserveData) by which contract is being called.
//
// Call3 ABI layout: aggregate3(Call3[] calls) where Call3 = (address target, bool
// allowFailure, bytes callData). The 4-byte selector is skipped, then the ABI-encoded
// Call3[] is parsed to get calls[0].target.
func ExtractFirstAggregate3CallTarget(t *testing.T, params json.RawMessage) string {
	t.Helper()

	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return ""
	}
	var callObj map[string]string
	if err := json.Unmarshal(p[0], &callObj); err != nil {
		return ""
	}
	// go-ethereum sends calldata as "input"; some clients use "data": handle both.
	rawField := callObj["input"]
	if rawField == "" {
		rawField = callObj["data"]
	}
	dataHex := strings.TrimPrefix(strings.ToLower(rawField), "0x")
	if len(dataHex) < 8+128 { // at least 4-byte selector + 1 encoded Call3 element header
		return ""
	}
	raw, err := hex.DecodeString(dataHex)
	if err != nil || len(raw) < 4+32+32+32 { // selector + offset + length + first element start
		return ""
	}

	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		return ""
	}

	// raw[4:] is the ABI-encoded (Call3[] calls) input.
	unpacked, err := multicallABI.Methods["aggregate3"].Inputs.Unpack(raw[4:])
	if err != nil || len(unpacked) == 0 {
		return ""
	}

	calls, ok := unpacked[0].([]struct {
		Target       common.Address `json:"target"`
		AllowFailure bool           `json:"allowFailure"`
		CallData     []byte         `json:"callData"`
	})
	if !ok || len(calls) == 0 {
		return ""
	}

	return strings.ToLower(calls[0].Target.Hex())
}

// ExtractAggregate3Calls parses the full Call3[] from a Multicall3 aggregate3 eth_call
// request. Returns nil if parsing fails.
func ExtractAggregate3Calls(params json.RawMessage) []struct {
	Target       common.Address `json:"target"`
	AllowFailure bool           `json:"allowFailure"`
	CallData     []byte         `json:"callData"`
} {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil
	}
	var callObj map[string]string
	if err := json.Unmarshal(p[0], &callObj); err != nil {
		return nil
	}
	rawField := callObj["input"]
	if rawField == "" {
		rawField = callObj["data"]
	}
	dataHex := strings.TrimPrefix(strings.ToLower(rawField), "0x")
	if len(dataHex) < 8+128 {
		return nil
	}
	raw, err := hex.DecodeString(dataHex)
	if err != nil || len(raw) < 4+32+32+32 {
		return nil
	}

	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		return nil
	}

	unpacked, err := multicallABI.Methods["aggregate3"].Inputs.Unpack(raw[4:])
	if err != nil || len(unpacked) == 0 {
		return nil
	}

	calls, ok := unpacked[0].([]struct {
		Target       common.Address `json:"target"`
		AllowFailure bool           `json:"allowFailure"`
		CallData     []byte         `json:"callData"`
	})
	if !ok {
		return nil
	}
	return calls
}

// ExtractAssetFromGetUserReserveDataCalldata extracts the first address parameter
// (the "asset") from a getUserReserveData(address asset, address user) calldata.
// The calldata layout is: 4-byte selector + 32-byte asset + 32-byte user.
func ExtractAssetFromGetUserReserveDataCalldata(callData []byte) common.Address {
	if len(callData) < 4+32 {
		return common.Address{}
	}
	return common.BytesToAddress(callData[4 : 4+32])
}

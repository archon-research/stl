package testutil

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

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

// SubcallDispatcher answers a single sub-call inside a Multicall3 aggregate3
// batch, returning the raw ABI return data and a success flag.
type SubcallDispatcher func(target common.Address, callData []byte) ([]byte, bool)

var parsedMulticall3ABI abi.ABI

func init() {
	var err error
	parsedMulticall3ABI, err = abi.JSON(strings.NewReader(multicall3ABIJSON))
	if err != nil {
		panic("parse multicall3 ABI: " + err.Error())
	}
}

// HandleMulticall3 decodes an aggregate3 eth_call, dispatches each sub-call,
// and returns the hex-encoded aggregate3 result. Mock RPC servers use it to
// answer multicall-batched reads in integration tests.
func HandleMulticall3(calldata []byte, dispatch SubcallDispatcher) (string, error) {
	if len(calldata) < 4 {
		return "", fmt.Errorf("calldata too short")
	}

	args, err := parsedMulticall3ABI.Methods["aggregate3"].Inputs.Unpack(calldata[4:])
	if err != nil {
		return "", fmt.Errorf("unpack aggregate3 inputs: %w", err)
	}

	rawCalls, ok := args[0].([]struct {
		Target       common.Address `json:"target"`
		AllowFailure bool           `json:"allowFailure"`
		CallData     []byte         `json:"callData"`
	})
	if !ok {
		return "", fmt.Errorf("unexpected type for calls: %T", args[0])
	}

	type abiResult struct {
		Success    bool   `abi:"success"`
		ReturnData []byte `abi:"returnData"`
	}

	encoded := make([]abiResult, len(rawCalls))
	for i, c := range rawCalls {
		returnData, success := dispatch(c.Target, c.CallData)
		encoded[i] = abiResult{Success: success, ReturnData: returnData}
	}

	packed, err := parsedMulticall3ABI.Methods["aggregate3"].Outputs.Pack(encoded)
	if err != nil {
		return "", fmt.Errorf("pack aggregate3 outputs: %w", err)
	}

	return "0x" + hex.EncodeToString(packed), nil
}

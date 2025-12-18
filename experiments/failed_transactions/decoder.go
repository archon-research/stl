package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// GetFunctionName returns the function name from calldata
func GetFunctionName(calldata []byte) (string, bool) {
	if len(calldata) < 4 {
		return "", false
	}

	selector := "0x" + hex.EncodeToString(calldata[:4])
	funcName, ok := SparkLendFunctions[selector]
	return funcName, ok
}

// DecodeCalldata decodes the calldata for a SparkLend function
func DecodeCalldata(funcName string, calldata []byte) (string, error) {
	if len(calldata) < 4 {
		return "{}", fmt.Errorf("calldata too short")
	}

	// Remove the function selector
	data := calldata[4:]

	abi, ok := SparkLendABIs[funcName]
	if !ok {
		return "{}", fmt.Errorf("unknown function: %s", funcName)
	}

	args := make(map[string]interface{})

	switch funcName {
	case "supply":
		if len(data) < 128 { // 4 params * 32 bytes
			return "{}", fmt.Errorf("insufficient data for supply")
		}
		args["asset"] = common.BytesToAddress(data[0:32]).Hex()
		args["amount"] = new(big.Int).SetBytes(data[32:64]).String()
		args["onBehalfOf"] = common.BytesToAddress(data[64:96]).Hex()
		args["referralCode"] = new(big.Int).SetBytes(data[96:128]).Uint64()

	case "borrow":
		if len(data) < 160 { // 5 params * 32 bytes
			return "{}", fmt.Errorf("insufficient data for borrow")
		}
		args["asset"] = common.BytesToAddress(data[0:32]).Hex()
		args["amount"] = new(big.Int).SetBytes(data[32:64]).String()
		args["interestRateMode"] = new(big.Int).SetBytes(data[64:96]).String()
		args["referralCode"] = new(big.Int).SetBytes(data[96:128]).Uint64()
		args["onBehalfOf"] = common.BytesToAddress(data[128:160]).Hex()

	case "repay":
		if len(data) < 128 { // 4 params * 32 bytes
			return "{}", fmt.Errorf("insufficient data for repay")
		}
		args["asset"] = common.BytesToAddress(data[0:32]).Hex()
		args["amount"] = new(big.Int).SetBytes(data[32:64]).String()
		args["interestRateMode"] = new(big.Int).SetBytes(data[64:96]).String()
		args["onBehalfOf"] = common.BytesToAddress(data[96:128]).Hex()

	case "withdraw":
		if len(data) < 96 { // 3 params * 32 bytes
			return "{}", fmt.Errorf("insufficient data for withdraw")
		}
		args["asset"] = common.BytesToAddress(data[0:32]).Hex()
		args["amount"] = new(big.Int).SetBytes(data[32:64]).String()
		args["to"] = common.BytesToAddress(data[64:96]).Hex()

	case "liquidationCall":
		if len(data) < 160 { // 5 params * 32 bytes
			return "{}", fmt.Errorf("insufficient data for liquidationCall")
		}
		args["collateralAsset"] = common.BytesToAddress(data[0:32]).Hex()
		args["debtAsset"] = common.BytesToAddress(data[32:64]).Hex()
		args["user"] = common.BytesToAddress(data[64:96]).Hex()
		args["debtToCover"] = new(big.Int).SetBytes(data[96:128]).String()
		args["receiveAToken"] = new(big.Int).SetBytes(data[128:160]).Cmp(big.NewInt(0)) != 0

	default:
		// Use the generic ABI to decode
		return decodeGeneric(abi, data)
	}

	jsonBytes, err := json.Marshal(args)
	if err != nil {
		return "{}", err
	}

	return string(jsonBytes), nil
}

// decodeGeneric attempts to decode calldata using ABI definition
func decodeGeneric(abi FunctionABI, data []byte) (string, error) {
	args := make(map[string]interface{})
	offset := 0

	for _, input := range abi.Inputs {
		if offset+32 > len(data) {
			break
		}

		chunk := data[offset : offset+32]
		switch input.Type {
		case "address":
			args[input.Name] = common.BytesToAddress(chunk).Hex()
		case "uint256":
			args[input.Name] = new(big.Int).SetBytes(chunk).String()
		case "uint16":
			args[input.Name] = new(big.Int).SetBytes(chunk).Uint64()
		case "bool":
			args[input.Name] = new(big.Int).SetBytes(chunk).Cmp(big.NewInt(0)) != 0
		default:
			args[input.Name] = hex.EncodeToString(chunk)
		}
		offset += 32
	}

	jsonBytes, err := json.Marshal(args)
	if err != nil {
		return "{}", err
	}

	return string(jsonBytes), nil
}

// ParseRevertReason attempts to decode the revert reason from error data
func ParseRevertReason(data []byte) string {
	if len(data) < 4 {
		return ""
	}

	// Check for Error(string) selector: 0x08c379a0
	if hex.EncodeToString(data[:4]) == "08c379a0" {
		if len(data) < 68 {
			return ""
		}
		// Skip selector (4) + offset (32) + length position
		strLen := new(big.Int).SetBytes(data[36:68]).Uint64()
		if uint64(len(data)) < 68+strLen {
			return ""
		}
		return string(data[68 : 68+strLen])
	}

	// Check for Panic(uint256) selector: 0x4e487b71
	if hex.EncodeToString(data[:4]) == "4e487b71" {
		if len(data) < 36 {
			return ""
		}
		panicCode := new(big.Int).SetBytes(data[4:36]).Uint64()
		return fmt.Sprintf("Panic(0x%x)", panicCode)
	}

	// Check for custom errors - just return the hex
	return "0x" + hex.EncodeToString(data)
}

// DecodeRevertReasonFromHex decodes a hex-encoded revert reason
func DecodeRevertReasonFromHex(hexData string) string {
	hexData = strings.TrimPrefix(hexData, "0x")
	data, err := hex.DecodeString(hexData)
	if err != nil {
		return hexData
	}
	return ParseRevertReason(data)
}

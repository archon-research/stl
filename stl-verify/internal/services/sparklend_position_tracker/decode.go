package sparklend_position_tracker

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

const wordSize = 32

// decodeUserReservesRaw is a fallback decoder for getUserReservesData output
// when the standard ABI decoder rejects non-strict boolean encoding (e.g.
// Avalanche C-Chain encodes booleans as arbitrary non-zero uint256 values).
// The correct ABI is already selected by protocol version in loadABIs();
// this only handles the wire-encoding edge case.
//
// Return layout (ABI-encoded):
//
//	word 0: offset to UserReserveData[] (dynamic array)
//	word 1: uint8 eMode category (we ignore this)
//	--- at offset ---
//	word 0: array length N
//	words 1..N*F: N structs, each F words
//
// Two possible struct layouts are supported:
//
// 7-field layout (legacy Aave V3):
//
//	[0] address  underlyingAsset
//	[1] uint256  scaledATokenBalance
//	[2] bool     usageAsCollateralEnabledOnUser  (encoded as uint256)
//	[3] uint256  scaledVariableDebt
//	[4] uint256  stableBorrowRate
//	[5] uint256  principalStableDebt
//	[6] uint256  stableBorrowLastUpdateTimestamp
//
// 4-field layout (Aave V3.1+ / SparkLend — stable borrowing removed):
//
//	[0] address  underlyingAsset
//	[1] uint256  scaledATokenBalance
//	[2] bool     usageAsCollateralEnabledOnUser  (encoded as uint256)
//	[3] uint256  scaledVariableDebt
func decodeUserReservesRaw(data []byte) ([]UserReserveData, error) {
	if len(data) < 2*wordSize {
		return nil, fmt.Errorf("response too short: %d bytes", len(data))
	}

	// Read offset to the dynamic array.
	// Use subtraction on the safe side to avoid uint64 overflow on the addition.
	// The len(data) >= 2*wordSize guard above ensures the subtraction is safe.
	arrayOffset := new(big.Int).SetBytes(data[0:wordSize]).Uint64()
	if arrayOffset > uint64(len(data))-wordSize {
		return nil, fmt.Errorf("array offset %d out of bounds (len=%d)", arrayOffset, len(data))
	}

	// Read array length
	arrayStart := arrayOffset
	arrayLen := new(big.Int).SetBytes(data[arrayStart : arrayStart+wordSize]).Uint64()
	if arrayLen == 0 {
		return []UserReserveData{}, nil
	}

	// Auto-detect struct width from available data
	structDataBytes := uint64(len(data)) - arrayStart - wordSize
	bytesPerStruct := structDataBytes / arrayLen

	var fieldsPerStruct uint64
	switch {
	case bytesPerStruct >= 7*wordSize:
		fieldsPerStruct = 7
	case bytesPerStruct >= 4*wordSize:
		fieldsPerStruct = 4
	default:
		return nil, fmt.Errorf("cannot determine struct layout: %d bytes per struct for %d reserves", bytesPerStruct, arrayLen)
	}

	expectedBytes := arrayStart + wordSize + arrayLen*fieldsPerStruct*wordSize
	if uint64(len(data)) < expectedBytes {
		return nil, fmt.Errorf("data too short for %d reserves (%d fields): need %d bytes, have %d",
			arrayLen, fieldsPerStruct, expectedBytes, len(data))
	}

	reserves := make([]UserReserveData, 0, arrayLen)
	for i := uint64(0); i < arrayLen; i++ {
		base := arrayStart + wordSize + i*fieldsPerStruct*wordSize

		underlyingAsset := common.BytesToAddress(data[base : base+wordSize])
		if underlyingAsset == (common.Address{}) {
			continue
		}

		scaledATokenBalance := new(big.Int).SetBytes(data[base+wordSize : base+2*wordSize])
		collateralEnabled := new(big.Int).SetBytes(data[base+2*wordSize:base+3*wordSize]).Sign() != 0
		scaledVariableDebt := new(big.Int).SetBytes(data[base+3*wordSize : base+4*wordSize])

		rd := UserReserveData{
			UnderlyingAsset:                underlyingAsset,
			ScaledATokenBalance:            scaledATokenBalance,
			UsageAsCollateralEnabledOnUser: collateralEnabled,
			ScaledVariableDebt:             scaledVariableDebt,
		}

		if fieldsPerStruct == 7 {
			rd.StableBorrowRate = new(big.Int).SetBytes(data[base+4*wordSize : base+5*wordSize])
			rd.PrincipalStableDebt = new(big.Int).SetBytes(data[base+5*wordSize : base+6*wordSize])
			rd.StableBorrowLastUpdateTimestamp = new(big.Int).SetBytes(data[base+6*wordSize : base+7*wordSize])
		} else {
			rd.StableBorrowRate = big.NewInt(0)
			rd.PrincipalStableDebt = big.NewInt(0)
			rd.StableBorrowLastUpdateTimestamp = big.NewInt(0)
		}

		reserves = append(reserves, rd)
	}

	return reserves, nil
}

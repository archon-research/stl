// Package transform provides data transformation logic for the S3 data loader.
// It converts source schema data from Brett's PostgreSQL dumps to the target Sentinel schema.
package transform

import (
	"hash/fnv"
	"strconv"
)

// GenerateID creates a deterministic int64 ID from a composite key.
// Uses FNV-1a hash with the high bit cleared to ensure positive values.
// This allows idempotent re-runs with consistent foreign key relationships.
func GenerateID(compositeKey string) int64 {
	h := fnv.New64a()
	h.Write([]byte(compositeKey))
	return int64(h.Sum64() & 0x7FFFFFFFFFFFFFFF) // Ensure positive
}

// GenerateTokenID creates a deterministic ID for a token.
// Key format: "token:{chainID}:{address}"
func GenerateTokenID(chainID int, address string) int64 {
	return GenerateID(tokenKey(chainID, address))
}

// GenerateProtocolID creates a deterministic ID for a protocol.
// Key format: "protocol:{chainID}:{address}"
func GenerateProtocolID(chainID int, address string) int64 {
	return GenerateID(protocolKey(chainID, address))
}

// GenerateUserID creates a deterministic ID for a user.
// Key format: "user:{chainID}:{address}"
func GenerateUserID(chainID int, address string) int64 {
	return GenerateID(userKey(chainID, address))
}

// GenerateBorrowerID creates a deterministic ID for a borrower position.
// Key format: "borrower:{userID}:{protocolID}:{tokenID}:{blockNumber}:{blockVersion}"
func GenerateBorrowerID(userID, protocolID, tokenID, blockNumber int64, blockVersion int) int64 {
	return GenerateID(borrowerKey(userID, protocolID, tokenID, blockNumber, blockVersion))
}

// GenerateBorrowerCollateralID creates a deterministic ID for a collateral position.
// Key format: "collateral:{userID}:{protocolID}:{tokenID}:{blockNumber}:{blockVersion}"
func GenerateBorrowerCollateralID(userID, protocolID, tokenID, blockNumber int64, blockVersion int) int64 {
	return GenerateID(collateralKey(userID, protocolID, tokenID, blockNumber, blockVersion))
}

// GenerateReceiptTokenID creates a deterministic ID for a receipt token.
// Key format: "receipt_token:{protocolID}:{underlyingTokenID}"
func GenerateReceiptTokenID(protocolID, underlyingTokenID int64) int64 {
	return GenerateID(receiptTokenKey(protocolID, underlyingTokenID))
}

// GenerateDebtTokenID creates a deterministic ID for a debt token.
// Key format: "debt_token:{protocolID}:{underlyingTokenID}"
func GenerateDebtTokenID(protocolID, underlyingTokenID int64) int64 {
	return GenerateID(debtTokenKey(protocolID, underlyingTokenID))
}

// GenerateSparkLendReserveDataID creates a deterministic ID for reserve data.
// Key format: "reserve_data:{protocolID}:{tokenID}:{blockNumber}"
func GenerateSparkLendReserveDataID(protocolID, tokenID, blockNumber int64) int64 {
	return GenerateID(reserveDataKey(protocolID, tokenID, blockNumber))
}

// GenerateUserProtocolMetadataID creates a deterministic ID for user protocol metadata.
// Key format: "user_metadata:{userID}:{protocolID}"
func GenerateUserProtocolMetadataID(userID, protocolID int64) int64 {
	return GenerateID(userMetadataKey(userID, protocolID))
}

// Key generation helpers
func tokenKey(chainID int, address string) string {
	return "token:" + strconv.Itoa(chainID) + ":" + address
}

func protocolKey(chainID int, address string) string {
	return "protocol:" + strconv.Itoa(chainID) + ":" + address
}

func userKey(chainID int, address string) string {
	return "user:" + strconv.Itoa(chainID) + ":" + address
}

func borrowerKey(userID, protocolID, tokenID, blockNumber int64, blockVersion int) string {
	return "borrower:" + strconv.FormatInt(userID, 10) + ":" + strconv.FormatInt(protocolID, 10) + ":" + strconv.FormatInt(tokenID, 10) + ":" + strconv.FormatInt(blockNumber, 10) + ":" + strconv.Itoa(blockVersion)
}

func collateralKey(userID, protocolID, tokenID, blockNumber int64, blockVersion int) string {
	return "collateral:" + strconv.FormatInt(userID, 10) + ":" + strconv.FormatInt(protocolID, 10) + ":" + strconv.FormatInt(tokenID, 10) + ":" + strconv.FormatInt(blockNumber, 10) + ":" + strconv.Itoa(blockVersion)
}

func receiptTokenKey(protocolID, underlyingTokenID int64) string {
	return "receipt_token:" + strconv.FormatInt(protocolID, 10) + ":" + strconv.FormatInt(underlyingTokenID, 10)
}

func debtTokenKey(protocolID, underlyingTokenID int64) string {
	return "debt_token:" + strconv.FormatInt(protocolID, 10) + ":" + strconv.FormatInt(underlyingTokenID, 10)
}

func reserveDataKey(protocolID, tokenID, blockNumber int64) string {
	return "reserve_data:" + strconv.FormatInt(protocolID, 10) + ":" + strconv.FormatInt(tokenID, 10) + ":" + strconv.FormatInt(blockNumber, 10)
}

func userMetadataKey(userID, protocolID int64) string {
	return "user_metadata:" + strconv.FormatInt(userID, 10) + ":" + strconv.FormatInt(protocolID, 10)
}

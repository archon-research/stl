package transform

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ReserveMarketSnapshotRow represents a row from the source ReserveMarketSnapshot table.
// Source schema (from S3):
//
//	id text NOT NULL,
//	protocol_id text NOT NULL,
//	reserve_id text NOT NULL,
//	underlying_asset text NOT NULL,
//	block_number numeric(78,0) NOT NULL,
//	timestamp numeric(78,0) NOT NULL,
//	liquidity_rate text NOT NULL,
//	variable_borrow_rate text NOT NULL,
//	stable_borrow_rate text NOT NULL,
//	liquidity_index text NOT NULL,
//	variable_borrow_index text NOT NULL,
//	price_in_eth text,
//	price_in_usd text,
//	total_a_token text,
//	total_variable_debt text,
//	total_stable_debt text,
//	available_liquidity text
type ReserveMarketSnapshotRow struct {
	ID                  string
	ProtocolID          string
	ReserveID           string
	UnderlyingAsset     string
	BlockNumber         int64
	Timestamp           int64
	LiquidityRate       *big.Int
	VariableBorrowRate  *big.Int
	StableBorrowRate    *big.Int
	LiquidityIndex      *big.Int
	VariableBorrowIndex *big.Int
	PriceInETH          *big.Int
	PriceInUSD          *big.Int
	TotalAToken         *big.Int
	TotalVariableDebt   *big.Int
	TotalStableDebt     *big.Int
	AvailableLiquidity  *big.Int
}

// ParseReserveMarketSnapshotRow parses a COPY row into a ReserveMarketSnapshotRow.
func ParseReserveMarketSnapshotRow(row []string, colIndex map[string]int) (*ReserveMarketSnapshotRow, error) {
	getField := func(name string) string {
		if idx, ok := colIndex[name]; ok && idx < len(row) {
			return row[idx]
		}
		return ""
	}

	blockNum, err := strconv.ParseInt(getField("block_number"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid block_number: %w", err)
	}

	timestamp, _ := strconv.ParseInt(getField("timestamp"), 10, 64)

	return &ReserveMarketSnapshotRow{
		ID:                  getField("id"),
		ProtocolID:          getField("protocol_id"),
		ReserveID:           getField("reserve_id"),
		UnderlyingAsset:     getField("underlying_asset"),
		BlockNumber:         blockNum,
		Timestamp:           timestamp,
		LiquidityRate:       parseBigInt(getField("liquidity_rate")),
		VariableBorrowRate:  parseBigInt(getField("variable_borrow_rate")),
		StableBorrowRate:    parseBigInt(getField("stable_borrow_rate")),
		LiquidityIndex:      parseBigInt(getField("liquidity_index")),
		VariableBorrowIndex: parseBigInt(getField("variable_borrow_index")),
		PriceInETH:          parseBigInt(getField("price_in_eth")),
		PriceInUSD:          parseBigInt(getField("price_in_usd")),
		TotalAToken:         parseBigInt(getField("total_a_token")),
		TotalVariableDebt:   parseBigInt(getField("total_variable_debt")),
		TotalStableDebt:     parseBigInt(getField("total_stable_debt")),
		AvailableLiquidity:  parseBigInt(getField("available_liquidity")),
	}, nil
}

// TransformReserveMarketSnapshot converts a ReserveMarketSnapshotRow to entity.SparkLendReserveData.
func TransformReserveMarketSnapshot(
	row *ReserveMarketSnapshotRow,
	protocolID, tokenID int64,
) *entity.SparkLendReserveData {
	dataID := GenerateSparkLendReserveDataID(protocolID, tokenID, row.BlockNumber)

	data := entity.NewSparkLendReserveData(dataID, protocolID, tokenID, row.BlockNumber)

	// Set rates
	data.WithRates(
		row.LiquidityRate,
		row.VariableBorrowRate,
		row.StableBorrowRate,
		nil, // average stable borrow rate - not in source
	)

	// Set indexes
	data.WithIndexes(
		row.LiquidityIndex,
		row.VariableBorrowIndex,
	)

	// Set totals
	data.WithTotals(
		nil, // unbacked - not in source
		nil, // accrued to treasury - not in source
		row.TotalAToken,
		row.TotalStableDebt,
		row.TotalVariableDebt,
	)

	data.LastUpdateTimestamp = row.Timestamp

	return data
}

// ReserveRow represents a row from the source Reserve table.
// This contains token configuration for the protocol.
type ReserveRow struct {
	ID                  string
	ProtocolID          string
	UnderlyingAsset     string
	ATokenAddress       string
	VariableDebtAddress string
	StableDebtAddress   string
	Symbol              string
	Name                string
	Decimals            int
	IsActive            bool
	IsFrozen            bool
	ReserveFactor       *big.Int
	LTV                 *big.Int
	LiquidationBonus    *big.Int
}

// ParseReserveRow parses a COPY row into a ReserveRow.
func ParseReserveRow(row []string, colIndex map[string]int) (*ReserveRow, error) {
	getField := func(name string) string {
		if idx, ok := colIndex[name]; ok && idx < len(row) {
			return row[idx]
		}
		return ""
	}

	decimals, _ := strconv.Atoi(getField("decimals"))

	return &ReserveRow{
		ID:                  getField("id"),
		ProtocolID:          getField("protocol_id"),
		UnderlyingAsset:     getField("underlying_asset"),
		ATokenAddress:       getField("a_token_address"),
		VariableDebtAddress: getField("variable_debt_token_address"),
		StableDebtAddress:   getField("stable_debt_token_address"),
		Symbol:              getField("symbol"),
		Name:                getField("name"),
		Decimals:            decimals,
		IsActive:            getField("is_active") == "t" || getField("is_active") == "true",
		IsFrozen:            getField("is_frozen") == "t" || getField("is_frozen") == "true",
		ReserveFactor:       parseBigInt(getField("reserve_factor")),
		LTV:                 parseBigInt(getField("base_ltv_as_collateral")),
		LiquidationBonus:    parseBigInt(getField("liquidation_bonus")),
	}, nil
}

// TransformReserveToReceiptToken creates a ReceiptToken from Reserve data.
func TransformReserveToReceiptToken(row *ReserveRow, protocolID, underlyingTokenID int64) (*entity.ReceiptToken, error) {
	aTokenAddr, err := HexToAddress(row.ATokenAddress)
	if err != nil {
		return nil, fmt.Errorf("invalid aToken address: %w", err)
	}

	receiptTokenID := GenerateReceiptTokenID(protocolID, underlyingTokenID)

	return entity.NewReceiptToken(
		receiptTokenID,
		protocolID,
		underlyingTokenID,
		aTokenAddr,
		"sp"+row.Symbol, // SparkLend uses sp prefix
	)
}

// TransformReserveToDebtToken creates a DebtToken from Reserve data.
func TransformReserveToDebtToken(row *ReserveRow, protocolID, underlyingTokenID int64) (*entity.DebtToken, error) {
	var variableDebtAddr, stableDebtAddr []byte
	var err error

	if row.VariableDebtAddress != "" {
		variableDebtAddr, err = HexToAddress(row.VariableDebtAddress)
		if err != nil {
			return nil, fmt.Errorf("invalid variable debt address: %w", err)
		}
	}

	if row.StableDebtAddress != "" {
		stableDebtAddr, err = HexToAddress(row.StableDebtAddress)
		if err != nil {
			return nil, fmt.Errorf("invalid stable debt address: %w", err)
		}
	}

	debtTokenID := GenerateDebtTokenID(protocolID, underlyingTokenID)

	variableSymbol := "variableDebt" + row.Symbol
	stableSymbol := ""
	if row.StableDebtAddress != "" {
		stableSymbol = "stableDebt" + row.Symbol
	}

	dt, err := entity.NewDebtToken(
		debtTokenID,
		protocolID,
		underlyingTokenID,
		variableDebtAddr,
		stableDebtAddr,
		variableSymbol,
		stableSymbol,
	)
	if err != nil {
		return nil, err
	}

	return dt, nil
}

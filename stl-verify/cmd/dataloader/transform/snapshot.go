package transform

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/archon-research/stl/stl-verify/cmd/dataloader/parser"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// UserReserveSnapshotRow represents a row from the source UserReserveSnapshot table.
// Source schema (from S3):
//
//	id text NOT NULL,
//	protocol_id text NOT NULL,
//	user_id text NOT NULL,       -- e.g., "mainnet-0x..."
//	reserve_id text NOT NULL,
//	"user" text NOT NULL,        -- address
//	underlying_asset text NOT NULL,
//	block_number numeric(78,0) NOT NULL,
//	"timestamp" numeric(78,0) NOT NULL,
//	scaled_a_token_balance text NOT NULL,      -- collateral (scaled)
//	usage_as_collateral_enabled boolean NOT NULL,
//	scaled_variable_debt text NOT NULL,        -- variable debt (scaled)
//	principal_stable_debt text NOT NULL,       -- stable debt
//	stable_borrow_rate text NOT NULL,
//	stable_borrow_last_update_timestamp text NOT NULL,
//	current_a_token_balance text,              -- actual collateral balance
//	current_variable_debt text                 -- actual variable debt
type UserReserveSnapshotRow struct {
	ID                              string
	ProtocolID                      string
	UserID                          string
	ReserveID                       string
	User                            string
	UnderlyingAsset                 string
	BlockNumber                     int64
	Timestamp                       int64
	ScaledATokenBalance             *big.Int
	UsageAsCollateralEnabled        bool
	ScaledVariableDebt              *big.Int
	PrincipalStableDebt             *big.Int
	StableBorrowRate                *big.Int
	StableBorrowLastUpdateTimestamp int64
	CurrentATokenBalance            *big.Int // actual collateral amount
	CurrentVariableDebt             *big.Int // actual debt amount
}

// UserReserveSnapshotColumns defines expected column order for UserReserveSnapshot.
var UserReserveSnapshotColumns = []string{
	"id", "protocol_id", "user_id", "reserve_id", "user", "underlying_asset",
	"block_number", "timestamp", "scaled_a_token_balance", "usage_as_collateral_enabled",
	"scaled_variable_debt", "principal_stable_debt", "stable_borrow_rate",
	"stable_borrow_last_update_timestamp", "current_a_token_balance", "current_variable_debt",
}

// ParseUserReserveSnapshotRow parses a COPY row into a UserReserveSnapshotRow.
func ParseUserReserveSnapshotRow(row []string, colIndex map[string]int) (*UserReserveSnapshotRow, error) {
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

	// These fields are optional and may be NULL or empty in source data
	var timestamp, stableBorrowLastUpdate int64
	if s := getField("timestamp"); s != "" {
		timestamp, _ = strconv.ParseInt(s, 10, 64)
	}
	if s := getField("stable_borrow_last_update_timestamp"); s != "" {
		stableBorrowLastUpdate, _ = strconv.ParseInt(s, 10, 64)
	}

	return &UserReserveSnapshotRow{
		ID:                              getField("id"),
		ProtocolID:                      getField("protocol_id"),
		UserID:                          getField("user_id"),
		ReserveID:                       getField("reserve_id"),
		User:                            getField("user"),
		UnderlyingAsset:                 getField("underlying_asset"),
		BlockNumber:                     blockNum,
		Timestamp:                       timestamp,
		ScaledATokenBalance:             parseBigInt(getField("scaled_a_token_balance")),
		UsageAsCollateralEnabled:        getField("usage_as_collateral_enabled") == "t" || getField("usage_as_collateral_enabled") == "true",
		ScaledVariableDebt:              parseBigInt(getField("scaled_variable_debt")),
		PrincipalStableDebt:             parseBigInt(getField("principal_stable_debt")),
		StableBorrowRate:                parseBigInt(getField("stable_borrow_rate")),
		StableBorrowLastUpdateTimestamp: stableBorrowLastUpdate,
		CurrentATokenBalance:            parseBigInt(getField("current_a_token_balance")),
		CurrentVariableDebt:             parseBigInt(getField("current_variable_debt")),
	}, nil
}

// parseBigInt parses a string to *big.Int, returning nil for NULL or empty.
func parseBigInt(s string) *big.Int {
	if s == "" || parser.IsNull(s) {
		return nil
	}
	val, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil
	}
	return val
}

// TransformUserReserveSnapshot converts a UserReserveSnapshotRow to Borrower and BorrowerCollateral entities.
// It requires pre-resolved IDs for the user, protocol, and token.
func TransformUserReserveSnapshot(
	row *UserReserveSnapshotRow,
	userID, protocolID, tokenID int64,
) (*entity.Borrower, *entity.BorrowerCollateral) {
	var borrower *entity.Borrower
	var collateral *entity.BorrowerCollateral

	// If there's debt (variable or stable), create a Borrower
	hasDebt := (row.CurrentVariableDebt != nil && row.CurrentVariableDebt.Sign() > 0) ||
		(row.PrincipalStableDebt != nil && row.PrincipalStableDebt.Sign() > 0)

	if hasDebt {
		// Sum variable and stable debt
		totalDebt := new(big.Int)
		if row.CurrentVariableDebt != nil {
			totalDebt.Add(totalDebt, row.CurrentVariableDebt)
		}
		if row.PrincipalStableDebt != nil {
			totalDebt.Add(totalDebt, row.PrincipalStableDebt)
		}

		borrowerID := GenerateBorrowerID(userID, protocolID, tokenID, row.BlockNumber, 0)
		borrower, _ = entity.NewBorrower(
			borrowerID,
			userID,
			protocolID,
			tokenID,
			row.BlockNumber,
			0, // block_version
			totalDebt,
			big.NewInt(0), // change - we don't track deltas in historical load
		)
	}

	// If there's collateral and it's enabled, create a BorrowerCollateral
	hasCollateral := row.CurrentATokenBalance != nil && row.CurrentATokenBalance.Sign() > 0 && row.UsageAsCollateralEnabled

	if hasCollateral {
		collateralID := GenerateBorrowerCollateralID(userID, protocolID, tokenID, row.BlockNumber, 0)
		collateral, _ = entity.NewBorrowerCollateral(
			collateralID,
			userID,
			protocolID,
			tokenID,
			row.BlockNumber,
			0, // block_version
			row.CurrentATokenBalance,
			big.NewInt(0), // change
		)
	}

	return borrower, collateral
}

// UserAccountSnapshotRow represents a row from the source UserAccountSnapshot table.
// This contains aggregate account health data.
type UserAccountSnapshotRow struct {
	ID                          string
	ProtocolID                  string
	UserID                      string
	User                        string
	BlockNumber                 int64
	Timestamp                   int64
	TotalCollateralBase         *big.Int
	TotalDebtBase               *big.Int
	AvailableBorrowsBase        *big.Int
	CurrentLiquidationThreshold *big.Int
	LTV                         *big.Int
	HealthFactor                *big.Int
	EModeCategory               int
}

// ParseUserAccountSnapshotRow parses a COPY row into a UserAccountSnapshotRow.
func ParseUserAccountSnapshotRow(row []string, colIndex map[string]int) (*UserAccountSnapshotRow, error) {
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

	// These fields are optional and may be NULL or empty in source data
	var timestamp int64
	var eMode int
	if s := getField("timestamp"); s != "" {
		timestamp, _ = strconv.ParseInt(s, 10, 64)
	}
	if s := getField("e_mode_category"); s != "" {
		eMode, _ = strconv.Atoi(s)
	}

	return &UserAccountSnapshotRow{
		ID:                          getField("id"),
		ProtocolID:                  getField("protocol_id"),
		UserID:                      getField("user_id"),
		User:                        getField("user"),
		BlockNumber:                 blockNum,
		Timestamp:                   timestamp,
		TotalCollateralBase:         parseBigInt(getField("total_collateral_base")),
		TotalDebtBase:               parseBigInt(getField("total_debt_base")),
		AvailableBorrowsBase:        parseBigInt(getField("available_borrows_base")),
		CurrentLiquidationThreshold: parseBigInt(getField("current_liquidation_threshold")),
		LTV:                         parseBigInt(getField("ltv")),
		HealthFactor:                parseBigInt(getField("health_factor")),
		EModeCategory:               eMode,
	}, nil
}

// TransformUserAccountSnapshotToMetadata converts UserAccountSnapshot to UserProtocolMetadata.
// The account-level data (health factor, LTV, etc.) is stored in metadata JSONB.
func TransformUserAccountSnapshotToMetadata(
	row *UserAccountSnapshotRow,
	userID, protocolID int64,
) *entity.UserProtocolMetadata {
	metadataID := GenerateUserProtocolMetadataID(userID, protocolID)
	metadata := entity.NewUserProtocolMetadata(metadataID, userID, protocolID)

	// Store account-level metrics in metadata
	if row.HealthFactor != nil {
		metadata.SetMetadata("health_factor", row.HealthFactor.String())
	}
	if row.LTV != nil {
		metadata.SetMetadata("ltv", row.LTV.String())
	}
	if row.CurrentLiquidationThreshold != nil {
		metadata.SetMetadata("liquidation_threshold", row.CurrentLiquidationThreshold.String())
	}
	if row.TotalCollateralBase != nil {
		metadata.SetMetadata("total_collateral_base", row.TotalCollateralBase.String())
	}
	if row.TotalDebtBase != nil {
		metadata.SetMetadata("total_debt_base", row.TotalDebtBase.String())
	}
	if row.EModeCategory > 0 {
		metadata.SetMetadata("e_mode_category", row.EModeCategory)
	}
	metadata.SetMetadata("last_snapshot_block", row.BlockNumber)

	return metadata
}

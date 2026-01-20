package transform

import (
	"fmt"
	"strconv"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// UserRow represents a row from the source User table.
// Source schema (from S3):
//
//	id text NOT NULL,           -- e.g., "mainnet-0x..."
//	chain text NOT NULL,        -- e.g., "mainnet"
//	address text NOT NULL,      -- e.g., "0x..."
//	first_seen_block numeric(78,0),
//	first_seen_timestamp numeric(78,0)
type UserRow struct {
	ID                 string
	Chain              string
	Address            string
	FirstSeenBlock     int64
	FirstSeenTimestamp int64
}

// ParseUserRow parses a COPY row into a UserRow.
func ParseUserRow(row []string, colIndex map[string]int) (*UserRow, error) {
	getField := func(name string) string {
		if idx, ok := colIndex[name]; ok && idx < len(row) {
			return row[idx]
		}
		return ""
	}

	// These fields are optional and may be NULL or empty in source data
	var firstSeenBlock, firstSeenTimestamp int64
	if s := getField("first_seen_block"); s != "" {
		firstSeenBlock, _ = strconv.ParseInt(s, 10, 64)
	}
	if s := getField("first_seen_timestamp"); s != "" {
		firstSeenTimestamp, _ = strconv.ParseInt(s, 10, 64)
	}

	return &UserRow{
		ID:                 getField("id"),
		Chain:              getField("chain"),
		Address:            getField("address"),
		FirstSeenBlock:     firstSeenBlock,
		FirstSeenTimestamp: firstSeenTimestamp,
	}, nil
}

// TransformUser converts a UserRow to an entity.User.
func TransformUser(row *UserRow) (*entity.User, error) {
	chainID, err := ParseChainID(row.Chain)
	if err != nil {
		return nil, fmt.Errorf("unknown chain %s: %w", row.Chain, err)
	}

	address, err := HexToAddress(row.Address)
	if err != nil {
		return nil, fmt.Errorf("invalid address %s: %w", row.Address, err)
	}

	userID := GenerateUserID(chainID, NormalizeAddress(row.Address))

	user, err := entity.NewUser(userID, chainID, address)
	if err != nil {
		return nil, err
	}

	user.FirstSeenBlock = row.FirstSeenBlock

	return user, nil
}

// CreateUserFromUserID creates a User entity from a composite user_id like "mainnet-0x..."
// This is useful when we encounter users in snapshot data that may not exist in the User table.
func CreateUserFromUserID(userIDStr string) (*entity.User, error) {
	chainID, err := ParseChainFromUserID(userIDStr)
	if err != nil {
		return nil, err
	}

	addressStr, err := ParseAddressFromUserID(userIDStr)
	if err != nil {
		return nil, err
	}

	address, err := HexToAddress(addressStr)
	if err != nil {
		return nil, fmt.Errorf("invalid address %s: %w", addressStr, err)
	}

	userID := GenerateUserID(chainID, NormalizeAddress(addressStr))

	return entity.NewUser(userID, chainID, address)
}

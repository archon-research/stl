package transform

import (
	"fmt"
	"strconv"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// TokenRow represents a row from the source Token table.
// Source schema (from S3):
//
//	id text NOT NULL,           -- e.g., "mainnet-0x..."
//	chain text NOT NULL,        -- e.g., "mainnet"
//	address text NOT NULL,      -- e.g., "0x..."
//	symbol text NOT NULL,
//	name text NOT NULL,
//	decimals integer NOT NULL,
//	first_seen_block numeric(78,0),
//	first_seen_timestamp numeric(78,0)
type TokenRow struct {
	ID                 string
	Chain              string
	Address            string
	Symbol             string
	Name               string
	Decimals           int
	FirstSeenBlock     int64
	FirstSeenTimestamp int64
}

// ParseTokenRow parses a COPY row into a TokenRow.
func ParseTokenRow(row []string, colIndex map[string]int) (*TokenRow, error) {
	getField := func(name string) string {
		if idx, ok := colIndex[name]; ok && idx < len(row) {
			return row[idx]
		}
		return ""
	}

	decimals, err := strconv.Atoi(getField("decimals"))
	if err != nil {
		decimals = 18 // default
	}

	// These fields are optional and may be NULL or empty in source data
	var firstSeenBlock, firstSeenTimestamp int64
	if s := getField("first_seen_block"); s != "" {
		firstSeenBlock, _ = strconv.ParseInt(s, 10, 64)
	}
	if s := getField("first_seen_timestamp"); s != "" {
		firstSeenTimestamp, _ = strconv.ParseInt(s, 10, 64)
	}

	return &TokenRow{
		ID:                 getField("id"),
		Chain:              getField("chain"),
		Address:            getField("address"),
		Symbol:             getField("symbol"),
		Name:               getField("name"),
		Decimals:           decimals,
		FirstSeenBlock:     firstSeenBlock,
		FirstSeenTimestamp: firstSeenTimestamp,
	}, nil
}

// TransformToken converts a TokenRow to an entity.Token.
func TransformToken(row *TokenRow) (*entity.Token, error) {
	chainID, err := ParseChainID(row.Chain)
	if err != nil {
		return nil, fmt.Errorf("unknown chain %s: %w", row.Chain, err)
	}

	address, err := HexToAddress(row.Address)
	if err != nil {
		return nil, fmt.Errorf("invalid address %s: %w", row.Address, err)
	}

	tokenID := GenerateTokenID(chainID, NormalizeAddress(row.Address))

	token, err := entity.NewToken(
		tokenID,
		chainID,
		address,
		row.Symbol,
		int16(row.Decimals),
		row.FirstSeenBlock,
	)
	if err != nil {
		return nil, err
	}
	if row.Name != "" {
		token.Metadata["name"] = row.Name
	}

	return token, nil
}

package transform

import (
	"fmt"
	"strconv"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ProtocolRow represents a row from the source Protocol table.
// Source schema (from S3):
//
//	id text NOT NULL,           -- e.g., "sparklend-mainnet"
//	chain text NOT NULL,        -- e.g., "mainnet"
//	address text NOT NULL,      -- pool address
//	name text NOT NULL,         -- e.g., "SparkLend"
//	protocol_type text NOT NULL, -- e.g., "lending"
//	first_seen_block numeric(78,0),
//	first_seen_timestamp numeric(78,0)
type ProtocolRow struct {
	ID                 string
	Chain              string
	Address            string
	Name               string
	ProtocolType       string
	FirstSeenBlock     int64
	FirstSeenTimestamp int64
}

// ParseProtocolRow parses a COPY row into a ProtocolRow.
func ParseProtocolRow(row []string, colIndex map[string]int) (*ProtocolRow, error) {
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

	return &ProtocolRow{
		ID:                 getField("id"),
		Chain:              getField("chain"),
		Address:            getField("address"),
		Name:               getField("name"),
		ProtocolType:       getField("protocol_type"),
		FirstSeenBlock:     firstSeenBlock,
		FirstSeenTimestamp: firstSeenTimestamp,
	}, nil
}

// TransformProtocol converts a ProtocolRow to an entity.Protocol.
func TransformProtocol(row *ProtocolRow) (*entity.Protocol, error) {
	chainID, err := ParseChainID(row.Chain)
	if err != nil {
		return nil, fmt.Errorf("unknown chain %s: %w", row.Chain, err)
	}

	address, err := HexToAddress(row.Address)
	if err != nil {
		return nil, fmt.Errorf("invalid address %s: %w", row.Address, err)
	}

	protocolID := GenerateProtocolID(chainID, NormalizeAddress(row.Address))

	protocol, err := entity.NewProtocol(
		protocolID,
		chainID,
		address,
		row.Name,
		row.ProtocolType,
		row.FirstSeenBlock,
	)
	if err != nil {
		return nil, err
	}

	return protocol, nil
}

// GetSparkLendProtocolID returns the protocol ID for SparkLend on mainnet.
// This is a known protocol used in the source data.
const SparkLendMainnetAddress = "0x2f39d218133afab8f2b819b1066c7e434ad94e9e" // Pool address

// GetSparkLendMainnetProtocolID returns the deterministic protocol ID for SparkLend on mainnet.
func GetSparkLendMainnetProtocolID() int64 {
	return GenerateProtocolID(1, NormalizeAddress(SparkLendMainnetAddress))
}

// CreateSparkLendProtocol creates the SparkLend mainnet protocol entity.
func CreateSparkLendProtocol() (*entity.Protocol, error) {
	address, _ := HexToAddress(SparkLendMainnetAddress)
	return entity.NewProtocol(
		GetSparkLendMainnetProtocolID(),
		1, // mainnet
		address,
		"SparkLend",
		"lending",
		19463078, // SparkLend deployment block on mainnet
	)
}

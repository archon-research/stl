package transform

import (
	"fmt"
	"strings"
)

// ChainNameToID maps chain names to their chain IDs.
// This mirrors the mapping in entity.ChainNameToID but is used
// for transforming source data where chain is specified by name.
var ChainNameToID = map[string]int{
	"mainnet":   1,
	"ethereum":  1,
	"goerli":    5,
	"sepolia":   11155111,
	"arbitrum":  42161,
	"optimism":  10,
	"polygon":   137,
	"base":      8453,
	"avalanche": 43114,
	"gnosis":    100,
}

// ChainIDToName maps chain IDs to their canonical names.
var ChainIDToName = map[int]string{
	1:        "mainnet",
	5:        "goerli",
	11155111: "sepolia",
	42161:    "arbitrum",
	10:       "optimism",
	137:      "polygon",
	8453:     "base",
	43114:    "avalanche",
	100:      "gnosis",
}

// ParseChainID extracts the chain ID from a text representation.
// Handles formats like:
//   - "mainnet" -> 1
//   - "1" -> 1
//   - "mainnet-0x..." (user_id format) -> 1
func ParseChainID(chainText string) (int, error) {
	// Handle user_id format: "mainnet-0x..."
	if idx := strings.Index(chainText, "-0x"); idx > 0 {
		chainText = chainText[:idx]
	}

	// Try as chain name
	chainText = strings.ToLower(strings.TrimSpace(chainText))
	if id, ok := ChainNameToID[chainText]; ok {
		return id, nil
	}

	// Try as numeric ID
	var id int
	if _, err := fmt.Sscanf(chainText, "%d", &id); err == nil {
		return id, nil
	}

	return 0, fmt.Errorf("unknown chain: %s", chainText)
}

// ParseChainFromUserID extracts chain name from a user_id like "mainnet-0x..."
func ParseChainFromUserID(userID string) (int, error) {
	parts := strings.SplitN(userID, "-", 2)
	if len(parts) < 2 {
		return 0, fmt.Errorf("invalid user_id format: %s", userID)
	}
	return ParseChainID(parts[0])
}

// ParseAddressFromUserID extracts the address from a user_id like "mainnet-0x..."
func ParseAddressFromUserID(userID string) (string, error) {
	parts := strings.SplitN(userID, "-", 2)
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid user_id format: %s", userID)
	}
	return parts[1], nil
}

package blockchain

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestIsKnownProtocol(t *testing.T) {
	tests := []struct {
		name     string
		address  string
		expected bool
	}{
		{
			name:     "Aave V2 mainnet",
			address:  "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
			expected: true,
		},
		{
			name:     "Aave V3 mainnet",
			address:  "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
			expected: true,
		},
		{
			name:     "Sparklend mainnet",
			address:  "0xC13e21B648A5Ee794902342038FF3aDAB66BE987",
			expected: true,
		},
		{
			name:     "Aave V3 Lido",
			address:  "0x4e033931ad43597d96d6bcc25c280717730b58b1",
			expected: true,
		},
		{
			name:     "Aave V3 RWA",
			address:  "0xAe05Cd22df81871bc7cC2a04BeCfb516bFe332C8",
			expected: true,
		},
		{
			name:     "unknown protocol - RedemptionIdle",
			address:  "0x4c21B7577C8FE8b0B0669165ee7C8f67fa1454Cf",
			expected: false,
		},
		{
			name:     "zero address",
			address:  "0x0000000000000000000000000000000000000000",
			expected: false,
		},
		{
			name:     "random unknown address",
			address:  "0x1234567890123456789012345678901234567890",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := common.HexToAddress(tt.address)
			result := IsKnownProtocol(addr)
			if result != tt.expected {
				t.Errorf("IsKnownProtocol(%s) = %v, want %v", tt.address, result, tt.expected)
			}
		})
	}
}

func TestGetProtocolConfig(t *testing.T) {
	tests := []struct {
		name         string
		address      string
		expectExists bool
		expectName   string
		expectType   string
	}{
		{
			name:         "Sparklend mainnet",
			address:      "0xC13e21B648A5Ee794902342038FF3aDAB66BE987",
			expectExists: true,
			expectName:   "Sparklend",
			expectType:   "lending",
		},
		{
			name:         "Aave V3 mainnet",
			address:      "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
			expectExists: true,
			expectName:   "Aave V3",
			expectType:   "lending",
		},
		{
			name:         "unknown address",
			address:      "0x4c21B7577C8FE8b0B0669165ee7C8f67fa1454Cf",
			expectExists: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := common.HexToAddress(tt.address)
			config, exists := GetProtocolConfig(addr)
			if exists != tt.expectExists {
				t.Errorf("GetProtocolConfig(%s) exists = %v, want %v", tt.address, exists, tt.expectExists)
				return
			}
			if tt.expectExists {
				if config.Name != tt.expectName {
					t.Errorf("GetProtocolConfig(%s).Name = %v, want %v", tt.address, config.Name, tt.expectName)
				}
				if config.ProtocolType != tt.expectType {
					t.Errorf("GetProtocolConfig(%s).ProtocolType = %v, want %v", tt.address, config.ProtocolType, tt.expectType)
				}
			}
		})
	}
}

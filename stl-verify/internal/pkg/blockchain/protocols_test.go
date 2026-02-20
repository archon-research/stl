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
			name:     "Aave V3 Avalanche",
			address:  "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
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
		chainID      int64
		address      string
		expectExists bool
		expectName   string
		expectType   string
	}{
		{
			name:         "Sparklend mainnet",
			chainID:      1,
			address:      "0xC13e21B648A5Ee794902342038FF3aDAB66BE987",
			expectExists: true,
			expectName:   "Sparklend",
			expectType:   "lending",
		},
		{
			name:         "Aave V3 mainnet",
			chainID:      1,
			address:      "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
			expectExists: true,
			expectName:   "Aave V3",
			expectType:   "lending",
		},
		{
			name:         "Aave V3 Avalanche",
			chainID:      43114,
			address:      "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
			expectExists: true,
			expectName:   "Aave V3 Avalanche",
			expectType:   "lending",
		},
		{
			name:         "Aave V3 address on wrong chain",
			chainID:      43114,
			address:      "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
			expectExists: false,
		},
		{
			name:         "Avalanche address on Ethereum",
			chainID:      1,
			address:      "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
			expectExists: false,
		},
		{
			name:         "unknown address",
			chainID:      1,
			address:      "0x4c21B7577C8FE8b0B0669165ee7C8f67fa1454Cf",
			expectExists: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := common.HexToAddress(tt.address)
			config, exists := GetProtocolConfig(tt.chainID, addr)
			if exists != tt.expectExists {
				t.Errorf("GetProtocolConfig(%d, %s) exists = %v, want %v", tt.chainID, tt.address, exists, tt.expectExists)
				return
			}
			if tt.expectExists {
				if config.Name != tt.expectName {
					t.Errorf("GetProtocolConfig(%d, %s).Name = %v, want %v", tt.chainID, tt.address, config.Name, tt.expectName)
				}
				if config.ProtocolType != tt.expectType {
					t.Errorf("GetProtocolConfig(%d, %s).ProtocolType = %v, want %v", tt.chainID, tt.address, config.ProtocolType, tt.expectType)
				}
			}
		})
	}
}

func TestProtocolRegistryPoolDataProviderHistoryIsSorted(t *testing.T) {
	registry := GetProtocolRegistry()

	for protocolAddress, config := range registry {
		history := config.PoolDataProviderHistory
		for i := 1; i < len(history); i++ {
			if history[i-1].ActiveAtBlock > history[i].ActiveAtBlock {
				t.Fatalf(
					"protocol %s history is not sorted at index %d: %d > %d",
					protocolAddress.Hex(),
					i,
					history[i-1].ActiveAtBlock,
					history[i].ActiveAtBlock,
				)
			}
		}
	}
}

func TestGetPoolDataProviderForBlock_AaveV3Boundaries(t *testing.T) {
	protocolAddress := common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2")

	tests := []struct {
		name          string
		blockNumber   uint64
		expectedAddr  string
		expectedFound bool
	}{
		{
			name:          "before first provider activation",
			blockNumber:   16291080,
			expectedFound: false,
		},
		{
			name:          "at first provider activation",
			blockNumber:   16291081,
			expectedAddr:  "0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3",
			expectedFound: true,
		},
		{
			name:          "at second provider activation",
			blockNumber:   20398674,
			expectedAddr:  "0x20e074F62EcBD8BC5E38211adCb6103006113A22",
			expectedFound: true,
		},
		{
			name:          "between fourth and fifth provider activation",
			blockNumber:   22000000,
			expectedAddr:  "0x497a1994c46d4f6C864904A9f1fac6328Cb7C8a6",
			expectedFound: true,
		},
		{
			name:          "at latest provider activation",
			blockNumber:   22839362,
			expectedAddr:  "0x0a16f2FCC0D44FaE41cc54e079281D84A363bECD",
			expectedFound: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAddr, found := GetPoolDataProviderForBlock(protocolAddress, tt.blockNumber)
			if found != tt.expectedFound {
				t.Fatalf("GetPoolDataProviderForBlock(..., %d) found = %v, want %v", tt.blockNumber, found, tt.expectedFound)
			}

			if !tt.expectedFound {
				if gotAddr != (common.Address{}) {
					t.Fatalf("GetPoolDataProviderForBlock(..., %d) addr = %s, want zero address", tt.blockNumber, gotAddr.Hex())
				}
				return
			}

			expectedAddr := common.HexToAddress(tt.expectedAddr)
			if gotAddr != expectedAddr {
				t.Fatalf("GetPoolDataProviderForBlock(..., %d) addr = %s, want %s", tt.blockNumber, gotAddr.Hex(), expectedAddr.Hex())
			}
		})
	}
}

package blockchain

import "github.com/ethereum/go-ethereum/common"

type ProtocolVersion string

const (
	ProtocolVersionAaveV2    ProtocolVersion = "aave-v2"
	ProtocolVersionAaveV3    ProtocolVersion = "aave-v3"
	ProtocolVersionSparkLend ProtocolVersion = "sparklend"
)

// ContractWithBlock represents a contract address with its deployment block.
type ContractWithBlock struct {
	Address        common.Address
	CreatedAtBlock uint64
}

// PoolDataProviderHistory represents the history of PoolDataProvider addresses for a protocol.
// Each entry is valid from its CreatedAtBlock until the next entry's CreatedAtBlock - 1.
type PoolDataProviderHistory []ContractWithBlock

type ProtocolConfig struct {
	Name                  string
	ProtocolType          string
	PoolAddress           ContractWithBlock
	UIPoolDataProvider    ContractWithBlock
	PoolAddressesProvider ContractWithBlock
	ProtocolVersion       ProtocolVersion
	// PoolDataProviderHistory contains all historical PoolDataProvider addresses.
	// Sorted by CreatedAtBlock ascending. Use GetPoolDataProviderForBlock to get the correct one.
	PoolDataProviderHistory PoolDataProviderHistory
}

var protocolRegistry = map[common.Address]ProtocolConfig{
	// Aave V2 - Pool created at block 11362579
	// PoolDataProvider has been stable since deployment
	common.HexToAddress("0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9"): {
		Name:                  "Aave V2",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9"), CreatedAtBlock: 11362579},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x00e50FAB64eBB37b87df06Aa46b8B35d5f1A4e1A"), CreatedAtBlock: 16384806},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0xB53C1a33016B2DC2fF3653530bfF1848a515c8c5"), CreatedAtBlock: 11362562},
		ProtocolVersion:       ProtocolVersionAaveV2,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d"), CreatedAtBlock: 11362589},
		},
	},

	// Aave V3 - Pool created at block 16291127
	// PoolDataProvider has been updated multiple times
	common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"): {
		Name:                  "Aave V3",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"), CreatedAtBlock: 16291127},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d"), CreatedAtBlock: 16291263},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"), CreatedAtBlock: 16291071},
		ProtocolVersion:       ProtocolVersionAaveV3,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3"), CreatedAtBlock: 16291081},
			{Address: common.HexToAddress("0x20e074F62EcBD8BC5E38211adCb6103006113A22"), CreatedAtBlock: 20398674},
			{Address: common.HexToAddress("0x41393e5e337606dc3821075Af65AeE84D7688CBD"), CreatedAtBlock: 20920979},
			{Address: common.HexToAddress("0x497a1994c46d4f6C864904A9f1fac6328Cb7C8a6"), CreatedAtBlock: 21917056},
			{Address: common.HexToAddress("0x0a16f2FCC0D44FaE41cc54e079281D84A363bECD"), CreatedAtBlock: 22839362},
		},
	},

	// SparkLend - Pool created at block 16776401
	// PoolDataProvider has been stable since deployment
	common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987"): {
		Name:                  "Sparklend",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987"), CreatedAtBlock: 16776401},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x56b7A1012765C285afAC8b8F25C69Bf10ccfE978"), CreatedAtBlock: 24033627},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0x02C3eA4e34C0cBd694D2adFa2c690EECbC1793eE"), CreatedAtBlock: 16776389},
		ProtocolVersion:       ProtocolVersionSparkLend,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0xFc21d6d146E6086B8359705C8b28512a983db0cb"), CreatedAtBlock: 16776400},
		},
	},

	// Aave V3 Lido - Pool created at block 20262414
	// PoolDataProvider has been updated multiple times
	common.HexToAddress("0x4e033931ad43597d96d6bcc25c280717730b58b1"): {
		Name:                  "Aave V3 Lido",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0x4e033931ad43597d96d6bcc25c280717730b58b1"), CreatedAtBlock: 20262414},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d"), CreatedAtBlock: 16291263},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0xcfBf336fe147D643B9Cb705648500e101504B16d"), CreatedAtBlock: 20262370},
		ProtocolVersion:       ProtocolVersionAaveV3,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0xa3206d66cF94AA1e93B21a9D8d409d6375309F4A"), CreatedAtBlock: 20262414},
			{Address: common.HexToAddress("0x08795CFE08C7a81dCDFf482BbAAF474B240f31cD"), CreatedAtBlock: 20920979},
			{Address: common.HexToAddress("0x66FeAe868EBEd74A34A7043e88742AAE00D2bC53"), CreatedAtBlock: 21917056},
			{Address: common.HexToAddress("0xB85B2bFEbeC4F5f401dbf92ac147A3076391fCD5"), CreatedAtBlock: 22839362},
		},
	},

	// Aave V3 RWA - Pool created at block 23125535
	// PoolDataProvider has been stable since deployment
	common.HexToAddress("0xAe05Cd22df81871bc7cC2a04BeCfb516bFe332C8"): {
		Name:                  "Aave V3 RWA",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0xAe05Cd22df81871bc7cC2a04BeCfb516bFe332C8"), CreatedAtBlock: 23125535},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d"), CreatedAtBlock: 16291263},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0x5D39E06b825C1F2B80bf2756a73e28eFAA128ba0"), CreatedAtBlock: 23125530},
		ProtocolVersion:       ProtocolVersionAaveV3,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0x53519c32f73fE1797d10210c4950fFeBa3b21504"), CreatedAtBlock: 23125535},
		},
	},
}

// GetPoolDataProviderForBlock returns the PoolDataProvider address that was active at the given block.
// Returns the zero address if no PoolDataProvider was active at that block.
func (h PoolDataProviderHistory) GetForBlock(blockNumber uint64) common.Address {
	if len(h) == 0 {
		return common.Address{}
	}

	// Find the latest entry that was created at or before the given block
	var result common.Address
	for _, entry := range h {
		if entry.CreatedAtBlock <= blockNumber {
			result = entry.Address
		} else {
			break
		}
	}
	return result
}

// GetLatest returns the most recent PoolDataProvider address.
func (h PoolDataProviderHistory) GetLatest() common.Address {
	if len(h) == 0 {
		return common.Address{}
	}
	return h[len(h)-1].Address
}

func GetProtocolConfig(protocolAddress common.Address) (ProtocolConfig, bool) {
	config, exists := protocolRegistry[protocolAddress]
	return config, exists
}

// GetProtocolRegistry returns a copy of all known protocol configs keyed by pool address.
func GetProtocolRegistry() map[common.Address]ProtocolConfig {
	registry := make(map[common.Address]ProtocolConfig, len(protocolRegistry))
	for address, config := range protocolRegistry {
		registry[address] = config
	}
	return registry
}

func IsKnownProtocol(protocolAddress common.Address) bool {
	_, exists := protocolRegistry[protocolAddress]
	return exists
}

// GetPoolDataProviderForBlock is a convenience function that returns the correct
// PoolDataProvider for a given protocol at a specific block.
func GetPoolDataProviderForBlock(protocolAddress common.Address, blockNumber uint64) (common.Address, bool) {
	config, exists := protocolRegistry[protocolAddress]
	if !exists {
		return common.Address{}, false
	}
	provider := config.PoolDataProviderHistory.GetForBlock(blockNumber)
	if provider == (common.Address{}) {
		return common.Address{}, false
	}
	return provider, true
}

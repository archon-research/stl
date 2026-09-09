package blockchain

// Data sources for protocol addresses and block numbers:
//
// Aave Address Book (canonical source for Aave protocols):
//   https://github.com/aave-dao/aave-address-book
//
// PoolDataProvider history was obtained by querying on-chain events:
//   - AddressSet(bytes32 indexed id, address indexed oldAddress, address indexed newAddress)
//     emitted by PoolAddressesProvider when POOL_DATA_PROVIDER is updated
//   - PoolDataProviderUpdated(address indexed oldAddress, address indexed newAddress)
//     emitted by some PoolAddressesProvider implementations
//
// The CreatedAtBlock for PoolDataProviderHistory entries is the block when the
// provider was REGISTERED (became active), not when the contract was deployed.
// A contract may be deployed earlier but only becomes the active provider when
// the PoolAddressesProvider is updated.

import (
	"sort"

	"github.com/ethereum/go-ethereum/common"
)

type ProtocolVersion string

const (
	ProtocolVersionAaveV2    ProtocolVersion = "aave-v2"
	ProtocolVersionAaveV3    ProtocolVersion = "aave-v3"
	ProtocolVersionSparkLend ProtocolVersion = "sparklend"
)

// ProtocolKey uniquely identifies a protocol by chain and pool address.
// Required because the same pool address can exist on multiple chains (e.g. Aave V3
// uses CREATE2, so 0x794a... is the same on Avalanche, Arbitrum, Polygon, etc.).
type ProtocolKey struct {
	ChainID     int64
	PoolAddress common.Address
}

// ContractWithBlock represents a contract address with its activation block.
type ContractWithBlock struct {
	Address       common.Address
	ActiveAtBlock uint64
}

// PoolDataProviderHistory represents the history of PoolDataProvider addresses for a protocol.
// Each entry is valid from its ActiveAtBlock until the next entry's ActiveAtBlock - 1.
type PoolDataProviderHistory []ContractWithBlock

type ProtocolConfig struct {
	Name         string
	Slug         string
	ProtocolType string
	PoolAddress  ContractWithBlock
	// ActiveAtBlock is documentation only: position reads call this address at
	// whatever block they are given, so a read below it reverts (backfill only).
	UIPoolDataProvider    ContractWithBlock
	PoolAddressesProvider ContractWithBlock
	ProtocolVersion       ProtocolVersion
	// PoolDataProviderHistory contains all historical PoolDataProvider addresses.
	// Sorted by ActiveAtBlock ascending. Use GetForBlock to get the correct one.
	PoolDataProviderHistory PoolDataProviderHistory
}

var protocolRegistry = map[ProtocolKey]ProtocolConfig{
	// ═══════════════════════════════════════════════════════════════════════════
	// Ethereum (chainID 1)
	// ═══════════════════════════════════════════════════════════════════════════

	// Aave V2 - Pool created at block 11362579
	// PoolDataProvider has been stable since deployment
	// Source: https://github.com/aave-dao/aave-address-book/blob/main/src/AaveV2Ethereum.sol
	{1, common.HexToAddress("0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9")}: {
		Name:                  "Aave V2",
		Slug:                  "aave_v2_ethereum",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9"), ActiveAtBlock: 11362579},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x00e50FAB64eBB37b87df06Aa46b8B35d5f1A4e1A"), ActiveAtBlock: 16384806},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0xB53C1a33016B2DC2fF3653530bfF1848a515c8c5"), ActiveAtBlock: 11362562},
		ProtocolVersion:       ProtocolVersionAaveV2,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0x057835Ad21a177dbdd3090bB1CAE03EaCF78Fc6d"), ActiveAtBlock: 11362589},
		},
	},

	// Aave V3 - Pool created at block 16291127
	// PoolDataProvider has been updated multiple times via AddressSet events
	// UIPoolDataProvider updated to 0x56b7A101... at block 24033627 (Dec 2025)
	// Source: https://github.com/aave-dao/aave-address-book/blob/main/src/AaveV3Ethereum.sol
	{1, common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2")}: {
		Name:                  "Aave V3",
		Slug:                  "aave_v3_ethereum",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"), ActiveAtBlock: 16291127},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x56b7A1012765C285afAC8b8F25C69Bf10ccfE978"), ActiveAtBlock: 24033627},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"), ActiveAtBlock: 16291071},
		ProtocolVersion:       ProtocolVersionAaveV3,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0x7B4EB56E7CD4b454BA8ff71E4518426369a138a3"), ActiveAtBlock: 16291081},
			{Address: common.HexToAddress("0x20e074F62EcBD8BC5E38211adCb6103006113A22"), ActiveAtBlock: 20398674},
			{Address: common.HexToAddress("0x41393e5e337606dc3821075Af65AeE84D7688CBD"), ActiveAtBlock: 20920979},
			{Address: common.HexToAddress("0x497a1994c46d4f6C864904A9f1fac6328Cb7C8a6"), ActiveAtBlock: 21917056},
			{Address: common.HexToAddress("0x0a16f2FCC0D44FaE41cc54e079281D84A363bECD"), ActiveAtBlock: 22839362},
		},
	},

	// SparkLend - Pool created at block 16776401
	// PoolDataProvider has been stable since deployment
	// Source: https://github.com/marsfoundation/spark-address-registry
	{1, common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")}: {
		Name:                  "Sparklend",
		Slug:                  "spark_ethereum",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987"), ActiveAtBlock: 16776401},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x56b7A1012765C285afAC8b8F25C69Bf10ccfE978"), ActiveAtBlock: 24033627},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0x02C3eA4e34C0cBd694D2adFa2c690EECbC1793eE"), ActiveAtBlock: 16776389},
		ProtocolVersion:       ProtocolVersionSparkLend,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0xFc21d6d146E6086B8359705C8b28512a983db0cb"), ActiveAtBlock: 16776400},
		},
	},

	// Aave V3 Lido - Pool created at block 20262414
	// PoolDataProvider has been updated multiple times via AddressSet events
	// UIPoolDataProvider updated to 0x56b7A101... at block 24033627 (Dec 2025)
	// Source: https://github.com/aave-dao/aave-address-book/blob/main/src/AaveV3EthereumLido.sol
	{1, common.HexToAddress("0x4e033931ad43597d96d6bcc25c280717730b58b1")}: {
		Name:                  "Aave V3 Lido",
		Slug:                  "aave_v3_lido_ethereum",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0x4e033931ad43597d96d6bcc25c280717730b58b1"), ActiveAtBlock: 20262414},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x56b7A1012765C285afAC8b8F25C69Bf10ccfE978"), ActiveAtBlock: 24033627},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0xcfBf336fe147D643B9Cb705648500e101504B16d"), ActiveAtBlock: 20262370},
		ProtocolVersion:       ProtocolVersionAaveV3,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0xa3206d66cF94AA1e93B21a9D8d409d6375309F4A"), ActiveAtBlock: 20262414},
			{Address: common.HexToAddress("0x08795CFE08C7a81dCDFf482BbAAF474B240f31cD"), ActiveAtBlock: 20920979},
			{Address: common.HexToAddress("0x66FeAe868EBEd74A34A7043e88742AAE00D2bC53"), ActiveAtBlock: 21917056},
			{Address: common.HexToAddress("0xB85B2bFEbeC4F5f401dbf92ac147A3076391fCD5"), ActiveAtBlock: 22839362},
		},
	},

	// Aave V3 RWA - Pool created at block 23125535
	// PoolDataProvider has been stable since deployment
	// UIPoolDataProvider updated to 0x56b7A101... at block 24033627 (Dec 2025)
	// Source: https://github.com/aave-dao/aave-address-book (check for RWA instance)
	{1, common.HexToAddress("0xAe05Cd22df81871bc7cC2a04BeCfb516bFe332C8")}: {
		Name:                  "Aave V3 RWA",
		Slug:                  "aave_v3_rwa_ethereum",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0xAe05Cd22df81871bc7cC2a04BeCfb516bFe332C8"), ActiveAtBlock: 23125535},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x56b7A1012765C285afAC8b8F25C69Bf10ccfE978"), ActiveAtBlock: 24033627},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0x5D39E06b825C1F2B80bf2756a73e28eFAA128ba0"), ActiveAtBlock: 23125530},
		ProtocolVersion:       ProtocolVersionAaveV3,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0x53519c32f73fE1797d10210c4950fFeBa3b21504"), ActiveAtBlock: 23125535},
		},
	},

	// ═══════════════════════════════════════════════════════════════════════════
	// Avalanche C-Chain (chainID 43114)
	// ═══════════════════════════════════════════════════════════════════════════

	// Aave V3 Avalanche - Pool deployed via CREATE2 (same address across chains)
	// Source: https://github.com/bgd-labs/aave-address-book (AaveV3Avalanche)
	{43114, common.HexToAddress("0x794a61358D6845594F94dc1DB02A252b5b4814aD")}: {
		Name:                  "Aave V3 Avalanche",
		Slug:                  "aave_v3_avalanche",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0x794a61358D6845594F94dc1DB02A252b5b4814aD"), ActiveAtBlock: 11970506},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x3518E8927A7827CDdAf841872453003CA95906A3"), ActiveAtBlock: 11970506},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb"), ActiveAtBlock: 11970454},
		ProtocolVersion:       ProtocolVersionAaveV3,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"), ActiveAtBlock: 11970506},
			{Address: common.HexToAddress("0x243Aa95cAC2a25651eda86e80bEe66114413c43b"), ActiveAtBlock: 40714595},
		},
	},

	// ═══════════════════════════════════════════════════════════════════════════
	// Arbitrum One (chainID 42161)
	// ═══════════════════════════════════════════════════════════════════════════

	// Aave V3 Arbitrum - Pool created at block 7742429
	// PoolDataProvider rotated 6 times, via PoolDataProviderUpdated not AddressSet
	// Source: https://github.com/bgd-labs/aave-address-book (AaveV3Arbitrum)
	{42161, common.HexToAddress("0x794a61358D6845594F94dc1DB02A252b5b4814aD")}: {
		Name:                  "Aave V3 Arbitrum",
		Slug:                  "aave_v3_arbitrum",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0x794a61358D6845594F94dc1DB02A252b5b4814aD"), ActiveAtBlock: 7742429},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x91E04cf78e53aEBe609e8a7f2003e7EECD743F2B"), ActiveAtBlock: 451098138},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb"), ActiveAtBlock: 7736680},
		ProtocolVersion:       ProtocolVersionAaveV3,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"), ActiveAtBlock: 7736699},
			{Address: common.HexToAddress("0x6b4E260b765B3cA1514e618C0215A6B7839fF93e"), ActiveAtBlock: 89267099},
			{Address: common.HexToAddress("0x7deEB8aCE4220643D8edeC871a23807E4d006eE5"), ActiveAtBlock: 236626700},
			{Address: common.HexToAddress("0x7F23D86Ee20D869112572136221e173428DD740B"), ActiveAtBlock: 261667030},
			{Address: common.HexToAddress("0x14496b405D62c24F91f04Cda1c69Dc526D56fDE5"), ActiveAtBlock: 309447822},
			{Address: common.HexToAddress("0x243Aa95cAC2a25651eda86e80bEe66114413c43b"), ActiveAtBlock: 353846925},
		},
	},

	// ═══════════════════════════════════════════════════════════════════════════
	// OP Mainnet (chainID 10)
	// ═══════════════════════════════════════════════════════════════════════════

	// Aave V3 Optimism - Pool created at block 4365693
	// PoolDataProvider rotated 6 times, via PoolDataProviderUpdated not AddressSet
	// Source: https://github.com/bgd-labs/aave-address-book (AaveV3Optimism)
	{10, common.HexToAddress("0x794a61358D6845594F94dc1DB02A252b5b4814aD")}: {
		Name:                  "Aave V3 Optimism",
		Slug:                  "aave_v3_optimism",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0x794a61358D6845594F94dc1DB02A252b5b4814aD"), ActiveAtBlock: 4365693},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x68100bD5345eA474D93577127C11F39FF8463e93"), ActiveAtBlock: 150123046},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0xa97684ead0e402dC232d5A977953DF7ECBaB3CDb"), ActiveAtBlock: 4365470},
		ProtocolVersion:       ProtocolVersionAaveV3,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0x69FA688f1Dc47d4B5d8029D5a35FB7a548310654"), ActiveAtBlock: 4365508},
			{Address: common.HexToAddress("0xd9Ca4878dd38B021583c1B669905592EAe76E044"), ActiveAtBlock: 97221914},
			{Address: common.HexToAddress("0x7deEB8aCE4220643D8edeC871a23807E4d006eE5"), ActiveAtBlock: 123246825},
			{Address: common.HexToAddress("0x7F23D86Ee20D869112572136221e173428DD740B"), ActiveAtBlock: 126396433},
			{Address: common.HexToAddress("0x14496b405D62c24F91f04Cda1c69Dc526D56fDE5"), ActiveAtBlock: 132405959},
			{Address: common.HexToAddress("0x243Aa95cAC2a25651eda86e80bEe66114413c43b"), ActiveAtBlock: 137977405},
		},
	},

	// ═══════════════════════════════════════════════════════════════════════════
	// Base (chainID 8453)
	// ═══════════════════════════════════════════════════════════════════════════

	// Aave V3 Base - Pool created at block 2357134, its own address rather than
	// the CREATE2 0x794a61... that Arbitrum/Optimism/Avalanche share.
	// PoolDataProvider rotated 5 times, via PoolDataProviderUpdated not AddressSet
	// SparkLend's Ethereum PoolAddressesProvider address also has unrelated code
	// here; it reverts on getMarketId/getPool, so Base gets no SparkLend entry.
	// Source: https://github.com/bgd-labs/aave-address-book (AaveV3Base)
	{8453, common.HexToAddress("0xA238Dd80C259a72e81d7e4664a9801593F98d1c5")}: {
		Name:                  "Aave V3 Base",
		Slug:                  "aave_v3_base",
		ProtocolType:          "lending",
		PoolAddress:           ContractWithBlock{Address: common.HexToAddress("0xA238Dd80C259a72e81d7e4664a9801593F98d1c5"), ActiveAtBlock: 2357134},
		UIPoolDataProvider:    ContractWithBlock{Address: common.HexToAddress("0x0C6BC4a12039788be08F87e87Cff87FEDbd1D386"), ActiveAtBlock: 44526436},
		PoolAddressesProvider: ContractWithBlock{Address: common.HexToAddress("0xe20fCBdBfFC4Dd138cE8b2E6FBb6CB49777ad64D"), ActiveAtBlock: 2357105},
		ProtocolVersion:       ProtocolVersionAaveV3,
		PoolDataProviderHistory: PoolDataProviderHistory{
			{Address: common.HexToAddress("0x2d8A3C5677189723C4cB8873CfC9C8976FDF38Ac"), ActiveAtBlock: 2357134},
			{Address: common.HexToAddress("0x793177a6Cf520C7fE5B2E45660EBB48132184BBC"), ActiveAtBlock: 17651562},
			{Address: common.HexToAddress("0xd82a47fdebB5bf5329b09441C3DaB4b5df2153Ad"), ActiveAtBlock: 20801170},
			{Address: common.HexToAddress("0xC4Fcf9893072d61Cc2899C0054877Cb752587981"), ActiveAtBlock: 26810726},
			{Address: common.HexToAddress("0x0F43731EB8d45A581f4a36DD74F5f358bc90C73A"), ActiveAtBlock: 32382140},
		},
	},

	// ═══════════════════════════════════════════════════════════════════════════
	// Unichain (chainID 130) - deliberately empty
	// ═══════════════════════════════════════════════════════════════════════════
	//
	// Neither Aave V3 nor SparkLend is deployed on Unichain (verified 2026-09-08):
	// aave-address-book has no AaveV3Unichain, spark-address-registry's Unichain.sol
	// declares PSM3/ALM/SSR but no POOL, and a chain-wide eth_getLogs sweep over the
	// Aave-family topics found only an abandoned unbranded fork (~$300 TVL).
	// Revisit if Aave launches on Unichain (ARCT-213).
}

func init() {
	// Sort all PoolDataProviderHistory slices by ActiveAtBlock ascending.
	// This ensures GetForBlock works correctly regardless of declaration order.
	for key, config := range protocolRegistry {
		sort.Slice(config.PoolDataProviderHistory, func(i, j int) bool {
			return config.PoolDataProviderHistory[i].ActiveAtBlock < config.PoolDataProviderHistory[j].ActiveAtBlock
		})
		protocolRegistry[key] = config
	}
}

// GetForBlock returns the PoolDataProvider address that was active at the given block.
// Returns the zero address if no PoolDataProvider was active at that block.
func (h PoolDataProviderHistory) GetForBlock(blockNumber uint64) common.Address {
	if len(h) == 0 {
		return common.Address{}
	}

	// Find the latest entry that was created at or before the given block
	var result common.Address
	for _, entry := range h {
		if entry.ActiveAtBlock <= blockNumber {
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

func GetProtocolConfig(chainID int64, protocolAddress common.Address) (ProtocolConfig, bool) {
	config, exists := protocolRegistry[ProtocolKey{chainID, protocolAddress}]
	return config, exists
}

// GetProtocolsForChain returns all protocol configs for a given chain.
func GetProtocolsForChain(chainID int64) map[common.Address]ProtocolConfig {
	result := make(map[common.Address]ProtocolConfig)
	for key, config := range protocolRegistry {
		if key.ChainID == chainID {
			result[key.PoolAddress] = config
		}
	}
	return result
}

func IsKnownProtocol(chainID int64, protocolAddress common.Address) bool {
	_, exists := protocolRegistry[ProtocolKey{chainID, protocolAddress}]
	return exists
}

// GetProtocolBySlug returns the protocol key and config for a given slug.
func GetProtocolBySlug(slug string) (ProtocolKey, ProtocolConfig, bool) {
	for key, config := range protocolRegistry {
		if config.Slug == slug {
			return key, config, true
		}
	}
	return ProtocolKey{}, ProtocolConfig{}, false
}

// GetPoolDataProviderForBlock returns the correct PoolDataProvider for a given
// protocol at a specific block.
func GetPoolDataProviderForBlock(chainID int64, protocolAddress common.Address, blockNumber uint64) (common.Address, bool) {
	config, exists := protocolRegistry[ProtocolKey{chainID, protocolAddress}]
	if !exists {
		return common.Address{}, false
	}
	provider := config.PoolDataProviderHistory.GetForBlock(blockNumber)
	if provider == (common.Address{}) {
		return common.Address{}, false
	}
	return provider, true
}

package blockchain

import "github.com/ethereum/go-ethereum/common"

type ProtocolConfig struct {
	Name                  string
	PoolAddress           common.Address
	UIPoolDataProvider    common.Address
	PoolDataProvider      common.Address
	PoolAddressesProvider common.Address
	UseAaveABI            bool // NEW: flag to determine which ABI
}

var ProtocolRegistry = map[common.Address]ProtocolConfig{
	common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"): {
		Name:                  "Aave V3",
		PoolAddress:           common.HexToAddress("0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2"),
		UIPoolDataProvider:    common.HexToAddress("0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d"),
		PoolDataProvider:      common.HexToAddress("0x0a16f2FCC0D44FaE41cc54e079281D84A363bECD"),
		PoolAddressesProvider: common.HexToAddress("0x2f39d218133AFaB8F2B819B1066c7E434Ad94E9e"),
		UseAaveABI:            true,
	},
	common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987"): {
		Name:                  "Sparklend",
		PoolAddress:           common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987"),
		UIPoolDataProvider:    common.HexToAddress("0x56b7A1012765C285afAC8b8F25C69Bf10ccfE978"),
		PoolDataProvider:      common.HexToAddress("0xFc21d6d146E6086B8359705C8b28512a983db0cb"),
		PoolAddressesProvider: common.HexToAddress("0x02C3eA4e34C0cBd694D2adFa2c690EECbC1793eE"),
		UseAaveABI:            false,
	},
	common.HexToAddress("0x4e033931ad43597d96d6bcc25c280717730b58b1"): {
		Name:                  "Aave V3 Lido",
		PoolAddress:           common.HexToAddress("0x4e033931ad43597d96d6bcc25c280717730b58b1"),
		UIPoolDataProvider:    common.HexToAddress("0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d"),
		PoolDataProvider:      common.HexToAddress("0xB85B2bFEbeC4F5f401dbf92ac147A3076391fCD5"),
		PoolAddressesProvider: common.HexToAddress("0xcfBf336fe147D643B9Cb705648500e101504B16d"),
		UseAaveABI:            true,
	},
	common.HexToAddress("0xAe05Cd22df81871bc7cC2a04BeCfb516bFe332C8"): {
		Name:                  "Aave V3 RWA",
		PoolAddress:           common.HexToAddress("0xAe05Cd22df81871bc7cC2a04BeCfb516bFe332C8"),
		UIPoolDataProvider:    common.HexToAddress("0x91c0eA31b49B69Ea18607702c5d9aC360bf3dE7d"),
		PoolDataProvider:      common.HexToAddress("0xB85B2bFEbeC4F5f401dbf92ac147A3076391fCD5"),
		PoolAddressesProvider: common.HexToAddress("0x5D39E06b825C1F2B80bf2756a73e28eFAA128ba0"),
		UseAaveABI:            true,
	},
}

func GetProtocolConfig(protocolAddress common.Address) (ProtocolConfig, bool) {
	config, exists := ProtocolRegistry[protocolAddress]
	return config, exists
}

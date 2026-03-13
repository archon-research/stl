package abis

import (
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

const vatABIJSON = `[
	{"name":"ilks","type":"function","inputs":[{"name":"","type":"bytes32"}],"outputs":[
		{"name":"Art","type":"uint256"},
		{"name":"rate","type":"uint256"},
		{"name":"spot","type":"uint256"},
		{"name":"line","type":"uint256"},
		{"name":"dust","type":"uint256"}
	]},
	{"name":"urns","type":"function","inputs":[
		{"name":"","type":"bytes32"},
		{"name":"","type":"address"}
	],"outputs":[
		{"name":"ink","type":"uint256"},
		{"name":"art","type":"uint256"}
	]}
]`

const vaultABIJSON = `[
	{"name":"ilk","type":"function","inputs":[],"outputs":[{"name":"","type":"bytes32"}]}
]`

// GetVatABI returns the parsed MakerDAO Vat contract ABI.
func GetVatABI() (*abi.ABI, error) {
	parsed, err := abi.JSON(strings.NewReader(vatABIJSON))
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

// GetVaultABI returns the parsed vault contract ABI (ilk getter).
func GetVaultABI() (*abi.ABI, error) {
	parsed, err := abi.JSON(strings.NewReader(vaultABIJSON))
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

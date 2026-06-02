package axis_synome_contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/ethereum/go-ethereum/common"
)

const (
	DefaultContractPath = "contracts/axis-synome/axis_synome_entities.json"
	DefaultSchemaPath   = "contracts/axis-synome/axis_synome_entities.schema.json"
)

type Bundle struct {
	Contract Contract
	Schema   map[string]any
}

type Contract struct {
	Version             string          `json:"version"`
	AxisSynomeGitCommit string          `json:"axis_synome_git_commit"`
	AxisSynome          AxisSynomeModel `json:"axis_synome"`
}

type AxisSynomeModel struct {
	Spec SpecModel `json:"spec"`
}

type SpecModel struct {
	ASC ASCModel `json:"asc"`
}

type ASCModel struct {
	Entities EntitiesModel `json:"entities"`
}

type EntitiesModel struct {
	AssetsByPrime AssetsByPrimeModel `json:"assets_by_prime"`
	AlmProxies    AlmProxiesModel    `json:"alm_proxies"`
}

type AssetsByPrimeModel struct {
	ASSETSByPrime map[string][]TokenEntry `json:"ASSETS_BY_PRIME"`
}

type AlmProxiesModel struct {
	AlmProxy map[string]map[string]ProxyConfig `json:"AlmProxy"`
}

type ProxyConfig struct {
	Star    string `json:"star"`
	Chain   string `json:"chain"`
	Address string `json:"address"`
}

type TokenEntry struct {
	ContractAddress string  `json:"contract_address"`
	WalletAddress   string  `json:"wallet_address"`
	AssetAddress    *string `json:"asset_address"`
	Star            string  `json:"star"`
	Chain           string  `json:"chain"`
	Protocol        string  `json:"protocol"`
	AllocationType  string  `json:"allocation_type"`
	TokenType       string  `json:"token_type"`
	CreatedAtBlock  *int64  `json:"created_at_block"`
}

func LoadDefault() (*Bundle, error) {
	return Load(DefaultContractPath, DefaultSchemaPath)
}

func Load(contractPath string, schemaPath string) (*Bundle, error) {
	contract, err := LoadContract(contractPath)
	if err != nil {
		return nil, err
	}

	schema, err := LoadSchema(schemaPath)
	if err != nil {
		return nil, err
	}

	return &Bundle{Contract: *contract, Schema: schema}, nil
}

func LoadContract(path string) (*Contract, error) {
	bytesData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading axis-synome contract file %q: %w", path, err)
	}

	var contract Contract
	if err := unmarshalStrict(bytesData, &contract); err != nil {
		return nil, fmt.Errorf("decoding axis-synome contract file %q: %w", path, err)
	}

	if contract.Version == "" {
		return nil, fmt.Errorf("decoding axis-synome contract file %q: missing version", path)
	}
	if contract.AxisSynomeGitCommit == "" {
		return nil, fmt.Errorf("decoding axis-synome contract file %q: missing axis_synome_git_commit", path)
	}
	if err := validateAddresses(&contract); err != nil {
		return nil, fmt.Errorf("decoding axis-synome contract file %q: %w", path, err)
	}

	return &contract, nil
}

func validateAddresses(contract *Contract) error {
	for star, byChain := range contract.AxisSynome.Spec.ASC.Entities.AlmProxies.AlmProxy {
		for chain, proxy := range byChain {
			context := fmt.Sprintf("alm_proxy star=%s chain=%s", star, chain)
			if err := validateEthereumAddress(proxy.Address, "address", context); err != nil {
				return err
			}
		}
	}

	for star, entries := range contract.AxisSynome.Spec.ASC.Entities.AssetsByPrime.ASSETSByPrime {
		for i, entry := range entries {
			context := fmt.Sprintf("token_entry star=%s index=%d", star, i)
			if err := validateEthereumAddress(entry.ContractAddress, "contract_address", context); err != nil {
				return err
			}
			if err := validateEthereumAddress(entry.WalletAddress, "wallet_address", context); err != nil {
				return err
			}
			if entry.AssetAddress != nil {
				if err := validateEthereumAddress(*entry.AssetAddress, "asset_address", context); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

func validateEthereumAddress(value string, field string, context string) error {
	if !common.IsHexAddress(value) {
		return fmt.Errorf("invalid ethereum address for %s in %s: %q", field, context, value)
	}
	return nil
}

func LoadSchema(path string) (map[string]any, error) {
	bytesData, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading axis-synome schema file %q: %w", path, err)
	}

	var schema map[string]any
	if err := unmarshalStrict(bytesData, &schema); err != nil {
		return nil, fmt.Errorf("decoding axis-synome schema file %q: %w", path, err)
	}

	return schema, nil
}

func unmarshalStrict(data []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(target); err != nil {
		return err
	}

	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON content: %w", err)
	}

	return nil
}

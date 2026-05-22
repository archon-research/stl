package axis_synome_contract

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
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
	ContractAddress string `json:"contract_address"`
	WalletAddress   string `json:"wallet_address"`
	AssetAddress    string `json:"asset_address"`
	Star            string `json:"star"`
	Chain           string `json:"chain"`
	Protocol        string `json:"protocol"`
	AllocationType  string `json:"allocation_type"`
	TokenType       string `json:"token_type"`
	CreatedAtBlock  *int64 `json:"created_at_block"`
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

	return &contract, nil
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

	if decoder.More() {
		return fmt.Errorf("unexpected trailing JSON content")
	}

	return nil
}

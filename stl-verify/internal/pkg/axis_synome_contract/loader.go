package axis_synome_contract

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

const DefaultContractPath = "contracts/axis-synome/axis_synome_entities.json"

type Contract struct {
	Version             string          `json:"version"`
	AxisSynomeGitCommit string          `json:"axis_synome_git_commit"`
	AxisSynome          AxisSynomeModel `json:"axis_synome"`
}

type AxisSynomeModel struct {
	Spec SpecModel `json:"spec"`
}

// SpecModel accepts both contract shapes during the axis-synome spec
// restructure (TEN-224): the legacy {spec:{asc:{entities:...}}} and the
// current {spec:{entities:...}}. Exactly one of the two must be present;
// LoadContract enforces this.
type SpecModel struct {
	ASC      *ASCModel      `json:"asc"`
	Entities *EntitiesModel `json:"entities"`
}

// Deprecated: legacy {spec:{asc:{entities:...}}} wrapper (TEN-224).
//
// TODO(TEN-224): delete ASCModel, the SpecModel.ASC field, the legacy branch
// in (*Contract).entities() and validateSpecShape, and the "legacy asc shape" test cases once
// the committed contracts/axis-synome files and all axis-synome exporters
// emit {spec:{entities:...}}.
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
	// AlmProxy is keyed by star then chain, and holds a list of proxies: the
	// canonical ALM proxy plus any additional SubProxy/treasury wallets that
	// must be tracked separately for the same (star, chain).
	AlmProxy map[string]map[string][]ProxyConfig `json:"AlmProxy"`
}

type ProxyConfig struct {
	Star    string `json:"star"`
	Chain   string `json:"chain"`
	Address string `json:"address"`
	// Role distinguishes the canonical ALM proxy ("alm") from additional
	// SubProxy/treasury wallets ("subproxy") for the same (star, chain).
	Role string `json:"role"`
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
	// created_at_block is intentionally absent: it is on-chain observed data,
	// not Atlas-sourced, so the axis-synome contract does not carry it. The
	// allocation tracker owns it via knownCreatedAtBlocks (see created_at_blocks.go).
}

// entities resolves the entities payload regardless of which contract shape
// carried it. Returns nil when neither shape is present; LoadContract rejects
// such contracts, so this only happens for hand-built values.
func (c *Contract) entities() *EntitiesModel {
	if c.AxisSynome.Spec.Entities != nil {
		return c.AxisSynome.Spec.Entities
	}
	if c.AxisSynome.Spec.ASC != nil {
		return &c.AxisSynome.Spec.ASC.Entities
	}
	return nil
}

// GetAlmProxies returns the ALM proxy configurations keyed by star and then
// chain. Each (star, chain) maps to a list of proxies (canonical ALM proxy plus
// any additional SubProxy/treasury wallets).
func (c *Contract) GetAlmProxies() map[string]map[string][]ProxyConfig {
	entities := c.entities()
	if entities == nil {
		return nil
	}
	return entities.AlmProxies.AlmProxy
}

// GetAssetsByPrime returns the token entries keyed by star.
func (c *Contract) GetAssetsByPrime() map[string][]TokenEntry {
	entities := c.entities()
	if entities == nil {
		return nil
	}
	return entities.AssetsByPrime.ASSETSByPrime
}

func LoadDefaultContract() (*Contract, error) {
	contractPath, err := resolveDefaultPath(DefaultContractPath)
	if err != nil {
		return nil, err
	}

	return LoadContract(contractPath)
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
	if err := validateSpecShape(&contract); err != nil {
		return nil, fmt.Errorf("decoding axis-synome contract file %q: %w", path, err)
	}
	if err := validateAddresses(&contract); err != nil {
		return nil, fmt.Errorf("decoding axis-synome contract file %q: %w", path, err)
	}

	return &contract, nil
}

// validateSpecShape requires exactly one of the legacy ({spec:{asc:{entities}}})
// and current ({spec:{entities}}) shapes.
func validateSpecShape(contract *Contract) error {
	hasLegacy := contract.AxisSynome.Spec.ASC != nil
	hasCurrent := contract.AxisSynome.Spec.Entities != nil
	switch {
	case hasLegacy && hasCurrent:
		return errors.New("spec carries both the legacy asc shape and the current entities shape; expected exactly one")
	case !hasLegacy && !hasCurrent:
		return errors.New("spec carries neither the legacy asc shape nor the current entities shape; expected exactly one")
	}
	return nil
}

func validateAddresses(contract *Contract) error {
	for star, byChain := range contract.GetAlmProxies() {
		for chain, proxies := range byChain {
			for i, proxy := range proxies {
				context := fmt.Sprintf("alm_proxy star=%s chain=%s index=%d", star, chain, i)
				if err := validateEthereumAddress(proxy.Address, "address", context); err != nil {
					return err
				}
			}
		}
	}

	for star, entries := range contract.GetAssetsByPrime() {
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

func resolveDefaultPath(relativePath string) (string, error) {
	candidates, err := defaultPathCandidates(relativePath)
	if err != nil {
		return "", err
	}

	tried := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		} else if !os.IsNotExist(err) {
			return "", fmt.Errorf("stat axis-synome default path %q: %w", candidate, err)
		}
		tried = append(tried, candidate)
	}

	return "", fmt.Errorf("axis-synome default file %q not found; tried: %s", relativePath, strings.Join(tried, ", "))
}

func defaultPathCandidates(relativePath string) ([]string, error) {
	candidates := make([]string, 0, 2)
	if cwd, err := os.Getwd(); err == nil {
		candidates = append(candidates, filepath.Clean(filepath.Join(cwd, relativePath)))
	}

	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		if len(candidates) > 0 {
			return uniquePaths(candidates), nil
		}
		return nil, fmt.Errorf("resolve axis-synome default path: runtime caller unavailable")
	}

	candidates = append(candidates, filepath.Clean(filepath.Join(
		filepath.Dir(currentFile),
		"..",
		"..",
		"..",
		relativePath,
	)))

	return uniquePaths(candidates), nil
}

func uniquePaths(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	unique := make([]string, 0, len(paths))
	for _, path := range paths {
		if _, ok := seen[path]; ok {
			continue
		}
		seen[path] = struct{}{}
		unique = append(unique, path)
	}
	return unique
}

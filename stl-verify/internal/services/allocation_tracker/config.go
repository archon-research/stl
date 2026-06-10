package allocation_tracker

import (
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
)

type ProxyConfig struct {
	Star    string
	Chain   string
	Address common.Address
	// Role distinguishes the canonical ALM proxy ("alm") from additional
	// SubProxy/treasury wallets ("subproxy").
	Role string
}

type Config struct {
	MaxMessages       int
	PollInterval      time.Duration
	SweepEveryNBlocks int
	ChainID           int64
	Logger            *slog.Logger
}

func ConfigDefaults() Config {
	return Config{
		MaxMessages:       10,
		PollInterval:      100 * time.Millisecond,
		SweepEveryNBlocks: 75,
		Logger:            slog.Default(),
	}
}

func DefaultProxies() []ProxyConfig {
	proxies, err := LoadDefaultProxies()
	if err != nil {
		panic(fmt.Sprintf("load allocation tracker default proxies: %v", err))
	}

	return proxies
}

func LoadDefaultProxies() ([]ProxyConfig, error) {
	contract, err := axis_synome_contract.LoadDefaultContract()
	if err != nil {
		return nil, fmt.Errorf("load default axis-synome contract: %w", err)
	}

	proxies, err := proxiesFromAlmProxy(contract.GetAlmProxies())
	if err != nil {
		return nil, err
	}

	chainCounts := make(map[string]int)
	for _, p := range proxies {
		chainCounts[p.Chain]++
	}
	if err := validateChainVocabulary("proxies", chainCounts); err != nil {
		return nil, err
	}

	return proxies, nil
}

// proxiesFromAlmProxy flattens the contract's star -> chain -> [proxy] map into a
// sorted []ProxyConfig, rejecting duplicate (chain, address) pairs and an empty
// result.
func proxiesFromAlmProxy(almProxyByStar map[string]map[string][]axis_synome_contract.ProxyConfig) ([]ProxyConfig, error) {
	proxies := make([]ProxyConfig, 0)
	proxyByAddress := make(map[proxyConfigKey]ProxyConfig)

	for star, byChain := range almProxyByStar {
		for chain, chainProxies := range byChain {
			for _, proxy := range chainProxies {
				proxyConfig := ProxyConfig{
					Star:    star,
					Chain:   chain,
					Address: common.HexToAddress(proxy.Address),
					Role:    proxy.Role,
				}
				key := proxyConfigKey{Chain: proxyConfig.Chain, Address: proxyConfig.Address}
				if existing, ok := proxyByAddress[key]; ok {
					return nil, fmt.Errorf(
						"duplicate proxy address %s for chain %s (%s and %s)",
						proxyConfig.Address.Hex(),
						proxyConfig.Chain,
						existing.Star,
						proxyConfig.Star,
					)
				}

				proxyByAddress[key] = proxyConfig
				proxies = append(proxies, proxyConfig)
			}
		}
	}

	if len(proxies) == 0 {
		return nil, fmt.Errorf("no ALM proxies loaded from axis-synome contract")
	}

	sort.Slice(proxies, func(i int, j int) bool {
		if proxies[i].Star != proxies[j].Star {
			return proxies[i].Star < proxies[j].Star
		}
		if proxies[i].Chain != proxies[j].Chain {
			return proxies[i].Chain < proxies[j].Chain
		}
		return proxies[i].Address.Hex() < proxies[j].Address.Hex()
	})

	return proxies, nil
}

type proxyConfigKey struct {
	Chain   string
	Address common.Address
}

func BuildProxyLookup(proxies []ProxyConfig) map[common.Address]ProxyConfig {
	m := make(map[common.Address]ProxyConfig, len(proxies))
	for _, p := range proxies {
		m[p.Address] = p
	}
	return m
}

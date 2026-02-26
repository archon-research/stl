package allocation_tracker

import (
	"log/slog"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

type ProxyConfig struct {
	Star    string
	Chain   string
	Address common.Address
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
	return []ProxyConfig{
		{
			Star:    "spark",
			Chain:   "mainnet",
			Address: common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e"),
		},
		{
			Star:    "spark",
			Chain:   "base",
			Address: common.HexToAddress("0x2917956eff0b5eaf030abdb4ef4296df775009ca"),
		},
		{
			Star:    "spark",
			Chain:   "arbitrum",
			Address: common.HexToAddress("0x92afd6f2385a90e44da3a8b60fe36f6cbe1d8709"),
		},
		{
			Star:    "spark",
			Chain:   "optimism",
			Address: common.HexToAddress("0x876664f0c9ff24d1aa355ce9f1680ae1a5bf36fb"),
		},
		{
			Star:    "spark",
			Chain:   "unichain",
			Address: common.HexToAddress("0x345e368fccd62266b3f5f37c9a131fd1c39f5869"),
		},
		{
			Star:    "spark",
			Chain:   "avalanche-c",
			Address: common.HexToAddress("0xece6b0e8a54c2f44e066fbb9234e7157b15b7fec"),
		},
		{
			Star:    "grove",
			Chain:   "mainnet",
			Address: common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e"),
		},
		{
			Star:    "grove",
			Chain:   "avalanche-c",
			Address: common.HexToAddress("0x7107dd8f56642327945294a18a4280c78e153644"),
		},
		{
			Star:    "grove",
			Chain:   "plume",
			Address: common.HexToAddress("0x1db91ad50446a671e2231f77e00948e68876f812"),
		},
		{
			Star:    "grove",
			Chain:   "monad",
			Address: common.HexToAddress("0x94b398acb2fce988871218221ea6a4a2b26cccbc"),
		},
	}
}

func BuildProxyLookup(proxies []ProxyConfig) map[common.Address]ProxyConfig {
	m := make(map[common.Address]ProxyConfig, len(proxies))
	for _, p := range proxies {
		m[p.Address] = p
	}
	return m
}

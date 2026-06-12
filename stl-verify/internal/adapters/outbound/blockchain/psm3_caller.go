package blockchain

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/axis_synome_contract"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that PSM3Caller implements the port interface.
var _ outbound.PSM3Caller = (*PSM3Caller)(nil)

// PSM3Config holds the static per-chain Spark PSM3 deployment addresses
// (source: spark-address-registry, cross-checked against axis-synome).
type PSM3Config struct {
	PSM3  common.Address
	USDS  common.Address
	SUSDS common.Address
	USDC  common.Address
}

var psm3Configs = map[int64]PSM3Config{
	8453: { // base
		PSM3:  common.HexToAddress("0x1601843c5E9bC251A3272907010AFa41Fa18347E"),
		USDS:  common.HexToAddress("0x820C137fa70C8691f0e44Dc420a5e53c168921Dc"),
		SUSDS: common.HexToAddress("0x5875eEE11Cf8398102FdAd704C9E96607675467a"),
		USDC:  common.HexToAddress("0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913"),
	},
	10: { // optimism
		PSM3:  common.HexToAddress("0xe0F9978b907853F354d79188A3dEfbD41978af62"),
		USDS:  common.HexToAddress("0x4F13a96EC5C4Cf34e442b46Bbd98a0791F20edC3"),
		SUSDS: common.HexToAddress("0xb5B2dc7fd34C249F4be7fB1fCea07950784229e0"),
		USDC:  common.HexToAddress("0x0b2C639c533813f4Aa9D7837CAf62653d097Ff85"),
	},
	42161: { // arbitrum
		PSM3:  common.HexToAddress("0x2B05F8e1cACC6974fD79A673a341Fe1f58d27266"),
		USDS:  common.HexToAddress("0x6491c05A82219b8D1479057361ff1654749b876b"),
		SUSDS: common.HexToAddress("0xdDb46999F8891663a8F2828d25298f70416d7610"),
		USDC:  common.HexToAddress("0xaf88d065e77c8cC2239327C5EDb3A432268e5831"),
	},
	130: { // unichain
		PSM3:  common.HexToAddress("0x7b42Ed932f26509465F7cE3FAF76FfCe1275312f"),
		USDS:  common.HexToAddress("0x7E10036Acc4B56d4dFCa3b77810356CE52313F9C"),
		SUSDS: common.HexToAddress("0xA06b10Db9F390990364A3984C04FaDf1c13691b5"),
		USDC:  common.HexToAddress("0x078D782b760474a361dDA0AF3839290b0EF57AD6"),
	},
}

// PSM3ConfigForChain returns the PSM3 deployment addresses for chainID.
func PSM3ConfigForChain(chainID int64) (PSM3Config, error) {
	cfg, ok := psm3Configs[chainID]
	if !ok {
		return PSM3Config{}, fmt.Errorf("no PSM3 deployment configured for chain ID %d", chainID)
	}
	return cfg, nil
}

// ValidateAgainstAxisSynome cross-checks the configured PSM3 address against
// the axis-synome protocol=psm3 entry for the chain so the two registries
// cannot drift silently.
func (cfg PSM3Config) ValidateAgainstAxisSynome(contract *axis_synome_contract.Contract, chainID int64) error {
	chainName, err := entity.ChainName(chainID)
	if err != nil {
		return fmt.Errorf("resolve chain name: %w", err)
	}

	found := false
	for star, entries := range contract.GetAssetsByPrime() {
		for _, e := range entries {
			if e.Protocol != "psm3" || e.Chain != chainName {
				continue
			}
			found = true
			if common.HexToAddress(e.ContractAddress) != cfg.PSM3 {
				return fmt.Errorf("axis-synome psm3 entry for chain %s (star %s) has contract %s, config has %s",
					chainName, star, e.ContractAddress, cfg.PSM3.Hex())
			}
		}
	}
	if !found {
		return fmt.Errorf("no psm3 entry in axis-synome for chain %s", chainName)
	}
	return nil
}

// PSM3Caller reads Spark PSM3 reserve state using batched multicall3 reads.
type PSM3Caller struct {
	multicaller  outbound.Multicaller
	cfg          PSM3Config
	psm3ABI      abi.ABI
	erc20ABI     abi.ABI
	rateABI      abi.ABI
	rateProvider common.Address // set by ResolveImmutables
}

// NewPSM3Caller creates a new PSM3Caller backed by a Multicaller.
func NewPSM3Caller(multicaller outbound.Multicaller, cfg PSM3Config) (*PSM3Caller, error) {
	if multicaller == nil {
		return nil, fmt.Errorf("multicaller is required")
	}

	psm3ABI, err := abis.GetPSM3ABI()
	if err != nil {
		return nil, fmt.Errorf("parse psm3 abi: %w", err)
	}
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return nil, fmt.Errorf("parse erc20 abi: %w", err)
	}
	rateABI, err := abis.GetRateProviderABI()
	if err != nil {
		return nil, fmt.Errorf("parse rate provider abi: %w", err)
	}

	return &PSM3Caller{
		multicaller: multicaller,
		cfg:         cfg,
		psm3ABI:     *psm3ABI,
		erc20ABI:    *erc20ABI,
		rateABI:     *rateABI,
	}, nil
}

// ResolveImmutables reads rateProvider(), usds(), susds() and usdc() from the
// PSM3 contract in one multicall, fails hard if the on-chain token addresses
// do not match the configured ones, and caches the rate provider for ReadState.
func (c *PSM3Caller) ResolveImmutables(ctx context.Context, blockNumber *big.Int) (common.Address, error) {
	methods := []string{"rateProvider", "usds", "susds", "usdc"}
	calls := make([]outbound.Call, len(methods))
	for i, m := range methods {
		data, err := c.psm3ABI.Pack(m)
		if err != nil {
			return common.Address{}, fmt.Errorf("pack %s: %w", m, err)
		}
		calls[i] = outbound.Call{Target: c.cfg.PSM3, AllowFailure: false, CallData: data}
	}

	results, err := c.execute(ctx, calls, blockNumber)
	if err != nil {
		return common.Address{}, fmt.Errorf("multicall psm3 immutables: %w", err)
	}

	addrs := make([]common.Address, len(methods))
	for i, m := range methods {
		addrs[i], err = unpackAddress(&c.psm3ABI, m, results[i])
		if err != nil {
			return common.Address{}, err
		}
	}

	checks := []struct {
		method    string
		got, want common.Address
	}{
		{"usds", addrs[1], c.cfg.USDS},
		{"susds", addrs[2], c.cfg.SUSDS},
		{"usdc", addrs[3], c.cfg.USDC},
	}
	for _, chk := range checks {
		if chk.got != chk.want {
			return common.Address{}, fmt.Errorf("psm3 %s() = %s, config has %s", chk.method, chk.got.Hex(), chk.want.Hex())
		}
	}

	if addrs[0] == (common.Address{}) {
		return common.Address{}, fmt.Errorf("psm3 rateProvider() returned the zero address")
	}
	c.rateProvider = addrs[0]
	return c.rateProvider, nil
}

// ReadState reads the PSM3 reserve state pinned to blockNumber in two rounds:
// round 1 reads pocket(), USDS/sUSDS balances, totalAssets() and the
// conversion rate in one multicall; round 2 reads USDC.balanceOf(pocket).
// The pocket is governance-settable (PocketSet), so it is resolved every call
// and never cached.
func (c *PSM3Caller) ReadState(ctx context.Context, blockNumber *big.Int) (*entity.PSM3State, error) {
	if c.rateProvider == (common.Address{}) {
		return nil, fmt.Errorf("rate provider not resolved; call ResolveImmutables first")
	}

	balanceOfPSM3, err := c.erc20ABI.Pack("balanceOf", c.cfg.PSM3)
	if err != nil {
		return nil, fmt.Errorf("pack balanceOf(psm3): %w", err)
	}
	pocketData, err := c.psm3ABI.Pack("pocket")
	if err != nil {
		return nil, fmt.Errorf("pack pocket: %w", err)
	}
	totalAssetsData, err := c.psm3ABI.Pack("totalAssets")
	if err != nil {
		return nil, fmt.Errorf("pack totalAssets: %w", err)
	}
	rateData, err := c.rateABI.Pack("getConversionRate")
	if err != nil {
		return nil, fmt.Errorf("pack getConversionRate: %w", err)
	}

	calls := []outbound.Call{
		{Target: c.cfg.PSM3, CallData: pocketData},
		{Target: c.cfg.USDS, CallData: balanceOfPSM3},
		{Target: c.cfg.SUSDS, CallData: balanceOfPSM3},
		{Target: c.cfg.PSM3, CallData: totalAssetsData},
		{Target: c.rateProvider, CallData: rateData},
	}

	results, err := c.execute(ctx, calls, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("multicall psm3 state: %w", err)
	}

	pocket, err := unpackAddress(&c.psm3ABI, "pocket", results[0])
	if err != nil {
		return nil, err
	}
	usdsBalance, err := unpackUint256(&c.erc20ABI, "balanceOf", results[1])
	if err != nil {
		return nil, fmt.Errorf("usds balance: %w", err)
	}
	susdsBalance, err := unpackUint256(&c.erc20ABI, "balanceOf", results[2])
	if err != nil {
		return nil, fmt.Errorf("susds balance: %w", err)
	}
	totalAssets, err := unpackUint256(&c.psm3ABI, "totalAssets", results[3])
	if err != nil {
		return nil, err
	}
	conversionRate, err := unpackUint256(&c.rateABI, "getConversionRate", results[4])
	if err != nil {
		return nil, err
	}

	usdcBalance, err := c.readUSDCAtPocket(ctx, pocket, blockNumber)
	if err != nil {
		return nil, err
	}

	return &entity.PSM3State{
		USDSBalance:    usdsBalance,
		SUSDSBalance:   susdsBalance,
		USDCBalance:    usdcBalance,
		TotalAssets:    totalAssets,
		ConversionRate: conversionRate,
	}, nil
}

// readUSDCAtPocket reads USDC.balanceOf(pocket) at the given block.
func (c *PSM3Caller) readUSDCAtPocket(ctx context.Context, pocket common.Address, blockNumber *big.Int) (*big.Int, error) {
	data, err := c.erc20ABI.Pack("balanceOf", pocket)
	if err != nil {
		return nil, fmt.Errorf("pack balanceOf(pocket): %w", err)
	}

	results, err := c.execute(ctx, []outbound.Call{{Target: c.cfg.USDC, CallData: data}}, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("multicall usdc balance at pocket %s: %w", pocket.Hex(), err)
	}

	balance, err := unpackUint256(&c.erc20ABI, "balanceOf", results[0])
	if err != nil {
		return nil, fmt.Errorf("usdc balance at pocket %s: %w", pocket.Hex(), err)
	}
	return balance, nil
}

// execute runs a multicall and verifies the result count. Callers build their
// calls with AllowFailure=false, so any reverted or missing call is a hard error.
func (c *PSM3Caller) execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	results, err := c.multicaller.Execute(ctx, calls, blockNumber)
	if err != nil {
		return nil, err
	}
	if len(results) != len(calls) {
		return nil, fmt.Errorf("expected %d multicall results, got %d", len(calls), len(results))
	}
	return results, nil
}

// unpackAddress unpacks a single address return from a named ABI method.
func unpackAddress(parsed *abi.ABI, method string, res outbound.Result) (common.Address, error) {
	if !res.Success {
		return common.Address{}, fmt.Errorf("%s call failed", method)
	}
	out, err := parsed.Unpack(method, res.ReturnData)
	if err != nil {
		return common.Address{}, fmt.Errorf("unpack %s: %w", method, err)
	}
	addr, ok := out[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("unexpected type for %s: %T", method, out[0])
	}
	return addr, nil
}

// unpackUint256 unpacks a single uint256 return from a named ABI method.
func unpackUint256(parsed *abi.ABI, method string, res outbound.Result) (*big.Int, error) {
	if !res.Success {
		return nil, fmt.Errorf("%s call failed", method)
	}
	out, err := parsed.Unpack(method, res.ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpack %s: %w", method, err)
	}
	v, ok := out[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("unexpected type for %s: %T", method, out[0])
	}
	return v, nil
}

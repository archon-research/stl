package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Selectors for the atoken-only extra multicall entries. The ERC20 ABI does not
// include totalSupply, scaledBalanceOf, or scaledTotalSupply, so we pack calldata
// directly from selector + argument.
//
//	balanceOf(address)        → comes from the ERC20 ABI (packed via abi.Pack).
//	totalSupply()             → 0x18160ddd
//	scaledBalanceOf(address)  → 0x1da24f3e
//	scaledTotalSupply()       → 0xb1bf962d
var (
	totalSupplySelector       = []byte{0x18, 0x16, 0x0d, 0xdd}
	scaledBalanceOfSelector   = []byte{0x1d, 0xa2, 0x4f, 0x3e}
	scaledTotalSupplySelector = []byte{0xb1, 0xbf, 0x96, 0x2d}
)

// BalanceOfSource calls balanceOf(proxy) on the contract. For entries whose
// TokenType is "atoken" it additionally calls scaledBalanceOf(proxy),
// totalSupply(), and scaledTotalSupply() in the same multicall, and surfaces
// the supply readings through FetchResult.Supplies.
type BalanceOfSource struct {
	multicaller    outbound.Multicaller
	erc20ABI       *abi.ABI
	logger         *slog.Logger
	supportedTypes map[string]bool
}

func NewBalanceOfSource(multicaller outbound.Multicaller, erc20ABI *abi.ABI, logger *slog.Logger) *BalanceOfSource {
	return &BalanceOfSource{
		multicaller: multicaller,
		erc20ABI:    erc20ABI,
		logger:      logger.With("source", "balanceof"),
		supportedTypes: map[string]bool{
			"erc20":      true,
			"atoken":     true,
			"buidl":      true,
			"securitize": true,
			"superstate": true,
			"proxy":      true,
		},
	}
}

func (s *BalanceOfSource) Name() string { return "balanceof" }

func (s *BalanceOfSource) Supports(tokenType string, protocol string) bool {
	return s.supportedTypes[tokenType]
}

// callKind tags each multicall entry so the result mapper knows how to interpret it.
type callKind int

const (
	kindBalanceOf callKind = iota
	kindScaledBalanceOf
	kindTotalSupply
	kindScaledTotalSupply
)

type callContext struct {
	kind     callKind
	entry    *TokenEntry    // non-nil for balance/scaledBalance
	contract common.Address // for supply calls (entry is nil)
}

func (s *BalanceOfSource) FetchBalances(ctx context.Context, entries []*TokenEntry, blockNumber int64) (*FetchResult, error) {
	result := NewFetchResult()
	if len(entries) == 0 {
		return result, nil
	}

	calls, contexts := s.buildCalls(entries)
	if len(calls) == 0 {
		return result, nil
	}

	var block *big.Int
	if blockNumber > 0 {
		block = big.NewInt(blockNumber)
	}

	mc, err := s.multicaller.Execute(ctx, calls, block)
	if err != nil {
		return nil, fmt.Errorf("multicall: %w", err)
	}

	// Initialize balance rows for every entry that we built a balanceOf call for.
	for i, c := range contexts {
		if c.kind != kindBalanceOf {
			continue
		}
		_ = i
		result.Balances[c.entry.Key()] = &PositionBalance{Balance: big.NewInt(0)}
	}

	// Per-contract supply accumulators. We only emit a supply row when totalSupply
	// succeeded; scaledTotalSupply is optional and may stay nil on failure.
	type pending struct {
		totalSupply       *big.Int
		scaledTotalSupply *big.Int
		totalSupplyOK     bool
	}
	supplyAcc := make(map[common.Address]*pending)

	for i, c := range contexts {
		if i >= len(mc) {
			break
		}
		ok := mc[i].Success && len(mc[i].ReturnData) > 0
		switch c.kind {
		case kindBalanceOf:
			if ok {
				if v := unpackUint256(s.erc20ABI, "balanceOf", mc[i].ReturnData); v != nil {
					result.Balances[c.entry.Key()].Balance = v
				}
			}
		case kindScaledBalanceOf:
			if ok {
				if v := unpackRawUint256(mc[i].ReturnData); v != nil {
					if bal := result.Balances[c.entry.Key()]; bal != nil {
						bal.ScaledBalance = v
					}
				}
			} else {
				s.logger.Warn("scaledBalanceOf failed",
					"contract", c.entry.ContractAddress.Hex(),
					"wallet", c.entry.WalletAddress.Hex())
			}
		case kindTotalSupply:
			p := supplyAcc[c.contract]
			if p == nil {
				p = &pending{}
				supplyAcc[c.contract] = p
			}
			if ok {
				if v := unpackRawUint256(mc[i].ReturnData); v != nil {
					p.totalSupply = v
					p.totalSupplyOK = true
				}
			} else {
				s.logger.Warn("totalSupply failed", "contract", c.contract.Hex())
			}
		case kindScaledTotalSupply:
			p := supplyAcc[c.contract]
			if p == nil {
				p = &pending{}
				supplyAcc[c.contract] = p
			}
			if ok {
				if v := unpackRawUint256(mc[i].ReturnData); v != nil {
					p.scaledTotalSupply = v
				}
			} else {
				s.logger.Warn("scaledTotalSupply failed", "contract", c.contract.Hex())
			}
		}
	}

	for addr, p := range supplyAcc {
		if !p.totalSupplyOK {
			// Never write a zero supply row; Python reader will return 503 for
			// this token until the next successful read.
			continue
		}
		result.Supplies[addr] = &PoolSupply{
			TotalSupply:       p.totalSupply,
			ScaledTotalSupply: p.scaledTotalSupply,
		}
	}

	return result, nil
}

// buildCalls returns the multicall payload and a parallel slice of per-call
// context entries, so the response mapper can tell each result apart.
// For each entry we schedule balanceOf; for atoken entries we additionally
// schedule scaledBalanceOf. Per unique atoken contract in the batch we also
// schedule one totalSupply + one scaledTotalSupply.
func (s *BalanceOfSource) buildCalls(entries []*TokenEntry) ([]outbound.Call, []callContext) {
	calls := make([]outbound.Call, 0, len(entries)*2)
	contexts := make([]callContext, 0, len(entries)*2)

	for _, e := range entries {
		data, err := s.erc20ABI.Pack("balanceOf", e.WalletAddress)
		if err != nil {
			s.logger.Warn("pack balanceOf failed", "contract", e.ContractAddress.Hex(), "error", err)
			continue
		}
		calls = append(calls, outbound.Call{Target: e.ContractAddress, AllowFailure: true, CallData: data})
		contexts = append(contexts, callContext{kind: kindBalanceOf, entry: e})

		if e.TokenType == "atoken" {
			sbData := append([]byte{}, scaledBalanceOfSelector...)
			sbData = append(sbData, common.LeftPadBytes(e.WalletAddress.Bytes(), 32)...)
			calls = append(calls, outbound.Call{Target: e.ContractAddress, AllowFailure: true, CallData: sbData})
			contexts = append(contexts, callContext{kind: kindScaledBalanceOf, entry: e})
		}
	}

	// One supply pair per unique atoken contract address.
	seen := make(map[common.Address]bool)
	for _, e := range entries {
		if e.TokenType != "atoken" {
			continue
		}
		if seen[e.ContractAddress] {
			continue
		}
		seen[e.ContractAddress] = true

		calls = append(calls, outbound.Call{Target: e.ContractAddress, AllowFailure: true, CallData: append([]byte{}, totalSupplySelector...)})
		contexts = append(contexts, callContext{kind: kindTotalSupply, contract: e.ContractAddress})

		calls = append(calls, outbound.Call{Target: e.ContractAddress, AllowFailure: true, CallData: append([]byte{}, scaledTotalSupplySelector...)})
		contexts = append(contexts, callContext{kind: kindScaledTotalSupply, contract: e.ContractAddress})
	}

	return calls, contexts
}

// unpackUint256 unpacks a single uint256 return from a named ABI method.
func unpackUint256(parsed *abi.ABI, method string, data []byte) *big.Int {
	unpacked, err := parsed.Unpack(method, data)
	if err != nil || len(unpacked) == 0 {
		return nil
	}
	v, ok := unpacked[0].(*big.Int)
	if !ok {
		return nil
	}
	return v
}

// unpackRawUint256 decodes a 32-byte uint256 from raw calldata returns where
// we don't have a matching ABI entry (totalSupply / scaledBalanceOf / scaledTotalSupply).
func unpackRawUint256(data []byte) *big.Int {
	if len(data) < 32 {
		return nil
	}
	return new(big.Int).SetBytes(data[len(data)-32:])
}

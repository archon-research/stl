package blockchain

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// Compile-time check that VatCaller implements the port interface.
var _ outbound.VatCaller = (*VatCaller)(nil)

const (
	vatABIJSON = `[
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

	vaultABIJSON = `[
		{"name":"ilk","type":"function","inputs":[],"outputs":[{"name":"","type":"bytes32"}]}
	]`
)

// VatCaller reads debt data from the MakerDAO/Sky Vat contract using batched
// multicall3 reads. All on-chain calls for a given operation (ilk resolution
// or debt reading) are packed into a single RPC round-trip.
type VatCaller struct {
	multicaller outbound.Multicaller
	vatAddress  common.Address
	vatABI      abi.ABI
	vaultABI    abi.ABI
}

// NewVatCaller creates a new VatCaller backed by a Multicaller.
func NewVatCaller(multicaller outbound.Multicaller, vatAddress common.Address) (*VatCaller, error) {
	if multicaller == nil {
		return nil, fmt.Errorf("multicaller is required")
	}
	if vatAddress == (common.Address{}) {
		return nil, fmt.Errorf("vat address must not be the zero address")
	}

	parsedVatABI, err := abi.JSON(strings.NewReader(vatABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse vat abi: %w", err)
	}

	parsedVaultABI, err := abi.JSON(strings.NewReader(vaultABIJSON))
	if err != nil {
		return nil, fmt.Errorf("parse vault abi: %w", err)
	}

	return &VatCaller{
		multicaller: multicaller,
		vatAddress:  vatAddress,
		vatABI:      parsedVatABI,
		vaultABI:    parsedVaultABI,
	}, nil
}

// ResolveIlks reads the ilk identifier from each vault contract in a single
// multicall at the given block. AllowFailure is false — if any vault is
// unreachable the entire batch fails, since startup cannot proceed without ilks.
func (c *VatCaller) ResolveIlks(ctx context.Context, vaults []common.Address, blockNumber *big.Int) (map[common.Address][32]byte, error) {
	if len(vaults) == 0 {
		return make(map[common.Address][32]byte), nil
	}

	ilkData, err := c.vaultABI.Pack("ilk")
	if err != nil {
		return nil, fmt.Errorf("pack ilk call: %w", err)
	}

	calls := make([]outbound.Call, len(vaults))
	for i, vault := range vaults {
		calls[i] = outbound.Call{
			Target:       vault,
			AllowFailure: false,
			CallData:     ilkData,
		}
	}

	results, err := c.multicaller.Execute(ctx, calls, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("multicall ilk resolution: %w", err)
	}

	ilks := make(map[common.Address][32]byte, len(vaults))
	for i, vault := range vaults {
		if i >= len(results) {
			return nil, fmt.Errorf("missing result for vault %s (index %d)", vault, i)
		}
		if !results[i].Success {
			return nil, fmt.Errorf("ilk() call failed for vault %s", vault)
		}

		out, err := c.vaultABI.Unpack("ilk", results[i].ReturnData)
		if err != nil {
			return nil, fmt.Errorf("unpack ilk for %s: %w", vault, err)
		}

		ilk, ok := out[0].([32]byte)
		if !ok {
			return nil, fmt.Errorf("unexpected type for ilk from %s: %T", vault, out[0])
		}

		ilks[vault] = ilk
	}

	return ilks, nil
}

// ReadDebts reads rate (via vat.ilks) and art (via vat.urns) for each query
// in a single multicall at the given block. Per-vault failures are reported
// via DebtResult.Err rather than failing the entire batch.
//
// Call layout per query: [ilks(ilk), urns(ilk, vault)] — 2 calls per query.
func (c *VatCaller) ReadDebts(ctx context.Context, queries []entity.DebtQuery, blockNumber *big.Int) ([]entity.DebtResult, error) {
	if len(queries) == 0 {
		return nil, nil
	}

	// Build 2N calls: ilks + urns per query.
	calls := make([]outbound.Call, 0, len(queries)*2)
	for _, q := range queries {
		ilksData, err := c.vatABI.Pack("ilks", q.Ilk)
		if err != nil {
			return nil, fmt.Errorf("pack ilks for %s: %w", q.VaultAddress, err)
		}

		urnsData, err := c.vatABI.Pack("urns", q.Ilk, q.VaultAddress)
		if err != nil {
			return nil, fmt.Errorf("pack urns for %s: %w", q.VaultAddress, err)
		}

		calls = append(calls,
			outbound.Call{Target: c.vatAddress, AllowFailure: true, CallData: ilksData},
			outbound.Call{Target: c.vatAddress, AllowFailure: true, CallData: urnsData},
		)
	}

	mcResults, err := c.multicaller.Execute(ctx, calls, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("multicall read debts: %w", err)
	}

	// Parse 2 results per query.
	results := make([]entity.DebtResult, len(queries))
	for i, q := range queries {
		ilksIdx := i * 2
		urnsIdx := i*2 + 1

		results[i].VaultAddress = q.VaultAddress

		if ilksIdx >= len(mcResults) || urnsIdx >= len(mcResults) {
			results[i].Err = fmt.Errorf("missing multicall results")
			continue
		}

		// Parse rate from ilks(ilk) → [Art, rate, spot, line, dust]
		if !mcResults[ilksIdx].Success {
			results[i].Err = fmt.Errorf("vat.ilks call failed")
			continue
		}
		ilksOut, err := c.vatABI.Unpack("ilks", mcResults[ilksIdx].ReturnData)
		if err != nil {
			results[i].Err = fmt.Errorf("unpack ilks: %w", err)
			continue
		}
		rate, ok := ilksOut[1].(*big.Int)
		if !ok {
			results[i].Err = fmt.Errorf("unexpected type for rate: %T", ilksOut[1])
			continue
		}

		// Parse art from urns(ilk, vault) → [ink, art]
		if !mcResults[urnsIdx].Success {
			results[i].Err = fmt.Errorf("vat.urns call failed")
			continue
		}
		urnsOut, err := c.vatABI.Unpack("urns", mcResults[urnsIdx].ReturnData)
		if err != nil {
			results[i].Err = fmt.Errorf("unpack urns: %w", err)
			continue
		}
		art, ok := urnsOut[1].(*big.Int)
		if !ok {
			results[i].Err = fmt.Errorf("unexpected type for art: %T", urnsOut[1])
			continue
		}

		results[i].Rate = rate
		results[i].Art = art
	}

	return results, nil
}

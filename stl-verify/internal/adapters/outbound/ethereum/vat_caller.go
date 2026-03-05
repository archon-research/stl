package ethereum

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

const (
	vatABI = `[
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

	vaultABI = `[
		{"name":"ilk","type":"function","inputs":[],"outputs":[{"name":"","type":"bytes32"}]}
	]`
)

const (
	// wadDecimals is the number of decimal places in a wad (1e18).
	// USDS and DAI both use 18 decimals — this is fixed by the MakerDAO spec.
	wadDecimals = 18
)

var (
	ray = new(big.Int).Exp(big.NewInt(10), big.NewInt(27), nil) // 1e27 — RAY
	wad = new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil) // 1e18 — WAD

	// rayWad = RAY * WAD = 1e45 (rad).
	// art (wad) * rate (ray) = rad (1e45).
	// Dividing rad by rayWad gives a human-readable USDS amount:
	// e.g. "2948696280.290761728641811098" = 2.948 billion USDS.
	rayWad = new(big.Int).Mul(ray, wad)
)

// ContractCaller is a minimal interface for read-only contract calls.
// *alchemy.Client implements this via its CallContract method.
type ContractCaller interface {
	CallContract(ctx context.Context, to string, data []byte) ([]byte, error)
}

// VatCaller reads debt data from the MakerDAO/Sky Vat contract.
type VatCaller struct {
	client     ContractCaller
	vatAddress common.Address
	vatABI     abi.ABI
	vaultABI   abi.ABI
}

// NewVatCaller creates a new VatCaller backed by the provided ContractCaller.
func NewVatCaller(client ContractCaller, vatAddress common.Address) (*VatCaller, error) {
	if client == nil {
		return nil, fmt.Errorf("contract caller is required")
	}

	parsedVatABI, err := abi.JSON(strings.NewReader(vatABI))
	if err != nil {
		return nil, fmt.Errorf("parse vat abi: %w", err)
	}

	parsedVaultABI, err := abi.JSON(strings.NewReader(vaultABI))
	if err != nil {
		return nil, fmt.Errorf("parse vault abi: %w", err)
	}

	return &VatCaller{
		client:     client,
		vatAddress: vatAddress,
		vatABI:     parsedVatABI,
		vaultABI:   parsedVaultABI,
	}, nil
}

// GetIlk reads the ilk identifier from a vault contract.
func (c *VatCaller) GetIlk(ctx context.Context, vaultAddress common.Address) ([32]byte, error) {
	data, err := c.vaultABI.Pack("ilk")
	if err != nil {
		return [32]byte{}, fmt.Errorf("pack ilk call: %w", err)
	}

	result, err := c.client.CallContract(ctx, vaultAddress.Hex(), data)
	if err != nil {
		return [32]byte{}, fmt.Errorf("call vault.ilk: %w", err)
	}

	out, err := c.vaultABI.Unpack("ilk", result)
	if err != nil {
		return [32]byte{}, fmt.Errorf("unpack ilk result: %w", err)
	}

	ilk, ok := out[0].([32]byte)
	if !ok {
		return [32]byte{}, fmt.Errorf("unexpected type for ilk: %T", out[0])
	}

	return ilk, nil
}

// GetRate reads the cumulative rate for an ilk from the Vat.
func (c *VatCaller) GetRate(ctx context.Context, ilk [32]byte) (*big.Int, error) {
	data, err := c.vatABI.Pack("ilks", ilk)
	if err != nil {
		return nil, fmt.Errorf("pack ilks call: %w", err)
	}

	result, err := c.client.CallContract(ctx, c.vatAddress.Hex(), data)
	if err != nil {
		return nil, fmt.Errorf("call vat.ilks: %w", err)
	}

	out, err := c.vatABI.Unpack("ilks", result)
	if err != nil {
		return nil, fmt.Errorf("unpack ilks result: %w", err)
	}

	// out: [Art, rate, spot, line, dust]
	rate, ok := out[1].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("unexpected type for rate: %T", out[1])
	}

	return rate, nil
}

// GetNormalizedDebt reads the normalized debt (art) for a specific urn from the Vat.
func (c *VatCaller) GetNormalizedDebt(ctx context.Context, ilk [32]byte, urnAddress common.Address) (*big.Int, error) {
	data, err := c.vatABI.Pack("urns", ilk, urnAddress)
	if err != nil {
		return nil, fmt.Errorf("pack urns call: %w", err)
	}

	result, err := c.client.CallContract(ctx, c.vatAddress.Hex(), data)
	if err != nil {
		return nil, fmt.Errorf("call vat.urns: %w", err)
	}

	out, err := c.vatABI.Unpack("urns", result)
	if err != nil {
		return nil, fmt.Errorf("unpack urns result: %w", err)
	}

	// out: [ink, art]
	art, ok := out[1].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("unexpected type for art: %T", out[1])
	}

	return art, nil
}

// ComputeDebtWad returns the vault debt as a human-readable USDS decimal string
// with 18 decimal places of precision.
//
// Formula: art (wad) * rate (ray) = rad (1e45) → divide by rayWad (1e45) → USDS
//
// Example output: "2948696280.290761728641811098" = 2.948 billion USDS
func ComputeDebtWad(art, rate *big.Int) string {
	rad := new(big.Int).Mul(art, rate)
	result := new(big.Rat).SetFrac(rad, rayWad)
	return result.FloatString(wadDecimals)
}

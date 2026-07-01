package fluid_vault_indexer

import (
	"context"
	"fmt"
	"math/big"
	"reflect"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/erc20meta"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ethSentinel is Fluid's native-ETH designator: contract code stores the
// address 0xEeee…EEeE (Fluid's NATIVE_TOKEN constant, e.g.
// fluid-contracts-public contracts/protocols/dexLite/other/constantVariables.sol)
// wherever a vault's collateral is raw ETH rather than an ERC-20. There is no
// contract at that address, so symbol()/decimals() revert; the indexer maps it
// to the canonical ETH metadata instead of failing the read. This is not a
// per-repo hack: 0xEeee…EEeE is the de-facto native-asset sentinel across DeFi
// (Aave, 1inch, Fluid, …) and the only correct value to store for an
// ETH-collateral vault.
var ethSentinel = common.HexToAddress("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE")

// TokenMetadata is the ERC-20 metadata the indexer needs to register a token.
type TokenMetadata struct {
	Symbol   string
	Decimals int
}

// FluidVaultResolverAddress is the deployed mainnet Fluid VaultResolver
// periphery contract (Instadapp/fluid-contracts-public
// deployments/mainnet/VaultResolver.json).
var FluidVaultResolverAddress = common.HexToAddress("0xA5C3E16523eeeDDcC34706b0E6bE88b4c6EA95cC")

// Field indices into the decoded getVaultEntireData top-level tuple and the
// nested constantVariables tuple. The resolver returns positional tuples, so
// these offsets are load-bearing; see GetFluidVaultResolverABI for the verified
// field order.
const (
	vedFieldVault                  = 0
	vedFieldIsSmartCol             = 1
	vedFieldIsSmartDebt            = 2
	vedFieldConstantVariables      = 3
	vedFieldExchangePricesAndRates = 5
	vedFieldTotalSupplyAndBorrow   = 6

	cvFieldSupplyToken = 8
	cvFieldBorrowToken = 9
	cvFieldVaultType   = 11
)

// abiTokens / abiExchangePricesAndRates / abiTotalSupplyAndBorrow mirror the
// resolver's nested tuples for abi.ConvertType. Field names must match the
// capitalized ABI parameter names exactly — ConvertType matches by name, not
// position. The unused fields are retained so the conversion target matches the
// full sub-tuple shape.
type abiTokens struct {
	Token0 common.Address
	Token1 common.Address
}

type abiExchangePricesAndRates struct {
	LastStoredLiquiditySupplyExchangePrice *big.Int
	LastStoredLiquidityBorrowExchangePrice *big.Int
	LastStoredVaultSupplyExchangePrice     *big.Int
	LastStoredVaultBorrowExchangePrice     *big.Int
	LiquiditySupplyExchangePrice           *big.Int
	LiquidityBorrowExchangePrice           *big.Int
	VaultSupplyExchangePrice               *big.Int
	VaultBorrowExchangePrice               *big.Int
	SupplyRateLiquidity                    *big.Int
	BorrowRateLiquidity                    *big.Int
	SupplyRateVault                        *big.Int
	BorrowRateVault                        *big.Int
	RewardsOrFeeRateSupply                 *big.Int
	RewardsOrFeeRateBorrow                 *big.Int
}

type abiTotalSupplyAndBorrow struct {
	TotalSupplyVault          *big.Int
	TotalBorrowVault          *big.Int
	TotalSupplyLiquidityOrDex *big.Int
	TotalBorrowLiquidityOrDex *big.Int
	AbsorbedSupply            *big.Int
	AbsorbedBorrow            *big.Int
}

// blockchainService reads Fluid's VaultResolver via the Multicall3 adapter.
type blockchainService struct {
	multicaller  outbound.Multicaller
	resolverABI  *abi.ABI
	erc20ABI     *abi.ABI
	resolverAddr common.Address
}

func newBlockchainService(multicaller outbound.Multicaller) (*blockchainService, error) {
	if multicaller == nil {
		return nil, fmt.Errorf("multicaller is required")
	}
	resolverABI, err := abis.GetFluidVaultResolverABI()
	if err != nil {
		return nil, fmt.Errorf("loading Fluid VaultResolver ABI: %w", err)
	}
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		return nil, fmt.Errorf("loading ERC20 ABI: %w", err)
	}
	return &blockchainService{
		multicaller:  multicaller,
		resolverABI:  resolverABI,
		erc20ABI:     erc20ABI,
		resolverAddr: FluidVaultResolverAddress,
	}, nil
}

// GetTokenMetadata reads symbol() and decimals() for token, pinned to
// blockNumber. The Fluid native-ETH sentinel is short-circuited to canonical
// ETH metadata (it is not an ERC-20). decimals() is mandatory — a revert is a
// hard error because a silent 0 would corrupt every downstream amount; symbol()
// is best-effort and yields "" on revert/undecodable data.
func (s *blockchainService) GetTokenMetadata(ctx context.Context, token common.Address, blockNumber int64) (TokenMetadata, error) {
	if token == ethSentinel {
		return TokenMetadata{Symbol: "ETH", Decimals: 18}, nil
	}

	symbolData, err := s.erc20ABI.Pack("symbol")
	if err != nil {
		return TokenMetadata{}, fmt.Errorf("packing symbol(): %w", err)
	}
	decimalsData, err := s.erc20ABI.Pack("decimals")
	if err != nil {
		return TokenMetadata{}, fmt.Errorf("packing decimals(): %w", err)
	}
	results, err := s.multicaller.Execute(ctx, []outbound.Call{
		{Target: token, AllowFailure: true, CallData: symbolData},
		{Target: token, AllowFailure: true, CallData: decimalsData},
	}, big.NewInt(blockNumber))
	if err != nil {
		return TokenMetadata{}, fmt.Errorf("multicall token metadata for %s: %w", token.Hex(), err)
	}
	if len(results) != 2 {
		return TokenMetadata{}, fmt.Errorf("token metadata for %s: expected 2 results, got %d", token.Hex(), len(results))
	}
	if !results[1].Success || len(results[1].ReturnData) == 0 {
		return TokenMetadata{}, fmt.Errorf("decimals() reverted for %s", token.Hex())
	}

	md := TokenMetadata{}
	if results[0].Success && len(results[0].ReturnData) > 0 {
		if sym, err := erc20meta.DecodeStringOrBytes32(s.erc20ABI, "symbol", results[0].ReturnData); err == nil {
			md.Symbol = sym
		}
	}
	decimalsUnpacked, err := s.erc20ABI.Unpack("decimals", results[1].ReturnData)
	if err != nil {
		return TokenMetadata{}, fmt.Errorf("unpacking decimals() for %s: %w", token.Hex(), err)
	}
	if len(decimalsUnpacked) == 0 {
		return TokenMetadata{}, fmt.Errorf("decimals() returned no values for %s", token.Hex())
	}
	dec, ok := decimalsUnpacked[0].(uint8)
	if !ok {
		return TokenMetadata{}, fmt.Errorf("decimals() returned unexpected type %T for %s", decimalsUnpacked[0], token.Hex())
	}
	md.Decimals = int(dec)
	return md, nil
}

// GetAllVaultAddresses enumerates every vault the resolver knows about, pinned
// to blockNumber. Used for startup reconcile and unknown-vault discovery.
func (s *blockchainService) GetAllVaultAddresses(ctx context.Context, blockNumber int64) ([]common.Address, error) {
	callData, err := s.resolverABI.Pack("getAllVaultsAddresses")
	if err != nil {
		return nil, fmt.Errorf("packing getAllVaultsAddresses: %w", err)
	}
	results, err := s.multicaller.Execute(ctx, []outbound.Call{{
		Target:       s.resolverAddr,
		AllowFailure: false,
		CallData:     callData,
	}}, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall getAllVaultsAddresses: %w", err)
	}
	if len(results) != 1 {
		return nil, fmt.Errorf("getAllVaultsAddresses: expected 1 result, got %d", len(results))
	}
	if !results[0].Success || len(results[0].ReturnData) == 0 {
		return nil, fmt.Errorf("getAllVaultsAddresses call failed")
	}
	unpacked, err := s.resolverABI.Unpack("getAllVaultsAddresses", results[0].ReturnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking getAllVaultsAddresses: %w", err)
	}
	if len(unpacked) == 0 {
		return nil, fmt.Errorf("getAllVaultsAddresses returned no values")
	}
	addrs, ok := unpacked[0].([]common.Address)
	if !ok {
		return nil, fmt.Errorf("getAllVaultsAddresses returned unexpected type %T", unpacked[0])
	}
	return addrs, nil
}

// GetVaultsEntireData reads getVaultEntireData for each vault in a single
// Multicall3 batch, pinned to blockNumber. Results are returned in the same
// order as vaults. A vault whose sub-call reverted or fails to decode aborts
// the batch with an error — the on-chain read is the truth, so a partial read
// must not silently produce a snapshot.
func (s *blockchainService) GetVaultsEntireData(ctx context.Context, vaults []common.Address, blockNumber int64) ([]*VaultEntireData, error) {
	if len(vaults) == 0 {
		return nil, nil
	}
	calls := make([]outbound.Call, len(vaults))
	for i, v := range vaults {
		callData, err := s.resolverABI.Pack("getVaultEntireData", v)
		if err != nil {
			return nil, fmt.Errorf("packing getVaultEntireData(%s): %w", v.Hex(), err)
		}
		calls[i] = outbound.Call{Target: s.resolverAddr, AllowFailure: false, CallData: callData}
	}

	results, err := s.multicaller.Execute(ctx, calls, big.NewInt(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("multicall getVaultEntireData batch: %w", err)
	}
	if len(results) != len(vaults) {
		return nil, fmt.Errorf("getVaultEntireData batch: expected %d results, got %d", len(vaults), len(results))
	}

	out := make([]*VaultEntireData, len(vaults))
	for i, r := range results {
		if !r.Success || len(r.ReturnData) == 0 {
			return nil, fmt.Errorf("getVaultEntireData(%s) call failed", vaults[i].Hex())
		}
		decoded, err := s.decodeVaultEntireData(r.ReturnData)
		if err != nil {
			return nil, fmt.Errorf("decoding getVaultEntireData(%s): %w", vaults[i].Hex(), err)
		}
		out[i] = decoded
	}
	return out, nil
}

// decodeVaultEntireData unpacks a getVaultEntireData return blob into the subset
// of fields the indexer reads. The full tuple is unpacked, then the top-level
// fields are read positionally and the small inner sub-tuples are converted via
// abi.ConvertType (the full struct cannot be ConvertType'd directly because
// go-ethereum's reflection cannot set the fixed-bytes32 slot fields).
func (s *blockchainService) decodeVaultEntireData(returnData []byte) (*VaultEntireData, error) {
	unpacked, err := s.resolverABI.Unpack("getVaultEntireData", returnData)
	if err != nil {
		return nil, fmt.Errorf("unpacking getVaultEntireData: %w", err)
	}
	if len(unpacked) == 0 {
		return nil, fmt.Errorf("getVaultEntireData returned no values")
	}

	top := reflect.ValueOf(unpacked[0])
	if top.Kind() != reflect.Struct || top.NumField() <= vedFieldTotalSupplyAndBorrow {
		return nil, fmt.Errorf("getVaultEntireData returned unexpected shape %T", unpacked[0])
	}

	vault, ok := top.Field(vedFieldVault).Interface().(common.Address)
	if !ok {
		return nil, fmt.Errorf("vault field has unexpected type %T", top.Field(vedFieldVault).Interface())
	}
	isSmartCol, ok := top.Field(vedFieldIsSmartCol).Interface().(bool)
	if !ok {
		return nil, fmt.Errorf("isSmartCol field has unexpected type")
	}
	isSmartDebt, ok := top.Field(vedFieldIsSmartDebt).Interface().(bool)
	if !ok {
		return nil, fmt.Errorf("isSmartDebt field has unexpected type")
	}

	cv := top.Field(vedFieldConstantVariables)
	if cv.Kind() != reflect.Struct || cv.NumField() <= cvFieldVaultType {
		return nil, fmt.Errorf("constantVariables has unexpected shape")
	}
	supplyToken, ok := abi.ConvertType(cv.Field(cvFieldSupplyToken).Interface(), abiTokens{}).(abiTokens)
	if !ok {
		return nil, fmt.Errorf("converting supplyToken tuple")
	}
	borrowToken, ok := abi.ConvertType(cv.Field(cvFieldBorrowToken).Interface(), abiTokens{}).(abiTokens)
	if !ok {
		return nil, fmt.Errorf("converting borrowToken tuple")
	}
	vaultType, ok := cv.Field(cvFieldVaultType).Interface().(*big.Int)
	if !ok {
		return nil, fmt.Errorf("vaultType field has unexpected type")
	}

	epr, ok := abi.ConvertType(top.Field(vedFieldExchangePricesAndRates).Interface(), abiExchangePricesAndRates{}).(abiExchangePricesAndRates)
	if !ok {
		return nil, fmt.Errorf("converting exchangePricesAndRates tuple")
	}
	tsb, ok := abi.ConvertType(top.Field(vedFieldTotalSupplyAndBorrow).Interface(), abiTotalSupplyAndBorrow{}).(abiTotalSupplyAndBorrow)
	if !ok {
		return nil, fmt.Errorf("converting totalSupplyAndBorrow tuple")
	}

	return &VaultEntireData{
		Vault:               vault,
		IsSmartCol:          isSmartCol,
		IsSmartDebt:         isSmartDebt,
		CollateralToken:     supplyToken.Token0,
		DebtToken:           borrowToken.Token0,
		VaultType:           vaultType,
		TotalSupplyVault:    tsb.TotalSupplyVault,
		TotalBorrowVault:    tsb.TotalBorrowVault,
		SupplyExchangePrice: epr.VaultSupplyExchangePrice,
		BorrowExchangePrice: epr.VaultBorrowExchangePrice,
		SupplyRate:          epr.SupplyRateVault,
		BorrowRate:          epr.BorrowRateVault,
	}, nil
}

package fluid_vault_indexer

import (
	"context"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

func mustResolverABI(t *testing.T) *abi.ABI {
	t.Helper()
	parsed, err := abis.GetFluidVaultResolverABI()
	if err != nil {
		t.Fatalf("loading resolver ABI: %v", err)
	}
	return parsed
}

func mustERC20ABI(t *testing.T) *abi.ABI {
	t.Helper()
	parsed, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("loading ERC20 ABI: %v", err)
	}
	return parsed
}

// readFixture loads a captured getVaultEntireData return blob (real mainnet
// resolver output) as raw bytes.
func readFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	decoded, err := hexutil.Decode(string(raw))
	if err != nil {
		t.Fatalf("decoding fixture hex %s: %v", name, err)
	}
	return decoded
}

func mustBig(t *testing.T, s string) *big.Int {
	t.Helper()
	b, ok := new(big.Int).SetString(s, 10)
	if !ok {
		t.Fatalf("bad big.Int %q", s)
	}
	return b
}

var errMC = errors.New("multicall failed")

// stubMulticaller returns canned results for each Execute call in order.
type stubMulticaller struct {
	calls     [][]outbound.Call
	blocks    []*big.Int
	responses [][]outbound.Result
	err       error
	addr      common.Address
}

func (m *stubMulticaller) Execute(_ context.Context, calls []outbound.Call, block *big.Int) ([]outbound.Result, error) {
	m.calls = append(m.calls, calls)
	m.blocks = append(m.blocks, block)
	if m.err != nil {
		return nil, m.err
	}
	idx := len(m.calls) - 1
	if idx >= len(m.responses) {
		return nil, nil
	}
	return m.responses[idx], nil
}

func (m *stubMulticaller) Address() common.Address { return m.addr }

func newBlockchainServiceForTest(t *testing.T, mc outbound.Multicaller) *blockchainService {
	t.Helper()
	svc, err := newBlockchainService(mc)
	if err != nil {
		t.Fatalf("newBlockchainService: %v", err)
	}
	return svc
}

// TestDecodeVaultEntireData_SinglePlainVault decodes a real mainnet
// getVaultEntireData blob for VaultT1 ETH/sUSDS (plain single-collateral,
// single-debt) and asserts the fields the indexer reads.
func TestDecodeVaultEntireData_SinglePlainVault(t *testing.T) {
	svc := newBlockchainServiceForTest(t, &stubMulticaller{})
	raw := readFixture(t, "vault_entire_data_single_susds.hex")

	got, err := svc.decodeVaultEntireData(raw)
	if err != nil {
		t.Fatalf("decodeVaultEntireData: %v", err)
	}

	wantVault := common.HexToAddress("0x75305a6a8977E998573076FA3293A235E23C32Ad")
	if got.Vault != wantVault {
		t.Errorf("vault = %s, want %s", got.Vault, wantVault)
	}
	if got.IsSmartCol || got.IsSmartDebt {
		t.Errorf("isSmartCol=%v isSmartDebt=%v, want both false", got.IsSmartCol, got.IsSmartDebt)
	}
	if !got.IsPlainSingle() {
		t.Errorf("IsPlainSingle() = false, want true")
	}
	wantColl := common.HexToAddress("0xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE")
	if got.CollateralToken != wantColl {
		t.Errorf("collateral = %s, want %s (ETH sentinel)", got.CollateralToken, wantColl)
	}
	wantDebt := common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")
	if got.DebtToken != wantDebt {
		t.Errorf("debt = %s, want %s (sUSDS)", got.DebtToken, wantDebt)
	}
	if got.VaultType.Cmp(big.NewInt(10000)) != 0 {
		t.Errorf("vaultType = %s, want 10000", got.VaultType)
	}
	if got.TotalSupplyVault.Cmp(mustBig(t, "11328444893030209")) != 0 {
		t.Errorf("totalSupplyVault = %s", got.TotalSupplyVault)
	}
	if got.TotalBorrowVault.Cmp(mustBig(t, "8021962986715460141")) != 0 {
		t.Errorf("totalBorrowVault = %s", got.TotalBorrowVault)
	}
	if got.SupplyExchangePrice.Cmp(mustBig(t, "1030050163867")) != 0 {
		t.Errorf("supplyExchangePrice = %s", got.SupplyExchangePrice)
	}
	if got.BorrowExchangePrice.Cmp(mustBig(t, "1002745372336")) != 0 {
		t.Errorf("borrowExchangePrice = %s", got.BorrowExchangePrice)
	}
	if got.SupplyRate.Cmp(big.NewInt(157)) != 0 {
		t.Errorf("supplyRate = %s, want 157", got.SupplyRate)
	}
	if got.BorrowRate.Sign() != 0 {
		t.Errorf("borrowRate = %s, want 0", got.BorrowRate)
	}
}

// TestDecodeVaultEntireData_SmartVault decodes a real mainnet smart (DEX)
// vault and asserts the smart flags are set so the service skips it.
func TestDecodeVaultEntireData_SmartVault(t *testing.T) {
	svc := newBlockchainServiceForTest(t, &stubMulticaller{})
	raw := readFixture(t, "vault_entire_data_smart.hex")

	got, err := svc.decodeVaultEntireData(raw)
	if err != nil {
		t.Fatalf("decodeVaultEntireData: %v", err)
	}
	if !got.IsSmartCol || !got.IsSmartDebt {
		t.Errorf("isSmartCol=%v isSmartDebt=%v, want both true", got.IsSmartCol, got.IsSmartDebt)
	}
	if got.IsPlainSingle() {
		t.Errorf("IsPlainSingle() = true, want false for smart vault")
	}
	if got.VaultType.Cmp(big.NewInt(40000)) != 0 {
		t.Errorf("vaultType = %s, want 40000", got.VaultType)
	}
}

func TestDecodeVaultEntireData_Garbage(t *testing.T) {
	svc := newBlockchainServiceForTest(t, &stubMulticaller{})
	if _, err := svc.decodeVaultEntireData([]byte{0x01, 0x02, 0x03}); err == nil {
		t.Fatal("expected error decoding garbage, got nil")
	}
}

func TestGetAllVaultAddresses(t *testing.T) {
	resolverABI := mustResolverABI(t)
	addrs := []common.Address{
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
		common.HexToAddress("0x2222222222222222222222222222222222222222"),
	}
	packed, err := resolverABI.Methods["getAllVaultsAddresses"].Outputs.Pack(addrs)
	if err != nil {
		t.Fatalf("packing return: %v", err)
	}
	mc := &stubMulticaller{responses: [][]outbound.Result{{{Success: true, ReturnData: packed}}}}
	svc := newBlockchainServiceForTest(t, mc)

	got, err := svc.GetAllVaultAddresses(context.Background(), 100)
	if err != nil {
		t.Fatalf("GetAllVaultAddresses: %v", err)
	}
	if len(got) != 2 || got[0] != addrs[0] || got[1] != addrs[1] {
		t.Errorf("got %v, want %v", got, addrs)
	}
	if len(mc.blocks) != 1 || mc.blocks[0].Int64() != 100 {
		t.Errorf("expected block 100 pinned, got %v", mc.blocks)
	}
	if mc.calls[0][0].Target != FluidVaultResolverAddress {
		t.Errorf("call target = %s, want resolver", mc.calls[0][0].Target)
	}
}

func TestGetAllVaultAddresses_CallReverted(t *testing.T) {
	mc := &stubMulticaller{responses: [][]outbound.Result{{{Success: false}}}}
	svc := newBlockchainServiceForTest(t, mc)
	if _, err := svc.GetAllVaultAddresses(context.Background(), 1); err == nil {
		t.Fatal("expected error on reverted call")
	}
}

func TestGetVaultsEntireData_BatchesAndDecodes(t *testing.T) {
	single := readFixture(t, "vault_entire_data_single_susds.hex")
	smart := readFixture(t, "vault_entire_data_smart.hex")
	vaults := []common.Address{
		common.HexToAddress("0x75305a6a8977E998573076FA3293A235E23C32Ad"),
		common.HexToAddress("0x57fed7c9b3c763999c519264931790cBcA331417"),
	}
	mc := &stubMulticaller{responses: [][]outbound.Result{{
		{Success: true, ReturnData: single},
		{Success: true, ReturnData: smart},
	}}}
	svc := newBlockchainServiceForTest(t, mc)

	got, err := svc.GetVaultsEntireData(context.Background(), vaults, 200)
	if err != nil {
		t.Fatalf("GetVaultsEntireData: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}
	if !got[0].IsPlainSingle() {
		t.Errorf("vault[0] should be plain single")
	}
	if got[1].IsPlainSingle() {
		t.Errorf("vault[1] should be smart")
	}
	// All sub-calls must be batched into a single Execute.
	if len(mc.calls) != 1 {
		t.Errorf("expected 1 batched Execute, got %d", len(mc.calls))
	}
	if len(mc.calls[0]) != 2 {
		t.Errorf("expected 2 sub-calls, got %d", len(mc.calls[0]))
	}
}

func TestGetVaultsEntireData_SubCallReverted(t *testing.T) {
	single := readFixture(t, "vault_entire_data_single_susds.hex")
	vaults := []common.Address{
		common.HexToAddress("0x75305a6a8977E998573076FA3293A235E23C32Ad"),
		common.HexToAddress("0x57fed7c9b3c763999c519264931790cBcA331417"),
	}
	mc := &stubMulticaller{responses: [][]outbound.Result{{
		{Success: true, ReturnData: single},
		{Success: false},
	}}}
	svc := newBlockchainServiceForTest(t, mc)
	if _, err := svc.GetVaultsEntireData(context.Background(), vaults, 1); err == nil {
		t.Fatal("expected error when a sub-call reverts (no partial reads)")
	}
}

func TestGetVaultsEntireData_Empty(t *testing.T) {
	svc := newBlockchainServiceForTest(t, &stubMulticaller{})
	got, err := svc.GetVaultsEntireData(context.Background(), nil, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

func TestGetAllVaultAddresses_ResultCountMismatch(t *testing.T) {
	mc := &stubMulticaller{responses: [][]outbound.Result{{}}} // 0 results
	svc := newBlockchainServiceForTest(t, mc)
	if _, err := svc.GetAllVaultAddresses(context.Background(), 1); err == nil {
		t.Fatal("expected error on result-count mismatch")
	}
}

func TestGetAllVaultAddresses_MulticallError(t *testing.T) {
	mc := &stubMulticaller{err: errMC}
	svc := newBlockchainServiceForTest(t, mc)
	if _, err := svc.GetAllVaultAddresses(context.Background(), 1); err == nil {
		t.Fatal("expected error from multicaller")
	}
}

func TestGetVaultsEntireData_ResultCountMismatch(t *testing.T) {
	vaults := []common.Address{common.HexToAddress("0x1"), common.HexToAddress("0x2")}
	mc := &stubMulticaller{responses: [][]outbound.Result{{{Success: true}}}} // 1 result for 2 calls
	svc := newBlockchainServiceForTest(t, mc)
	if _, err := svc.GetVaultsEntireData(context.Background(), vaults, 1); err == nil {
		t.Fatal("expected error on result-count mismatch")
	}
}

func TestNewBlockchainService_NilMulticaller(t *testing.T) {
	if _, err := newBlockchainService(nil); err == nil {
		t.Fatal("expected error for nil multicaller")
	}
}

func TestGetTokenMetadata_ETHSentinel(t *testing.T) {
	mc := &stubMulticaller{}
	svc := newBlockchainServiceForTest(t, mc)
	md, err := svc.GetTokenMetadata(context.Background(), ethSentinel, 1)
	if err != nil {
		t.Fatalf("GetTokenMetadata: %v", err)
	}
	if md.Symbol != "ETH" || md.Decimals != 18 {
		t.Errorf("got %+v, want {ETH 18}", md)
	}
	if len(mc.calls) != 0 {
		t.Errorf("ETH sentinel must not issue an RPC call, got %d", len(mc.calls))
	}
}

func TestGetTokenMetadata_ERC20(t *testing.T) {
	erc20 := mustERC20ABI(t)
	symbol, err := erc20.Methods["symbol"].Outputs.Pack("sUSDS")
	if err != nil {
		t.Fatalf("pack symbol: %v", err)
	}
	decimals, err := erc20.Methods["decimals"].Outputs.Pack(uint8(18))
	if err != nil {
		t.Fatalf("pack decimals: %v", err)
	}
	mc := &stubMulticaller{responses: [][]outbound.Result{{
		{Success: true, ReturnData: symbol},
		{Success: true, ReturnData: decimals},
	}}}
	svc := newBlockchainServiceForTest(t, mc)

	md, err := svc.GetTokenMetadata(context.Background(), common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD"), 1)
	if err != nil {
		t.Fatalf("GetTokenMetadata: %v", err)
	}
	if md.Symbol != "sUSDS" || md.Decimals != 18 {
		t.Errorf("got %+v, want {sUSDS 18}", md)
	}
}

func TestGetTokenMetadata_DecimalsReverted(t *testing.T) {
	erc20 := mustERC20ABI(t)
	symbol, _ := erc20.Methods["symbol"].Outputs.Pack("X")
	mc := &stubMulticaller{responses: [][]outbound.Result{{
		{Success: true, ReturnData: symbol},
		{Success: false},
	}}}
	svc := newBlockchainServiceForTest(t, mc)
	if _, err := svc.GetTokenMetadata(context.Background(), common.HexToAddress("0xabc0000000000000000000000000000000000001"), 1); err == nil {
		t.Fatal("expected error when decimals() reverts")
	}
}

func TestGetTokenMetadata_SymbolRevertedTolerated(t *testing.T) {
	erc20 := mustERC20ABI(t)
	decimals, _ := erc20.Methods["decimals"].Outputs.Pack(uint8(6))
	mc := &stubMulticaller{responses: [][]outbound.Result{{
		{Success: false},
		{Success: true, ReturnData: decimals},
	}}}
	svc := newBlockchainServiceForTest(t, mc)
	md, err := svc.GetTokenMetadata(context.Background(), common.HexToAddress("0xabc0000000000000000000000000000000000002"), 1)
	if err != nil {
		t.Fatalf("symbol revert should be tolerated: %v", err)
	}
	if md.Symbol != "" || md.Decimals != 6 {
		t.Errorf("got %+v, want {\"\" 6}", md)
	}
}

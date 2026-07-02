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

// readFixture loads a testdata/*.hex fixture as raw bytes. Each fixture is the
// hex-encoded ABI return blob of a real mainnet VaultResolver.getVaultEntireData
// call, captured with `cast call <resolver> "getVaultEntireData(address)" <vault>`
// (single_susds = the ETH/sUSDS VaultT1 0x75305a6a…; smart = a DEX vault). We
// decode against captured on-chain output rather than a hand-encoded tuple so a
// silent drift in Fluid's return shape or in our field offsets fails the test.
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
	svc, err := newBlockchainService(mc, testLogger())
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

// TestGetVaultsEntireData_ChunksLargeBatch verifies that a vault set exceeding
// vaultEntireDataBatchSize is split across multiple Execute calls while
// preserving input order in the returned slice. The `smart` fixture is placed at
// indices 49 and 50 (straddling the chunk boundary at 50) so per-index identity
// is observable — an ordering bug would surface a plain vault where a smart one
// is expected, or vice versa.
func TestGetVaultsEntireData_ChunksLargeBatch(t *testing.T) {
	single := readFixture(t, "vault_entire_data_single_susds.hex")
	smart := readFixture(t, "vault_entire_data_smart.hex")
	const n = vaultEntireDataBatchSize + 3

	smartIndices := map[int]bool{49: true, 50: true}

	vaults := make([]common.Address, n)
	for i := range vaults {
		vaults[i] = common.BigToAddress(big.NewInt(int64(i + 1)))
	}

	fixtureFor := func(i int) []byte {
		if smartIndices[i] {
			return smart
		}
		return single
	}
	firstChunk := make([]outbound.Result, vaultEntireDataBatchSize)
	for i := range firstChunk {
		firstChunk[i] = outbound.Result{Success: true, ReturnData: fixtureFor(i)}
	}
	secondChunk := make([]outbound.Result, n-vaultEntireDataBatchSize)
	for i := range secondChunk {
		secondChunk[i] = outbound.Result{Success: true, ReturnData: fixtureFor(vaultEntireDataBatchSize + i)}
	}
	mc := &stubMulticaller{responses: [][]outbound.Result{firstChunk, secondChunk}}
	svc := newBlockchainServiceForTest(t, mc)

	got, err := svc.GetVaultsEntireData(context.Background(), vaults, 200)
	if err != nil {
		t.Fatalf("GetVaultsEntireData: %v", err)
	}
	if len(got) != n {
		t.Fatalf("got %d results, want %d", len(got), n)
	}
	for i, ved := range got {
		if ved == nil {
			t.Fatalf("result[%d] is nil", i)
		}
		wantPlain := !smartIndices[i]
		if ved.IsPlainSingle() != wantPlain {
			t.Errorf("result[%d].IsPlainSingle() = %v, want %v (ordering not preserved)", i, ved.IsPlainSingle(), wantPlain)
		}
	}
	if len(mc.calls) != 2 {
		t.Fatalf("expected 2 chunked Execute calls, got %d", len(mc.calls))
	}
	if len(mc.calls[0]) != vaultEntireDataBatchSize {
		t.Errorf("first chunk = %d sub-calls, want %d", len(mc.calls[0]), vaultEntireDataBatchSize)
	}
	if len(mc.calls[1]) != n-vaultEntireDataBatchSize {
		t.Errorf("second chunk = %d sub-calls, want %d", len(mc.calls[1]), n-vaultEntireDataBatchSize)
	}

	// The packed getVaultEntireData(address) calldata ends with the 32-byte
	// left-padded vault address; its trailing 20 bytes must be the expected vault
	// at boundary indices, confirming the right address is packed into the right
	// chunk slot. Indices 0/49 fall in the first chunk, 50/52 in the second.
	callAt := func(globalIdx int) outbound.Call {
		if globalIdx < vaultEntireDataBatchSize {
			return mc.calls[0][globalIdx]
		}
		return mc.calls[1][globalIdx-vaultEntireDataBatchSize]
	}
	for _, idx := range []int{0, 49, 50, 52} {
		cd := callAt(idx).CallData
		gotAddr := common.BytesToAddress(cd[len(cd)-20:])
		if gotAddr != vaults[idx] {
			t.Errorf("calldata at index %d packs vault %s, want %s", idx, gotAddr, vaults[idx])
		}
	}
}

// TestGetVaultsEntireDataBestEffort_SkipsFailedVault verifies the classification
// path tolerates a per-vault failure: the failed vault yields a nil entry while
// the servable vault is still decoded, with order preserved.
func TestGetVaultsEntireDataBestEffort_SkipsFailedVault(t *testing.T) {
	single := readFixture(t, "vault_entire_data_single_susds.hex")
	smart := readFixture(t, "vault_entire_data_smart.hex")
	vaults := []common.Address{
		common.HexToAddress("0x75305a6a8977E998573076FA3293A235E23C32Ad"),
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
		common.HexToAddress("0x57fed7c9b3c763999c519264931790cBcA331417"),
	}
	mc := &stubMulticaller{responses: [][]outbound.Result{{
		{Success: true, ReturnData: single},
		{Success: false},
		{Success: true, ReturnData: smart},
	}}}
	svc := newBlockchainServiceForTest(t, mc)

	got, err := svc.GetVaultsEntireDataBestEffort(context.Background(), vaults, 1)
	if err != nil {
		t.Fatalf("GetVaultsEntireDataBestEffort: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d results, want 3", len(got))
	}
	if got[0] == nil || !got[0].IsPlainSingle() {
		t.Errorf("vault[0] should be decoded plain single, got %v", got[0])
	}
	if got[1] != nil {
		t.Errorf("vault[1] failed and must be nil, got %v", got[1])
	}
	if got[2] == nil || got[2].IsPlainSingle() {
		t.Errorf("vault[2] should be decoded smart vault, got %v", got[2])
	}
	// The best-effort path must mark sub-calls AllowFailure so the resolver
	// serves the servable vaults instead of the whole batch reverting.
	for _, c := range mc.calls[0] {
		if !c.AllowFailure {
			t.Errorf("best-effort sub-call must set AllowFailure=true")
		}
	}
}

// TestGetVaultsEntireDataBestEffort_SkipsUndecodableVault verifies the
// best-effort path tolerates a sub-call that succeeds but returns undecodable
// data: the garbage vault yields a nil entry (decode-failure branch) while the
// servable vault is still decoded.
func TestGetVaultsEntireDataBestEffort_SkipsUndecodableVault(t *testing.T) {
	single := readFixture(t, "vault_entire_data_single_susds.hex")
	vaults := []common.Address{
		common.HexToAddress("0x75305a6a8977E998573076FA3293A235E23C32Ad"),
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
	}
	mc := &stubMulticaller{responses: [][]outbound.Result{{
		{Success: true, ReturnData: single},
		{Success: true, ReturnData: []byte{0x01, 0x02, 0x03}}, // succeeds but cannot decode
	}}}
	svc := newBlockchainServiceForTest(t, mc)

	got, err := svc.GetVaultsEntireDataBestEffort(context.Background(), vaults, 1)
	if err != nil {
		t.Fatalf("GetVaultsEntireDataBestEffort: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2", len(got))
	}
	if got[0] == nil || !got[0].IsPlainSingle() {
		t.Errorf("vault[0] should be decoded plain single, got %v", got[0])
	}
	if got[1] != nil {
		t.Errorf("vault[1] returned undecodable data and must be nil, got %v", got[1])
	}
}

// TestGetVaultsEntireData_StrictRejectsUndecodable confirms the strict snapshot
// path aborts the whole batch when one sub-call succeeds but returns undecodable
// data, even if a sibling decodes cleanly.
func TestGetVaultsEntireData_StrictRejectsUndecodable(t *testing.T) {
	single := readFixture(t, "vault_entire_data_single_susds.hex")
	vaults := []common.Address{
		common.HexToAddress("0x75305a6a8977E998573076FA3293A235E23C32Ad"),
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
	}
	mc := &stubMulticaller{responses: [][]outbound.Result{{
		{Success: true, ReturnData: single},
		{Success: true, ReturnData: []byte{0x01, 0x02, 0x03}}, // succeeds but cannot decode
	}}}
	svc := newBlockchainServiceForTest(t, mc)
	if _, err := svc.GetVaultsEntireData(context.Background(), vaults, 1); err == nil {
		t.Fatal("expected error when one sub-call returns undecodable data on the strict path")
	}
}

// TestGetVaultsEntireData_StrictRejectsFailure confirms the strict snapshot path
// keeps AllowFailure=false and aborts on any sub-call failure.
func TestGetVaultsEntireData_StrictRejectsFailure(t *testing.T) {
	single := readFixture(t, "vault_entire_data_single_susds.hex")
	vaults := []common.Address{
		common.HexToAddress("0x75305a6a8977E998573076FA3293A235E23C32Ad"),
		common.HexToAddress("0x1111111111111111111111111111111111111111"),
	}
	mc := &stubMulticaller{responses: [][]outbound.Result{{
		{Success: true, ReturnData: single},
		{Success: false},
	}}}
	svc := newBlockchainServiceForTest(t, mc)
	if _, err := svc.GetVaultsEntireData(context.Background(), vaults, 1); err == nil {
		t.Fatal("expected error when a sub-call fails on the strict path")
	}
	for _, c := range mc.calls[0] {
		if c.AllowFailure {
			t.Errorf("strict sub-call must set AllowFailure=false")
		}
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
	if _, err := newBlockchainService(nil, testLogger()); err == nil {
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
	symbol, err := erc20.Methods["symbol"].Outputs.Pack("X")
	if err != nil {
		t.Fatalf("pack symbol: %v", err)
	}
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
	decimals, err := erc20.Methods["decimals"].Outputs.Pack(uint8(6))
	if err != nil {
		t.Fatalf("pack decimals: %v", err)
	}
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

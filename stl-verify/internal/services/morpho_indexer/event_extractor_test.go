package morpho_indexer

import (
	"math/big"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/testutils"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

func TestNewEventExtractor(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}
	if len(e.morphoBlueSignatures) != 10 {
		t.Errorf("expected 10 Morpho Blue event signatures, got %d", len(e.morphoBlueSignatures))
	}

	// metaMorphoSignatures must include the 4 typed-handler events plus the
	// full Morpho VaultV2 governance / allocation / cap / fee / role / timelock
	// surface. Verifying by topic-hash presence rather than total count so the
	// test stays meaningful as the V2 event set evolves.
	mustHaveTopics := map[string]string{
		// Typed handlers
		"Deposit":                     "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7",
		"Withdraw":                    "0xfbde797d201c681b91056529119e0b02407c7bb96a4a2c75c01fc9667232c8db",
		"Transfer":                    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
		"AccrueInterest (V2 4-field)": "0x4dec04e750ca11537cabcd8a9eab06494de08da3735bc8871cd41250e190bc04",
		// V2 surface — chain-verified against sparkUSDTbc on 2026-05-06.
		"Allocate":                   "0x2bc7948a96a066968d2a58aaf46eb0b305aa166b1d1951d2f7ef0919746b8c2a",
		"Deallocate":                 "0xd602b36fb24934aef1bc2a658de029b486fa4c664a6e45de1f48e3fd1be25dd9",
		"ForceDeallocate":            "0xb98216be0267fa550428a584fe6ac1ef0f39788e0198372100e813444afecd29",
		"AddAdapter":                 "0x8f125a24838c4c23e893904b255b5c672d43d4cb8af7e3d15841eaeabc1e68aa",
		"RemoveAdapter":              "0x34f33faa2592bc1f615ec3e91b55b7784665ce46461403c294824d85f9f66458",
		"IncreaseAbsoluteCap":        "0x7368d59ed82f6a538f6deef9baa54623fe3699ce07b19031f762f740c8c34b03",
		"DecreaseAbsoluteCap":        "0xbc2ffe81312f53db4f327eca188ebdc13df66a8ce25f7dec6f5b4495fe27b371",
		"IncreaseRelativeCap":        "0x2a343b9a1ceba40853d01c6adeea53f5c0e4b95b4eb870ed4af309a0ead7e399",
		"DecreaseRelativeCap":        "0xcedce6ffe8b7f89de49bd4f955667ca0a963a8059264196807b240ed470b25ce",
		"SetPerformanceFee":          "0x8b940a95968ad5b511f89b01075446a4fe9f614f2dc5fbb9e9a6b227d6d4fd70",
		"SetManagementFee":           "0xd87632b1c6ebfa21acbca0e3279b3cf6385a377cb8fda51e5b866baa6e6012ab",
		"SetCurator":                 "0xbd0a63c12948fbc9194a5839019f99c9d71db924e5c70018265bc778b8f1a506",
		"Submit":                     "0x8b18afeb361b83b025999ed5b42f1d90c68aaa5a0fd49c015f04c3b8b81e80eb",
		"Accept":                     "0x29aa42fc192ff77ef42105abba283197ac841341e196e417a7fc2784cdc4e5fb",
		"SetLiquidityAdapterAndData": "0x9deb43d71422af41853c3921fb364b7647f9a9b136e46d66d45c1bf707af706c",
		"SetMaxRate":                 "0x75fef0e2a5e934789b0723129a0d1bbfd4a50f39c5af9919626e4f3603aef5ff",
		"Constructor":                "0x612d665a88b3ae6bc3e53207bfc2db673e2e05e2aa4a68043b618cc81295a27d",
	}
	for label, topic := range mustHaveTopics {
		if _, ok := e.metaMorphoSignatures[common.HexToHash(topic)]; !ok {
			t.Errorf("metaMorphoSignatures missing %s (topic %s)", label, topic)
		}
	}
}

func TestIsMorphoBlueEvent(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	// Supply event topic: keccak256("Supply(bytes32,address,address,uint256,uint256)")
	supplyTopic := e.morphoBlueABI.Events["Supply"].ID.Hex()

	tests := []struct {
		name   string
		log    shared.Log
		expect bool
	}{
		{
			name:   "Supply event",
			log:    shared.Log{Topics: []string{supplyTopic}},
			expect: true,
		},
		{
			name:   "unknown topic",
			log:    shared.Log{Topics: []string{"0x0000000000000000000000000000000000000000000000000000000000000001"}},
			expect: false,
		},
		{
			name:   "no topics",
			log:    shared.Log{Topics: nil},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := e.IsMorphoBlueEvent(tt.log); got != tt.expect {
				t.Errorf("IsMorphoBlueEvent() = %v, want %v", got, tt.expect)
			}
		})
	}
}

func TestIsMetaMorphoEvent(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	depositTopic := e.metaMorphoABI.Events["Deposit"].ID.Hex()
	transferTopic := e.metaMorphoABI.Events["Transfer"].ID.Hex()

	tests := []struct {
		name   string
		log    shared.Log
		expect bool
	}{
		{
			name:   "Deposit event",
			log:    shared.Log{Topics: []string{depositTopic}},
			expect: true,
		},
		{
			name:   "Transfer event",
			log:    shared.Log{Topics: []string{transferTopic}},
			expect: true,
		},
		{
			name:   "unknown topic",
			log:    shared.Log{Topics: []string{"0x0000000000000000000000000000000000000000000000000000000000000001"}},
			expect: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := e.IsMetaMorphoEvent(tt.log); got != tt.expect {
				t.Errorf("IsMetaMorphoEvent() = %v, want %v", got, tt.expect)
			}
		})
	}
}

// TestIsVaultActivityEvent codifies the discovery-gate contract: only the
// Morpho VaultV2 4-field AccrueInterest topic triggers a probe. V1/V1.1
// vaults (Deposit, Withdraw, V1 2-field AccrueInterest) are discovered via
// the Morpho Blue path instead, and Transfer is excluded so plain ERC20s
// don't blow past Alchemy's 550M eth_call gas cap on legacy fallback
// `INVALID` (0xfe) — see VEC-198 multicall-gas-cap fix and the
// IsVaultActivityEvent docstring.
func TestIsVaultActivityEvent(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	depositTopic := e.metaMorphoABI.Events["Deposit"].ID.Hex()
	withdrawTopic := e.metaMorphoABI.Events["Withdraw"].ID.Hex()
	transferTopic := e.metaMorphoABI.Events["Transfer"].ID.Hex()
	accrueV1Topic := e.metaMorphoABI.Events["AccrueInterest"].ID.Hex()

	v2AccrueABI, err := abis.GetMetaMorphoV2AccrueInterestABI()
	if err != nil {
		t.Fatalf("GetMetaMorphoV2AccrueInterestABI() error: %v", err)
	}
	accrueV2Topic := v2AccrueABI.Events["AccrueInterest"].ID.Hex()

	supplyTopic := e.morphoBlueABI.Events["Supply"].ID.Hex()

	tests := []struct {
		name   string
		log    shared.Log
		expect bool
	}{
		// The only discovery-trigger topic.
		{name: "AccrueInterest V2 (4-field)", log: shared.Log{Topics: []string{accrueV2Topic}}, expect: true},

		// V1/V1.1 events are discovered via Morpho Blue, not via this gate.
		{name: "Deposit (V1/V1.1, discovered via Morpho Blue)", log: shared.Log{Topics: []string{depositTopic}}, expect: false},
		{name: "Withdraw (V1/V1.1, discovered via Morpho Blue)", log: shared.Log{Topics: []string{withdrawTopic}}, expect: false},
		{name: "AccrueInterest V1/V1.1 (2-field, discovered via Morpho Blue)", log: shared.Log{Topics: []string{accrueV1Topic}}, expect: false},

		// Excluded for gas-cap and family-of-events reasons.
		{name: "ERC20 Transfer", log: shared.Log{Topics: []string{transferTopic}}, expect: false},
		{name: "Morpho Blue Supply (different family)", log: shared.Log{Topics: []string{supplyTopic}}, expect: false},
		{name: "unknown topic", log: shared.Log{Topics: []string{"0x0000000000000000000000000000000000000000000000000000000000000001"}}, expect: false},
		{name: "no topics", log: shared.Log{Topics: nil}, expect: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := e.IsVaultActivityEvent(tt.log); got != tt.expect {
				t.Errorf("IsVaultActivityEvent() = %v, want %v", got, tt.expect)
			}
		})
	}
}

// TestMetaMorphoEventName covers the topic-hash → event-name lookup used by
// service.go to label protocol_event audit-log rows. Spot-checks the typed
// handlers, a sample of newly-registered V2 events, and a non-MetaMorpho topic.
func TestMetaMorphoEventName(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	tests := []struct {
		name     string
		topic    string
		wantName string
		wantOK   bool
	}{
		{"Deposit", "0xdcbc1c05240f31ff3ad067ef1ee35ce4997762752e3a095284754544f4c709d7", "Deposit", true},
		{"AccrueInterest V2 4-field", "0x4dec04e750ca11537cabcd8a9eab06494de08da3735bc8871cd41250e190bc04", "AccrueInterest", true},
		{"Allocate", "0x2bc7948a96a066968d2a58aaf46eb0b305aa166b1d1951d2f7ef0919746b8c2a", "Allocate", true},
		{"AddAdapter (correct name, not AdapterAdded)", "0x8f125a24838c4c23e893904b255b5c672d43d4cb8af7e3d15841eaeabc1e68aa", "AddAdapter", true},
		{"SetCurator", "0xbd0a63c12948fbc9194a5839019f99c9d71db924e5c70018265bc778b8f1a506", "SetCurator", true},
		{"unknown topic", "0x0000000000000000000000000000000000000000000000000000000000000001", "", false},
		{"no topics", "", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var log shared.Log
			if tt.topic != "" {
				log = shared.Log{Topics: []string{tt.topic}}
			}
			gotName, gotOK := e.MetaMorphoEventName(log)
			if gotOK != tt.wantOK {
				t.Errorf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotName != tt.wantName {
				t.Errorf("name = %q, want %q", gotName, tt.wantName)
			}
		})
	}
}

// TestExtractMetaMorphoEvent_RegisteredButNotTyped covers the new return
// signature for events that are registered (so IsMetaMorphoEvent matches and
// the indexer audit-logs them) but don't have a typed handler. Such events
// must return (nil, nil) — not an error — so the caller saves the audit-log
// row without aborting the receipt.
func TestExtractMetaMorphoEvent_RegisteredButNotTyped(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	// SetCurator is registered (verified by TestMetaMorphoEventName above) but
	// has no typed handler — the adapter / cap / fee events do, so use a
	// governance setter that remains audit-log-only.
	log := shared.Log{Topics: []string{"0xbd0a63c12948fbc9194a5839019f99c9d71db924e5c70018265bc778b8f1a506"}}
	event, extractErr := e.ExtractMetaMorphoEvent(log)
	if extractErr != nil {
		t.Fatalf("ExtractMetaMorphoEvent should not error for registered-but-not-typed events; got: %v", extractErr)
	}
	if event != nil {
		t.Errorf("event should be nil for registered-but-not-typed events; got %T", event)
	}
}

func TestExtractMorphoBlueEvent_AccrueInterest(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	accrueEvent := e.morphoBlueABI.Events["AccrueInterest"]
	marketID := common.HexToHash("0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc")

	// Pack non-indexed data: prevBorrowRate=1000, interest=500, feeShares=10
	data, err := accrueEvent.Inputs.NonIndexed().Pack(
		testutils.BigFromStr(t, "1000"),
		testutils.BigFromStr(t, "500"),
		testutils.BigFromStr(t, "10"),
	)
	if err != nil {
		t.Fatalf("packing data: %v", err)
	}

	log := shared.Log{
		Topics:          []string{accrueEvent.ID.Hex(), marketID.Hex()},
		Data:            common.Bytes2Hex(data),
		TransactionHash: "0xabc123",
	}

	result, err := e.ExtractMorphoBlueEvent(log)
	if err != nil {
		t.Fatalf("ExtractMorphoBlueEvent() error: %v", err)
	}

	evt, ok := result.(*AccrueInterestEvent)
	if !ok {
		t.Fatalf("expected *AccrueInterestEvent, got %T", result)
	}
	if evt.Type() != entity.MorphoEventAccrueInterest {
		t.Errorf("Type() = %s, want AccrueInterest", evt.Type())
	}
	if evt.MarketID() != marketID {
		t.Errorf("MarketID mismatch")
	}
	if evt.PrevBorrowRate.Int64() != 1000 {
		t.Errorf("PrevBorrowRate = %s, want 1000", evt.PrevBorrowRate)
	}
	if evt.Interest.Int64() != 500 {
		t.Errorf("Interest = %s, want 500", evt.Interest)
	}
	if evt.FeeShares.Int64() != 10 {
		t.Errorf("FeeShares = %s, want 10", evt.FeeShares)
	}
}

func TestExtractMorphoBlueEvent_SetFee(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	setFeeEvent := e.morphoBlueABI.Events["SetFee"]
	marketID := common.HexToHash("0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc")

	data, err := setFeeEvent.Inputs.NonIndexed().Pack(testutils.BigFromStr(t, "100000000000000000"))
	if err != nil {
		t.Fatalf("packing data: %v", err)
	}

	log := shared.Log{
		Topics:          []string{setFeeEvent.ID.Hex(), marketID.Hex()},
		Data:            common.Bytes2Hex(data),
		TransactionHash: "0xdef456",
	}

	result, err := e.ExtractMorphoBlueEvent(log)
	if err != nil {
		t.Fatalf("ExtractMorphoBlueEvent() error: %v", err)
	}

	evt, ok := result.(*SetFeeEvent)
	if !ok {
		t.Fatalf("expected *SetFeeEvent, got %T", result)
	}
	if evt.Type() != entity.MorphoEventSetFee {
		t.Errorf("Type() = %s, want SetFee", evt.Type())
	}
	if evt.NewFee.String() != "100000000000000000" {
		t.Errorf("NewFee = %s, want 100000000000000000", evt.NewFee)
	}
}

func TestExtractMetaMorphoEvent_Transfer(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	transferEvent := e.metaMorphoABI.Events["Transfer"]
	from := common.HexToAddress("0x1111111111111111111111111111111111111111")
	to := common.HexToAddress("0x2222222222222222222222222222222222222222")

	data, err := transferEvent.Inputs.NonIndexed().Pack(testutils.BigFromStr(t, "5000"))
	if err != nil {
		t.Fatalf("packing data: %v", err)
	}

	log := shared.Log{
		Topics: []string{
			transferEvent.ID.Hex(),
			common.BytesToHash(from.Bytes()).Hex(),
			common.BytesToHash(to.Bytes()).Hex(),
		},
		Data:            common.Bytes2Hex(data),
		TransactionHash: "0xaaa111",
	}

	result, err := e.ExtractMetaMorphoEvent(log)
	if err != nil {
		t.Fatalf("ExtractMetaMorphoEvent() error: %v", err)
	}

	evt, ok := result.(*VaultTransferEvent)
	if !ok {
		t.Fatalf("expected *VaultTransferEvent, got %T", result)
	}
	if evt.Type() != entity.MorphoEventVaultTransfer {
		t.Errorf("Type() = %s, want VaultTransfer", evt.Type())
	}
	if evt.From != from {
		t.Errorf("From = %s, want %s", evt.From.Hex(), from.Hex())
	}
	if evt.To != to {
		t.Errorf("To = %s, want %s", evt.To.Hex(), to.Hex())
	}
	if evt.Value.Int64() != 5000 {
		t.Errorf("Value = %s, want 5000", evt.Value)
	}
}

func TestExtractMetaMorphoEvent_AccrueInterest(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	accrueEvent := e.metaMorphoABI.Events["AccrueInterest"]

	data, err := accrueEvent.Inputs.NonIndexed().Pack(testutils.BigFromStr(t, "2000000"), testutils.BigFromStr(t, "100"))
	if err != nil {
		t.Fatalf("packing data: %v", err)
	}

	log := shared.Log{
		Topics:          []string{accrueEvent.ID.Hex()},
		Data:            common.Bytes2Hex(data),
		TransactionHash: "0xbbb222",
	}

	result, err := e.ExtractMetaMorphoEvent(log)
	if err != nil {
		t.Fatalf("ExtractMetaMorphoEvent() error: %v", err)
	}

	evt, ok := result.(*VaultAccrueInterestEvent)
	if !ok {
		t.Fatalf("expected *VaultAccrueInterestEvent, got %T", result)
	}
	if evt.Type() != entity.MorphoEventVaultAccrueInterest {
		t.Errorf("Type() = %s, want VaultAccrueInterest", evt.Type())
	}
	if evt.NewTotalAssets.Int64() != 2000000 {
		t.Errorf("NewTotalAssets = %s, want 2000000", evt.NewTotalAssets)
	}
	if evt.FeeShares.Int64() != 100 {
		t.Errorf("FeeShares = %s, want 100", evt.FeeShares)
	}
}

func TestAccrueInterestEvent_ToJSON(t *testing.T) {
	evt := &AccrueInterestEvent{
		morphoBlueBase: morphoBlueBase{marketID: [32]byte{0x01}, txHash: "0xabc"},
		PrevBorrowRate: testutils.BigFromStr(t, "1000"),
		Interest:       testutils.BigFromStr(t, "500"),
		FeeShares:      testutils.BigFromStr(t, "10"),
	}

	jsonData, err := evt.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error: %v", err)
	}
	if len(jsonData) == 0 {
		t.Fatal("ToJSON() returned empty")
	}
}

func TestVaultDepositEvent_ToJSON(t *testing.T) {
	evt := &VaultDepositEvent{
		metaMorphoBase: metaMorphoBase{txHash: "0xdef"},
		Sender:         common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Owner:          common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Assets:         testutils.BigFromStr(t, "1000"),
		Shares:         testutils.BigFromStr(t, "900"),
	}

	jsonData, err := evt.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error: %v", err)
	}
	if len(jsonData) == 0 {
		t.Fatal("ToJSON() returned empty")
	}
}

func TestExtractMetaMorphoEvent_AccrueInterestV2(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	// Get the V2 ABI to construct the V2 event log
	v2ABI, err := abis.GetMetaMorphoV2AccrueInterestABI()
	if err != nil {
		t.Fatalf("GetMetaMorphoV2AccrueInterestABI() error: %v", err)
	}
	v2Event := v2ABI.Events["AccrueInterest"]

	// Pack 4 fields: previousTotalAssets, newTotalAssets, performanceFeeShares, managementFeeShares
	data, err := v2Event.Inputs.NonIndexed().Pack(
		testutils.BigFromStr(t, "2900000"),
		testutils.BigFromStr(t, "3000000"),
		testutils.BigFromStr(t, "200"),
		testutils.BigFromStr(t, "150"),
	)
	if err != nil {
		t.Fatalf("packing data: %v", err)
	}

	log := shared.Log{
		Topics:          []string{v2Event.ID.Hex()},
		Data:            common.Bytes2Hex(data),
		TransactionHash: "0xccc333",
	}

	result, err := e.ExtractMetaMorphoEvent(log)
	if err != nil {
		t.Fatalf("ExtractMetaMorphoEvent() error: %v", err)
	}

	evt, ok := result.(*VaultAccrueInterestEvent)
	if !ok {
		t.Fatalf("expected *VaultAccrueInterestEvent, got %T", result)
	}
	if evt.Type() != entity.MorphoEventVaultAccrueInterest {
		t.Errorf("Type() = %s, want VaultAccrueInterest", evt.Type())
	}
	if evt.PreviousTotalAssets.Int64() != 2900000 {
		t.Errorf("PreviousTotalAssets = %s, want 2900000", evt.PreviousTotalAssets)
	}
	if evt.NewTotalAssets.Int64() != 3000000 {
		t.Errorf("NewTotalAssets = %s, want 3000000", evt.NewTotalAssets)
	}
	if evt.FeeShares.Int64() != 200 {
		t.Errorf("FeeShares (performanceFeeShares) = %s, want 200", evt.FeeShares)
	}
	if evt.ManagementFeeShares.Int64() != 150 {
		t.Errorf("ManagementFeeShares = %s, want 150", evt.ManagementFeeShares)
	}
}

func TestVaultAccrueInterestEvent_ToJSON_V2(t *testing.T) {
	evt := &VaultAccrueInterestEvent{
		metaMorphoBase:      metaMorphoBase{txHash: "0xv2test"},
		NewTotalAssets:      testutils.BigFromStr(t, "3000000"),
		FeeShares:           testutils.BigFromStr(t, "200"),
		PreviousTotalAssets: testutils.BigFromStr(t, "2900000"),
		ManagementFeeShares: testutils.BigFromStr(t, "150"),
	}

	jsonData, err := evt.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error: %v", err)
	}

	jsonStr := string(jsonData)
	for _, field := range []string{`"previousTotalAssets":"2900000"`, `"managementFeeShares":"150"`, `"newTotalAssets":"3000000"`, `"feeShares":"200"`} {
		if !strings.Contains(jsonStr, field) {
			t.Errorf("ToJSON() missing field %s in %s", field, jsonStr)
		}
	}
}

func TestVaultAccrueInterestEvent_ToJSON_V1(t *testing.T) {
	evt := &VaultAccrueInterestEvent{
		metaMorphoBase: metaMorphoBase{txHash: "0xv1test"},
		NewTotalAssets: testutils.BigFromStr(t, "2000000"),
		FeeShares:      testutils.BigFromStr(t, "100"),
	}

	jsonData, err := evt.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error: %v", err)
	}

	jsonStr := string(jsonData)
	// V1 should NOT have V2-only fields
	if strings.Contains(jsonStr, `"previousTotalAssets"`) {
		t.Errorf("V1 ToJSON() should not contain previousTotalAssets field, got %s", jsonStr)
	}
	if strings.Contains(jsonStr, `"managementFeeShares"`) {
		t.Errorf("V1 ToJSON() should not contain managementFeeShares field, got %s", jsonStr)
	}
}

// --- Morpho VaultV2 adapter / allocation / cap / fee extractor tests ---

func mustV2EventsABI(t *testing.T) *abi.ABI {
	t.Helper()
	a, err := abis.GetVaultV2EventsABI()
	if err != nil {
		t.Fatalf("GetVaultV2EventsABI: %v", err)
	}
	return a
}

// makeV2Log builds a VaultV2 event log: event.ID plus already-encoded indexed
// topics, with the non-indexed args ABI-packed into data.
func makeV2Log(t *testing.T, event abi.Event, indexed []common.Hash, nonIndexed ...any) shared.Log {
	t.Helper()
	data, err := event.Inputs.NonIndexed().Pack(nonIndexed...)
	if err != nil {
		t.Fatalf("packing %s data: %v", event.Name, err)
	}
	topics := make([]string, 0, len(indexed)+1)
	topics = append(topics, event.ID.Hex())
	for _, h := range indexed {
		topics = append(topics, h.Hex())
	}
	return shared.Log{
		Topics:          topics,
		Data:            common.Bytes2Hex(data),
		TransactionHash: "0xf00d",
	}
}

// TestExtractMetaMorphoEvent_TooFewTopicsErrors guards parseTopics: a registered
// event whose log carries fewer topics than its declared indexed params must
// return an error, not panic inside abi.ParseTopicsIntoMap.
func TestExtractMetaMorphoEvent_TooFewTopicsErrors(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor: %v", err)
	}
	v2 := mustV2EventsABI(t)
	ev := v2.Events["AddAdapter"] // AddAdapter(address indexed account): 1 indexed
	// A log with only topic0 (missing the indexed account topic) is malformed.
	log := shared.Log{Topics: []string{ev.ID.Hex()}, Data: "0x", TransactionHash: "0xf00d"}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("parseTopics panicked instead of returning an error: %v", r)
		}
	}()
	if _, err := e.ExtractMetaMorphoEvent(log); err == nil {
		t.Fatal("expected an error for a log with fewer topics than indexed params")
	}
}

// TestExtractVaultAccrueInterest_OptionalV2Fields covers the missing-vs-mistyped
// discrimination on the V2-only accrue fields: absent → left nil, present →
// carried, present-but-wrong-type → error (never a silent NULL).
func TestExtractVaultAccrueInterest_OptionalV2Fields(t *testing.T) {
	t.Run("absent leaves them nil", func(t *testing.T) {
		got, err := extractVaultAccrueInterest(map[string]any{
			"newTotalAssets": big.NewInt(1),
			"feeShares":      big.NewInt(2),
		}, "0xf00d")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.PreviousTotalAssets != nil || got.ManagementFeeShares != nil {
			t.Errorf("absent V2 fields should be nil, got prev=%v mgmt=%v", got.PreviousTotalAssets, got.ManagementFeeShares)
		}
	})
	t.Run("present are carried", func(t *testing.T) {
		got, err := extractVaultAccrueInterest(map[string]any{
			"newTotalAssets":       big.NewInt(1),
			"performanceFeeShares": big.NewInt(2),
			"previousTotalAssets":  big.NewInt(9),
			"managementFeeShares":  big.NewInt(7),
		}, "0xf00d")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.PreviousTotalAssets.Cmp(big.NewInt(9)) != 0 || got.ManagementFeeShares.Cmp(big.NewInt(7)) != 0 {
			t.Errorf("prev=%v mgmt=%v, want 9 and 7", got.PreviousTotalAssets, got.ManagementFeeShares)
		}
	})
	t.Run("wrong type propagates", func(t *testing.T) {
		_, err := extractVaultAccrueInterest(map[string]any{
			"newTotalAssets":      big.NewInt(1),
			"feeShares":           big.NewInt(2),
			"managementFeeShares": "not-a-bigint",
		}, "0xf00d")
		if err == nil {
			t.Fatal("expected an error when managementFeeShares has the wrong type")
		}
	})
}

func addrTopic(a common.Address) common.Hash { return common.BytesToHash(a.Bytes()) }

func hashSlice(hs ...common.Hash) [][32]byte {
	out := make([][32]byte, len(hs))
	for i, h := range hs {
		out[i] = h
	}
	return out
}

func TestExtractMetaMorphoEvent_AdapterEvents(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor: %v", err)
	}
	v2 := mustV2EventsABI(t)
	account := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")

	tests := []struct {
		name     string
		event    abi.Event
		wantType entity.MorphoEventType
	}{
		{"AddAdapter", v2.Events["AddAdapter"], entity.MorphoEventVaultAddAdapter},
		{"RemoveAdapter", v2.Events["RemoveAdapter"], entity.MorphoEventVaultRemoveAdapter},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := makeV2Log(t, tt.event, []common.Hash{addrTopic(account)})
			ev, err := e.ExtractMetaMorphoEvent(log)
			if err != nil {
				t.Fatalf("ExtractMetaMorphoEvent: %v", err)
			}
			if ev == nil {
				t.Fatal("expected typed event, got nil")
			}
			if ev.Type() != tt.wantType {
				t.Errorf("Type() = %s, want %s", ev.Type(), tt.wantType)
			}
			var got common.Address
			switch e := ev.(type) {
			case *AddAdapterEvent:
				got = e.Account
			case *RemoveAdapterEvent:
				got = e.Account
			default:
				t.Fatalf("unexpected type %T", ev)
			}
			if got != account {
				t.Errorf("Account = %s, want %s", got.Hex(), account.Hex())
			}
		})
	}
}

func TestExtractMetaMorphoEvent_Allocation(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor: %v", err)
	}
	v2 := mustV2EventsABI(t)
	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")
	adapter := common.HexToAddress("0x2222222222222222222222222222222222222222")
	id1 := common.HexToHash("0xaa")
	id2 := common.HexToHash("0xbb")

	tests := []struct {
		name     string
		event    abi.Event
		change   *big.Int
		wantType entity.MorphoEventType
	}{
		{"Allocate", v2.Events["Allocate"], big.NewInt(750), entity.MorphoEventVaultAllocate},
		{"Deallocate", v2.Events["Deallocate"], big.NewInt(-750), entity.MorphoEventVaultDeallocate},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := makeV2Log(t, tt.event,
				[]common.Hash{addrTopic(sender), addrTopic(adapter)},
				big.NewInt(123456), hashSlice(id1, id2), tt.change)
			ev, err := e.ExtractMetaMorphoEvent(log)
			if err != nil {
				t.Fatalf("ExtractMetaMorphoEvent: %v", err)
			}
			if ev == nil {
				t.Fatal("expected typed event, got nil")
			}
			if ev.Type() != tt.wantType {
				t.Errorf("Type() = %s, want %s", ev.Type(), tt.wantType)
			}
			var (
				gotSender, gotAdapter common.Address
				gotAssets, gotChange  *big.Int
				gotIDs                []common.Hash
			)
			switch e := ev.(type) {
			case *AllocateEvent:
				gotSender, gotAdapter, gotAssets, gotIDs, gotChange = e.Sender, e.Adapter, e.Assets, e.IDs, e.Change
			case *DeallocateEvent:
				gotSender, gotAdapter, gotAssets, gotIDs, gotChange = e.Sender, e.Adapter, e.Assets, e.IDs, e.Change
			default:
				t.Fatalf("unexpected type %T", ev)
			}
			if gotSender != sender {
				t.Errorf("Sender = %s, want %s", gotSender.Hex(), sender.Hex())
			}
			if gotAdapter != adapter {
				t.Errorf("Adapter = %s, want %s", gotAdapter.Hex(), adapter.Hex())
			}
			if gotAssets.Int64() != 123456 {
				t.Errorf("Assets = %s, want 123456", gotAssets)
			}
			if gotChange.Cmp(tt.change) != 0 {
				t.Errorf("Change = %s, want %s", gotChange, tt.change)
			}
			if len(gotIDs) != 2 || gotIDs[0] != id1 || gotIDs[1] != id2 {
				t.Errorf("IDs = %v, want [%s %s]", gotIDs, id1.Hex(), id2.Hex())
			}
		})
	}
}

func TestExtractMetaMorphoEvent_ForceDeallocate(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor: %v", err)
	}
	v2 := mustV2EventsABI(t)
	sender := common.HexToAddress("0x1111111111111111111111111111111111111111")
	adapter := common.HexToAddress("0x2222222222222222222222222222222222222222")
	onBehalf := common.HexToAddress("0x3333333333333333333333333333333333333333")
	id1 := common.HexToHash("0xcc")

	// ForceDeallocate: indexed(sender, onBehalf); non-indexed(adapter, assets, ids, penaltyAssets).
	log := makeV2Log(t, v2.Events["ForceDeallocate"],
		[]common.Hash{addrTopic(sender), addrTopic(onBehalf)},
		adapter, big.NewInt(9000), hashSlice(id1), big.NewInt(42))
	ev, err := e.ExtractMetaMorphoEvent(log)
	if err != nil {
		t.Fatalf("ExtractMetaMorphoEvent: %v", err)
	}
	fd, ok := ev.(*ForceDeallocateEvent)
	if !ok {
		t.Fatalf("expected *ForceDeallocateEvent, got %T", ev)
	}
	if fd.Sender != sender {
		t.Errorf("Sender = %s, want %s", fd.Sender.Hex(), sender.Hex())
	}
	if fd.Adapter != adapter {
		t.Errorf("Adapter = %s, want %s", fd.Adapter.Hex(), adapter.Hex())
	}
	if fd.OnBehalf != onBehalf {
		t.Errorf("OnBehalf = %s, want %s", fd.OnBehalf.Hex(), onBehalf.Hex())
	}
	if fd.Assets.Int64() != 9000 {
		t.Errorf("Assets = %s, want 9000", fd.Assets)
	}
	if fd.PenaltyAssets.Int64() != 42 {
		t.Errorf("PenaltyAssets = %s, want 42", fd.PenaltyAssets)
	}
	if len(fd.IDs) != 1 || fd.IDs[0] != id1 {
		t.Errorf("IDs = %v, want [%s]", fd.IDs, id1.Hex())
	}
}

func TestExtractMetaMorphoEvent_CapEvents(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor: %v", err)
	}
	v2 := mustV2EventsABI(t)
	sender := common.HexToAddress("0x9999999999999999999999999999999999999999")
	id := common.HexToHash("0x00000000000000000000000000000000000000000000000000000000000000ab")
	idData := []byte{0x01, 0x02, 0x03, 0x04}
	newCap := big.NewInt(1_000_000)

	// assertCap validates the fields common to all four cap events, plus the
	// per-event sender (zero for the Increase* events, which carry none).
	assertCap := func(t *testing.T, gotSender common.Address, gotID common.Hash, gotIDData []byte, gotCap *big.Int, wantSender common.Address) {
		if gotSender != wantSender {
			t.Errorf("Sender = %s, want %s", gotSender.Hex(), wantSender.Hex())
		}
		if gotID != id {
			t.Errorf("ID = %s, want %s", gotID.Hex(), id.Hex())
		}
		if string(gotIDData) != string(idData) {
			t.Errorf("IDData = %x, want %x", gotIDData, idData)
		}
		if gotCap.Cmp(newCap) != 0 {
			t.Errorf("newCap = %s, want %s", gotCap, newCap)
		}
	}

	t.Run("IncreaseAbsoluteCap", func(t *testing.T) {
		log := makeV2Log(t, v2.Events["IncreaseAbsoluteCap"], []common.Hash{id}, idData, newCap)
		ev, err := e.ExtractMetaMorphoEvent(log)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		c, ok := ev.(*IncreaseAbsoluteCapEvent)
		if !ok {
			t.Fatalf("got %T", ev)
		}
		assertCap(t, common.Address{}, c.ID, c.IDData, c.NewAbsoluteCap, common.Address{})
	})

	t.Run("DecreaseAbsoluteCap", func(t *testing.T) {
		log := makeV2Log(t, v2.Events["DecreaseAbsoluteCap"], []common.Hash{addrTopic(sender), id}, idData, newCap)
		ev, err := e.ExtractMetaMorphoEvent(log)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		c, ok := ev.(*DecreaseAbsoluteCapEvent)
		if !ok {
			t.Fatalf("got %T", ev)
		}
		assertCap(t, c.Sender, c.ID, c.IDData, c.NewAbsoluteCap, sender)
	})

	t.Run("IncreaseRelativeCap", func(t *testing.T) {
		log := makeV2Log(t, v2.Events["IncreaseRelativeCap"], []common.Hash{id}, idData, newCap)
		ev, err := e.ExtractMetaMorphoEvent(log)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		c, ok := ev.(*IncreaseRelativeCapEvent)
		if !ok {
			t.Fatalf("got %T", ev)
		}
		assertCap(t, common.Address{}, c.ID, c.IDData, c.NewRelativeCap, common.Address{})
	})

	t.Run("DecreaseRelativeCap", func(t *testing.T) {
		log := makeV2Log(t, v2.Events["DecreaseRelativeCap"], []common.Hash{addrTopic(sender), id}, idData, newCap)
		ev, err := e.ExtractMetaMorphoEvent(log)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		c, ok := ev.(*DecreaseRelativeCapEvent)
		if !ok {
			t.Fatalf("got %T", ev)
		}
		assertCap(t, c.Sender, c.ID, c.IDData, c.NewRelativeCap, sender)
	})
}

func TestExtractMetaMorphoEvent_FeeEvents(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor: %v", err)
	}
	v2 := mustV2EventsABI(t)
	fee := big.NewInt(100_000_000_000_000_000) // 0.1 WAD

	t.Run("SetPerformanceFee", func(t *testing.T) {
		log := makeV2Log(t, v2.Events["SetPerformanceFee"], nil, fee)
		ev, err := e.ExtractMetaMorphoEvent(log)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		pf, ok := ev.(*SetPerformanceFeeEvent)
		if !ok {
			t.Fatalf("got %T", ev)
		}
		if pf.NewPerformanceFee.Cmp(fee) != 0 {
			t.Errorf("NewPerformanceFee = %s, want %s", pf.NewPerformanceFee, fee)
		}
	})

	t.Run("SetManagementFee", func(t *testing.T) {
		log := makeV2Log(t, v2.Events["SetManagementFee"], nil, fee)
		ev, err := e.ExtractMetaMorphoEvent(log)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		mf, ok := ev.(*SetManagementFeeEvent)
		if !ok {
			t.Fatalf("got %T", ev)
		}
		if mf.NewManagementFee.Cmp(fee) != 0 {
			t.Errorf("NewManagementFee = %s, want %s", mf.NewManagementFee, fee)
		}
	})
}

func TestExtractMetaMorphoEvent_FeeRecipientEvents(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor: %v", err)
	}
	v2 := mustV2EventsABI(t)
	recipient := common.HexToAddress("0x5555555555555555555555555555555555555555")

	t.Run("SetPerformanceFeeRecipient", func(t *testing.T) {
		log := makeV2Log(t, v2.Events["SetPerformanceFeeRecipient"], []common.Hash{addrTopic(recipient)})
		ev, err := e.ExtractMetaMorphoEvent(log)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		r, ok := ev.(*SetPerformanceFeeRecipientEvent)
		if !ok {
			t.Fatalf("got %T", ev)
		}
		if r.NewPerformanceFeeRecipient != recipient {
			t.Errorf("recipient = %s, want %s", r.NewPerformanceFeeRecipient.Hex(), recipient.Hex())
		}
	})

	t.Run("SetManagementFeeRecipient", func(t *testing.T) {
		log := makeV2Log(t, v2.Events["SetManagementFeeRecipient"], []common.Hash{addrTopic(recipient)})
		ev, err := e.ExtractMetaMorphoEvent(log)
		if err != nil {
			t.Fatalf("extract: %v", err)
		}
		r, ok := ev.(*SetManagementFeeRecipientEvent)
		if !ok {
			t.Fatalf("got %T", ev)
		}
		if r.NewManagementFeeRecipient != recipient {
			t.Errorf("recipient = %s, want %s", r.NewManagementFeeRecipient.Hex(), recipient.Hex())
		}
	})
}

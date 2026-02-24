package morpho_position_tracker

import (
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
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
	if len(e.metaMorphoSignatures) != 5 {
		t.Errorf("expected 5 MetaMorpho event signatures, got %d", len(e.metaMorphoSignatures))
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

func TestExtractMorphoBlueEvent_AccrueInterest(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	accrueEvent := e.morphoBlueABI.Events["AccrueInterest"]
	marketID := common.HexToHash("0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc")

	// Pack non-indexed data: prevBorrowRate=1000, interest=500, feeShares=10
	data, err := accrueEvent.Inputs.NonIndexed().Pack(
		bigFromStr("1000"),
		bigFromStr("500"),
		bigFromStr("10"),
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

	if result.EventType != entity.MorphoEventAccrueInterest {
		t.Errorf("EventType = %s, want AccrueInterest", result.EventType)
	}
	if result.MarketID != marketID {
		t.Errorf("MarketID mismatch")
	}
	if result.PrevBorrowRate.Int64() != 1000 {
		t.Errorf("PrevBorrowRate = %s, want 1000", result.PrevBorrowRate)
	}
	if result.Interest.Int64() != 500 {
		t.Errorf("Interest = %s, want 500", result.Interest)
	}
	if result.FeeShares.Int64() != 10 {
		t.Errorf("FeeShares = %s, want 10", result.FeeShares)
	}
}

func TestExtractMorphoBlueEvent_SetFee(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	setFeeEvent := e.morphoBlueABI.Events["SetFee"]
	marketID := common.HexToHash("0xb323495f7e4148be5643a4ea4a8221eef163e4bccfdedc2a6f4696baacbc86cc")

	data, err := setFeeEvent.Inputs.NonIndexed().Pack(bigFromStr("100000000000000000"))
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

	if result.EventType != entity.MorphoEventSetFee {
		t.Errorf("EventType = %s, want SetFee", result.EventType)
	}
	if result.NewFee.String() != "100000000000000000" {
		t.Errorf("NewFee = %s, want 100000000000000000", result.NewFee)
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

	data, err := transferEvent.Inputs.NonIndexed().Pack(bigFromStr("5000"))
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

	if result.EventType != entity.MorphoEventVaultTransfer {
		t.Errorf("EventType = %s, want VaultTransfer", result.EventType)
	}
	if result.From != from {
		t.Errorf("From = %s, want %s", result.From.Hex(), from.Hex())
	}
	if result.To != to {
		t.Errorf("To = %s, want %s", result.To.Hex(), to.Hex())
	}
	if result.Value.Int64() != 5000 {
		t.Errorf("Value = %s, want 5000", result.Value)
	}
}

func TestExtractMetaMorphoEvent_AccrueInterest(t *testing.T) {
	e, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() error: %v", err)
	}

	accrueEvent := e.metaMorphoABI.Events["AccrueInterest"]

	data, err := accrueEvent.Inputs.NonIndexed().Pack(bigFromStr("2000000"), bigFromStr("100"))
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

	if result.EventType != entity.MorphoEventVaultAccrueInterest {
		t.Errorf("EventType = %s, want VaultAccrueInterest", result.EventType)
	}
	if result.NewTotalAssets.Int64() != 2000000 {
		t.Errorf("NewTotalAssets = %s, want 2000000", result.NewTotalAssets)
	}
	if result.FeeShares.Int64() != 100 {
		t.Errorf("FeeShares = %s, want 100", result.FeeShares)
	}
}

func TestMorphoBlueEventData_ToJSON(t *testing.T) {
	data := &MorphoBlueEventData{
		EventType:      entity.MorphoEventAccrueInterest,
		TxHash:         "0xabc",
		MarketID:       [32]byte{0x01},
		PrevBorrowRate: bigFromStr("1000"),
		Interest:       bigFromStr("500"),
		FeeShares:      bigFromStr("10"),
	}

	jsonData, err := data.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error: %v", err)
	}
	if len(jsonData) == 0 {
		t.Fatal("ToJSON() returned empty")
	}
}

func TestMetaMorphoEventData_ToJSON(t *testing.T) {
	data := &MetaMorphoEventData{
		EventType: entity.MorphoEventVaultDeposit,
		TxHash:    "0xdef",
		Sender:    common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Owner:     common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Assets:    bigFromStr("1000"),
		Shares:    bigFromStr("900"),
	}

	jsonData, err := data.ToJSON()
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

	// Pack 4 fields: newTotalAssets, interest, feeShares, feeAssets
	data, err := v2Event.Inputs.NonIndexed().Pack(
		bigFromStr("3000000"),
		bigFromStr("50000"),
		bigFromStr("200"),
		bigFromStr("150"),
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

	if result.EventType != entity.MorphoEventVaultAccrueInterest {
		t.Errorf("EventType = %s, want VaultAccrueInterest", result.EventType)
	}
	if result.NewTotalAssets.Int64() != 3000000 {
		t.Errorf("NewTotalAssets = %s, want 3000000", result.NewTotalAssets)
	}
	if result.Interest.Int64() != 50000 {
		t.Errorf("Interest = %s, want 50000", result.Interest)
	}
	if result.FeeShares.Int64() != 200 {
		t.Errorf("FeeShares = %s, want 200", result.FeeShares)
	}
	if result.FeeAssets.Int64() != 150 {
		t.Errorf("FeeAssets = %s, want 150", result.FeeAssets)
	}
}

func TestMetaMorphoEventData_ToJSON_AccrueInterestV2(t *testing.T) {
	data := &MetaMorphoEventData{
		EventType:      entity.MorphoEventVaultAccrueInterest,
		TxHash:         "0xv2test",
		NewTotalAssets: bigFromStr("3000000"),
		FeeShares:      bigFromStr("200"),
		Interest:       bigFromStr("50000"),
		FeeAssets:      bigFromStr("150"),
	}

	jsonData, err := data.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error: %v", err)
	}

	jsonStr := string(jsonData)
	for _, field := range []string{`"interest":"50000"`, `"feeAssets":"150"`, `"newTotalAssets":"3000000"`, `"feeShares":"200"`} {
		if !strings.Contains(jsonStr, field) {
			t.Errorf("ToJSON() missing field %s in %s", field, jsonStr)
		}
	}
}

func TestMetaMorphoEventData_ToJSON_AccrueInterestV1(t *testing.T) {
	data := &MetaMorphoEventData{
		EventType:      entity.MorphoEventVaultAccrueInterest,
		TxHash:         "0xv1test",
		NewTotalAssets: bigFromStr("2000000"),
		FeeShares:      bigFromStr("100"),
	}

	jsonData, err := data.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON() error: %v", err)
	}

	jsonStr := string(jsonData)
	// V1 should NOT have interest or feeAssets
	if strings.Contains(jsonStr, `"interest"`) {
		t.Errorf("V1 ToJSON() should not contain interest field, got %s", jsonStr)
	}
	if strings.Contains(jsonStr, `"feeAssets"`) {
		t.Errorf("V1 ToJSON() should not contain feeAssets field, got %s", jsonStr)
	}
}

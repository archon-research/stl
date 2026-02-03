package sparklend_position_tracker

import (
	"io"
	"log/slog"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func TestEventExtractor_NewEventExtractor(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("NewEventExtractor() failed: %v", err)
	}

	if extractor.positionEventsABI == nil {
		t.Fatal("positionEventsABI is nil after loading")
	}

	// Check all 7 position events are loaded
	expectedEvents := []string{
		"Borrow", "Repay", "Supply", "Withdraw",
		"LiquidationCall", "ReserveUsedAsCollateralEnabled", "ReserveUsedAsCollateralDisabled",
	}

	for _, eventName := range expectedEvents {
		event, ok := extractor.positionEventsABI.Events[eventName]
		if !ok {
			t.Errorf("%s event not found in ABI", eventName)
			continue
		}

		if _, exists := extractor.positionEventSignatures[event.ID]; !exists {
			t.Errorf("%s event signature not registered in positionEventSignatures map", eventName)
		}
	}

	// Verify Borrow event has expected inputs
	borrowEvent := extractor.positionEventsABI.Events["Borrow"]
	expectedInputs := map[string]bool{
		"reserve":          true,
		"user":             true,
		"onBehalfOf":       true,
		"amount":           true,
		"interestRateMode": true,
		"borrowRate":       true,
		"referralCode":     true,
	}

	for _, input := range borrowEvent.Inputs {
		if !expectedInputs[input.Name] {
			t.Errorf("unexpected input in Borrow event: %s", input.Name)
		}
		delete(expectedInputs, input.Name)
	}

	if len(expectedInputs) > 0 {
		t.Errorf("missing expected inputs in Borrow event: %v", expectedInputs)
	}

	// Check indexed parameters
	indexedParams := []string{"reserve", "onBehalfOf", "referralCode"}
	for _, input := range borrowEvent.Inputs {
		for _, expectedIndexed := range indexedParams {
			if input.Name == expectedIndexed && !input.Indexed {
				t.Errorf("expected %s to be indexed", input.Name)
			}
		}
	}
}

func TestEventExtractor_IsPositionEvent(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	tests := []struct {
		name     string
		log      Log
		expected bool
	}{
		{
			name: "valid borrow event",
			log: Log{
				Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
				Topics: []string{
					"0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
				},
			},
			expected: true,
		},
		{
			name: "valid supply event",
			log: Log{
				Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
				Topics: []string{
					"0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
				},
			},
			expected: true,
		},
		{
			name: "non-tracked event",
			log: Log{
				Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
				Topics: []string{
					"0x1111111111111111111111111111111111111111111111111111111111111111",
				},
			},
			expected: false,
		},
		{
			name: "empty topics",
			log: Log{
				Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
				Topics:  []string{},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractor.IsPositionEvent(tt.log)
			if result != tt.expected {
				t.Errorf("IsPositionEvent() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestEventExtractor_ExtractEventData_Borrow(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	log := Log{
		Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
		Topics: []string{
			"0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
			"0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
			"0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f0bEb0",
			"0x0000000000000000000000000000000000000000000000000000000000000000",
		},
		Data:             "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f0bEb00000000000000000000000000000000000000000000000056bc75e2d63100000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000002b5e3af16b18800000000000000000000000000000000000000000000000000000000000000000000",
		TransactionHash:  "0xabc123",
		BlockNumber:      "0x123456",
		TransactionIndex: "0x0",
		LogIndex:         "0x0",
	}

	eventData, err := extractor.ExtractEventData(log)
	if err != nil {
		t.Fatalf("ExtractEventData failed: %v", err)
	}

	if eventData.EventType != EventBorrow {
		t.Errorf("EventType = %v, want %v", eventData.EventType, EventBorrow)
	}

	if eventData.Reserve == (common.Address{}) {
		t.Error("Reserve address should not be empty")
	}

	expectedReserve := common.HexToAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
	if eventData.Reserve != expectedReserve {
		t.Errorf("Reserve = %v, want %v", eventData.Reserve.Hex(), expectedReserve.Hex())
	}

	expectedUser := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
	if eventData.User != expectedUser {
		t.Errorf("User = %v, want %v", eventData.User.Hex(), expectedUser.Hex())
	}

	if eventData.Amount == nil || eventData.Amount.Cmp(big.NewInt(0)) == 0 {
		t.Error("Amount should not be nil or zero")
	}

	if eventData.TxHash != "0xabc123" {
		t.Errorf("TxHash = %v, want %v", eventData.TxHash, "0xabc123")
	}
}

func TestService_ConvertToDecimalAdjusted(t *testing.T) {
	service := &Service{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	tests := []struct {
		name     string
		amount   *big.Int
		decimals int
		want     string
	}{
		{
			name:     "1 USDC (6 decimals)",
			amount:   big.NewInt(1000000),
			decimals: 6,
			want:     "1",
		},
		{
			name:     "1.5 USDC (6 decimals)",
			amount:   big.NewInt(1500000),
			decimals: 6,
			want:     "1.500000",
		},
		{
			name:     "1 ETH (18 decimals)",
			amount:   new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil),
			decimals: 18,
			want:     "1",
		},
		{
			name:     "0.5 ETH (18 decimals)",
			amount:   new(big.Int).Div(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil), big.NewInt(2)),
			decimals: 18,
			want:     "0.500000000000000000",
		},
		{
			name:     "no decimals",
			amount:   big.NewInt(12345),
			decimals: 0,
			want:     "12345",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := service.convertToDecimalAdjusted(tt.amount, tt.decimals)
			if result != tt.want {
				t.Errorf("convertToDecimalAdjusted() = %v, want %v", result, tt.want)
			}
		})
	}
}

func TestEventExtractor_ProcessReceipt_MultipleEventTypes(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	tests := []struct {
		name                 string
		logsPerReceipt       int
		relevantEvents       int
		expectedRelevantLogs int
	}{
		{
			name:                 "all borrow events",
			logsPerReceipt:       10,
			relevantEvents:       10,
			expectedRelevantLogs: 10,
		},
		{
			name:                 "half relevant events",
			logsPerReceipt:       10,
			relevantEvents:       5,
			expectedRelevantLogs: 5,
		},
		{
			name:                 "few relevant events in many logs",
			logsPerReceipt:       100,
			relevantEvents:       5,
			expectedRelevantLogs: 5,
		},
		{
			name:                 "no relevant events",
			logsPerReceipt:       10,
			relevantEvents:       0,
			expectedRelevantLogs: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := make([]Log, tt.logsPerReceipt)

			for i := 0; i < tt.logsPerReceipt; i++ {
				var topics []string
				if i < tt.relevantEvents {
					topics = []string{
						"0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
						"0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
						"0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f0bEb0",
						"0x0000000000000000000000000000000000000000000000000000000000000000",
					}
				} else {
					topics = []string{
						"0x1111111111111111111111111111111111111111111111111111111111111111",
					}
				}

				logs[i] = Log{
					Address:          "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
					Topics:           topics,
					Data:             "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f0bEb00000000000000000000000000000000000000000000000056bc75e2d63100000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000002b5e3af16b18800000000000000000000000000000000000000000000000000000000000000000000",
					BlockNumber:      "0x123456",
					TransactionHash:  "0xabc123",
					TransactionIndex: "0x0",
					LogIndex:         "0x0",
				}
			}

			receipt := TransactionReceipt{
				Type:              "0x2",
				Status:            "0x1",
				TransactionHash:   "0xabc123",
				TransactionIndex:  "0x0",
				BlockHash:         "0xdef456",
				BlockNumber:       "0x123456",
				CumulativeGasUsed: "0x123456",
				GasUsed:           "0x5208",
				EffectiveGasPrice: "0x3b9aca00",
				From:              "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0",
				To:                "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
				Logs:              logs,
			}

			relevantCount := 0
			for _, log := range receipt.Logs {
				if !extractor.IsPositionEvent(log) {
					continue
				}

				_, err := extractor.ExtractEventData(log)
				if err != nil {
					t.Fatalf("ExtractEventData failed: %v", err)
				}
				relevantCount++
			}

			if relevantCount != tt.expectedRelevantLogs {
				t.Errorf("processed %d relevant events, want %d", relevantCount, tt.expectedRelevantLogs)
			}
		})
	}
}

func TestEventTypeConstants(t *testing.T) {
	// Verify event type constants have expected values
	tests := []struct {
		eventType entity.EventType
		expected  string
	}{
		{EventBorrow, "Borrow"},
		{EventRepay, "Repay"},
		{EventSupply, "Supply"},
		{EventWithdraw, "Withdraw"},
		{EventLiquidationCall, "LiquidationCall"},
		{EventReserveUsedAsCollateralEnabled, "ReserveUsedAsCollateralEnabled"},
		{EventReserveUsedAsCollateralDisabled, "ReserveUsedAsCollateralDisabled"},
	}

	for _, tt := range tests {
		if string(tt.eventType) != tt.expected {
			t.Errorf("EventType constant %v = %v, want %v", tt.eventType, string(tt.eventType), tt.expected)
		}
	}
}

// Event signatures for all 7 SparkLend position-changing events
const (
	sigBorrow                          = "0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0"
	sigRepay                           = "0xa534c8dbe71f871f9f3530e97a74601fea17b426cae02e1c5aee42c96c784051"
	sigSupply                          = "0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61"
	sigWithdraw                        = "0x3115d1449a7b732c986cba18244e897a450f61e1bb8d589cd2e69e6c8924f9f7"
	sigLiquidationCall                 = "0xe413a321e8681d831f4dbccbca790d2952b56f977908e45be37335533e005286"
	sigReserveUsedAsCollateralEnabled  = "0x00058a56ea94653cdf4f152d227ace22d4c00ad99e2a43f58cb7d9e3feb295f2"
	sigReserveUsedAsCollateralDisabled = "0x44c58d81365b66dd4b1a7f36c25aa97b8c71c361ee4937adc1a00000227db5dd"
)

// Test addresses used across tests
const (
	testWETH       = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2" // WETH
	testUSDC       = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48" // USDC
	testUser1      = "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0"
	testUser2      = "0x8ba1f109551bD432803012645Hac136c9A4567d"
	testLiquidator = "0xDef1C0ded9bec7F1a1670819833240f027b25EfF"
	testPool       = "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2" // Aave V3 Pool
)

// Helper to pad address to 32 bytes for indexed topic
func padAddress(addr string) string {
	// Remove 0x prefix, pad to 64 chars (32 bytes)
	clean := addr[2:]
	return "0x000000000000000000000000" + clean
}

func TestEventExtractor_ExtractEventData_Repay(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	// Repay event: reserve (indexed), user (indexed), repayer (indexed), amount, useATokens
	// Data contains: amount (uint256), useATokens (bool)
	log := Log{
		Address: testPool,
		Topics: []string{
			sigRepay,
			padAddress(testWETH),  // reserve (indexed)
			padAddress(testUser1), // user (indexed) - the debt holder
			padAddress(testUser2), // repayer (indexed) - who paid
		},
		// amount = 1 ETH (1e18), useATokens = false
		Data:            "0x0000000000000000000000000000000000000000000000000de0b6b3a76400000000000000000000000000000000000000000000000000000000000000000000",
		TransactionHash: "0xrepay123",
		BlockNumber:     "0x100",
		LogIndex:        "0x1",
	}

	eventData, err := extractor.ExtractEventData(log)
	if err != nil {
		t.Fatalf("ExtractEventData failed: %v", err)
	}

	if eventData.EventType != EventRepay {
		t.Errorf("EventType = %v, want %v", eventData.EventType, EventRepay)
	}

	expectedReserve := common.HexToAddress(testWETH)
	if eventData.Reserve != expectedReserve {
		t.Errorf("Reserve = %v, want %v", eventData.Reserve.Hex(), expectedReserve.Hex())
	}

	// User should be the debt holder (user field), not the repayer
	expectedUser := common.HexToAddress(testUser1)
	if eventData.User != expectedUser {
		t.Errorf("User = %v, want %v", eventData.User.Hex(), expectedUser.Hex())
	}

	// Amount should be 1 ETH
	expectedAmount := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	if eventData.Amount.Cmp(expectedAmount) != 0 {
		t.Errorf("Amount = %v, want %v", eventData.Amount, expectedAmount)
	}

	if eventData.TxHash != "0xrepay123" {
		t.Errorf("TxHash = %v, want %v", eventData.TxHash, "0xrepay123")
	}
}

func TestEventExtractor_ExtractEventData_Supply(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	// Supply event: reserve (indexed), user, onBehalfOf (indexed), amount, referralCode (indexed)
	// Data contains: user (address), amount (uint256)
	log := Log{
		Address: testPool,
		Topics: []string{
			sigSupply,
			padAddress(testUSDC),  // reserve (indexed)
			padAddress(testUser1), // onBehalfOf (indexed)
			"0x0000000000000000000000000000000000000000000000000000000000000000", // referralCode (indexed)
		},
		// user = 0x8ba1f109551bD432803012645Ac136c22bCe14e7, amount = 1000 USDC (1000e6)
		Data:            "0x0000000000000000000000008ba1f109551bd432803012645ac136c22bce14e7000000000000000000000000000000000000000000000000000000003b9aca00",
		TransactionHash: "0xsupply456",
		BlockNumber:     "0x200",
		LogIndex:        "0x2",
	}

	eventData, err := extractor.ExtractEventData(log)
	if err != nil {
		t.Fatalf("ExtractEventData failed: %v", err)
	}

	if eventData.EventType != EventSupply {
		t.Errorf("EventType = %v, want %v", eventData.EventType, EventSupply)
	}

	expectedReserve := common.HexToAddress(testUSDC)
	if eventData.Reserve != expectedReserve {
		t.Errorf("Reserve = %v, want %v", eventData.Reserve.Hex(), expectedReserve.Hex())
	}

	// User should be onBehalfOf (the recipient of the supply)
	expectedUser := common.HexToAddress(testUser1)
	if eventData.User != expectedUser {
		t.Errorf("User = %v, want %v", eventData.User.Hex(), expectedUser.Hex())
	}

	// Amount should be 1000 USDC (1000 * 10^6 = 1000000000)
	expectedAmount := big.NewInt(1000000000)
	if eventData.Amount.Cmp(expectedAmount) != 0 {
		t.Errorf("Amount = %v, want %v", eventData.Amount, expectedAmount)
	}

	if eventData.TxHash != "0xsupply456" {
		t.Errorf("TxHash = %v, want %v", eventData.TxHash, "0xsupply456")
	}
}

func TestEventExtractor_ExtractEventData_Withdraw(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	// Withdraw event: reserve (indexed), user (indexed), to (indexed), amount
	// Data contains: amount (uint256)
	log := Log{
		Address: testPool,
		Topics: []string{
			sigWithdraw,
			padAddress(testWETH),  // reserve (indexed)
			padAddress(testUser1), // user (indexed) - who is withdrawing
			padAddress(testUser2), // to (indexed) - recipient
		},
		// amount = 0.5 ETH (5e17)
		Data:            "0x00000000000000000000000000000000000000000000000006f05b59d3b20000",
		TransactionHash: "0xwithdraw789",
		BlockNumber:     "0x300",
		LogIndex:        "0x3",
	}

	eventData, err := extractor.ExtractEventData(log)
	if err != nil {
		t.Fatalf("ExtractEventData failed: %v", err)
	}

	if eventData.EventType != EventWithdraw {
		t.Errorf("EventType = %v, want %v", eventData.EventType, EventWithdraw)
	}

	expectedReserve := common.HexToAddress(testWETH)
	if eventData.Reserve != expectedReserve {
		t.Errorf("Reserve = %v, want %v", eventData.Reserve.Hex(), expectedReserve.Hex())
	}

	// User should be the withdrawer
	expectedUser := common.HexToAddress(testUser1)
	if eventData.User != expectedUser {
		t.Errorf("User = %v, want %v", eventData.User.Hex(), expectedUser.Hex())
	}

	// Amount should be 0.5 ETH (5 * 10^17)
	expectedAmount := big.NewInt(500000000000000000)
	if eventData.Amount.Cmp(expectedAmount) != 0 {
		t.Errorf("Amount = %v, want %v", eventData.Amount, expectedAmount)
	}

	if eventData.TxHash != "0xwithdraw789" {
		t.Errorf("TxHash = %v, want %v", eventData.TxHash, "0xwithdraw789")
	}
}

func TestEventExtractor_ExtractEventData_LiquidationCall(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	// LiquidationCall event: collateralAsset (indexed), debtAsset (indexed), user (indexed),
	//                       debtToCover, liquidatedCollateralAmount, liquidator, receiveAToken
	// Data contains: debtToCover (uint256), liquidatedCollateralAmount (uint256), liquidator (address), receiveAToken (bool)
	log := Log{
		Address: testPool,
		Topics: []string{
			sigLiquidationCall,
			padAddress(testWETH),  // collateralAsset (indexed)
			padAddress(testUSDC),  // debtAsset (indexed)
			padAddress(testUser1), // user (indexed) - borrower being liquidated
		},
		// debtToCover = 500 USDC (500e6), liquidatedCollateralAmount = 0.3 ETH (3e17),
		// liquidator = testLiquidator, receiveAToken = false
		Data:            "0x000000000000000000000000000000000000000000000000000000001dcd65000000000000000000000000000000000000000000000000000429d069189e0000000000000000000000000000def1c0ded9bec7f1a1670819833240f027b25eff0000000000000000000000000000000000000000000000000000000000000000",
		TransactionHash: "0xliquidation101",
		BlockNumber:     "0x400",
		LogIndex:        "0x4",
	}

	eventData, err := extractor.ExtractEventData(log)
	if err != nil {
		t.Fatalf("ExtractEventData failed: %v", err)
	}

	if eventData.EventType != EventLiquidationCall {
		t.Errorf("EventType = %v, want %v", eventData.EventType, EventLiquidationCall)
	}

	// User should be the borrower being liquidated
	expectedUser := common.HexToAddress(testUser1)
	if eventData.User != expectedUser {
		t.Errorf("User = %v, want %v", eventData.User.Hex(), expectedUser.Hex())
	}

	// Liquidator should be extracted from data
	expectedLiquidator := common.HexToAddress(testLiquidator)
	if eventData.Liquidator != expectedLiquidator {
		t.Errorf("Liquidator = %v, want %v", eventData.Liquidator.Hex(), expectedLiquidator.Hex())
	}

	// CollateralAsset should be WETH
	expectedCollateral := common.HexToAddress(testWETH)
	if eventData.CollateralAsset != expectedCollateral {
		t.Errorf("CollateralAsset = %v, want %v", eventData.CollateralAsset.Hex(), expectedCollateral.Hex())
	}

	// DebtAsset should be USDC
	expectedDebt := common.HexToAddress(testUSDC)
	if eventData.DebtAsset != expectedDebt {
		t.Errorf("DebtAsset = %v, want %v", eventData.DebtAsset.Hex(), expectedDebt.Hex())
	}

	// DebtToCover should be 500 USDC (500 * 10^6)
	expectedDebtToCover := big.NewInt(500000000)
	if eventData.DebtToCover.Cmp(expectedDebtToCover) != 0 {
		t.Errorf("DebtToCover = %v, want %v", eventData.DebtToCover, expectedDebtToCover)
	}

	// LiquidatedCollateralAmount should be 0.3 ETH (3 * 10^17)
	expectedLiquidatedAmount := big.NewInt(300000000000000000)
	if eventData.LiquidatedCollateralAmount.Cmp(expectedLiquidatedAmount) != 0 {
		t.Errorf("LiquidatedCollateralAmount = %v, want %v", eventData.LiquidatedCollateralAmount, expectedLiquidatedAmount)
	}

	if eventData.TxHash != "0xliquidation101" {
		t.Errorf("TxHash = %v, want %v", eventData.TxHash, "0xliquidation101")
	}
}

func TestEventExtractor_ExtractEventData_ReserveUsedAsCollateralEnabled(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	// ReserveUsedAsCollateralEnabled event: reserve (indexed), user (indexed)
	// No data - all fields are indexed
	log := Log{
		Address: testPool,
		Topics: []string{
			sigReserveUsedAsCollateralEnabled,
			padAddress(testWETH),  // reserve (indexed)
			padAddress(testUser1), // user (indexed)
		},
		Data:            "0x",
		TransactionHash: "0xcollateral_enabled_202",
		BlockNumber:     "0x500",
		LogIndex:        "0x5",
	}

	eventData, err := extractor.ExtractEventData(log)
	if err != nil {
		t.Fatalf("ExtractEventData failed: %v", err)
	}

	if eventData.EventType != EventReserveUsedAsCollateralEnabled {
		t.Errorf("EventType = %v, want %v", eventData.EventType, EventReserveUsedAsCollateralEnabled)
	}

	expectedReserve := common.HexToAddress(testWETH)
	if eventData.Reserve != expectedReserve {
		t.Errorf("Reserve = %v, want %v", eventData.Reserve.Hex(), expectedReserve.Hex())
	}

	expectedUser := common.HexToAddress(testUser1)
	if eventData.User != expectedUser {
		t.Errorf("User = %v, want %v", eventData.User.Hex(), expectedUser.Hex())
	}

	// CollateralEnabled should be true for this event
	if !eventData.CollateralEnabled {
		t.Error("CollateralEnabled should be true for ReserveUsedAsCollateralEnabled event")
	}

	if eventData.TxHash != "0xcollateral_enabled_202" {
		t.Errorf("TxHash = %v, want %v", eventData.TxHash, "0xcollateral_enabled_202")
	}
}

func TestEventExtractor_ExtractEventData_ReserveUsedAsCollateralDisabled(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	// ReserveUsedAsCollateralDisabled event: reserve (indexed), user (indexed)
	// No data - all fields are indexed
	log := Log{
		Address: testPool,
		Topics: []string{
			sigReserveUsedAsCollateralDisabled,
			padAddress(testWETH),  // reserve (indexed)
			padAddress(testUser1), // user (indexed)
		},
		Data:            "0x",
		TransactionHash: "0xcollateral_disabled_303",
		BlockNumber:     "0x600",
		LogIndex:        "0x6",
	}

	eventData, err := extractor.ExtractEventData(log)
	if err != nil {
		t.Fatalf("ExtractEventData failed: %v", err)
	}

	if eventData.EventType != EventReserveUsedAsCollateralDisabled {
		t.Errorf("EventType = %v, want %v", eventData.EventType, EventReserveUsedAsCollateralDisabled)
	}

	expectedReserve := common.HexToAddress(testWETH)
	if eventData.Reserve != expectedReserve {
		t.Errorf("Reserve = %v, want %v", eventData.Reserve.Hex(), expectedReserve.Hex())
	}

	expectedUser := common.HexToAddress(testUser1)
	if eventData.User != expectedUser {
		t.Errorf("User = %v, want %v", eventData.User.Hex(), expectedUser.Hex())
	}

	// CollateralEnabled should be false for this event
	if eventData.CollateralEnabled {
		t.Error("CollateralEnabled should be false for ReserveUsedAsCollateralDisabled event")
	}

	if eventData.TxHash != "0xcollateral_disabled_303" {
		t.Errorf("TxHash = %v, want %v", eventData.TxHash, "0xcollateral_disabled_303")
	}
}

func TestEventExtractor_IsPositionEvent_AllEventTypes(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	tests := []struct {
		name      string
		signature string
		expected  bool
	}{
		{"Borrow", sigBorrow, true},
		{"Repay", sigRepay, true},
		{"Supply", sigSupply, true},
		{"Withdraw", sigWithdraw, true},
		{"LiquidationCall", sigLiquidationCall, true},
		{"ReserveUsedAsCollateralEnabled", sigReserveUsedAsCollateralEnabled, true},
		{"ReserveUsedAsCollateralDisabled", sigReserveUsedAsCollateralDisabled, true},
		{"Unknown event", "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", false},
		{"Transfer (ERC20)", "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", false},
		{"Approval (ERC20)", "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := Log{
				Address: testPool,
				Topics:  []string{tt.signature},
			}

			result := extractor.IsPositionEvent(log)
			if result != tt.expected {
				t.Errorf("IsPositionEvent(%s) = %v, want %v", tt.name, result, tt.expected)
			}
		})
	}
}

func TestEventExtractor_ExtractEventData_ErrorCases(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	tests := []struct {
		name        string
		log         Log
		expectError bool
		errorMsg    string
	}{
		{
			name: "empty topics",
			log: Log{
				Address: testPool,
				Topics:  []string{},
				Data:    "0x",
			},
			expectError: true,
			errorMsg:    "no topics",
		},
		{
			name: "unknown event signature",
			log: Log{
				Address: testPool,
				Topics:  []string{"0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"},
				Data:    "0x",
			},
			expectError: true,
			errorMsg:    "not a tracked position event",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := extractor.ExtractEventData(tt.log)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errorMsg)
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
			}
		})
	}
}

package aavelike_position_tracker

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/aavelike"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
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
		log      shared.Log
		expected bool
	}{
		{
			name: "valid borrow event",
			log: shared.Log{
				Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
				Topics: []string{
					"0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
				},
			},
			expected: true,
		},
		{
			name: "valid supply event",
			log: shared.Log{
				Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
				Topics: []string{
					"0x2b627736bca15cd5381dcf80b0bf11fd197d01a037c52b927a881a10fb73ba61",
				},
			},
			expected: true,
		},
		{
			name: "non-tracked event",
			log: shared.Log{
				Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
				Topics: []string{
					"0x1111111111111111111111111111111111111111111111111111111111111111",
				},
			},
			expected: false,
		},
		{
			name: "empty topics",
			log: shared.Log{
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

	log := shared.Log{
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
			logs := make([]shared.Log, tt.logsPerReceipt)

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

				logs[i] = shared.Log{
					Address:          "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
					Topics:           topics,
					Data:             "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f0bEb00000000000000000000000000000000000000000000000056bc75e2d63100000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000002b5e3af16b18800000000000000000000000000000000000000000000000000000000000000000000",
					BlockNumber:      "0x123456",
					TransactionHash:  "0xabc123",
					TransactionIndex: "0x0",
					LogIndex:         "0x0",
				}
			}

			receipt := shared.TransactionReceipt{
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
	testUser2      = "0x8ba1f109551bD432803012645Aac136c9A4567d0"
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
	log := shared.Log{
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
	log := shared.Log{
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
	log := shared.Log{
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
	log := shared.Log{
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
	log := shared.Log{
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
	log := shared.Log{
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
			log := shared.Log{
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
		log         shared.Log
		expectError bool
		errorMsg    string
	}{
		{
			name: "empty topics",
			log: shared.Log{
				Address: testPool,
				Topics:  []string{},
				Data:    "0x",
			},
			expectError: true,
			errorMsg:    "no topics",
		},
		{
			name: "unknown event signature",
			log: shared.Log{
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

func TestEventExtractor_ExtractReserveEventData(t *testing.T) {
	extractor, err := NewEventExtractor()
	if err != nil {
		t.Fatalf("failed to create extractor: %v", err)
	}

	// ReserveDataUpdated event signature
	// Calculated from: ReserveDataUpdated(address,uint256,uint256,uint256,uint256,uint256)
	reserveDataUpdatedSig := "0x804c9b842b2748a22bb64b345453a3de7ca54a6ca45ce00d415894979e22897a"
	testReserve := "0x6B175474E89094C44Da98b954EedeAC495271d0F" // DAI address
	testTxHash := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

	tests := []struct {
		name        string
		log         shared.Log
		expectError bool
		errorMsg    string
		wantReserve string
		wantTxHash  string
	}{
		{
			name: "valid ReserveDataUpdated event",
			log: shared.Log{
				Address:         testPool,
				Topics:          []string{reserveDataUpdatedSig, testReserve},
				Data:            "0x",
				TransactionHash: testTxHash,
			},
			expectError: false,
			wantReserve: testReserve,
			wantTxHash:  testTxHash,
		},
		{
			name: "missing reserve topic",
			log: shared.Log{
				Address:         testPool,
				Topics:          []string{reserveDataUpdatedSig}, // Only 1 topic
				Data:            "0x",
				TransactionHash: testTxHash,
			},
			expectError: true,
			errorMsg:    "requires at least 2 topics",
		},
		{
			name: "wrong event signature",
			log: shared.Log{
				Address:         testPool,
				Topics:          []string{"0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef", testReserve},
				Data:            "0x",
				TransactionHash: testTxHash,
			},
			expectError: true,
			errorMsg:    "not a ReserveDataUpdated event",
		},
		{
			name: "empty topics",
			log: shared.Log{
				Address:         testPool,
				Topics:          []string{},
				Data:            "0x",
				TransactionHash: testTxHash,
			},
			expectError: true,
			errorMsg:    "requires at least 2 topics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := extractor.ExtractReserveEventData(tt.log)

			if tt.expectError {
				if err == nil {
					t.Errorf("expected error containing %q, got nil", tt.errorMsg)
				} else if !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("expected error containing %q, got %q", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}

			if result == nil {
				t.Error("expected result, got nil")
				return
			}

			if result.Reserve.Hex() != tt.wantReserve {
				t.Errorf("Reserve = %s, want %s", result.Reserve.Hex(), tt.wantReserve)
			}

			if result.TxHash != tt.wantTxHash {
				t.Errorf("TxHash = %s, want %s", result.TxHash, tt.wantTxHash)
			}
		})
	}
}

func TestFetchAndProcessReceipts_CacheMiss_ReturnsError(t *testing.T) {
	cache := testutil.NewMockBlockCache()
	svc := &Service{
		cacheReader: cache,
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	// Don't store anything in cache — GetReceipts returns nil, nil.
	err := svc.fetchAndProcessReceipts(context.Background(), outbound.BlockEvent{
		ChainID: 1, BlockNumber: 99999, Version: 0,
	})
	if err == nil {
		t.Fatal("expected error for cache miss, got nil")
	}
}

func TestService_IsKnownProtocol_FilterLogic(t *testing.T) {
	tests := []struct {
		name       string
		chainID    int64
		address    string
		expectSkip bool
	}{
		{
			name:       "Aave V3 - should process",
			chainID:    1,
			address:    "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
			expectSkip: false,
		},
		{
			name:       "Sparklend - should process",
			chainID:    1,
			address:    "0xC13e21B648A5Ee794902342038FF3aDAB66BE987",
			expectSkip: false,
		},
		{
			name:       "Aave V2 - should process",
			chainID:    1,
			address:    "0x7d2768dE32b0b80b7a3454c06BdAc94A69DDc7A9",
			expectSkip: false,
		},
		{
			name:       "Aave V3 Avalanche - should process",
			chainID:    43114,
			address:    "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
			expectSkip: false,
		},
		{
			name:       "Aave V3 Avalanche on wrong chain - should skip",
			chainID:    1,
			address:    "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
			expectSkip: true,
		},
		{
			name:       "RedemptionIdle - should skip",
			chainID:    1,
			address:    "0x4c21B7577C8FE8b0B0669165ee7C8f67fa1454Cf",
			expectSkip: true,
		},
		{
			name:       "Random unknown - should skip",
			chainID:    1,
			address:    "0x1234567890123456789012345678901234567890",
			expectSkip: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			addr := common.HexToAddress(tt.address)
			skip := !blockchain.IsKnownProtocol(tt.chainID, addr)

			if skip != tt.expectSkip {
				t.Errorf("IsKnownProtocol(%d, %s) = %v, want skip=%v", tt.chainID, tt.address, !skip, tt.expectSkip)
			}
		})
	}
}

// --- Position snapshot tests (previously in position_snapshot_test.go) ---

type mockPositionRepository struct {
	saveBorrowerCalls            []saveBorrowerCall
	saveBorrowerCollateralsCalls [][]*entity.BorrowerCollateral

	SaveBorrowerFn            func(ctx context.Context, tx pgx.Tx, b *entity.Borrower) error
	SaveBorrowerCollateralFn  func(ctx context.Context, tx pgx.Tx, bc *entity.BorrowerCollateral) error
	SaveBorrowerCollateralsFn func(ctx context.Context, tx pgx.Tx, collaterals []*entity.BorrowerCollateral) error
}

type saveBorrowerCall struct {
	Amount    *big.Int
	Change    *big.Int
	EventType string
}

func (m *mockPositionRepository) SaveBorrower(ctx context.Context, tx pgx.Tx, b *entity.Borrower) error {
	m.saveBorrowerCalls = append(m.saveBorrowerCalls, saveBorrowerCall{
		Amount:    b.Amount,
		Change:    b.Change,
		EventType: string(b.EventType),
	})
	if m.SaveBorrowerFn != nil {
		return m.SaveBorrowerFn(ctx, tx, b)
	}
	return nil
}

func (m *mockPositionRepository) SaveBorrowerCollateral(ctx context.Context, tx pgx.Tx, bc *entity.BorrowerCollateral) error {
	if m.SaveBorrowerCollateralFn != nil {
		return m.SaveBorrowerCollateralFn(ctx, tx, bc)
	}
	return nil
}

func (m *mockPositionRepository) SaveBorrowers(ctx context.Context, tx pgx.Tx, borrowers []*entity.Borrower) error {
	for _, b := range borrowers {
		m.saveBorrowerCalls = append(m.saveBorrowerCalls, saveBorrowerCall{
			Amount:    b.Amount,
			Change:    b.Change,
			EventType: string(b.EventType),
		})
	}
	return nil
}

func (m *mockPositionRepository) SaveBorrowerCollaterals(ctx context.Context, tx pgx.Tx, collaterals []*entity.BorrowerCollateral) error {
	m.saveBorrowerCollateralsCalls = append(m.saveBorrowerCollateralsCalls, collaterals)
	if m.SaveBorrowerCollateralsFn != nil {
		return m.SaveBorrowerCollateralsFn(ctx, tx, collaterals)
	}
	return nil
}

func (m *mockPositionRepository) UpsertBorrowers(ctx context.Context, borrowers []*entity.Borrower) error {
	return nil
}

func (m *mockPositionRepository) UpsertBorrowerCollateral(ctx context.Context, collateral []*entity.BorrowerCollateral) error {
	return nil
}

type sparkLendUserReserve struct {
	UnderlyingAsset                common.Address
	ScaledATokenBalance            *big.Int
	UsageAsCollateralEnabledOnUser bool
	ScaledVariableDebt             *big.Int
}

func TestExtractUserPositionData_ReturnsDebtAndCollateral(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(24033627)

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
	wethAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdcAddress := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	debtUSDC := new(big.Int).Mul(big.NewInt(1500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
	zero := big.NewInt(0)

	ethClient, cleanup := newEthClientReturningUserReserves(t, []sparkLendUserReserve{
		{
			UnderlyingAsset:                wethAddress,
			ScaledATokenBalance:            oneETH,
			UsageAsCollateralEnabledOnUser: true,
			ScaledVariableDebt:             zero,
		},
		{
			UnderlyingAsset:                usdcAddress,
			ScaledATokenBalance:            zero,
			UsageAsCollateralEnabledOnUser: false,
			ScaledVariableDebt:             debtUSDC,
		},
	})
	defer cleanup()

	erc20ABI := mustERC20ABI(t)
	userReserveDataABI := mustUserReserveDataABI(t)

	multicaller := testutil.NewMockMulticaller()
	multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			methodID := hex.EncodeToString(call.CallData[:4])
			switch {
			case call.Target == wethAddress || call.Target == usdcAddress:
				returnData := packERC20MetadataResponse(t, erc20ABI, call.Target, methodID)
				results[i] = outbound.Result{Success: true, ReturnData: returnData}
			case methodID == methodIDHex(userReserveDataABI, "getUserReserveData"):
				asset := unpackUserReserveAsset(t, userReserveDataABI, call.CallData)
				returnData := packUserReserveDataResponse(t, userReserveDataABI, asset)
				results[i] = outbound.Result{Success: true, ReturnData: returnData}
			default:
				t.Fatalf("unexpected multicall target %s method %s", call.Target.Hex(), methodID)
			}
		}
		return results, nil
	}

	svc := newServiceWithCachedBlockchainService(t, ethClient, multicaller, chainID, protocolAddress)

	collaterals, debts, err := svc.extractUserPositionData(context.Background(), userAddress, protocolAddress, chainID, blockNumber, "0xabc")
	if err != nil {
		t.Fatalf("extractUserPositionData() failed: %v", err)
	}

	if len(collaterals) != 1 {
		t.Fatalf("expected 1 collateral, got %d", len(collaterals))
	}
	if collaterals[0].Asset != wethAddress {
		t.Errorf("collateral asset = %s, want %s", collaterals[0].Asset.Hex(), wethAddress.Hex())
	}
	if collaterals[0].ActualBalance.Cmp(oneETH) != 0 {
		t.Errorf("collateral balance = %s, want %s", collaterals[0].ActualBalance.String(), oneETH.String())
	}
	if !collaterals[0].CollateralEnabled {
		t.Error("expected collateral to be enabled")
	}

	if len(debts) != 1 {
		t.Fatalf("expected 1 debt position, got %d", len(debts))
	}
	if debts[0].Asset != usdcAddress {
		t.Errorf("debt asset = %s, want %s", debts[0].Asset.Hex(), usdcAddress.Hex())
	}
	if debts[0].CurrentDebt.Cmp(debtUSDC) != 0 {
		t.Errorf("debt amount = %s, want %s", debts[0].CurrentDebt.String(), debtUSDC.String())
	}
}

func TestSaveBorrowerRecord(t *testing.T) {
	reserveAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdcAddr := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	userAddr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	tests := []struct {
		name           string
		eventType      entity.EventType
		eventDelta     *big.Int
		currentDebt    *big.Int // nil means debt data is missing
		decimals       int
		reserveAddr    common.Address
		symbol         string
		tokenName      string
		wantErr        bool
		wantSaveCalled bool
		wantAmount     *big.Int
		wantChange     *big.Int
	}{
		{
			name:           "BorrowUsesCurrentDebtNotEventDelta",
			eventType:      EventBorrow,
			eventDelta:     new(big.Int).Mul(big.NewInt(1), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			currentDebt:    new(big.Int).Mul(big.NewInt(3), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			decimals:       18,
			reserveAddr:    reserveAddr,
			symbol:         "WETH",
			tokenName:      "Wrapped Ether",
			wantSaveCalled: true,
			wantAmount:     new(big.Int).Mul(big.NewInt(3), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			wantChange:     new(big.Int).Mul(big.NewInt(1), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		},
		{
			name:           "RepayUsesCurrentDebtNotEventDelta",
			eventType:      EventRepay,
			eventDelta:     new(big.Int).Mul(big.NewInt(500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)),
			currentDebt:    new(big.Int).Mul(big.NewInt(1500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)),
			decimals:       6,
			reserveAddr:    usdcAddr,
			symbol:         "USDC",
			tokenName:      "USD Coin",
			wantSaveCalled: true,
			wantAmount:     new(big.Int).Mul(big.NewInt(1500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)),
			wantChange:     new(big.Int).Mul(big.NewInt(500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil)),
		},
		{
			name:           "RepayToZeroUsesZeroAmountWhenDebtMissing",
			eventType:      EventRepay,
			eventDelta:     new(big.Int).Mul(big.NewInt(2), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			currentDebt:    nil,
			decimals:       18,
			reserveAddr:    reserveAddr,
			symbol:         "WETH",
			tokenName:      "Wrapped Ether",
			wantSaveCalled: true,
			wantAmount:     big.NewInt(0),
			wantChange:     new(big.Int).Mul(big.NewInt(2), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
		},
		{
			name:           "BorrowSkipsRecordWhenDebtMissing",
			eventType:      EventBorrow,
			eventDelta:     new(big.Int).Mul(big.NewInt(2), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			currentDebt:    nil,
			decimals:       18,
			reserveAddr:    reserveAddr,
			symbol:         "WETH",
			tokenName:      "Wrapped Ether",
			wantErr:        false,
			wantSaveCalled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			positionRepo := &mockPositionRepository{}
			svc := newBorrowerRecordTestService(positionRepo)

			var debtData []aavelike.DebtData
			if tt.currentDebt != nil {
				debtData = []aavelike.DebtData{{
					Asset:       tt.reserveAddr,
					Decimals:    tt.decimals,
					Symbol:      tt.symbol,
					Name:        tt.tokenName,
					CurrentDebt: tt.currentDebt,
				}}
			}

			err := svc.txManager.WithTransaction(context.Background(), func(tx pgx.Tx) error {
				return svc.saveBorrowerRecord(context.Background(), tx, &PositionEventData{
					EventType: tt.eventType,
					TxHash:    "0xtest",
					User:      userAddr,
					Reserve:   tt.reserveAddr,
					Amount:    tt.eventDelta,
				}, aavelike.TokenMetadata{Symbol: tt.symbol, Decimals: tt.decimals, Name: tt.tokenName}, debtData, 1, 1, 1, 20000000, 0, time.Unix(1700000000, 0).UTC())
			})

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if len(positionRepo.saveBorrowerCalls) != 0 {
					t.Fatalf("expected 0 SaveBorrower calls, got %d", len(positionRepo.saveBorrowerCalls))
				}
				return
			}

			if err != nil {
				t.Fatalf("saveBorrowerRecord() failed: %v", err)
			}

			if !tt.wantSaveCalled {
				if len(positionRepo.saveBorrowerCalls) != 0 {
					t.Fatalf("expected 0 SaveBorrower calls, got %d", len(positionRepo.saveBorrowerCalls))
				}
				return
			}

			if len(positionRepo.saveBorrowerCalls) != 1 {
				t.Fatalf("expected 1 SaveBorrower call, got %d", len(positionRepo.saveBorrowerCalls))
			}

			call := positionRepo.saveBorrowerCalls[0]
			if call.Amount.Cmp(tt.wantAmount) != 0 {
				t.Errorf("SaveBorrower amount = %s, want %s", call.Amount, tt.wantAmount)
			}
			if call.Change.Cmp(tt.wantChange) != 0 {
				t.Errorf("SaveBorrower change = %s, want %s", call.Change, tt.wantChange)
			}
		})
	}
}

func TestIndexUserPosition(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(24033627)
	const blockVersion = 0

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
	wethAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	usdcAddress := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")

	oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	debtUSDC := new(big.Int).Mul(big.NewInt(1500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
	zero := big.NewInt(0)

	ethClient, cleanup := newEthClientReturningUserReserves(t, []sparkLendUserReserve{
		{
			UnderlyingAsset:                wethAddress,
			ScaledATokenBalance:            oneETH,
			UsageAsCollateralEnabledOnUser: true,
			ScaledVariableDebt:             zero,
		},
		{
			UnderlyingAsset:                usdcAddress,
			ScaledATokenBalance:            zero,
			UsageAsCollateralEnabledOnUser: false,
			ScaledVariableDebt:             debtUSDC,
		},
	})
	defer cleanup()

	erc20ABI := mustERC20ABI(t)
	userReserveDataABI := mustUserReserveDataABI(t)

	multicaller := testutil.NewMockMulticaller()
	multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			methodID := hex.EncodeToString(call.CallData[:4])
			switch {
			case call.Target == wethAddress || call.Target == usdcAddress:
				returnData := packERC20MetadataResponse(t, erc20ABI, call.Target, methodID)
				results[i] = outbound.Result{Success: true, ReturnData: returnData}
			case methodID == methodIDHex(userReserveDataABI, "getUserReserveData"):
				asset := unpackUserReserveAsset(t, userReserveDataABI, call.CallData)
				returnData := packUserReserveDataResponse(t, userReserveDataABI, asset)
				results[i] = outbound.Result{Success: true, ReturnData: returnData}
			default:
				t.Fatalf("unexpected multicall target %s method %s", call.Target.Hex(), methodID)
			}
		}
		return results, nil
	}

	positionRepo := &mockPositionRepository{}
	svc := newPositionSnapshotTestService(t, ethClient, multicaller, positionRepo, chainID, protocolAddress)

	err := svc.IndexUserPosition(context.Background(), userAddress, protocolAddress, chainID, blockNumber, blockVersion, time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatalf("IndexUserPosition() failed: %v", err)
	}

	if len(positionRepo.saveBorrowerCalls) != 1 {
		t.Fatalf("expected 1 SaveBorrower call, got %d", len(positionRepo.saveBorrowerCalls))
	}
	borrowerCall := positionRepo.saveBorrowerCalls[0]
	if borrowerCall.EventType != string(entity.InternalSnapshot) {
		t.Errorf("SaveBorrower eventType = %q, want %q", borrowerCall.EventType, entity.InternalSnapshot)
	}
	if borrowerCall.Amount.Cmp(debtUSDC) != 0 {
		t.Errorf("SaveBorrower amount = %s, want %s", borrowerCall.Amount, debtUSDC)
	}
	if borrowerCall.Change.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("SaveBorrower change = %s, want 0", borrowerCall.Change)
	}

	if len(positionRepo.saveBorrowerCollateralsCalls) != 1 {
		t.Fatalf("expected 1 SaveBorrowerCollaterals call, got %d", len(positionRepo.saveBorrowerCollateralsCalls))
	}
	collateralRecords := positionRepo.saveBorrowerCollateralsCalls[0]
	if len(collateralRecords) != 1 {
		t.Fatalf("expected 1 collateral record, got %d", len(collateralRecords))
	}
	if collateralRecords[0].EventType != entity.InternalSnapshot {
		t.Errorf("collateral record eventType = %q, want %q", collateralRecords[0].EventType, entity.InternalSnapshot)
	}
	if collateralRecords[0].Change.Cmp(big.NewInt(0)) != 0 {
		t.Errorf("collateral record change = %s, want 0", collateralRecords[0].Change)
	}
}

func TestIndexUserPosition_NoPositions(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(24033627)
	const blockVersion = 0

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")

	ethClient, cleanup := newEthClientReturningUserReserves(t, []sparkLendUserReserve{})
	defer cleanup()

	multicaller := testutil.NewMockMulticaller()

	positionRepo := &mockPositionRepository{}
	svc := newPositionSnapshotTestService(t, ethClient, multicaller, positionRepo, chainID, protocolAddress)

	err := svc.IndexUserPosition(context.Background(), userAddress, protocolAddress, chainID, blockNumber, blockVersion, time.Unix(1700000000, 0).UTC())
	if err != nil {
		t.Fatalf("IndexUserPosition() should return nil for no positions, got: %v", err)
	}

	if len(positionRepo.saveBorrowerCalls) != 0 {
		t.Fatalf("expected 0 SaveBorrower calls, got %d", len(positionRepo.saveBorrowerCalls))
	}
	if len(positionRepo.saveBorrowerCollateralsCalls) != 0 {
		t.Fatalf("expected 0 SaveBorrowerCollaterals calls, got %d", len(positionRepo.saveBorrowerCollateralsCalls))
	}
}

func TestIndexUserPosition_PropagatesExtractionError(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(24033627)
	const blockVersion = 0

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
	wethAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	zero := big.NewInt(0)

	ethClient, cleanup := newEthClientReturningUserReserves(t, []sparkLendUserReserve{
		{
			UnderlyingAsset:                wethAddress,
			ScaledATokenBalance:            zero,
			UsageAsCollateralEnabledOnUser: false,
			ScaledVariableDebt:             oneETH,
		},
	})
	defer cleanup()

	erc20ABI := mustERC20ABI(t)
	multicaller := testutil.NewMockMulticaller()
	executeCount := 0
	multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		executeCount++
		switch executeCount {
		case 1:
			results := make([]outbound.Result, len(calls))
			for i, call := range calls {
				methodID := hex.EncodeToString(call.CallData[:4])
				results[i] = outbound.Result{Success: true, ReturnData: packERC20MetadataResponse(t, erc20ABI, call.Target, methodID)}
			}
			return results, nil
		case 2:
			return nil, errors.New("rpc failure")
		default:
			t.Fatalf("unexpected multicall execution count: %d", executeCount)
			return nil, nil
		}
	}

	positionRepo := &mockPositionRepository{}
	svc := newPositionSnapshotTestService(t, ethClient, multicaller, positionRepo, chainID, protocolAddress)

	err := svc.IndexUserPosition(context.Background(), userAddress, protocolAddress, chainID, blockNumber, blockVersion, time.Unix(1700000000, 0).UTC())
	if err == nil {
		t.Fatal("expected IndexUserPosition to fail, got nil")
	}

	if len(positionRepo.saveBorrowerCalls) != 0 {
		t.Fatalf("expected 0 SaveBorrower calls, got %d", len(positionRepo.saveBorrowerCalls))
	}
	if len(positionRepo.saveBorrowerCollateralsCalls) != 0 {
		t.Fatalf("expected 0 SaveBorrowerCollaterals calls, got %d", len(positionRepo.saveBorrowerCollateralsCalls))
	}
}

func TestSavePositionSnapshot_AllEventTypesFailWhenPositionExtractionFails(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(24033627)

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
	wethAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
	zero := big.NewInt(0)

	for _, eventType := range []entity.EventType{EventBorrow, EventRepay, EventSupply, EventWithdraw} {
		t.Run(string(eventType), func(t *testing.T) {
			ethClient, cleanup := newEthClientReturningUserReserves(t, []sparkLendUserReserve{{
				UnderlyingAsset:                wethAddress,
				ScaledATokenBalance:            zero,
				UsageAsCollateralEnabledOnUser: false,
				ScaledVariableDebt:             oneETH,
			}})
			defer cleanup()

			erc20ABI := mustERC20ABI(t)
			multicaller := testutil.NewMockMulticaller()
			executeCount := 0
			multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
				executeCount++
				switch executeCount {
				case 1:
					results := make([]outbound.Result, len(calls))
					for i, call := range calls {
						methodID := hex.EncodeToString(call.CallData[:4])
						results[i] = outbound.Result{Success: true, ReturnData: packERC20MetadataResponse(t, erc20ABI, call.Target, methodID)}
					}
					return results, nil
				case 2:
					return nil, errors.New("boom")
				default:
					t.Fatalf("unexpected multicall execution count: %d", executeCount)
					return nil, nil
				}
			}

			positionRepo := &mockPositionRepository{}
			svc := newPositionSnapshotTestService(t, ethClient, multicaller, positionRepo, chainID, protocolAddress)

			err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
				EventType: eventType,
				TxHash:    "0xdeadbeef",
				User:      userAddress,
				Reserve:   wethAddress,
				Amount:    oneETH,
			}, protocolAddress, chainID, blockNumber, 0, time.Unix(1700000000, 0).UTC())
			if err == nil {
				t.Fatal("expected savePositionSnapshot to fail, got nil")
			}
			if len(positionRepo.saveBorrowerCalls) != 0 {
				t.Fatalf("expected 0 SaveBorrower calls, got %d", len(positionRepo.saveBorrowerCalls))
			}
			if len(positionRepo.saveBorrowerCollateralsCalls) != 0 {
				t.Fatalf("expected 0 SaveBorrowerCollaterals calls, got %d", len(positionRepo.saveBorrowerCollateralsCalls))
			}
		})
	}
}

func TestSavePositionSnapshot_TokenMetadataFailurePropagatesError(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(24033627)

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	userAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
	wethAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

	ethClient, cleanup := newEthClientReturningUserReserves(t, []sparkLendUserReserve{{
		UnderlyingAsset:                wethAddress,
		ScaledATokenBalance:            oneETH,
		UsageAsCollateralEnabledOnUser: true,
		ScaledVariableDebt:             big.NewInt(0),
	}})
	defer cleanup()

	multicaller := testutil.NewMockMulticaller()
	multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		return nil, errors.New("metadata multicall failure")
	}

	positionRepo := &mockPositionRepository{}
	svc := newPositionSnapshotTestService(t, ethClient, multicaller, positionRepo, chainID, protocolAddress)

	err := svc.savePositionSnapshot(context.Background(), &PositionEventData{
		EventType: EventSupply,
		TxHash:    "0xdeadbeef",
		User:      userAddress,
		Reserve:   wethAddress,
		Amount:    oneETH,
	}, protocolAddress, chainID, blockNumber, 0, time.Unix(1700000000, 0).UTC())
	if err == nil {
		t.Fatal("expected error when token metadata fetch fails, got nil")
	}
	if len(positionRepo.saveBorrowerCalls) != 0 {
		t.Fatalf("expected 0 SaveBorrower calls, got %d", len(positionRepo.saveBorrowerCalls))
	}
	if len(positionRepo.saveBorrowerCollateralsCalls) != 0 {
		t.Fatalf("expected 0 SaveBorrowerCollaterals calls, got %d", len(positionRepo.saveBorrowerCollateralsCalls))
	}
}

func TestPersistUserPositionBatch(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(20000000)
	const blockVersion = 0
	protocolAddress := common.HexToAddress(testPool) // Aave V3 Ethereum (known protocol)

	tests := []struct {
		name                     string
		positions                []UserPositionData
		wantBorrowerCalls        int
		wantCollateralBatchCalls int
		wantErr                  bool
	}{
		{
			name:                     "empty positions returns nil without repo calls",
			positions:                nil,
			wantBorrowerCalls:        0,
			wantCollateralBatchCalls: 0,
		},
		{
			name: "single user with 1 collateral and 1 debt",
			positions: []UserPositionData{
				{
					User: common.HexToAddress(testUser1),
					Collaterals: []aavelike.CollateralData{
						{
							Asset:             common.HexToAddress(testWETH),
							Decimals:          18,
							Symbol:            "WETH",
							ActualBalance:     big.NewInt(1e18),
							CollateralEnabled: true,
						},
					},
					Debts: []aavelike.DebtData{
						{
							Asset:       common.HexToAddress(testUSDC),
							Decimals:    6,
							Symbol:      "USDC",
							CurrentDebt: big.NewInt(1500e6),
						},
					},
				},
			},
			wantBorrowerCalls:        1,
			wantCollateralBatchCalls: 1,
		},
		{
			name: "multiple users",
			positions: []UserPositionData{
				{
					User: common.HexToAddress(testUser1),
					Debts: []aavelike.DebtData{
						{
							Asset:       common.HexToAddress(testUSDC),
							Decimals:    6,
							Symbol:      "USDC",
							CurrentDebt: big.NewInt(1000e6),
						},
					},
				},
				{
					User: common.HexToAddress(testUser2),
					Collaterals: []aavelike.CollateralData{
						{
							Asset:             common.HexToAddress(testWETH),
							Decimals:          18,
							Symbol:            "WETH",
							ActualBalance:     big.NewInt(2e18),
							CollateralEnabled: true,
						},
					},
					Debts: []aavelike.DebtData{
						{
							Asset:       common.HexToAddress(testUSDC),
							Decimals:    6,
							Symbol:      "USDC",
							CurrentDebt: big.NewInt(500e6),
						},
					},
				},
			},
			wantBorrowerCalls:        2,
			wantCollateralBatchCalls: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			positionRepo := &mockPositionRepository{}
			svc := newBorrowerRecordTestService(positionRepo)

			err := svc.PersistUserPositionBatch(
				context.Background(),
				tt.positions,
				protocolAddress,
				chainID,
				blockNumber,
				blockVersion,
				time.Unix(1700000000, 0).UTC(),
			)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(positionRepo.saveBorrowerCalls) != tt.wantBorrowerCalls {
				t.Errorf("SaveBorrower calls: got %d, want %d", len(positionRepo.saveBorrowerCalls), tt.wantBorrowerCalls)
			}
			if len(positionRepo.saveBorrowerCollateralsCalls) != tt.wantCollateralBatchCalls {
				t.Errorf("SaveBorrowerCollaterals batch calls: got %d, want %d", len(positionRepo.saveBorrowerCollateralsCalls), tt.wantCollateralBatchCalls)
			}
		})
	}
}

func TestSaveReserveDataSnapshot_CreatesReceiptTokenWithBatchedSymbol(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(24033627)

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	reserve := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	aTokenAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")

	ethClient, cleanup := newEthClientReturningUserReserves(t, []sparkLendUserReserve{})
	defer cleanup()

	erc20ABI := mustERC20ABI(t)
	reserveDataABI := mustSparklendReserveDataABI(t)
	configABI := mustReserveConfigABI(t)
	tokenAddrsABI := mustReserveTokenAddrsABI(t)

	multicaller := testutil.NewMockMulticaller()
	executeCount := 0
	multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, bn *big.Int) ([]outbound.Result, error) {
		executeCount++
		switch executeCount {
		case 1:
			// GetFullReserveData multicall
			results := make([]outbound.Result, 3)
			results[0] = outbound.Result{Success: true, ReturnData: packSparklendReserveDataResponse(t, reserveDataABI)}
			results[1] = outbound.Result{Success: true, ReturnData: packReserveConfigResponse(t, configABI)}
			results[2] = outbound.Result{Success: true, ReturnData: packReserveTokenAddrsResponse(t, tokenAddrsABI, aTokenAddr)}
			return results, nil
		case 2:
			// BatchGetTokenMetadata for reserve + aToken
			results := make([]outbound.Result, len(calls))
			for i, call := range calls {
				methodID := hex.EncodeToString(call.CallData[:4])
				results[i] = outbound.Result{Success: true, ReturnData: packERC20MetadataResponse(t, erc20ABI, call.Target, methodID)}
			}
			return results, nil
		default:
			t.Fatalf("unexpected multicall execution count: %d", executeCount)
			return nil, nil
		}
	}

	var capturedToken entity.ReceiptToken
	receiptTokenRepo := &testutil.MockReceiptTokenRepository{
		GetOrCreateReceiptTokenFn: func(ctx context.Context, tx pgx.Tx, token entity.ReceiptToken) (int64, error) {
			capturedToken = token
			return 42, nil
		},
	}

	svc := newServiceWithCachedBlockchainService(t, ethClient, multicaller, chainID, protocolAddress)
	svc.userRepo = &testutil.MockUserRepository{}
	svc.protocolRepo = &testutil.MockProtocolRepository{}
	svc.tokenRepo = &testutil.MockTokenRepository{}
	svc.txManager = &testutil.MockTxManager{}
	svc.receiptTokenRepo = receiptTokenRepo

	err := svc.saveReserveDataSnapshot(context.Background(), reserve, protocolAddress, chainID, blockNumber, 0, "0xtest")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if capturedToken.ReceiptTokenAddress != aTokenAddr {
		t.Errorf("receipt token address = %s, want %s", capturedToken.ReceiptTokenAddress.Hex(), aTokenAddr.Hex())
	}
	if capturedToken.Symbol != "spWETH" {
		t.Errorf("receipt token symbol = %q, want %q (fetched from batched metadata call)", capturedToken.Symbol, "spWETH")
	}
	if capturedToken.ChainID != chainID {
		t.Errorf("receipt token chainID = %d, want %d", capturedToken.ChainID, chainID)
	}
}

func TestSaveReserveDataSnapshot_ReceiptTokenRepoErrorPropagates(t *testing.T) {
	const chainID = int64(1)
	const blockNumber = int64(24033627)

	protocolAddress := common.HexToAddress("0xC13e21B648A5Ee794902342038FF3aDAB66BE987")
	reserve := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	aTokenAddr := common.HexToAddress("0x1234567890123456789012345678901234567890")

	ethClient, cleanup := newEthClientReturningUserReserves(t, []sparkLendUserReserve{})
	defer cleanup()

	erc20ABI := mustERC20ABI(t)
	reserveDataABI := mustSparklendReserveDataABI(t)
	configABI := mustReserveConfigABI(t)
	tokenAddrsABI := mustReserveTokenAddrsABI(t)

	multicaller := testutil.NewMockMulticaller()
	executeCount := 0
	multicaller.ExecuteFn = func(ctx context.Context, calls []outbound.Call, bn *big.Int) ([]outbound.Result, error) {
		executeCount++
		switch executeCount {
		case 1:
			// GetFullReserveData multicall: getReserveData, getReserveConfigurationData, getReserveTokensAddresses
			results := make([]outbound.Result, 3)
			results[0] = outbound.Result{Success: true, ReturnData: packSparklendReserveDataResponse(t, reserveDataABI)}
			results[1] = outbound.Result{Success: true, ReturnData: packReserveConfigResponse(t, configABI)}
			results[2] = outbound.Result{Success: true, ReturnData: packReserveTokenAddrsResponse(t, tokenAddrsABI, aTokenAddr)}
			return results, nil
		case 2:
			// BatchGetTokenMetadata for reserve + aToken (decimals, symbol, name for each)
			results := make([]outbound.Result, len(calls))
			for i, call := range calls {
				methodID := hex.EncodeToString(call.CallData[:4])
				results[i] = outbound.Result{Success: true, ReturnData: packERC20MetadataResponse(t, erc20ABI, call.Target, methodID)}
				_ = call
			}
			return results, nil
		default:
			t.Fatalf("unexpected multicall execution count: %d", executeCount)
			return nil, nil
		}
	}

	receiptTokenRepo := &testutil.MockReceiptTokenRepository{
		GetOrCreateReceiptTokenFn: func(ctx context.Context, tx pgx.Tx, token entity.ReceiptToken) (int64, error) {
			return 0, errors.New("receipt token repo failure")
		},
	}

	svc := newServiceWithCachedBlockchainService(t, ethClient, multicaller, chainID, protocolAddress)
	svc.userRepo = &testutil.MockUserRepository{}
	svc.protocolRepo = &testutil.MockProtocolRepository{}
	svc.tokenRepo = &testutil.MockTokenRepository{}
	svc.txManager = &testutil.MockTxManager{}
	svc.receiptTokenRepo = receiptTokenRepo

	err := svc.saveReserveDataSnapshot(context.Background(), reserve, protocolAddress, chainID, blockNumber, 0, "0xtest")
	if err == nil {
		t.Fatal("expected error when receipt token repo fails, got nil")
	}
	if !strings.Contains(err.Error(), "failed to upsert receipt token") {
		t.Errorf("expected error to contain 'failed to upsert receipt token', got: %v", err)
	}
}

// --- Test helpers ---

func mustSparklendReserveDataABI(t *testing.T) *abi.ABI {
	t.Helper()
	parsedABI, err := abis.GetSparklendPoolDataProviderReserveDataABI()
	if err != nil {
		t.Fatalf("load sparklend reserve data ABI: %v", err)
	}
	return parsedABI
}

func mustReserveConfigABI(t *testing.T) *abi.ABI {
	t.Helper()
	parsedABI, err := abis.GetPoolDataProviderReserveConfigurationABI()
	if err != nil {
		t.Fatalf("load reserve config ABI: %v", err)
	}
	return parsedABI
}

func mustReserveTokenAddrsABI(t *testing.T) *abi.ABI {
	t.Helper()
	parsedABI, err := abis.GetPoolDataProviderReserveTokensAddressesABI()
	if err != nil {
		t.Fatalf("load reserve token addrs ABI: %v", err)
	}
	return parsedABI
}

func packSparklendReserveDataResponse(t *testing.T, reserveDataABI *abi.ABI) []byte {
	t.Helper()
	zero := big.NewInt(0)
	return mustPackOutput(t, reserveDataABI, "getReserveData",
		zero, zero, zero, zero, zero, // unbacked, accruedToTreasuryScaled, totalAToken, totalStableDebt, totalVariableDebt
		zero, zero, zero, // liquidityRate, variableBorrowRate, stableBorrowRate
		zero, zero, // liquidityIndex, variableBorrowIndex
		zero, // lastUpdateTimestamp (uint40)
	)
}

func packReserveConfigResponse(t *testing.T, configABI *abi.ABI) []byte {
	t.Helper()
	return mustPackOutput(t, configABI, "getReserveConfigurationData",
		big.NewInt(18),    // decimals
		big.NewInt(8000),  // ltv
		big.NewInt(8500),  // liquidationThreshold
		big.NewInt(10500), // liquidationBonus
		big.NewInt(1000),  // reserveFactor
		true,              // usageAsCollateralEnabled
		true,              // borrowingEnabled
		true,              // stableBorrowRateEnabled
		true,              // isActive
		false,             // isFrozen
	)
}

func packReserveTokenAddrsResponse(t *testing.T, tokenAddrsABI *abi.ABI, aTokenAddr common.Address) []byte {
	t.Helper()
	return mustPackOutput(t, tokenAddrsABI, "getReserveTokensAddresses",
		aTokenAddr,       // aTokenAddress
		common.Address{}, // stableDebtTokenAddress
		common.Address{}, // variableDebtTokenAddress
	)
}

func newBorrowerRecordTestService(positionRepo *mockPositionRepository) *Service {
	return &Service{
		logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		positionRepo: positionRepo,
		userRepo:     &testutil.MockUserRepository{},
		protocolRepo: &testutil.MockProtocolRepository{},
		tokenRepo:    &testutil.MockTokenRepository{},
		txManager:    &testutil.MockTxManager{},
	}
}

func newPositionSnapshotTestService(t *testing.T, ethClient *ethclient.Client, multicaller outbound.Multicaller, positionRepo *mockPositionRepository, chainID int64, protocolAddress common.Address) *Service {
	t.Helper()

	svc := newServiceWithCachedBlockchainService(t, ethClient, multicaller, chainID, protocolAddress)
	svc.positionRepo = positionRepo
	svc.userRepo = &testutil.MockUserRepository{}
	svc.protocolRepo = &testutil.MockProtocolRepository{}
	svc.tokenRepo = &testutil.MockTokenRepository{}
	svc.txManager = &testutil.MockTxManager{}

	return svc
}

func newServiceWithCachedBlockchainService(t *testing.T, ethClient *ethclient.Client, multicaller outbound.Multicaller, chainID int64, protocolAddress common.Address) *Service {
	t.Helper()

	reader := aavelike.NewPositionReader(ethClient, multicaller, mustERC20ABI(t), slog.New(slog.NewTextHandler(io.Discard, nil)))

	return &Service{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		reader: reader,
	}
}

func newEthClientReturningUserReserves(t *testing.T, reserves []sparkLendUserReserve) (*ethclient.Client, func()) {
	t.Helper()

	response := packSparkLendUserReserves(t, reserves)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var req rpcutil.Request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			testutil.WriteRPCError(w, json.RawMessage(`1`), -32700, "parse error")
			return
		}

		switch req.Method {
		case "eth_call":
			resultHex := "0x" + hex.EncodeToString(response)
			resultJSON, err := json.Marshal(resultHex)
			if err != nil {
				t.Fatalf("marshal eth_call result: %v", err)
			}
			testutil.WriteRPCResult(w, req.ID, json.RawMessage(resultJSON))
		default:
			testutil.WriteRPCError(w, req.ID, -32601, "method not found: "+req.Method)
		}
	}))

	client, err := ethclient.Dial(server.URL)
	if err != nil {
		server.Close()
		t.Fatalf("ethclient.Dial() failed: %v", err)
	}

	cleanup := func() {
		client.Close()
		server.Close()
	}

	return client, cleanup
}

func packSparkLendUserReserves(t *testing.T, reserves []sparkLendUserReserve) []byte {
	t.Helper()

	sparklendABI, err := abis.GetSparklendUserReservesDataABI()
	if err != nil {
		t.Fatalf("load SparkLend ABI: %v", err)
	}

	data, err := sparklendABI.Methods["getUserReservesData"].Outputs.Pack(reserves, uint8(0))
	if err != nil {
		t.Fatalf("pack getUserReservesData: %v", err)
	}

	return data
}

func packERC20MetadataResponse(t *testing.T, erc20ABI *abi.ABI, token common.Address, methodID string) []byte {
	t.Helper()

	switch token {
	case common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"):
		switch methodID {
		case methodIDHex(erc20ABI, "decimals"):
			return mustPackOutput(t, erc20ABI, "decimals", uint8(18))
		case methodIDHex(erc20ABI, "symbol"):
			return mustPackOutput(t, erc20ABI, "symbol", "WETH")
		case methodIDHex(erc20ABI, "name"):
			return mustPackOutput(t, erc20ABI, "name", "Wrapped Ether")
		}
	case common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"):
		switch methodID {
		case methodIDHex(erc20ABI, "decimals"):
			return mustPackOutput(t, erc20ABI, "decimals", uint8(6))
		case methodIDHex(erc20ABI, "symbol"):
			return mustPackOutput(t, erc20ABI, "symbol", "USDC")
		case methodIDHex(erc20ABI, "name"):
			return mustPackOutput(t, erc20ABI, "name", "USD Coin")
		}
	case common.HexToAddress("0x1234567890123456789012345678901234567890"):
		switch methodID {
		case methodIDHex(erc20ABI, "decimals"):
			return mustPackOutput(t, erc20ABI, "decimals", uint8(18))
		case methodIDHex(erc20ABI, "symbol"):
			return mustPackOutput(t, erc20ABI, "symbol", "spWETH")
		case methodIDHex(erc20ABI, "name"):
			return mustPackOutput(t, erc20ABI, "name", "Spark WETH")
		}
	}

	t.Fatalf("unexpected metadata request for token %s method %s", token.Hex(), methodID)
	return nil
}

func packUserReserveDataResponse(t *testing.T, userReserveDataABI *abi.ABI, asset common.Address) []byte {
	t.Helper()

	zero := big.NewInt(0)
	stableRateLastUpdated := big.NewInt(0)

	switch asset {
	case common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"):
		oneETH := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)
		return mustPackOutput(t, userReserveDataABI, "getUserReserveData",
			oneETH, zero, zero, zero, zero, zero, zero, stableRateLastUpdated, true,
		)
	case common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"):
		debtUSDC := new(big.Int).Mul(big.NewInt(1500), new(big.Int).Exp(big.NewInt(10), big.NewInt(6), nil))
		return mustPackOutput(t, userReserveDataABI, "getUserReserveData",
			zero, zero, debtUSDC, zero, zero, zero, zero, stableRateLastUpdated, false,
		)
	default:
		t.Fatalf("unexpected asset in getUserReserveData response: %s", asset.Hex())
		return nil
	}
}

func unpackUserReserveAsset(t *testing.T, userReserveDataABI *abi.ABI, callData []byte) common.Address {
	t.Helper()

	args, err := userReserveDataABI.Methods["getUserReserveData"].Inputs.Unpack(callData[4:])
	if err != nil {
		t.Fatalf("unpack getUserReserveData call: %v", err)
	}
	asset, ok := args[0].(common.Address)
	if !ok {
		t.Fatalf("unexpected asset type %T", args[0])
	}
	return asset
}

func mustPackOutput(t *testing.T, parsedABI *abi.ABI, method string, values ...any) []byte {
	t.Helper()

	data, err := parsedABI.Methods[method].Outputs.Pack(values...)
	if err != nil {
		t.Fatalf("pack %s output: %v", method, err)
	}
	return data
}

func mustERC20ABI(t *testing.T) *abi.ABI {
	t.Helper()

	parsedABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("load ERC20 ABI: %v", err)
	}
	return parsedABI
}

func mustUserReserveDataABI(t *testing.T) *abi.ABI {
	t.Helper()

	parsedABI, err := abis.GetPoolDataProviderUserReserveDataABI()
	if err != nil {
		t.Fatalf("load user reserve data ABI: %v", err)
	}
	return parsedABI
}

func methodIDHex(parsedABI *abi.ABI, method string) string {
	return hex.EncodeToString(parsedABI.Methods[method].ID)
}

// --- resolvePositionTokens unit tests ---

// newResolveTokensTestService returns a Service wired with a capturing token
// repo mock and harmless other deps. The capture function the caller passes in
// receives the exact []outbound.TokenInput slice that resolvePositionTokens
// hands to GetOrCreateTokens, in call order.
func newResolveTokensTestService(capture *[]outbound.TokenInput) *Service {
	return &Service{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		tokenRepo: &testutil.MockTokenRepository{
			GetOrCreateTokensFn: func(_ context.Context, _ pgx.Tx, tokens []outbound.TokenInput) (map[common.Address]int64, error) {
				*capture = append(*capture, tokens...)
				result := make(map[common.Address]int64, len(tokens))
				for i, t := range tokens {
					result[t.Address] = int64(i + 1)
				}
				return result, nil
			},
		},
	}
}

func TestResolvePositionTokens_DedupesAcrossCollateralsDebtsAndExtras(t *testing.T) {
	tokenA := common.HexToAddress("0xAAaAaAAAaAaAAaAaAaAAaAaaAAAAAAaAAAaaaAaA")
	tokenB := common.HexToAddress("0xBBbBbBBbbBBBBbBBbBBbBBBBBBBBBBBbBBbbBbbB")

	collaterals := []aavelike.CollateralData{
		{Asset: tokenA, Symbol: "USDC", Decimals: 6},
		{Asset: tokenA, Symbol: "USDC", Decimals: 6}, // duplicate
		{Asset: tokenB, Symbol: "WETH", Decimals: 18},
	}
	debts := []aavelike.DebtData{
		{Asset: tokenA, Symbol: "USDC", Decimals: 6}, // overlaps with collateral
	}
	extras := []outbound.TokenInput{
		{ChainID: 1, Address: tokenB, Symbol: "WETH", Decimals: 18, CreatedAtBlock: 100}, // overlaps with collateral
	}

	var captured []outbound.TokenInput
	svc := newResolveTokensTestService(&captured)

	tokenIDs, err := svc.resolvePositionTokens(context.Background(), nil, 1, 100, collaterals, debts, extras...)
	if err != nil {
		t.Fatalf("resolvePositionTokens: %v", err)
	}
	if len(tokenIDs) != 2 {
		t.Errorf("returned map size: got %d, want 2 (one per unique address)", len(tokenIDs))
	}
	if len(captured) != 2 {
		t.Fatalf("GetOrCreateTokens received %d inputs, want 2 (deduped); captured: %+v", len(captured), captured)
	}
	gotAddrs := map[common.Address]bool{captured[0].Address: true, captured[1].Address: true}
	if !gotAddrs[tokenA] || !gotAddrs[tokenB] {
		t.Errorf("captured addresses missing one of {tokenA, tokenB}; got %+v", gotAddrs)
	}
}

func TestResolvePositionTokens_IncludesBorrowReserveExtras(t *testing.T) {
	collateralToken := common.HexToAddress("0x1111111111111111111111111111111111111111")
	reserveToken := common.HexToAddress("0x2222222222222222222222222222222222222222")

	collaterals := []aavelike.CollateralData{
		{Asset: collateralToken, Symbol: "WETH", Decimals: 18},
	}
	extras := []outbound.TokenInput{
		{ChainID: 1, Address: reserveToken, Symbol: "USDC", Decimals: 6, CreatedAtBlock: 100},
	}

	var captured []outbound.TokenInput
	svc := newResolveTokensTestService(&captured)

	tokenIDs, err := svc.resolvePositionTokens(context.Background(), nil, 1, 100, collaterals, nil, extras...)
	if err != nil {
		t.Fatalf("resolvePositionTokens: %v", err)
	}
	if _, ok := tokenIDs[reserveToken]; !ok {
		t.Errorf("returned map missing reserve token %s", reserveToken.Hex())
	}
	gotAddrs := map[common.Address]bool{}
	for _, c := range captured {
		gotAddrs[c.Address] = true
	}
	if !gotAddrs[collateralToken] || !gotAddrs[reserveToken] {
		t.Errorf("captured addresses missing one of {collateral, reserve}; got %+v", gotAddrs)
	}
}

func TestResolvePositionTokens_EmptyInputs(t *testing.T) {
	var captured []outbound.TokenInput
	svc := newResolveTokensTestService(&captured)

	tokenIDs, err := svc.resolvePositionTokens(context.Background(), nil, 1, 100, nil, nil)
	if err != nil {
		t.Fatalf("resolvePositionTokens with empty inputs: %v", err)
	}
	if len(tokenIDs) != 0 {
		t.Errorf("returned map should be empty; got %d entries", len(tokenIDs))
	}
	if len(captured) != 0 {
		t.Errorf("GetOrCreateTokens should not see any inputs; got %d", len(captured))
	}
}

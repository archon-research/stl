package borrow_processor

import (
	"io"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

func TestService_LoadBorrowABI(t *testing.T) {
	service := &Service{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		eventSignatures: make(map[common.Hash]*abi.Event),
	}

	err := service.loadBorrowABI()
	if err != nil {
		t.Fatalf("loadBorrowABI() failed: %v", err)
	}

	if service.borrowABI == nil {
		t.Fatal("borrowABI is nil after loading")
	}

	borrowEvent, ok := service.borrowABI.Events["Borrow"]
	if !ok {
		t.Fatal("Borrow event not found in ABI")
	}

	if _, exists := service.eventSignatures[borrowEvent.ID]; !exists {
		t.Errorf("Borrow event signature not registered in eventSignatures map")
	}

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

	indexedParams := []string{"reserve", "onBehalfOf", "referralCode"}
	for _, input := range borrowEvent.Inputs {
		for _, expectedIndexed := range indexedParams {
			if input.Name == expectedIndexed && !input.Indexed {
				t.Errorf("expected %s to be indexed", input.Name)
			}
		}
	}
}

func TestService_IsBorrowEvent(t *testing.T) {
	service := &Service{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		eventSignatures: make(map[common.Hash]*abi.Event),
	}

	if err := service.loadBorrowABI(); err != nil {
		t.Fatalf("failed to load ABI: %v", err)
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
			name: "non-borrow event",
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
			result := service.isBorrowEvent(tt.log)
			if result != tt.expected {
				t.Errorf("isBorrowEvent() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestService_ExtractBorrowEventData(t *testing.T) {
	service := &Service{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		eventSignatures: make(map[common.Hash]*abi.Event),
	}

	if err := service.loadBorrowABI(); err != nil {
		t.Fatalf("failed to load ABI: %v", err)
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

	eventData, err := service.extractBorrowEventData(log)
	if err != nil {
		t.Fatalf("extractBorrowEventData failed: %v", err)
	}

	if eventData.Reserve == (common.Address{}) {
		t.Error("Reserve address should not be empty")
	}

	expectedReserve := common.HexToAddress("0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2")
	if eventData.Reserve != expectedReserve {
		t.Errorf("Reserve = %v, want %v", eventData.Reserve.Hex(), expectedReserve.Hex())
	}

	expectedOnBehalfOf := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0")
	if eventData.OnBehalfOf != expectedOnBehalfOf {
		t.Errorf("OnBehalfOf = %v, want %v", eventData.OnBehalfOf.Hex(), expectedOnBehalfOf.Hex())
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

func TestService_ProcessReceipt(t *testing.T) {
	service := &Service{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		eventSignatures: make(map[common.Hash]*abi.Event),
	}

	if err := service.loadBorrowABI(); err != nil {
		t.Fatalf("failed to load ABI: %v", err)
	}

	tests := []struct {
		name               string
		logsPerReceipt     int
		borrowEvents       int
		expectedBorrowLogs int
	}{
		{
			name:               "all borrow events",
			logsPerReceipt:     10,
			borrowEvents:       10,
			expectedBorrowLogs: 10,
		},
		{
			name:               "half borrow events",
			logsPerReceipt:     10,
			borrowEvents:       5,
			expectedBorrowLogs: 5,
		},
		{
			name:               "few borrow events in many logs",
			logsPerReceipt:     100,
			borrowEvents:       5,
			expectedBorrowLogs: 5,
		},
		{
			name:               "no borrow events",
			logsPerReceipt:     10,
			borrowEvents:       0,
			expectedBorrowLogs: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logs := make([]Log, tt.logsPerReceipt)

			for i := 0; i < tt.logsPerReceipt; i++ {
				var topics []string
				if i < tt.borrowEvents {
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

			borrowCount := 0
			for _, log := range receipt.Logs {
				if !service.isBorrowEvent(log) {
					continue
				}

				_, err := service.extractBorrowEventData(log)
				if err != nil {
					t.Fatalf("extractBorrowEventData failed: %v", err)
				}
				borrowCount++
			}

			if borrowCount != tt.expectedBorrowLogs {
				t.Errorf("processed %d borrow events, want %d", borrowCount, tt.expectedBorrowLogs)
			}
		})
	}
}

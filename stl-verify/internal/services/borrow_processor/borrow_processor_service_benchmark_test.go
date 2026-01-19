package borrow_processor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

func BenchmarkService_ExtractBorrowEventData(b *testing.B) {
	service := &Service{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		eventSignatures: make(map[common.Hash]*abi.Event),
	}

	if err := service.loadBorrowABI(); err != nil {
		b.Fatalf("failed to load ABI: %v", err)
	}

	log := Log{
		Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
		Topics: []string{
			"0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0", // signature
			"0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", // reserve (indexed)
			"0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f0bEb0", // onBehalfOf (indexed)
			"0x0000000000000000000000000000000000000000000000000000000000000000", // referralCode (indexed)
		},
		Data:             "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f0bEb00000000000000000000000000000000000000000000000056bc75e2d63100000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000002b5e3af16b18800000000000000000000000000000000000000000000000000000000000000000000",
		BlockNumber:      "0x123456",
		TransactionHash:  "0xabc123",
		TransactionIndex: "0x0",
		LogIndex:         "0x0",
	}

	b.ReportAllocs()

	for b.Loop() {
		eventData, err := service.extractBorrowEventData(log)
		if err != nil {
			b.Fatalf("extractBorrowEventData failed: %v", err)
		}
		if eventData.Reserve == (common.Address{}) {
			b.Fatal("invalid event data")
		}
	}
}

func BenchmarkService_IsBorrowEvent(b *testing.B) {
	service := &Service{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		eventSignatures: make(map[common.Hash]*abi.Event),
	}

	if err := service.loadBorrowABI(); err != nil {
		b.Fatalf("failed to load ABI: %v", err)
	}

	b.Run("BorrowEvent", func(b *testing.B) {
		log := Log{
			Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
			Topics: []string{
				"0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
			},
		}

		b.ReportAllocs()
		for b.Loop() {
			if !service.isBorrowEvent(log) {
				b.Fatal("should be borrow event")
			}
		}
	})

	b.Run("NonBorrowEvent", func(b *testing.B) {
		log := Log{
			Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
			Topics: []string{
				"0x1111111111111111111111111111111111111111111111111111111111111111",
			},
		}

		b.ReportAllocs()
		for b.Loop() {
			if service.isBorrowEvent(log) {
				b.Fatal("should not be borrow event")
			}
		}
	})
}

func BenchmarkService_ConvertToDecimalAdjusted(b *testing.B) {
	service := &Service{
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	cases := []struct {
		name     string
		amount   *big.Int
		decimals int
	}{
		{
			name:     "SmallAmount_6decimals",
			amount:   big.NewInt(1000000),
			decimals: 6,
		},
		{
			name:     "MediumAmount_18decimals",
			amount:   new(big.Int).Mul(big.NewInt(1000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			decimals: 18,
		},
		{
			name:     "LargeAmount_18decimals",
			amount:   new(big.Int).Mul(big.NewInt(1000000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)),
			decimals: 18,
		},
		{
			name:     "NoDecimals",
			amount:   big.NewInt(12345),
			decimals: 0,
		},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				result := service.convertToDecimalAdjusted(tc.amount, tc.decimals)
				if result == "" {
					b.Fatal("empty result")
				}
			}
		})
	}
}

func BenchmarkJSONUnmarshal_Receipts(b *testing.B) {
	cases := []struct {
		name           string
		receiptCount   int
		logsPerReceipt int
	}{
		{"SingleReceipt_SingleLog", 1, 1},
		{"SingleReceipt_ManyLogs", 1, 10},
		{"ManyReceipts_SingleLog", 10, 1},
		{"ManyReceipts_ManyLogs", 10, 10},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			receipts := make([]TransactionReceipt, tc.receiptCount)

			for i := 0; i < tc.receiptCount; i++ {
				logs := make([]Log, tc.logsPerReceipt)
				for j := 0; j < tc.logsPerReceipt; j++ {
					logs[j] = Log{
						Address: "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
						Topics: []string{
							"0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0",
							"0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
							"0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f0bEb0",
							"0x0000000000000000000000000000000000000000000000000000000000000000",
						},
						Data:             "0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f0bEb00000000000000000000000000000000000000000000000056bc75e2d63100000000000000000000000000000000000000000000000000000000000000000002000000000000000000000000000000000000000000000002b5e3af16b18800000000000000000000000000000000000000000000000000000000000000000000",
						BlockNumber:      fmt.Sprintf("0x%x", 19000000+i),
						TransactionHash:  fmt.Sprintf("0x%064x", i*1000+j),
						TransactionIndex: fmt.Sprintf("0x%x", j),
						LogIndex:         fmt.Sprintf("0x%x", j),
					}
				}

				receipts[i] = TransactionReceipt{
					Type:              "0x2",
					Status:            "0x1",
					TransactionHash:   fmt.Sprintf("0x%064x", i),
					TransactionIndex:  fmt.Sprintf("0x%x", i),
					BlockHash:         "0xdef456",
					BlockNumber:       fmt.Sprintf("0x%x", 19000000+i),
					CumulativeGasUsed: "0x123456",
					GasUsed:           "0x5208",
					EffectiveGasPrice: "0x3b9aca00",
					From:              "0x742d35Cc6634C0532925a3b844Bc9e7595f0bEb0",
					To:                "0x87870Bca3F3fD6335C3F4ce8392D69350B4fA4E2",
					Logs:              logs,
				}
			}

			receiptsJSON, _ := json.Marshal(receipts)

			b.ReportAllocs()
			b.SetBytes(int64(len(receiptsJSON)))

			for b.Loop() {
				var decoded []TransactionReceipt
				if err := json.Unmarshal(receiptsJSON, &decoded); err != nil {
					b.Fatalf("unmarshal failed: %v", err)
				}
			}
		})
	}
}

func BenchmarkSNSMessageParsing(b *testing.B) {
	metadata := ReceiptMetadata{
		ChainId:     1,
		BlockNumber: 19000000,
		Version:     1,
		BlockHash:   "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
		ReceivedAt:  time.Now().Format(time.RFC3339),
		CacheKey:    "receipts:19000000",
	}

	metadataJSON, _ := json.Marshal(metadata)

	snsMsg := SNSMessage{
		Type:      "Notification",
		MessageId: "msg-123",
		Message:   string(metadataJSON),
	}

	snsJSON, _ := json.Marshal(snsMsg)

	b.ReportAllocs()
	b.SetBytes(int64(len(snsJSON)))

	for b.Loop() {
		var decoded SNSMessage
		if err := json.Unmarshal(snsJSON, &decoded); err != nil {
			b.Fatalf("SNS unmarshal failed: %v", err)
		}

		var meta ReceiptMetadata
		if err := json.Unmarshal([]byte(decoded.Message), &meta); err != nil {
			b.Fatalf("metadata unmarshal failed: %v", err)
		}
	}
}

func BenchmarkService_ProcessReceipt(b *testing.B) {
	service := &Service{
		logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
		eventSignatures: make(map[common.Hash]*abi.Event),
	}

	if err := service.loadBorrowABI(); err != nil {
		b.Fatalf("failed to load ABI: %v", err)
	}

	cases := []struct {
		name           string
		logsPerReceipt int
		borrowEvents   int
	}{
		{"AllBorrowEvents_10", 10, 10},
		{"HalfBorrowEvents_10", 10, 5},
		{"FewBorrowEvents_100", 100, 5},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			logs := make([]Log, tc.logsPerReceipt)

			for i := 0; i < tc.logsPerReceipt; i++ {
				var topics []string
				if i < tc.borrowEvents {
					topics = []string{
						"0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0", // signature
						"0x000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", // reserve (indexed)
						"0x000000000000000000000000742d35Cc6634C0532925a3b844Bc9e7595f0bEb0", // onBehalfOf (indexed)
						"0x0000000000000000000000000000000000000000000000000000000000000000", // referralCode (indexed)
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
					TransactionHash:  fmt.Sprintf("0x%064x", i),
					TransactionIndex: "0x0",
					LogIndex:         fmt.Sprintf("0x%x", i),
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

			ctx := context.Background()
			borrowCount := 0

			b.ReportAllocs()

			for b.Loop() {
				for _, log := range receipt.Logs {
					if !service.isBorrowEvent(log) {
						continue
					}

					_, err := service.extractBorrowEventData(log)
					if err != nil {
						b.Fatalf("extractBorrowEventData failed: %v", err)
					}
					borrowCount++
				}
			}

			_ = ctx
		})
	}
}

func BenchmarkBlockchainService_ParseUserReservesData(b *testing.B) {
	cases := []struct {
		name        string
		collaterals int
	}{
		{"NoCollaterals", 0},
		{"FewCollaterals_5", 5},
		{"ManyCollaterals_20", 20},
	}

	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			arrayLength := tc.collaterals
			totalSize := 64 + (arrayLength * 224)
			result := make([]byte, totalSize)

			offset := big.NewInt(32)
			copy(result[0:32], common.LeftPadBytes(offset.Bytes(), 32))

			length := big.NewInt(int64(arrayLength))
			copy(result[32:64], common.LeftPadBytes(length.Bytes(), 32))

			for i := 0; i < arrayLength; i++ {
				structStart := 64 + (i * 224)

				asset := common.HexToAddress(fmt.Sprintf("0x%040x", i+1))
				copy(result[structStart:structStart+32], common.LeftPadBytes(asset.Bytes(), 32))

				balance := new(big.Int).Mul(big.NewInt(1000), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
				copy(result[structStart+32:structStart+64], common.LeftPadBytes(balance.Bytes(), 32))

				copy(result[structStart+64:structStart+96], common.LeftPadBytes(big.NewInt(1).Bytes(), 32))
			}

			b.ReportAllocs()
			b.SetBytes(int64(len(result)))

			for b.Loop() {
				if len(result) < 64 {
					b.Fatal("invalid result")
				}

				offset := new(big.Int).SetBytes(result[0:32]).Uint64()
				arrayLengthClaimed := new(big.Int).SetBytes(result[offset : offset+32]).Uint64()

				const structSize = uint64(224)
				dataStart := offset + 32
				availableBytes := uint64(len(result)) - dataStart
				actualArrayLength := availableBytes / structSize

				arrayLen := arrayLengthClaimed
				if actualArrayLength < arrayLengthClaimed {
					arrayLen = actualArrayLength
				}

				reserves := make([]UserReserveData, 0, arrayLen)

				for i := uint64(0); i < arrayLen; i++ {
					structOffset := dataStart + (i * structSize)
					structData := result[structOffset : structOffset+structSize]

					underlyingAsset := common.BytesToAddress(structData[0:32])
					if underlyingAsset == (common.Address{}) {
						continue
					}

					reserves = append(reserves, UserReserveData{
						UnderlyingAsset:                 underlyingAsset,
						ScaledATokenBalance:             new(big.Int).SetBytes(structData[32:64]),
						UsageAsCollateralEnabledOnUser:  new(big.Int).SetBytes(structData[64:96]).Uint64() != 0,
						StableBorrowRate:                new(big.Int).SetBytes(structData[96:128]),
						ScaledVariableDebt:              new(big.Int).SetBytes(structData[128:160]),
						PrincipalStableDebt:             new(big.Int).SetBytes(structData[160:192]),
						StableBorrowLastUpdateTimestamp: new(big.Int).SetBytes(structData[192:224]),
					})
				}

				if len(reserves) != tc.collaterals {
					b.Fatalf("expected %d collaterals, got %d", tc.collaterals, len(reserves))
				}
			}
		})
	}
}

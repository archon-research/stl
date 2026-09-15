package uniswapv4indexer

import (
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

const (
	scanPosmAddress = "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e"
	scanHolderTopic = "0x000000000000000000000000e588dDd10E5Ca07c0Cf6a1F0e0e6b0e1d1b2C3d4"
	scanZeroTopic   = "0x0000000000000000000000000000000000000000000000000000000000000000"
	scanTxHash      = "0x1111111111111111111111111111111111111111111111111111111111111111"
)

func scanPosm() RegisteredPositionManager {
	return RegisteredPositionManager{ID: 7, Address: common.HexToAddress(scanPosmAddress)}
}

// tokenIDTopic renders a token id as the 32-byte hex word topics[3] carries.
func tokenIDTopic(tokenID int64) string {
	return common.BigToHash(big.NewInt(tokenID)).Hex()
}

func scannedTransferLog(mut ...func(*shared.Log)) shared.Log {
	log := shared.Log{
		Address:          scanPosmAddress,
		Topics:           []string{abis.TransferTopic0().Hex(), scanZeroTopic, scanHolderTopic, tokenIDTopic(388720)},
		Data:             "0x",
		BlockHash:        scanTxHash,
		BlockNumber:      "0x18a1d36",
		BlockTimestamp:   "0x6793d267",
		TransactionHash:  scanTxHash,
		TransactionIndex: "0x1",
		LogIndex:         "0x2a",
	}
	for _, m := range mut {
		m(&log)
	}
	return log
}

func TestNFTTransfersFromLogs_TakesEveryFieldFromTheLog(t *testing.T) {
	got, err := NFTTransfersFromLogs([]shared.Log{scannedTransferLog()}, scanPosm())
	if err != nil {
		t.Fatalf("NFTTransfersFromLogs: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("decoded %d transfers, want 1", len(got))
	}
	transfer := got[0]
	if transfer.PositionManagerID != 7 {
		t.Errorf("PositionManagerID = %d, want 7", transfer.PositionManagerID)
	}
	if transfer.TokenID.Cmp(big.NewInt(388720)) != 0 {
		t.Errorf("TokenID = %s, want 388720", transfer.TokenID)
	}
	if transfer.BlockNumber != 0x18a1d36 {
		t.Errorf("BlockNumber = %d, want %d", transfer.BlockNumber, 0x18a1d36)
	}
	if want := time.Unix(0x6793d267, 0).UTC(); !transfer.BlockTimestamp.Equal(want) {
		t.Errorf("BlockTimestamp = %s, want %s", transfer.BlockTimestamp, want)
	}
	if transfer.BlockVersion != 0 {
		t.Errorf("BlockVersion = %d, want 0: every scanned row is past finality", transfer.BlockVersion)
	}
	if transfer.LogIndex != 42 {
		t.Errorf("LogIndex = %d, want 42", transfer.LogIndex)
	}
	if transfer.From != (common.Address{}) {
		t.Errorf("From = %s, want the zero address (a mint)", transfer.From)
	}
	if want := common.HexToAddress("0xe588dDd10E5Ca07c0Cf6a1F0e0e6b0e1d1b2C3d4"); transfer.To != want {
		t.Errorf("To = %s, want %s", transfer.To, want)
	}
}

// The address filter is the only thing separating a posm transfer from any
// ERC-20 one, so each of these means the filter or the registry is wrong. None
// may be skipped: a skipped log is a hole no rerun would look for again.
func TestNFTTransfersFromLogs_RefusesALogTheFilterShouldNotHaveReturned(t *testing.T) {
	tests := []struct {
		name    string
		mut     func(*shared.Log)
		wantErr string
	}{
		{"another contract's log", func(l *shared.Log) {
			l.Address = "0x000000000004444c5dc75cB358380D2e3dE08A90"
		}, "not the PositionManager"},
		{"another event's log", func(l *shared.Log) {
			l.Topics[0] = "0x40c10f1900000000000000000000000000000000000000000000000000000000"
		}, "topic0 is not Transfer"},
		{"an ERC-20 Transfer, which shares topic0", func(l *shared.Log) {
			l.Topics = l.Topics[:3]
			l.Data = "0x" + strings.Repeat("0", 63) + "1"
		}, "carries 3 topics, want 4"},
		{"no blockTimestamp", func(l *shared.Log) {
			l.BlockTimestamp = ""
		}, "carries no blockTimestamp"},
		{"a zero blockTimestamp", func(l *shared.Log) {
			l.BlockTimestamp = "0x0"
		}, "blockTimestamp 0"},
		{"an unparsable blockTimestamp", func(l *shared.Log) {
			l.BlockTimestamp = "not-hex"
		}, "parsing blockTimestamp"},
		{"an unparsable block number", func(l *shared.Log) {
			l.BlockNumber = "not-hex"
		}, "parsing block number"},
		{"a truncated topic", func(l *shared.Log) {
			l.Topics[3] = "0xdeadbeef"
		}, "is not a 32-byte hex word"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NFTTransfersFromLogs([]shared.Log{scannedTransferLog(tc.mut)}, scanPosm())
			if err == nil {
				t.Fatalf("NFTTransfersFromLogs succeeded, want an error containing %q", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestNFTTransfersFromLogs_DecodesABurnToTheZeroAddress(t *testing.T) {
	burn := scannedTransferLog(func(l *shared.Log) {
		l.Topics[1] = scanHolderTopic
		l.Topics[2] = scanZeroTopic
	})
	got, err := NFTTransfersFromLogs([]shared.Log{burn}, scanPosm())
	if err != nil {
		t.Fatalf("NFTTransfersFromLogs: %v", err)
	}
	if got[0].To != (common.Address{}) {
		t.Errorf("To = %s, want the zero address (a burn)", got[0].To)
	}
}

package uniswapv4indexer

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

const (
	// The singleton v4 PositionManager (ERC-721) on mainnet, and the id of its
	// registry row in the fixtures below.
	positionManagerAddr   = "0xbD216513d74C8cf14cf4747E6AaA6420FF64ee9e"
	positionManagerRowID  = int64(11)
	erc721TransferTopic0  = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	erc721ApprovalTopic0  = "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"
	posmMintFixtureTxHash = "0x4e63fcc0dd42a2b317e77d17e236cadf77464a08ccece33a354bd8648b5f7419"
	posmMoveFixtureTxHash = "0x41904e8dc4f2218019baaf8a7195e264ccd1530f5f56ae0db0027c1f0772c6e4"
)

func testPositionManager() RegisteredPositionManager {
	return RegisteredPositionManager{
		ID:      positionManagerRowID,
		Address: common.HexToAddress(positionManagerAddr),
	}
}

// posmLog builds a log verbatim from an on-chain topics capture, the way rawLog
// does for the PoolManager. ERC-721 Transfer indexes all three arguments, so a
// real log's data block is always empty.
func posmLog(txHash string, topics []string, data, logIndexHex string) shared.Log {
	return shared.Log{
		Address:         positionManagerAddr,
		Topics:          topics,
		Data:            data,
		TransactionHash: txHash,
		LogIndex:        logIndexHex,
	}
}

// posmMintFixtureLog is the FIRST posm Transfer ever emitted on mainnet: block
// 21695956, log index 0x43, minting token 1 to 0x4423B0D6…
func posmMintFixtureLog() shared.Log {
	return posmLog(posmMintFixtureTxHash, []string{
		erc721TransferTopic0,
		"0x0000000000000000000000000000000000000000000000000000000000000000",
		"0x0000000000000000000000004423b0d6955af39b48cf215577a79ce574299d3f",
		"0x0000000000000000000000000000000000000000000000000000000000000001",
	}, "0x", "0x43")
}

// posmMoveFixtureLog is a later mainnet Transfer between two non-zero holders:
// block 25873334, log index 0x4c3, token 0x5ee70.
func posmMoveFixtureLog() shared.Log {
	return posmLog(posmMoveFixtureTxHash, []string{
		erc721TransferTopic0,
		"0x0000000000000000000000003b0a17a75a14eaaef42002a4891acf8f9fd8a72e",
		"0x000000000000000000000000e588ddd13a8bdbee578eaa7c4fd9780180b2f10c",
		"0x000000000000000000000000000000000000000000000000000000000005ee70",
	}, "0x", "0x4c3")
}

func decodePosmFixture(t *testing.T, log shared.Log) (DecodedEvents, map[int64]bool) {
	t.Helper()
	got, touched, err := DecodeEvents(receiptOf(log), poolsByIDOf(decodeTestPool(7, swapFixturePoolID)),
		poolManagerAddress(), testPositionManager(), blockNumber, blockVer, blockTS)
	if err != nil {
		t.Fatalf("DecodeEvents: %v", err)
	}
	return got, touched
}

// The expectations are hand-decoded from the topic words above, never derived by
// running the ABI: topics[3] read as a big-endian integer is the token id, and
// the low 20 bytes of topics[1]/topics[2] are the two holders.
func TestDecodeEvents_PositionManagerTransfer(t *testing.T) {
	tests := []struct {
		name      string
		log       shared.Log
		wantTx    string
		wantLog   int
		wantFrom  string
		wantTo    string
		wantToken *big.Int
	}{
		{
			name:      "mint of token 1",
			log:       posmMintFixtureLog(),
			wantTx:    posmMintFixtureTxHash,
			wantLog:   67,
			wantFrom:  "0x0000000000000000000000000000000000000000",
			wantTo:    "0x4423B0D6955aF39B48cf215577a79Ce574299D3f",
			wantToken: big.NewInt(1),
		},
		{
			name:      "move between two holders",
			log:       posmMoveFixtureLog(),
			wantTx:    posmMoveFixtureTxHash,
			wantLog:   1219,
			wantFrom:  "0x3b0a17a75A14EAaEF42002a4891AcF8F9fD8A72E",
			wantTo:    "0xe588dDd13a8bDBee578eAa7c4Fd9780180b2f10C",
			wantToken: big.NewInt(388720),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, _ := decodePosmFixture(t, tt.log)
			if len(got.NFTTransfers) != 1 {
				t.Fatalf("NFTTransfers = %d, want 1", len(got.NFTTransfers))
			}
			transfer := got.NFTTransfers[0]
			if transfer.PositionManagerID != positionManagerRowID {
				t.Errorf("PositionManagerID = %d, want %d", transfer.PositionManagerID, positionManagerRowID)
			}
			if transfer.TokenID.Cmp(tt.wantToken) != 0 {
				t.Errorf("TokenID = %s, want %s", transfer.TokenID, tt.wantToken)
			}
			if got := transfer.From; got != common.HexToAddress(tt.wantFrom) {
				t.Errorf("From = %s, want %s", got, tt.wantFrom)
			}
			if got := transfer.To; got != common.HexToAddress(tt.wantTo) {
				t.Errorf("To = %s, want %s", got, tt.wantTo)
			}
			if got := transfer.TxHash; got != common.HexToHash(tt.wantTx) {
				t.Errorf("TxHash = %s, want %s", got, tt.wantTx)
			}
			if transfer.LogIndex != tt.wantLog {
				t.Errorf("LogIndex = %d, want %d", transfer.LogIndex, tt.wantLog)
			}
			if transfer.BlockNumber != blockNumber || transfer.BlockVersion != blockVer || !transfer.BlockTimestamp.Equal(blockTS) {
				t.Errorf("block coords = (%d, %d, %s), want (%d, %d, %s)",
					transfer.BlockNumber, transfer.BlockVersion, transfer.BlockTimestamp, blockNumber, blockVer, blockTS)
			}
		})
	}
}

// ERC-20's Transfer hashes to the same topic0 with a 3-topic layout, so a
// mis-set posm address would feed a token contract's transfers into this table.
// The generic ABI decoder rejects the arity too, but only as "topic/field count
// mismatch", which sends an operator hunting an ABI bug; the assertion is on
// the message because that difference is the guard's whole reason to exist.
func TestDecodeEvents_PositionManagerTransferWithWrongTopicCountErrors(t *testing.T) {
	tests := []struct {
		name   string
		topics []string
		data   string
	}{
		{
			name: "erc20-shaped transfer carrying the amount in data",
			topics: []string{
				erc721TransferTopic0,
				"0x0000000000000000000000003b0a17a75a14eaaef42002a4891acf8f9fd8a72e",
				"0x000000000000000000000000e588ddd13a8bdbee578eaa7c4fd9780180b2f10c",
			},
			data: "0x000000000000000000000000000000000000000000000000000000000005ee70",
		},
		{
			name: "a fourth indexed argument",
			topics: []string{
				erc721TransferTopic0,
				"0x0000000000000000000000003b0a17a75a14eaaef42002a4891acf8f9fd8a72e",
				"0x000000000000000000000000e588ddd13a8bdbee578eaa7c4fd9780180b2f10c",
				"0x000000000000000000000000000000000000000000000000000000000005ee70",
				"0x0000000000000000000000000000000000000000000000000000000000000001",
			},
			data: "0x",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeEvents(receiptOf(posmLog(posmMoveFixtureTxHash, tt.topics, tt.data, "0x4c3")),
				poolsByIDOf(decodeTestPool(7, swapFixturePoolID)),
				poolManagerAddress(), testPositionManager(), blockNumber, blockVer, blockTS)
			if err == nil {
				t.Fatal("DecodeEvents: want an error for a Transfer log that is not 4 topics, got nil")
			}
			if want := "topics, want 4"; !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q: the arity guard must name the topic count, not defer to the ABI decoder", err, want)
			}
			if want := "address must be wrong"; !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not contain %q: the guard must point at the PositionManager address", err, want)
			}
		})
	}
}

// The posm emits Approval, ApprovalForAll and subscription events too. None is
// indexed here, and none is an error: only Transfer answers who holds a token.
func TestDecodeEvents_PositionManagerNonTransferLogIsIgnored(t *testing.T) {
	log := posmLog(posmMoveFixtureTxHash, []string{
		erc721ApprovalTopic0,
		"0x0000000000000000000000003b0a17a75a14eaaef42002a4891acf8f9fd8a72e",
		"0x000000000000000000000000e588ddd13a8bdbee578eaa7c4fd9780180b2f10c",
		"0x000000000000000000000000000000000000000000000000000000000005ee70",
	}, "0x", "0x1")

	got, touched := decodePosmFixture(t, log)
	if len(got.NFTTransfers) != 0 {
		t.Errorf("NFTTransfers = %d, want 0", len(got.NFTTransfers))
	}
	if len(got.Captured) != 0 {
		t.Errorf("Captured = %d, want 0", len(got.Captured))
	}
	if len(touched) != 0 {
		t.Errorf("touched = %v, want empty", touched)
	}
}

// protocol_event is scoped to the PoolManager's protocol_id, so a posm log
// mirrored into the capture net would be filed under the wrong protocol; and a
// transfer touches no pool, so it must not pull one into the snapshot due set.
func TestDecodeEvents_PositionManagerTransferIsNeitherCapturedNorTouchesAPool(t *testing.T) {
	got, touched := decodePosmFixture(t, posmMoveFixtureLog())
	if len(got.Captured) != 0 {
		t.Errorf("Captured = %d, want 0 (protocol_event belongs to the PoolManager's protocol)", len(got.Captured))
	}
	if len(touched) != 0 {
		t.Errorf("touched = %v, want empty (an NFT transfer touches no pool)", touched)
	}
}

// A malformed topic must not reach common.HexToHash, which left-pads a short
// word and truncates at the first non-hex character.
func TestDecodeEvents_PositionManagerMalformedTopicErrors(t *testing.T) {
	log := posmMoveFixtureLog()
	log.Topics[3] = corruptHexWord(log.Topics[3])

	_, _, err := DecodeEvents(receiptOf(log), poolsByIDOf(decodeTestPool(7, swapFixturePoolID)),
		poolManagerAddress(), testPositionManager(), blockNumber, blockVer, blockTS)
	if err == nil {
		t.Fatal("DecodeEvents: want an error for a corrupted token id topic, got nil")
	}
}

// A posm whose registry row is missing would arrive as the zero address, and
// LogBelongsTo would then claim every log emitted by address(0).
func TestPositionManagerFor_RejectsAMixedRegistry(t *testing.T) {
	first := servicePool()
	first.PositionManagerID = positionManagerRowID
	first.PositionManager = common.HexToAddress(positionManagerAddr)
	second := secondServicePool()
	second.PositionManagerID = positionManagerRowID
	second.PositionManager = common.HexToAddress(positionManagerAddr)

	if got, err := PositionManagerFor([]RegisteredPool{first, second}); err != nil {
		t.Fatalf("PositionManagerFor on one deployment: %v", err)
	} else if got != testPositionManager() {
		t.Errorf("PositionManagerFor = %+v, want %+v", got, testPositionManager())
	}

	for _, tc := range []struct {
		name string
		mut  func(*RegisteredPool)
	}{
		{"two addresses", func(p *RegisteredPool) { p.PositionManager = common.HexToAddress("0xdead") }},
		{"two registry rows", func(p *RegisteredPool) { p.PositionManagerID = positionManagerRowID + 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			odd := second
			tc.mut(&odd)
			if _, err := PositionManagerFor([]RegisteredPool{first, odd}); err == nil {
				t.Fatal("PositionManagerFor: want an error for a registry naming two PositionManagers, got nil")
			}
		})
	}
}

// Both registry tables reach their address through protocol_id, so a posm row
// pointing at the PoolManager's protocol row collides the two addresses. The
// posm branch runs first in decodeLog, so every pool event would then vanish
// with nothing raising an error.
func TestNewUniswapV4Service_RefusesAPositionManagerThatIsThePoolManager(t *testing.T) {
	pool := servicePool()
	pool.PositionManager = pool.PoolManager

	deps, _, _, _ := validServiceDeps(t, []RegisteredPool{pool})
	_, err := NewUniswapV4Service(context.Background(), deps)
	if err == nil {
		t.Fatal("NewUniswapV4Service: want an error when the PositionManager address is the PoolManager's, got nil")
	}
	if !strings.Contains(err.Error(), pool.PoolManager.String()) {
		t.Errorf("error %q does not name the colliding address %s", err, pool.PoolManager)
	}
}

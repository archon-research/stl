package morpho_v2_bootstrap

import (
	"errors"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
)

func TestChunkBlockRange(t *testing.T) {
	tests := []struct {
		name       string
		from, to   int64
		size       int64
		wantRanges []blockRange
	}{
		{
			name: "single block range",
			from: 100, to: 100, size: 10,
			wantRanges: []blockRange{{From: 100, To: 100}},
		},
		{
			name: "range shorter than one chunk",
			from: 100, to: 104, size: 10,
			wantRanges: []blockRange{{From: 100, To: 104}},
		},
		{
			name: "range that divides exactly",
			from: 100, to: 119, size: 10,
			wantRanges: []blockRange{{From: 100, To: 109}, {From: 110, To: 119}},
		},
		{
			name: "range with a short trailing chunk",
			from: 100, to: 122, size: 10,
			wantRanges: []blockRange{{From: 100, To: 109}, {From: 110, To: 119}, {From: 120, To: 122}},
		},
		{
			name: "chunks of one block",
			from: 7, to: 9, size: 1,
			wantRanges: []blockRange{{From: 7, To: 7}, {From: 8, To: 8}, {From: 9, To: 9}},
		},
		{
			name: "inverted range yields nothing",
			from: 200, to: 100, size: 10,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := chunkBlockRange(tc.from, tc.to, tc.size)
			if len(got) != len(tc.wantRanges) {
				t.Fatalf("got %d chunks %v, want %d %v", len(got), got, len(tc.wantRanges), tc.wantRanges)
			}
			for i := range got {
				if got[i] != tc.wantRanges[i] {
					t.Fatalf("chunk %d = %+v, want %+v", i, got[i], tc.wantRanges[i])
				}
			}
		})
	}
}

// TestChunkBlockRange_CoversEveryBlockExactlyOnce is the property that matters
// for correctness: a gap silently drops config events, an overlap re-replays
// them. Idempotent writes make an overlap harmless but a gap permanent.
func TestChunkBlockRange_CoversEveryBlockExactlyOnce(t *testing.T) {
	const from, to, size = 1_000, 1_097, 7
	seen := make(map[int64]int)
	for _, r := range chunkBlockRange(from, to, size) {
		if r.From > r.To {
			t.Fatalf("chunk %+v is inverted", r)
		}
		for b := r.From; b <= r.To; b++ {
			seen[b]++
		}
	}
	for b := int64(from); b <= to; b++ {
		if seen[b] != 1 {
			t.Fatalf("block %d covered %d times, want exactly 1", b, seen[b])
		}
	}
	if len(seen) != to-from+1 {
		t.Fatalf("covered %d blocks, want %d (chunks reach outside [from,to])", len(seen), to-from+1)
	}
}

func TestBatchAddresses(t *testing.T) {
	addr := func(i int64) common.Address { return common.BigToAddress(big.NewInt(i)) }
	all := []common.Address{addr(1), addr(2), addr(3), addr(4), addr(5)}

	tests := []struct {
		name      string
		addresses []common.Address
		size      int
		want      [][]common.Address
	}{
		{
			name:      "exact multiple",
			addresses: all[:4],
			size:      2,
			want:      [][]common.Address{{addr(1), addr(2)}, {addr(3), addr(4)}},
		},
		{
			name:      "short trailing batch",
			addresses: all,
			size:      2,
			want:      [][]common.Address{{addr(1), addr(2)}, {addr(3), addr(4)}, {addr(5)}},
		},
		{
			name:      "batch larger than the input",
			addresses: all[:2],
			size:      100,
			want:      [][]common.Address{{addr(1), addr(2)}},
		},
		{
			name:      "no addresses",
			addresses: nil,
			size:      10,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := batchAddresses(tc.addresses, tc.size)
			if len(got) != len(tc.want) {
				t.Fatalf("got %d batches %v, want %d", len(got), got, len(tc.want))
			}
			for i := range got {
				if len(got[i]) != len(tc.want[i]) {
					t.Fatalf("batch %d has %d addresses, want %d", i, len(got[i]), len(tc.want[i]))
				}
				for j := range got[i] {
					if got[i][j] != tc.want[i][j] {
						t.Fatalf("batch %d address %d = %s, want %s", i, j, got[i][j], tc.want[i][j])
					}
				}
			}
		})
	}
}

// TestSortLogs orders strictly by (block, log index). Ordering no longer decides
// the answers (see sortLogs); logs arrive interleaved because one chunk is
// fetched as several per-address-batch requests.
func TestSortLogs(t *testing.T) {
	logs := []ethtypes.Log{
		{BlockNumber: 20, Index: 1},
		{BlockNumber: 10, Index: 5},
		{BlockNumber: 20, Index: 0},
		{BlockNumber: 10, Index: 2},
	}
	sortLogs(logs)

	want := []struct {
		block uint64
		index uint
	}{
		{10, 2}, {10, 5}, {20, 0}, {20, 1},
	}
	for i, w := range want {
		if logs[i].BlockNumber != w.block || logs[i].Index != w.index {
			t.Fatalf("logs[%d] = (block %d, index %d), want (block %d, index %d)",
				i, logs[i].BlockNumber, logs[i].Index, w.block, w.index)
		}
	}
}

// TestToSharedLog converts a go-ethereum log into the wire-shaped shared.Log the
// handlers decode, using the same hex encodings the live SQS path receives from
// the node — anything else and the replayed audit rows would not match live ones.
func TestToSharedLog(t *testing.T) {
	topic0 := common.HexToHash("0x1111111111111111111111111111111111111111111111111111111111111111")
	topic1 := common.HexToHash("0x2222222222222222222222222222222222222222222222222222222222222222")
	address := common.HexToAddress("0x7481968709b8f155652D42ebf468b22945907dC2")
	txHash := common.HexToHash("0x3333333333333333333333333333333333333333333333333333333333333333")
	blockHash := common.HexToHash("0x4444444444444444444444444444444444444444444444444444444444444444")

	got := toSharedLog(ethtypes.Log{
		Address:     address,
		Topics:      []common.Hash{topic0, topic1},
		Data:        []byte{0xde, 0xad, 0xbe, 0xef},
		BlockNumber: 26,
		BlockHash:   blockHash,
		TxHash:      txHash,
		TxIndex:     3,
		Index:       11,
	})

	if got.Address != address.Hex() {
		t.Errorf("Address = %q, want %q", got.Address, address.Hex())
	}
	if len(got.Topics) != 2 || got.Topics[0] != topic0.Hex() || got.Topics[1] != topic1.Hex() {
		t.Errorf("Topics = %v, want [%s %s]", got.Topics, topic0.Hex(), topic1.Hex())
	}
	if got.Data != "0xdeadbeef" {
		t.Errorf("Data = %q, want 0xdeadbeef", got.Data)
	}
	if got.BlockNumber != "0x1a" {
		t.Errorf("BlockNumber = %q, want 0x1a", got.BlockNumber)
	}
	if got.LogIndex != "0xb" {
		t.Errorf("LogIndex = %q, want 0xb", got.LogIndex)
	}
	if got.TransactionIndex != "0x3" {
		t.Errorf("TransactionIndex = %q, want 0x3", got.TransactionIndex)
	}
	if got.TransactionHash != txHash.Hex() || got.BlockHash != blockHash.Hex() {
		t.Errorf("tx/block hash = %q/%q, want %q/%q", got.TransactionHash, got.BlockHash, txHash.Hex(), blockHash.Hex())
	}
}

// TestIsRangeTooLargeError separates the one provider signal the sweep responds
// to by narrowing its block range from every other failure, which must bubble.
// Treating an unrelated error as "too large" would silently halve down to
// single blocks and then fail anyway, hiding the real cause.
func TestIsRangeTooLargeError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "no error", err: nil, want: false},
		{
			name: "alchemy result cap",
			err:  errors.New("query returned more than 10000 results"),
			want: true,
		},
		{
			name: "alchemy block-range cap",
			err:  errors.New("You can make eth_getLogs requests with up to a 500 block range"),
			want: true,
		},
		{
			name: "response size cap",
			err:  errors.New("Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range"),
			want: true,
		},
		{
			name: "generic range-too-large phrasing",
			err:  errors.New("block range is too large"),
			want: true,
		},
		{
			name: "a malformed-parameter error mentioning a block range must still bubble",
			err:  errors.New("invalid block range params"),
			want: false,
		},
		{
			name: "query timeout asking for a smaller range",
			err:  errors.New("query timeout exceeded. Consider reducing your block range"),
			want: true,
		},
		{
			name: "wrapped provider error is still detected",
			err:  fmt.Errorf("eth_getLogs [100,200]: %w", errors.New("query returned more than 10000 results")),
			want: true,
		},
		{
			name: "unrelated rpc failure must bubble",
			err:  errors.New("connection reset by peer"),
			want: false,
		},
		{
			name: "auth failure must bubble",
			err:  errors.New("401 Unauthorized: invalid api key"),
			want: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := isRangeTooLargeError(tc.err); got != tc.want {
				t.Fatalf("isRangeTooLargeError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

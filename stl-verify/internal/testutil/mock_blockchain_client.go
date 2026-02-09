package testutil

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// BlockTestData holds all the data for a single block in MockBlockchainClient.
type BlockTestData struct {
	Header   outbound.BlockHeader
	Receipts json.RawMessage
	Traces   json.RawMessage
	Blobs    json.RawMessage
}

// MockBlockchainClient implements outbound.BlockchainClient for testing.
// It supports multiple behavior modes controlled by exported fields.
type MockBlockchainClient struct {
	mu     sync.RWMutex
	blocks map[int64]BlockTestData
	Delay  time.Duration

	// ExtraBlocks holds uncle/orphan blocks accessible by hash only,
	// not as the canonical block at any number.
	ExtraBlocks map[string]BlockTestData

	// HashLookupErr is returned by all ByHash methods.
	// If HashLookupErrFor is set, the error only applies to that hash.
	HashLookupErr    error
	HashLookupErrFor string

	// ReturnNilForUnknownHash makes GetBlockByHash return (nil, nil) instead
	// of an error when a hash is not found, matching real RPC behavior.
	ReturnNilForUnknownHash bool

	// UseBlockErrForMissing makes GetBlocksBatch set BlockErr on missing blocks
	// instead of leaving them empty.
	UseBlockErrForMissing bool
}

func NewMockBlockchainClient() *MockBlockchainClient {
	return &MockBlockchainClient{
		blocks:      make(map[int64]BlockTestData),
		ExtraBlocks: make(map[string]BlockTestData),
	}
}

// AddBlock adds a canonical block. If parentHash is empty and num > 0,
// a default parent hash is generated from num-1.
func (m *MockBlockchainClient) AddBlock(num int64, parentHash string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	hash := fmt.Sprintf("0x%064x", num)
	if parentHash == "" && num > 0 {
		parentHash = fmt.Sprintf("0x%064x", num-1)
	}

	m.blocks[num] = BlockTestData{
		Header: outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", num),
			Hash:       hash,
			ParentHash: parentHash,
			Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
		},
		Receipts: json.RawMessage(`[]`),
		Traces:   json.RawMessage(`[]`),
		Blobs:    json.RawMessage(`[]`),
	}
}

// GetHeader returns the stored header for a block number.
func (m *MockBlockchainClient) GetHeader(num int64) outbound.BlockHeader {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.blocks[num].Header
}

// AddUncle adds a block accessible by hash but not as the canonical block at any number.
func (m *MockBlockchainClient) AddUncle(num int64, hash string, parentHash string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ExtraBlocks[hash] = BlockTestData{
		Header: outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", num),
			Hash:       hash,
			ParentHash: parentHash,
			Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
		},
		Receipts: json.RawMessage(`[]`),
		Traces:   json.RawMessage(`[]`),
		Blobs:    json.RawMessage(`[]`),
	}
}

// SetBlockHeader overrides a block's header data at a given number.
func (m *MockBlockchainClient) SetBlockHeader(num int64, hash, parentHash string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.blocks[num] = BlockTestData{
		Header: outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", num),
			Hash:       hash,
			ParentHash: parentHash,
			Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
		},
		Receipts: json.RawMessage(`[]`),
		Traces:   json.RawMessage(`[]`),
		Blobs:    json.RawMessage(`[]`),
	}
}

func (m *MockBlockchainClient) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.Delay > 0 {
		time.Sleep(m.Delay)
	}

	if bd, ok := m.blocks[blockNum]; ok {
		data, _ := json.Marshal(bd.Header)
		return data, nil
	}
	return nil, fmt.Errorf("block %d not found", blockNum)
}

func (m *MockBlockchainClient) GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*outbound.BlockHeader, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.HashLookupErr != nil && (m.HashLookupErrFor == "" || m.HashLookupErrFor == hash) {
		return nil, m.HashLookupErr
	}

	for _, bd := range m.blocks {
		if bd.Header.Hash == hash {
			h := bd.Header
			return &h, nil
		}
	}

	if bd, ok := m.ExtraBlocks[hash]; ok {
		h := bd.Header
		return &h, nil
	}

	if m.ReturnNilForUnknownHash {
		return nil, nil
	}
	return nil, fmt.Errorf("block %s not found", hash)
}

func (m *MockBlockchainClient) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.HashLookupErr != nil && (m.HashLookupErrFor == "" || m.HashLookupErrFor == hash) {
		return nil, m.HashLookupErr
	}

	for _, bd := range m.blocks {
		if bd.Header.Hash == hash {
			data, _ := json.Marshal(bd.Header)
			return data, nil
		}
	}
	return nil, fmt.Errorf("block %s not found", hash)
}

func (m *MockBlockchainClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.Receipts, nil
	}
	return nil, fmt.Errorf("receipts for block %d not found", blockNum)
}

func (m *MockBlockchainClient) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.HashLookupErr != nil && (m.HashLookupErrFor == "" || m.HashLookupErrFor == hash) {
		return nil, m.HashLookupErr
	}

	for _, bd := range m.blocks {
		if bd.Header.Hash == hash {
			return bd.Receipts, nil
		}
	}
	return nil, fmt.Errorf("receipts for block %s not found", hash)
}

func (m *MockBlockchainClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.Traces, nil
	}
	return nil, fmt.Errorf("traces for block %d not found", blockNum)
}

func (m *MockBlockchainClient) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.HashLookupErr != nil && (m.HashLookupErrFor == "" || m.HashLookupErrFor == hash) {
		return nil, m.HashLookupErr
	}

	for _, bd := range m.blocks {
		if bd.Header.Hash == hash {
			return bd.Traces, nil
		}
	}
	return nil, fmt.Errorf("traces for block %s not found", hash)
}

func (m *MockBlockchainClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if bd, ok := m.blocks[blockNum]; ok {
		return bd.Blobs, nil
	}
	return nil, fmt.Errorf("blobs for block %d not found", blockNum)
}

func (m *MockBlockchainClient) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.HashLookupErr != nil && (m.HashLookupErrFor == "" || m.HashLookupErrFor == hash) {
		return nil, m.HashLookupErr
	}

	for _, bd := range m.blocks {
		if bd.Header.Hash == hash {
			return bd.Blobs, nil
		}
	}
	return nil, fmt.Errorf("blobs for block %s not found", hash)
}

func (m *MockBlockchainClient) GetCurrentBlockNumber(ctx context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var max int64
	for num := range m.blocks {
		if num > max {
			max = num
		}
	}
	return max, nil
}

func (m *MockBlockchainClient) GetBlocksBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]outbound.BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.Delay > 0 {
		time.Sleep(m.Delay)
	}

	result := make([]outbound.BlockData, len(blockNums))
	for i, num := range blockNums {
		result[i] = outbound.BlockData{BlockNumber: num}
		if bd, ok := m.blocks[num]; ok {
			blockJSON, _ := json.Marshal(bd.Header)
			result[i].Block = blockJSON
			result[i].Receipts = bd.Receipts
			result[i].Traces = bd.Traces
			result[i].Blobs = bd.Blobs
		} else if m.UseBlockErrForMissing {
			result[i].BlockErr = fmt.Errorf("block %d not found", num)
		}
	}
	return result, nil
}

func (m *MockBlockchainClient) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.Delay > 0 {
		time.Sleep(m.Delay)
	}

	if m.HashLookupErr != nil && (m.HashLookupErrFor == "" || m.HashLookupErrFor == hash) {
		return outbound.BlockData{}, m.HashLookupErr
	}

	for _, bd := range m.blocks {
		if bd.Header.Hash == hash {
			blockJSON, _ := json.Marshal(bd.Header)
			return outbound.BlockData{
				BlockNumber: blockNum,
				Block:       blockJSON,
				Receipts:    bd.Receipts,
				Traces:      bd.Traces,
				Blobs:       bd.Blobs,
			}, nil
		}
	}
	return outbound.BlockData{}, fmt.Errorf("block %s not found", hash)
}

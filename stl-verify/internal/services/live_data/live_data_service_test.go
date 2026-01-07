package live_data

import (
	"testing"
)

func TestLiveService_AddToUnfinalizedChain_MaintainsSortedOrder(t *testing.T) {
	service := &LiveService{
		config: LiveConfig{
			MaxUnfinalizedBlocks: 100,
		},
		unfinalizedBlocks: make([]LightBlock, 0),
	}

	// Add blocks out of order
	blocks := []LightBlock{
		{Number: 5, Hash: "0x5", ParentHash: "0x4"},
		{Number: 3, Hash: "0x3", ParentHash: "0x2"},
		{Number: 7, Hash: "0x7", ParentHash: "0x6"},
		{Number: 1, Hash: "0x1", ParentHash: "0x0"},
		{Number: 4, Hash: "0x4", ParentHash: "0x3"},
		{Number: 2, Hash: "0x2", ParentHash: "0x1"},
		{Number: 6, Hash: "0x6", ParentHash: "0x5"},
	}

	for _, b := range blocks {
		service.addBlock(b)
	}

	// Verify sorted order
	for i := 1; i < len(service.unfinalizedBlocks); i++ {
		if service.unfinalizedBlocks[i].Number < service.unfinalizedBlocks[i-1].Number {
			t.Errorf("chain not sorted at index %d: %d < %d",
				i, service.unfinalizedBlocks[i].Number, service.unfinalizedBlocks[i-1].Number)
		}
	}

	// Verify all blocks present
	if len(service.unfinalizedBlocks) != 7 {
		t.Errorf("expected 7 blocks, got %d", len(service.unfinalizedBlocks))
	}
}

func TestLiveService_AddBlock_HandlesForks(t *testing.T) {
	service := &LiveService{
		config: LiveConfig{
			MaxUnfinalizedBlocks: 100,
		},
		unfinalizedBlocks: make([]LightBlock, 0),
	}

	// Add two blocks at same height with different hashes (fork)
	block1 := LightBlock{Number: 5, Hash: "0x5a", ParentHash: "0x4"}
	block2 := LightBlock{Number: 5, Hash: "0x5b", ParentHash: "0x4"}

	service.addBlock(block1)
	service.addBlock(block2)

	// Both should be present (reorg handling will clean up later)
	if len(service.unfinalizedBlocks) != 2 {
		t.Errorf("expected 2 blocks for fork, got %d", len(service.unfinalizedBlocks))
	}
}

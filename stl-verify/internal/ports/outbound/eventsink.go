package outbound

import (
	"context"
	"fmt"
	"time"
)

// EventType represents the type of event.
type EventType string

// Event type constants.
const (
	EventTypeBlock EventType = "block"
)

// Event is the interface that all event types implement.
type Event interface {
	// EventType returns the type of the event.
	EventType() EventType
	// GetBlockNumber returns the block number.
	GetBlockNumber() int64
	// GetChainID returns the chain ID.
	GetChainID() int64
	// DeduplicationID returns a deterministic ID for SNS FIFO deduplication.
	// This must be based only on immutable block properties (hash, version),
	// NOT timestamps or other instance-specific values.
	DeduplicationID() string
}

// BlockEvent is published when block data is ready in cache.
// Consumers should derive cache keys using the convention:
//
//	stl:{chainId}:{blockNumber}:{version}:{dataType}
//
// where dataType is one of: block, receipts, traces, blobs
// Example: stl:1:12345:0:block, stl:1:12345:0:receipts
type BlockEvent struct {
	// ChainID identifies which blockchain this data is from.
	ChainID int64 `json:"chainId"`

	// BlockNumber is the block number.
	BlockNumber int64 `json:"blockNumber"`

	// Version is incremented each time this block number is reorged.
	// First occurrence of a block has version 0. After a reorg, the new
	// canonical block at the same height has version = previous_version + 1.
	Version int `json:"version"`

	// BlockHash is the block hash.
	BlockHash string `json:"blockHash"`

	// ParentHash is the parent block's hash.
	ParentHash string `json:"parentHash"`

	// BlockTimestamp is when the block was produced (unix timestamp).
	BlockTimestamp int64 `json:"blockTimestamp"`

	// ReceivedAt is when we received and cached this data.
	ReceivedAt time.Time `json:"receivedAt"`

	// IsReorg indicates this block is part of a chain reorganization.
	IsReorg bool `json:"isReorg,omitempty"`

	// IsBackfill indicates this data was fetched during reconnection backfill.
	IsBackfill bool `json:"isBackfill,omitempty"`
}

func (e BlockEvent) EventType() EventType  { return EventTypeBlock }
func (e BlockEvent) GetBlockNumber() int64 { return e.BlockNumber }
func (e BlockEvent) GetChainID() int64     { return e.ChainID }

// DeduplicationID returns a deterministic ID for SNS FIFO deduplication.
// Format: {chainId}:{blockHash}:{version}
// This ensures that if two watcher instances process the same block,
// only one message is delivered to consumers.
func (e BlockEvent) DeduplicationID() string {
	return fmt.Sprintf("%d:%s:%d", e.ChainID, e.BlockHash, e.Version)
}

// EventSink defines the interface for publishing block data events.
// Events contain only metadata; actual data is in the cache.
type EventSink interface {
	// Publish publishes an event indicating data is ready in cache.
	Publish(ctx context.Context, event Event) error

	// Close closes the sink and releases any resources.
	Close() error
}

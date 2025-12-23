package outbound

import (
	"context"
	"time"
)

// EventType represents the type of event.
type EventType string

// Event type constants.
const (
	EventTypeBlock    EventType = "block"
	EventTypeReceipts EventType = "receipts"
	EventTypeTraces   EventType = "traces"
	EventTypeBlobs    EventType = "blobs"
)

// Event is the interface that all event types implement.
type Event interface {
	// EventType returns the type of the event.
	EventType() EventType
	// GetBlockNumber returns the block number.
	GetBlockNumber() int64
	// GetChainID returns the chain ID.
	GetChainID() int64
	// GetCacheKey returns the cache key where the data is stored.
	GetCacheKey() string
}

// BlockEvent is published when block data is ready in cache.
type BlockEvent struct {
	// ChainID identifies which blockchain this data is from.
	ChainID int64 `json:"chainId"`

	// BlockNumber is the block number.
	BlockNumber int64 `json:"blockNumber"`

	// BlockHash is the block hash.
	BlockHash string `json:"blockHash"`

	// ParentHash is the parent block's hash.
	ParentHash string `json:"parentHash"`

	// BlockTimestamp is when the block was produced (unix timestamp).
	BlockTimestamp int64 `json:"blockTimestamp"`

	// ReceivedAt is when we received and cached this data.
	ReceivedAt time.Time `json:"receivedAt"`

	// CacheKey is the key where the data is stored in cache.
	CacheKey string `json:"cacheKey"`

	// IsReorg indicates this block is part of a chain reorganization.
	IsReorg bool `json:"isReorg,omitempty"`

	// IsBackfill indicates this data was fetched during reconnection backfill.
	IsBackfill bool `json:"isBackfill,omitempty"`
}

func (e BlockEvent) EventType() EventType  { return EventTypeBlock }
func (e BlockEvent) GetBlockNumber() int64 { return e.BlockNumber }
func (e BlockEvent) GetChainID() int64     { return e.ChainID }
func (e BlockEvent) GetCacheKey() string   { return e.CacheKey }

// ReceiptsEvent is published when receipts data is ready in cache.
type ReceiptsEvent struct {
	// ChainID identifies which blockchain this data is from.
	ChainID int64 `json:"chainId"`

	// BlockNumber is the block number.
	BlockNumber int64 `json:"blockNumber"`

	// BlockHash is the block hash.
	BlockHash string `json:"blockHash"`

	// ReceivedAt is when we received and cached this data.
	ReceivedAt time.Time `json:"receivedAt"`

	// CacheKey is the key where the data is stored in cache.
	CacheKey string `json:"cacheKey"`

	// IsReorg indicates this block is part of a chain reorganization.
	IsReorg bool `json:"isReorg,omitempty"`

	// IsBackfill indicates this data was fetched during reconnection backfill.
	IsBackfill bool `json:"isBackfill,omitempty"`
}

func (e ReceiptsEvent) EventType() EventType  { return EventTypeReceipts }
func (e ReceiptsEvent) GetBlockNumber() int64 { return e.BlockNumber }
func (e ReceiptsEvent) GetChainID() int64     { return e.ChainID }
func (e ReceiptsEvent) GetCacheKey() string   { return e.CacheKey }

// TracesEvent is published when traces data is ready in cache.
type TracesEvent struct {
	// ChainID identifies which blockchain this data is from.
	ChainID int64 `json:"chainId"`

	// BlockNumber is the block number.
	BlockNumber int64 `json:"blockNumber"`

	// BlockHash is the block hash.
	BlockHash string `json:"blockHash"`

	// ReceivedAt is when we received and cached this data.
	ReceivedAt time.Time `json:"receivedAt"`

	// CacheKey is the key where the data is stored in cache.
	CacheKey string `json:"cacheKey"`

	// IsReorg indicates this block is part of a chain reorganization.
	IsReorg bool `json:"isReorg,omitempty"`

	// IsBackfill indicates this data was fetched during reconnection backfill.
	IsBackfill bool `json:"isBackfill,omitempty"`
}

func (e TracesEvent) EventType() EventType  { return EventTypeTraces }
func (e TracesEvent) GetBlockNumber() int64 { return e.BlockNumber }
func (e TracesEvent) GetChainID() int64     { return e.ChainID }
func (e TracesEvent) GetCacheKey() string   { return e.CacheKey }

// BlobsEvent is published when blobs data is ready in cache.
type BlobsEvent struct {
	// ChainID identifies which blockchain this data is from.
	ChainID int64 `json:"chainId"`

	// BlockNumber is the block number.
	BlockNumber int64 `json:"blockNumber"`

	// BlockHash is the block hash.
	BlockHash string `json:"blockHash"`

	// ReceivedAt is when we received and cached this data.
	ReceivedAt time.Time `json:"receivedAt"`

	// CacheKey is the key where the data is stored in cache.
	CacheKey string `json:"cacheKey"`

	// IsReorg indicates this block is part of a chain reorganization.
	IsReorg bool `json:"isReorg,omitempty"`

	// IsBackfill indicates this data was fetched during reconnection backfill.
	IsBackfill bool `json:"isBackfill,omitempty"`
}

func (e BlobsEvent) EventType() EventType  { return EventTypeBlobs }
func (e BlobsEvent) GetBlockNumber() int64 { return e.BlockNumber }
func (e BlobsEvent) GetChainID() int64     { return e.ChainID }
func (e BlobsEvent) GetCacheKey() string   { return e.CacheKey }

// EventSink defines the interface for publishing block data events.
// Events contain only metadata; actual data is in the cache.
type EventSink interface {
	// Publish publishes an event indicating data is ready in cache.
	// Accepts BlockEvent, ReceiptsEvent, TracesEvent, or BlobsEvent.
	Publish(ctx context.Context, event Event) error

	// Close closes the sink and releases any resources.
	Close() error
}

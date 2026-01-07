// eventsink.go provides an in-memory implementation of EventSink.
//
// This adapter stores all published events in memory for testing purposes.
// It provides helper methods for inspecting events during tests:
//   - GetEvents(): Returns all published events
//   - GetEventsByType(): Filters events by type
//   - GetBlockEvents()/GetReorgEvents(): Type-specific event retrieval
//   - SetOnPublish(): Register callback for event assertions
//
// All operations are thread-safe. For production, use a message queue adapter.
package memory

import (
	"context"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that EventSink implements outbound.EventSink
var _ outbound.EventSink = (*EventSink)(nil)

// EventSink is an in-memory implementation of the EventSink port for testing.
// It stores all published events for later inspection.
type EventSink struct {
	mu     sync.RWMutex
	events []outbound.Event
	closed bool

	// Callback for test assertions
	onPublish func(outbound.Event)
}

// NewEventSink creates a new in-memory event sink for testing.
func NewEventSink() *EventSink {
	return &EventSink{
		events: make([]outbound.Event, 0),
	}
}

// Publish stores the event in memory.
func (s *EventSink) Publish(ctx context.Context, event outbound.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.events = append(s.events, event)

	if s.onPublish != nil {
		s.onPublish(event)
	}

	return nil
}

// Close marks the sink as closed.
func (s *EventSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// GetEvents returns all published events.
func (s *EventSink) GetEvents() []outbound.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]outbound.Event, len(s.events))
	copy(result, s.events)
	return result
}

// GetEventsByType returns events filtered by type.
func (s *EventSink) GetEventsByType(eventType outbound.EventType) []outbound.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]outbound.Event, 0)
	for _, e := range s.events {
		if e.EventType() == eventType {
			result = append(result, e)
		}
	}
	return result
}

// GetBlockEvents returns all block events.
func (s *EventSink) GetBlockEvents() []outbound.BlockEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]outbound.BlockEvent, 0)
	for _, e := range s.events {
		if be, ok := e.(outbound.BlockEvent); ok {
			result = append(result, be)
		}
	}
	return result
}

// GetReceiptsEvents returns all receipts events.
func (s *EventSink) GetReceiptsEvents() []outbound.ReceiptsEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]outbound.ReceiptsEvent, 0)
	for _, e := range s.events {
		if re, ok := e.(outbound.ReceiptsEvent); ok {
			result = append(result, re)
		}
	}
	return result
}

// GetTracesEvents returns all traces events.
func (s *EventSink) GetTracesEvents() []outbound.TracesEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]outbound.TracesEvent, 0)
	for _, e := range s.events {
		if te, ok := e.(outbound.TracesEvent); ok {
			result = append(result, te)
		}
	}
	return result
}

// GetBlobsEvents returns all blobs events.
func (s *EventSink) GetBlobsEvents() []outbound.BlobsEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]outbound.BlobsEvent, 0)
	for _, e := range s.events {
		if be, ok := e.(outbound.BlobsEvent); ok {
			result = append(result, be)
		}
	}
	return result
}

// GetEventCount returns the number of published events.
func (s *EventSink) GetEventCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.events)
}

// GetEventsForBlock returns all events for a specific block number.
func (s *EventSink) GetEventsForBlock(blockNumber int64) []outbound.Event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]outbound.Event, 0)
	for _, e := range s.events {
		if e.GetBlockNumber() == blockNumber {
			result = append(result, e)
		}
	}
	return result
}

// Clear removes all stored events.
func (s *EventSink) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = make([]outbound.Event, 0)
}

// OnPublish sets a callback to be called when an event is published.
func (s *EventSink) OnPublish(fn func(outbound.Event)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onPublish = fn
}

package testutil

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// MockEventRepository implements outbound.EventRepository for testing.
type MockEventRepository struct {
	SaveEventFn func(ctx context.Context, tx pgx.Tx, event *entity.ProtocolEvent) error
	SaveBatchFn func(ctx context.Context, tx pgx.Tx, evts []*entity.ProtocolEvent) error
}

func (m *MockEventRepository) SaveEvent(ctx context.Context, tx pgx.Tx, event *entity.ProtocolEvent) error {
	if m.SaveEventFn != nil {
		return m.SaveEventFn(ctx, tx, event)
	}
	return nil
}

func (m *MockEventRepository) SaveBatch(ctx context.Context, tx pgx.Tx, evts []*entity.ProtocolEvent) error {
	if m.SaveBatchFn != nil {
		return m.SaveBatchFn(ctx, tx, evts)
	}
	return nil
}

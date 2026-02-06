//go:build integration

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

type eventTestFixture struct {
	repo       *EventRepository
	pool       *pgxpool.Pool
	cleanup    func()
	protocolID int64
}

func setupEventTest(t *testing.T) *eventTestFixture {
	t.Helper()
	ctx := context.Background()

	container, err := postgres.Run(ctx,
		"timescale/timescaledb:latest-pg17",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.WithSQLDriver("pgx"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second)),
	)
	if err != nil {
		t.Fatalf("failed to start container: %v", err)
	}

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("failed to create pgxpool: %v", err)
	}

	for i := 0; i < 30; i++ {
		if err := pool.Ping(ctx); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../../../db/migrations")
	m := migrator.New(pool, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		t.Fatalf("failed to apply migrations: %v", err)
	}

	repo, err := NewEventRepository(pool, nil)
	if err != nil {
		t.Fatalf("failed to create repository: %v", err)
	}

	fixture := &eventTestFixture{
		repo: repo,
		pool: pool,
		cleanup: func() {
			pool.Close()
			container.Terminate(ctx)
		},
	}

	// Get seeded protocol ID
	err = pool.QueryRow(ctx, `SELECT id FROM protocol WHERE name = 'SparkLend' LIMIT 1`).Scan(&fixture.protocolID)
	if err != nil {
		t.Fatalf("failed to get protocol: %v", err)
	}

	return fixture
}

func TestSaveEvent_SingleEvent(t *testing.T) {
	fixture := setupEventTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	eventData := json.RawMessage(`{"user":"0xabc","reserve":"0xdef","amount":"1000000"}`)
	event, err := entity.NewProtocolEvent(1, fixture.protocolID, 1000, 0, []byte{0x01, 0x02}, 5, []byte{0xaa, 0xbb}, "Borrow", eventData)
	if err != nil {
		t.Fatalf("failed to create event: %v", err)
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	err = fixture.repo.SaveEvent(ctx, tx, event)
	if err != nil {
		t.Fatalf("SaveEvent failed: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("failed to commit: %v", err)
	}

	// Verify record was inserted
	var count int
	err = fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_event WHERE event_name = 'Borrow'`).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 record, got %d", count)
	}

	// Verify stored data matches
	var storedEventName string
	var storedEventData json.RawMessage
	var storedBlockNumber int64
	err = fixture.pool.QueryRow(ctx,
		`SELECT event_name, event_data, block_number FROM protocol_event WHERE event_name = 'Borrow'`).
		Scan(&storedEventName, &storedEventData, &storedBlockNumber)
	if err != nil {
		t.Fatalf("failed to query stored event: %v", err)
	}
	if storedEventName != "Borrow" {
		t.Errorf("event_name = %v, want Borrow", storedEventName)
	}
	if storedBlockNumber != 1000 {
		t.Errorf("block_number = %v, want 1000", storedBlockNumber)
	}
	// Compare parsed JSON (JSONB normalizes key ordering)
	var storedMap, expectedMap map[string]interface{}
	if err := json.Unmarshal(storedEventData, &storedMap); err != nil {
		t.Fatalf("failed to unmarshal stored event_data: %v", err)
	}
	if err := json.Unmarshal(eventData, &expectedMap); err != nil {
		t.Fatalf("failed to unmarshal expected event_data: %v", err)
	}
	for key, expectedVal := range expectedMap {
		storedVal, ok := storedMap[key]
		if !ok {
			t.Errorf("missing key %q in stored event_data", key)
		} else if fmt.Sprintf("%v", storedVal) != fmt.Sprintf("%v", expectedVal) {
			t.Errorf("event_data[%q] = %v, want %v", key, storedVal, expectedVal)
		}
	}
}

func TestSaveEvent_DuplicateIgnored(t *testing.T) {
	fixture := setupEventTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	eventData := json.RawMessage(`{"user":"0xabc"}`)
	event, err := entity.NewProtocolEvent(1, fixture.protocolID, 2000, 0, []byte{0x01, 0x02}, 3, []byte{0xaa}, "Supply", eventData)
	if err != nil {
		t.Fatalf("failed to create event: %v", err)
	}

	// Insert first time
	tx1, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	if err := fixture.repo.SaveEvent(ctx, tx1, event); err != nil {
		t.Fatalf("first SaveEvent failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx1: %v", err)
	}

	// Insert same event again — should be silently ignored
	tx2, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	if err := fixture.repo.SaveEvent(ctx, tx2, event); err != nil {
		t.Fatalf("duplicate SaveEvent failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx2: %v", err)
	}

	// Still only 1 record
	var count int
	err = fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_event WHERE block_number = 2000`).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 record after duplicate insert, got %d", count)
	}
}

func TestSaveEvent_DifferentBlockVersionsAllowed(t *testing.T) {
	fixture := setupEventTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	eventData := json.RawMessage(`{"user":"0xabc"}`)

	// Same block_number, tx_hash, log_index but different block_version (reorg scenario)
	event0, err := entity.NewProtocolEvent(1, fixture.protocolID, 3000, 0, []byte{0x01, 0x02}, 3, []byte{0xaa}, "Supply", eventData)
	if err != nil {
		t.Fatalf("failed to create event v0: %v", err)
	}
	event1, err := entity.NewProtocolEvent(1, fixture.protocolID, 3000, 1, []byte{0x01, 0x02}, 3, []byte{0xaa}, "Supply", eventData)
	if err != nil {
		t.Fatalf("failed to create event v1: %v", err)
	}

	// Insert version 0
	tx1, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx1: %v", err)
	}
	if err := fixture.repo.SaveEvent(ctx, tx1, event0); err != nil {
		t.Fatalf("SaveEvent v0 failed: %v", err)
	}
	if err := tx1.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx1: %v", err)
	}

	// Insert version 1 — should succeed (different block_version)
	tx2, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin tx2: %v", err)
	}
	if err := fixture.repo.SaveEvent(ctx, tx2, event1); err != nil {
		t.Fatalf("SaveEvent v1 failed: %v", err)
	}
	if err := tx2.Commit(ctx); err != nil {
		t.Fatalf("failed to commit tx2: %v", err)
	}

	// Should have 2 records
	var count int
	err = fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_event WHERE block_number = 3000`).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 2 {
		t.Errorf("expected 2 records for different block versions, got %d", count)
	}
}

func TestSaveEvent_Rollback(t *testing.T) {
	fixture := setupEventTest(t)
	t.Cleanup(fixture.cleanup)

	ctx := context.Background()

	eventData := json.RawMessage(`{"user":"0xabc"}`)
	event, err := entity.NewProtocolEvent(1, fixture.protocolID, 4000, 0, []byte{0x01, 0x02}, 0, []byte{0xaa}, "Withdraw", eventData)
	if err != nil {
		t.Fatalf("failed to create event: %v", err)
	}

	tx, err := fixture.pool.Begin(ctx)
	if err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	if err := fixture.repo.SaveEvent(ctx, tx, event); err != nil {
		t.Fatalf("SaveEvent failed: %v", err)
	}

	// Rollback instead of commit
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("failed to rollback: %v", err)
	}

	// Verify no records exist after rollback
	var count int
	err = fixture.pool.QueryRow(ctx, `SELECT COUNT(*) FROM protocol_event WHERE block_number = 4000`).Scan(&count)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records after rollback, got %d", count)
	}
}

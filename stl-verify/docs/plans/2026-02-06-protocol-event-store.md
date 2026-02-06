# Protocol Event Store Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Store decoded protocol events (SparkLend and future protocols) in a single `protocol_event` table for future analytics and auditability.

**Architecture:** A new `ProtocolEvent` domain entity, `EventRepository` outbound port, and Postgres adapter. The existing `processEventLog` method in the SparkLend position tracker service saves each decoded event before dispatching to event-type-specific handlers. The event_data is stored as JSONB containing the decoded fields from `PositionEventData`.

**Tech Stack:** Go, PostgreSQL (pgx/v5), JSONB, existing migration system

---

### Task 1: SQL Migration

**Files:**
- Create: `stl-verify/db/migrations/20260206_150000_create_protocol_event.sql`

**Step 1: Write the migration file**

```sql
-- Create protocol_event table for storing decoded protocol events
-- Stores all events across all protocols in a single table with JSONB event data

CREATE TABLE IF NOT EXISTS protocol_event (
    id               BIGSERIAL PRIMARY KEY,
    chain_id         INT       NOT NULL REFERENCES chain(chain_id),
    protocol_id      BIGINT    NOT NULL REFERENCES protocol(id),
    block_number     BIGINT    NOT NULL,
    block_version    INT       NOT NULL DEFAULT 0,
    tx_hash          BYTEA     NOT NULL,
    log_index        INT       NOT NULL,
    contract_address BYTEA     NOT NULL,
    event_name       TEXT      NOT NULL,
    event_data       JSONB     NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chain_id, block_number, tx_hash, log_index)
);

-- Grant permissions to application roles
GRANT SELECT ON protocol_event TO stl_readonly;
GRANT SELECT, INSERT, UPDATE, DELETE ON protocol_event TO stl_readwrite;
GRANT USAGE, SELECT ON SEQUENCE protocol_event_id_seq TO stl_readwrite;

-- Track this migration
INSERT INTO migrations (filename) VALUES ('20260206_150000_create_protocol_event.sql')
ON CONFLICT (filename) DO NOTHING;
```

**Step 2: Verify migration applies locally**

Run: `go run cmd/migrate/main.go` (from `stl-verify/`)
Expected: Migration applied successfully

**Step 3: Commit**

```bash
git add stl-verify/db/migrations/20260206_150000_create_protocol_event.sql
git commit -m "feat: add protocol_event table migration"
```

---

### Task 2: Domain Entity — ProtocolEvent

**Files:**
- Create: `stl-verify/internal/domain/entity/protocol_event.go`
- Create: `stl-verify/internal/domain/entity/protocol_event_test.go`

**Step 1: Write the failing test**

```go
package entity

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewProtocolEvent(t *testing.T) {
	validEventData := json.RawMessage(`{"user":"0x1234","amount":"1000"}`)
	validTxHash := []byte{0x01, 0x02, 0x03}
	validContractAddr := []byte{0xaa, 0xbb, 0xcc}

	tests := []struct {
		name            string
		chainID         int
		protocolID      int64
		blockNumber     int64
		blockVersion    int
		txHash          []byte
		logIndex        int
		contractAddress []byte
		eventName       string
		eventData       json.RawMessage
		wantErr         bool
		errContains     string
	}{
		{
			name:            "valid event",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         false,
		},
		{
			name:            "zero chainID",
			chainID:         0,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "chainID must be positive",
		},
		{
			name:            "zero protocolID",
			chainID:         1,
			protocolID:      0,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "protocolID must be positive",
		},
		{
			name:            "zero blockNumber",
			chainID:         1,
			protocolID:      1,
			blockNumber:     0,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "blockNumber must be positive",
		},
		{
			name:            "negative blockVersion",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    -1,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "blockVersion must be non-negative",
		},
		{
			name:            "empty txHash",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          nil,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "txHash must not be empty",
		},
		{
			name:            "negative logIndex",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        -1,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "logIndex must be non-negative",
		},
		{
			name:            "empty contractAddress",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: nil,
			eventName:       "Borrow",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "contractAddress must not be empty",
		},
		{
			name:            "empty eventName",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "",
			eventData:       validEventData,
			wantErr:         true,
			errContains:     "eventName must not be empty",
		},
		{
			name:            "nil eventData",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Borrow",
			eventData:       nil,
			wantErr:         true,
			errContains:     "eventData must not be empty",
		},
		{
			name:            "logIndex zero is valid",
			chainID:         1,
			protocolID:      1,
			blockNumber:     1000,
			blockVersion:    0,
			txHash:          validTxHash,
			logIndex:        0,
			contractAddress: validContractAddr,
			eventName:       "Supply",
			eventData:       validEventData,
			wantErr:         false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := NewProtocolEvent(tt.chainID, tt.protocolID, tt.blockNumber, tt.blockVersion, tt.txHash, tt.logIndex, tt.contractAddress, tt.eventName, tt.eventData)
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewProtocolEvent() expected error, got nil")
					return
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("NewProtocolEvent() error = %v, want error containing %v", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Errorf("NewProtocolEvent() unexpected error = %v", err)
				return
			}
			if event == nil {
				t.Errorf("NewProtocolEvent() returned nil")
				return
			}
			if event.ChainID != tt.chainID {
				t.Errorf("ChainID = %v, want %v", event.ChainID, tt.chainID)
			}
			if event.EventName != tt.eventName {
				t.Errorf("EventName = %v, want %v", event.EventName, tt.eventName)
			}
		})
	}
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./internal/domain/entity/ -run TestNewProtocolEvent -v` (from `stl-verify/`)
Expected: FAIL — `NewProtocolEvent` not defined

**Step 3: Write the entity implementation**

```go
package entity

import (
	"encoding/json"
	"fmt"
)

// ProtocolEvent represents a decoded protocol event stored for analytics and auditability.
type ProtocolEvent struct {
	ID              int64
	ChainID         int
	ProtocolID      int64
	BlockNumber     int64
	BlockVersion    int
	TxHash          []byte
	LogIndex        int
	ContractAddress []byte
	EventName       string
	EventData       json.RawMessage
}

// NewProtocolEvent creates a new ProtocolEvent with validation.
func NewProtocolEvent(chainID int, protocolID, blockNumber int64, blockVersion int, txHash []byte, logIndex int, contractAddress []byte, eventName string, eventData json.RawMessage) (*ProtocolEvent, error) {
	e := &ProtocolEvent{
		ChainID:         chainID,
		ProtocolID:      protocolID,
		BlockNumber:     blockNumber,
		BlockVersion:    blockVersion,
		TxHash:          txHash,
		LogIndex:        logIndex,
		ContractAddress: contractAddress,
		EventName:       eventName,
		EventData:       eventData,
	}
	if err := e.validate(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *ProtocolEvent) validate() error {
	if e.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", e.ChainID)
	}
	if e.ProtocolID <= 0 {
		return fmt.Errorf("protocolID must be positive, got %d", e.ProtocolID)
	}
	if e.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", e.BlockNumber)
	}
	if e.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", e.BlockVersion)
	}
	if len(e.TxHash) == 0 {
		return fmt.Errorf("txHash must not be empty")
	}
	if e.LogIndex < 0 {
		return fmt.Errorf("logIndex must be non-negative, got %d", e.LogIndex)
	}
	if len(e.ContractAddress) == 0 {
		return fmt.Errorf("contractAddress must not be empty")
	}
	if e.EventName == "" {
		return fmt.Errorf("eventName must not be empty")
	}
	if len(e.EventData) == 0 {
		return fmt.Errorf("eventData must not be empty")
	}
	return nil
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./internal/domain/entity/ -run TestNewProtocolEvent -v` (from `stl-verify/`)
Expected: PASS

**Step 5: Commit**

```bash
git add stl-verify/internal/domain/entity/protocol_event.go stl-verify/internal/domain/entity/protocol_event_test.go
git commit -m "feat: add ProtocolEvent domain entity"
```

---

### Task 3: Outbound Port — EventRepository

**Files:**
- Create: `stl-verify/internal/ports/outbound/event_repository.go`

**Step 1: Write the port interface**

```go
package outbound

import (
	"context"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/jackc/pgx/v5"
)

// EventRepository defines the interface for protocol event persistence.
type EventRepository interface {
	// SaveEvent saves a single protocol event within an external transaction.
	// Uses ON CONFLICT DO NOTHING — duplicate events are silently ignored.
	SaveEvent(ctx context.Context, tx pgx.Tx, event *entity.ProtocolEvent) error
}
```

**Step 2: Commit**

```bash
git add stl-verify/internal/ports/outbound/event_repository.go
git commit -m "feat: add EventRepository outbound port"
```

---

### Task 4: Postgres Adapter — EventRepository

**Files:**
- Create: `stl-verify/internal/adapters/outbound/postgres/event_repository.go`
- Create: `stl-verify/internal/adapters/outbound/postgres/event_repository_integration_test.go`

**Step 1: Write the integration test**

Note: Uses the same testcontainers pattern as `position_repository_integration_test.go`.

```go
//go:build integration

package postgres

import (
	"context"
	"encoding/json"
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
```

**Step 2: Write the adapter implementation**

```go
package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Compile-time check that EventRepository implements outbound.EventRepository
var _ outbound.EventRepository = (*EventRepository)(nil)

// EventRepository is a PostgreSQL implementation of the outbound.EventRepository port.
type EventRepository struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewEventRepository creates a new PostgreSQL Event repository.
func NewEventRepository(pool *pgxpool.Pool, logger *slog.Logger) (*EventRepository, error) {
	if pool == nil {
		return nil, fmt.Errorf("database pool cannot be nil")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &EventRepository{
		pool:   pool,
		logger: logger,
	}, nil
}

// SaveEvent saves a single protocol event within an external transaction.
// Uses ON CONFLICT DO NOTHING — duplicate events are silently ignored.
func (r *EventRepository) SaveEvent(ctx context.Context, tx pgx.Tx, event *entity.ProtocolEvent) error {
	_, err := tx.Exec(ctx,
		`INSERT INTO protocol_event (chain_id, protocol_id, block_number, block_version, tx_hash, log_index, contract_address, event_name, event_data)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		 ON CONFLICT (chain_id, block_number, tx_hash, log_index) DO NOTHING`,
		event.ChainID, event.ProtocolID, event.BlockNumber, event.BlockVersion,
		event.TxHash, event.LogIndex, event.ContractAddress, event.EventName, event.EventData)

	if err != nil {
		return fmt.Errorf("failed to save protocol event: %w", err)
	}
	return nil
}
```

**Step 3: Run integration test to verify it passes**

Run: `go test -tags=integration ./internal/adapters/outbound/postgres/ -run TestSaveEvent -v -timeout 120s` (from `stl-verify/`)
Expected: PASS

**Step 4: Commit**

```bash
git add stl-verify/internal/adapters/outbound/postgres/event_repository.go stl-verify/internal/adapters/outbound/postgres/event_repository_integration_test.go
git commit -m "feat: add EventRepository postgres adapter with integration tests"
```

---

### Task 5: Build Event Data JSON from PositionEventData

**Files:**
- Modify: `stl-verify/internal/services/sparklend_position_tracker/event_extractor.go`

**Step 1: Add a method to serialize PositionEventData to JSON**

Add a `ToJSON` method on `PositionEventData` that converts the decoded event fields to a JSON-serializable map. Addresses are stored as hex strings, amounts as decimal strings.

```go
// ToJSON converts PositionEventData to a JSON-serializable map.
// Addresses are hex strings, amounts are decimal strings.
func (p *PositionEventData) ToJSON() (json.RawMessage, error) {
	data := make(map[string]interface{})
	data["user"] = p.User.Hex()

	if p.Reserve != (common.Address{}) {
		data["reserve"] = p.Reserve.Hex()
	}
	if p.Amount != nil {
		data["amount"] = p.Amount.String()
	}
	if p.Liquidator != (common.Address{}) {
		data["liquidator"] = p.Liquidator.Hex()
	}
	if p.CollateralAsset != (common.Address{}) {
		data["collateralAsset"] = p.CollateralAsset.Hex()
	}
	if p.DebtAsset != (common.Address{}) {
		data["debtAsset"] = p.DebtAsset.Hex()
	}
	if p.DebtToCover != nil {
		data["debtToCover"] = p.DebtToCover.String()
	}
	if p.LiquidatedCollateralAmount != nil {
		data["liquidatedCollateralAmount"] = p.LiquidatedCollateralAmount.String()
	}
	if p.EventType == EventReserveUsedAsCollateralEnabled || p.EventType == EventReserveUsedAsCollateralDisabled {
		data["collateralEnabled"] = p.CollateralEnabled
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal event data: %w", err)
	}
	return raw, nil
}
```

Also add `"encoding/json"` to the imports in `event_extractor.go`.

**Step 2: Write tests for ToJSON**

Create a test in `stl-verify/internal/services/sparklend_position_tracker/event_extractor_test.go` (or add to existing test file):

```go
package sparklend_position_tracker

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestPositionEventData_ToJSON(t *testing.T) {
	tests := []struct {
		name      string
		event     *PositionEventData
		wantKeys  []string
		wantErr   bool
	}{
		{
			name: "borrow event",
			event: &PositionEventData{
				EventType: EventBorrow,
				User:      common.HexToAddress("0x1234"),
				Reserve:   common.HexToAddress("0x5678"),
				Amount:    big.NewInt(1000000),
			},
			wantKeys: []string{"user", "reserve", "amount"},
		},
		{
			name: "liquidation event",
			event: &PositionEventData{
				EventType:                  EventLiquidationCall,
				User:                       common.HexToAddress("0x1234"),
				Liquidator:                 common.HexToAddress("0xabcd"),
				CollateralAsset:            common.HexToAddress("0x5678"),
				DebtAsset:                  common.HexToAddress("0x9abc"),
				DebtToCover:                big.NewInt(500),
				LiquidatedCollateralAmount: big.NewInt(600),
			},
			wantKeys: []string{"user", "liquidator", "collateralAsset", "debtAsset", "debtToCover", "liquidatedCollateralAmount"},
		},
		{
			name: "collateral enabled event",
			event: &PositionEventData{
				EventType:         EventReserveUsedAsCollateralEnabled,
				User:              common.HexToAddress("0x1234"),
				Reserve:           common.HexToAddress("0x5678"),
				CollateralEnabled: true,
			},
			wantKeys: []string{"user", "reserve", "collateralEnabled"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			raw, err := tt.event.ToJSON()
			if (err != nil) != tt.wantErr {
				t.Fatalf("ToJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			var result map[string]interface{}
			if err := json.Unmarshal(raw, &result); err != nil {
				t.Fatalf("failed to unmarshal result: %v", err)
			}

			for _, key := range tt.wantKeys {
				if _, ok := result[key]; !ok {
					t.Errorf("expected key %q in JSON output", key)
				}
			}
		})
	}
}
```

**Step 3: Run tests**

Run: `go test ./internal/services/sparklend_position_tracker/ -run TestPositionEventData_ToJSON -v` (from `stl-verify/`)
Expected: PASS

**Step 4: Commit**

```bash
git add stl-verify/internal/services/sparklend_position_tracker/event_extractor.go stl-verify/internal/services/sparklend_position_tracker/event_extractor_test.go
git commit -m "feat: add ToJSON method on PositionEventData"
```

---

### Task 6: Wire EventRepository into the Service

**Files:**
- Modify: `stl-verify/internal/services/sparklend_position_tracker/service.go`
- Modify: `stl-verify/cmd/sparklend-position-tracker/main.go`

**Step 1: Add eventRepo to Service struct and constructor**

In `service.go`, add `eventRepo *postgres.EventRepository` to the `Service` struct (line 135, after `positionRepo`). Add to `NewService` parameters and `validateDependencies`.

**Step 2: Add event saving to processEventLog**

In `processEventLog` (around line 407, after `ExtractEventData` succeeds), add the event save logic:

```go
	// Save the raw decoded event for analytics/auditability
	logIndex, err := strconv.ParseInt(strings.TrimPrefix(log.LogIndex, "0x"), 16, 64)
	if err != nil {
		s.logger.Warn("failed to parse log index", "logIndex", log.LogIndex, "error", err)
		logIndex = 0
	}

	if err := s.saveProtocolEvent(ctx, eventData, protocolAddress, chainID, blockNumber, blockVersion, log, int(logIndex)); err != nil {
		s.logger.Warn("failed to save protocol event", "error", err, "tx", txHash, "block", blockNumber)
		// Non-fatal: position tracking continues even if event storage fails
	}
```

Add the `saveProtocolEvent` method:

```go
func (s *Service) saveProtocolEvent(ctx context.Context, eventData *PositionEventData, protocolAddress common.Address, chainID, blockNumber int64, blockVersion int, log Log, logIndex int) error {
	eventJSON, err := eventData.ToJSON()
	if err != nil {
		return fmt.Errorf("failed to serialize event data: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		protocolConfig, exists := blockchain.GetProtocolConfig(protocolAddress)
		if !exists {
			return fmt.Errorf("unknown protocol: %s", protocolAddress.Hex())
		}

		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, protocolAddress, protocolConfig.Name, blockNumber)
		if err != nil {
			return fmt.Errorf("failed to get protocol: %w", err)
		}

		event, err := entity.NewProtocolEvent(
			int(chainID),
			protocolID,
			blockNumber,
			blockVersion,
			common.FromHex(eventData.TxHash),
			logIndex,
			protocolAddress.Bytes(),
			string(eventData.EventType),
			eventJSON,
		)
		if err != nil {
			return fmt.Errorf("failed to create protocol event entity: %w", err)
		}

		return s.eventRepo.SaveEvent(ctx, tx, event)
	})
}
```

Also add `"strconv"` to the imports in `service.go`.

**Step 3: Wire in main.go**

After creating `positionRepo` in `cmd/sparklend-position-tracker/main.go`, add:

```go
	eventRepo, err := postgres.NewEventRepository(pool, logger)
	if err != nil {
		logger.Error("failed to create event repository", "error", err)
		os.Exit(1)
	}
```

Pass `eventRepo` to `NewService`.

**Step 4: Verify compilation**

Run: `go build ./...` (from `stl-verify/`)
Expected: Clean build

**Step 5: Commit**

```bash
git add stl-verify/internal/services/sparklend_position_tracker/service.go stl-verify/cmd/sparklend-position-tracker/main.go
git commit -m "feat: wire EventRepository into position tracker service"
```

---

### Task 7: Update entity_relation.md

**Files:**
- Modify: `docs/entity_relation.md`

**Step 1: Add ProtocolEvent to the ER diagram**

Add the `ProtocolEvent` entity and its relationships to Chain and Protocol:

```
    ProtocolEvent {
        bigint id PK
        int chain_id FK "UK1"
        bigint protocol_id FK
        bigint block_number "UK1"
        int block_version
        bytea tx_hash "UK1"
        int log_index "UK1"
        bytea contract_address
        text event_name
        jsonb event_data
        timestamptz created_at
    }
```

Add relationships:
```
    Chain ||--o{ ProtocolEvent : ""
    Protocol ||--o{ ProtocolEvent : ""
```

**Step 2: Commit**

```bash
git add docs/entity_relation.md
git commit -m "docs: add ProtocolEvent to entity relation diagram"
```

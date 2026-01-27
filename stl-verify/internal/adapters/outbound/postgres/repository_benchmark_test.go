//go:build integration

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/archon-research/stl/stl-verify/db/migrator"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// Benchmark row counts
const (
	BenchmarkSmallRowCount  = 1_000
	BenchmarkMediumRowCount = 100_000
	BenchmarkLargeRowCount  = 1_000_000
)

// setupBenchmarkPostgres creates a TimescaleDB container for benchmarks.
func setupBenchmarkPostgres(b *testing.B) (*sql.DB, func()) {
	b.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg17",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "bench",
			"POSTGRES_PASSWORD": "bench",
			"POSTGRES_DB":       "benchdb",
		},
		Cmd: []string{
			"postgres",
			"-c", "shared_buffers=256MB",
			"-c", "work_mem=64MB",
			"-c", "maintenance_work_mem=128MB",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").
			WithOccurrence(2).
			WithStartupTimeout(120 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		b.Fatalf("failed to start postgres container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		b.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "5432")
	if err != nil {
		b.Fatalf("failed to get container port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://bench:bench@%s:%s/benchdb?sslmode=disable", host, port.Port())

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		b.Fatalf("failed to connect to database: %v", err)
	}

	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Wait for connection
	for i := 0; i < 60; i++ {
		if err := db.Ping(); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Run migrations
	_, currentFile, _, _ := runtime.Caller(0)
	migrationsDir := filepath.Join(filepath.Dir(currentFile), "../../../../db/migrations")
	m := migrator.New(db, migrationsDir)
	if err := m.ApplyAll(ctx); err != nil {
		b.Fatalf("failed to apply migrations: %v", err)
	}

	cleanup := func() {
		db.Close()
		container.Terminate(ctx)
	}

	return db, cleanup
}

// generateBorrowers creates test borrower entities.
func generateBorrowers(count int, startBlock int64) []*entity.Borrower {
	borrowers := make([]*entity.Borrower, count)
	for i := 0; i < count; i++ {
		borrowers[i] = &entity.Borrower{
			UserID:       int64((i % 1000) + 1), // 1000 unique users
			ProtocolID:   int64((i % 10) + 1),   // 10 protocols
			TokenID:      int64((i % 50) + 1),   // 50 tokens
			BlockNumber:  startBlock + int64(i/100),
			BlockVersion: 0,
			Amount:       big.NewInt(int64(1000000 + i)),
			Change:       big.NewInt(int64(i % 10000)),
		}
	}
	return borrowers
}

// generateBorrowerCollateral creates test collateral entities.
func generateBorrowerCollateral(count int, startBlock int64) []*entity.BorrowerCollateral {
	collateral := make([]*entity.BorrowerCollateral, count)
	for i := 0; i < count; i++ {
		collateral[i] = &entity.BorrowerCollateral{
			UserID:       int64((i % 1000) + 1),
			ProtocolID:   int64((i % 10) + 1),
			TokenID:      int64((i % 50) + 1),
			BlockNumber:  startBlock + int64(i/100),
			BlockVersion: 0,
			Amount:       big.NewInt(int64(5000000 + i)),
			Change:       big.NewInt(int64(i % 50000)),
		}
	}
	return collateral
}

// BenchmarkPositionRepository_UpsertBorrowers benchmarks borrower upserts at various scales.
func BenchmarkPositionRepository_UpsertBorrowers(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	// Seed required reference data
	ctx := context.Background()
	seedReferenceData(b, db, ctx)

	repo, err := NewPositionRepository(db, nil, 1000)
	if err != nil {
		b.Fatalf("failed to create repository: %v", err)
	}

	testCases := []struct {
		name  string
		count int
	}{
		{"1K_rows", BenchmarkSmallRowCount},
		{"100K_rows", BenchmarkMediumRowCount},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			borrowers := generateBorrowers(tc.count, 1000000)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := repo.UpsertBorrowers(ctx, borrowers); err != nil {
					b.Fatalf("upsert failed: %v", err)
				}
			}
			b.StopTimer()

			// Report throughput
			b.ReportMetric(float64(tc.count*b.N)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
}

// BenchmarkPositionRepository_UpsertBorrowerCollateral benchmarks collateral upserts.
func BenchmarkPositionRepository_UpsertBorrowerCollateral(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	ctx := context.Background()
	seedReferenceData(b, db, ctx)

	repo, err := NewPositionRepository(db, nil, 1000)
	if err != nil {
		b.Fatalf("failed to create repository: %v", err)
	}

	testCases := []struct {
		name  string
		count int
	}{
		{"1K_rows", BenchmarkSmallRowCount},
		{"100K_rows", BenchmarkMediumRowCount},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			collateral := generateBorrowerCollateral(tc.count, 2000000)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := repo.UpsertBorrowerCollateral(ctx, collateral); err != nil {
					b.Fatalf("upsert failed: %v", err)
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(tc.count*b.N)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
}

// BenchmarkPositionRepository_BatchSizes compares different batch sizes.
func BenchmarkPositionRepository_BatchSizes(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	ctx := context.Background()
	seedReferenceData(b, db, ctx)

	batchSizes := []int{100, 500, 1000, 2000, 5000}
	rowCount := 50_000

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			repo, err := NewPositionRepository(db, nil, batchSize)
			if err != nil {
				b.Fatalf("failed to create repository: %v", err)
			}

			borrowers := generateBorrowers(rowCount, int64(batchSize*1000000))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := repo.UpsertBorrowers(ctx, borrowers); err != nil {
					b.Fatalf("upsert failed: %v", err)
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(rowCount*b.N)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
}

// generateTokens creates test token entities.
func generateTokens(count int) []*entity.Token {
	tokens := make([]*entity.Token, count)
	for i := 0; i < count; i++ {
		tokens[i] = &entity.Token{
			ChainID:        1,
			Address:        []byte(fmt.Sprintf("%040d", 10000+i)),
			Symbol:         fmt.Sprintf("TKN%d", i),
			Decimals:       18,
			CreatedAtBlock: int64(i + 1),
			Metadata:       map[string]any{"name": fmt.Sprintf("Token %d", i)},
		}
	}
	return tokens
}

// generateSparkLendReserveData creates test SparkLend reserve data entities.
func generateSparkLendReserveData(count int, startBlock int64) []*entity.SparkLendReserveData {
	data := make([]*entity.SparkLendReserveData, count)
	for i := 0; i < count; i++ {
		data[i] = &entity.SparkLendReserveData{
			ProtocolID:              int64((i % 10) + 1),
			TokenID:                 int64((i % 50) + 1),
			BlockNumber:             startBlock + int64(i/50),
			BlockVersion:            0,
			Unbacked:                big.NewInt(int64(i * 1000)),
			AccruedToTreasuryScaled: big.NewInt(int64(i * 100)),
			TotalAToken:             big.NewInt(int64(1000000 + i)),
			TotalStableDebt:         big.NewInt(int64(500000 + i)),
			TotalVariableDebt:       big.NewInt(int64(300000 + i)),
			LiquidityRate:           big.NewInt(int64(i % 10000)),
			VariableBorrowRate:      big.NewInt(int64(i % 20000)),
			StableBorrowRate:        big.NewInt(int64(i % 15000)),
			AverageStableBorrowRate: big.NewInt(int64(i % 12000)),
			LiquidityIndex:          big.NewInt(int64(1000000000 + i)),
			VariableBorrowIndex:     big.NewInt(int64(1000000000 + i*2)),
			LastUpdateTimestamp:     int64(1700000000 + i),
		}
	}
	return data
}

// BenchmarkTokenRepository_UpsertTokens benchmarks token upserts at various scales.
func BenchmarkTokenRepository_UpsertTokens(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	ctx := context.Background()
	// Seed chain for FK constraint
	_, err := db.ExecContext(ctx, `INSERT INTO chains (chain_id, name) VALUES (1, 'mainnet') ON CONFLICT DO NOTHING`)
	if err != nil {
		b.Fatalf("failed to seed chain: %v", err)
	}

	repo, err := NewTokenRepository(db, nil, 1000)
	if err != nil {
		b.Fatalf("failed to create repository: %v", err)
	}

	testCases := []struct {
		name  string
		count int
	}{
		{"1K_rows", BenchmarkSmallRowCount},
		{"100K_rows", BenchmarkMediumRowCount},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			tokens := generateTokens(tc.count)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := repo.UpsertTokens(ctx, tokens); err != nil {
					b.Fatalf("upsert failed: %v", err)
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(tc.count*b.N)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
}

// BenchmarkProtocolRepository_UpsertSparkLendReserveData benchmarks SparkLend reserve data upserts.
func BenchmarkProtocolRepository_UpsertSparkLendReserveData(b *testing.B) {
	db, cleanup := setupBenchmarkPostgres(b)
	defer cleanup()

	ctx := context.Background()
	seedReferenceData(b, db, ctx)

	repo, err := NewProtocolRepository(db, nil, 1000)
	if err != nil {
		b.Fatalf("failed to create repository: %v", err)
	}

	testCases := []struct {
		name  string
		count int
	}{
		{"1K_rows", BenchmarkSmallRowCount},
		{"100K_rows", BenchmarkMediumRowCount},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			data := generateSparkLendReserveData(tc.count, int64(tc.count*1000))

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := repo.UpsertSparkLendReserveData(ctx, data); err != nil {
					b.Fatalf("upsert failed: %v", err)
				}
			}
			b.StopTimer()

			b.ReportMetric(float64(tc.count*b.N)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
}

// seedReferenceData inserts the chains, protocols, tokens, and users needed for FK constraints.
func seedReferenceData(b *testing.B, db *sql.DB, ctx context.Context) {
	b.Helper()

	// Insert chains
	_, err := db.ExecContext(ctx, `INSERT INTO chains (chain_id, name) VALUES (1, 'mainnet') ON CONFLICT DO NOTHING`)
	if err != nil {
		b.Fatalf("failed to seed chains: %v", err)
	}

	// Insert protocols
	for i := 1; i <= 10; i++ {
		_, err := db.ExecContext(ctx, `
			INSERT INTO protocols (id, chain_id, address, name, protocol_type, metadata)
			VALUES ($1, 1, $2, $3, 'lending', '{}')
			ON CONFLICT DO NOTHING`,
			i, fmt.Sprintf("\\x%040d", i), fmt.Sprintf("Protocol %d", i))
		if err != nil {
			b.Fatalf("failed to seed protocol %d: %v", i, err)
		}
	}

	// Insert tokens
	for i := 1; i <= 50; i++ {
		_, err := db.ExecContext(ctx, `
			INSERT INTO tokens (id, chain_id, address, symbol, decimals, metadata)
			VALUES ($1, 1, $2, $3, 18, '{}')
			ON CONFLICT DO NOTHING`,
			i, fmt.Sprintf("\\x%040d", i), fmt.Sprintf("TKN%d", i))
		if err != nil {
			b.Fatalf("failed to seed token %d: %v", i, err)
		}
	}

	// Insert users
	for i := 1; i <= 1000; i++ {
		_, err := db.ExecContext(ctx, `
			INSERT INTO users (id, chain_id, address, metadata)
			VALUES ($1, 1, $2, '{}')
			ON CONFLICT DO NOTHING`,
			i, fmt.Sprintf("\\x%040d", i))
		if err != nil {
			b.Fatalf("failed to seed user %d: %v", i, err)
		}
	}
}

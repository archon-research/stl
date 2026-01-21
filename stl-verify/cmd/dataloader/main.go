// Command dataloader loads historical SparkLend data from S3 into PostgreSQL.
//
// It reads PostgreSQL dump files (COPY format) from S3, transforms the data to match
// the target Sentinel schema, and upserts records into the database.
//
// Usage:
//
//	dataloader --postgres-url="postgres://..." [options]
//
// Options:
//
//	--postgres-url   PostgreSQL connection URL (required)
//	--s3-bucket      S3 bucket name (default: stl-external-uploads-4981403a)
//	--s3-prefix      S3 prefix for data files (default: brett/)
//	--aws-profile    AWS profile name (default: sentinel-stg)
//	--dry-run        Parse and transform only, no DB writes
//	--tables         Comma-separated table names or "all" (default: all)
//	--verbose        Enable verbose logging
//	--migrate        Run database migrations before loading
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"

	"github.com/archon-research/stl/stl-verify/cmd/dataloader/parser"
	"github.com/archon-research/stl/stl-verify/cmd/dataloader/transform"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres"
	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/s3"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

var (
	postgresURL = flag.String("postgres-url", "", "PostgreSQL connection URL (required)")
	s3Bucket    = flag.String("s3-bucket", "stl-external-uploads-4981403a", "S3 bucket name")
	s3Prefix    = flag.String("s3-prefix", "brett/", "S3 prefix for data files")
	awsProfile  = flag.String("aws-profile", "sentinel-stg", "AWS profile name")
	dryRun      = flag.Bool("dry-run", false, "Parse and transform only, no DB writes")
	tables      = flag.String("tables", "all", "Comma-separated tables or 'all'")
	verbose     = flag.Bool("verbose", false, "Enable verbose logging")
	migrate     = flag.Bool("migrate", false, "Run database migrations before loading")
)

// Table loading order respects foreign key dependencies
var loadOrder = []string{
	"Token",
	"User",
	"Protocol",              // If we have protocol table in source
	"Reserve",               // → receipt_tokens + debt_tokens
	"UserReserveSnapshot",   // → borrowers + borrower_collateral
	"ReserveMarketSnapshot", // → sparklend_reserve_data
	"UserAccountSnapshot",   // → user_protocol_metadata
}

// Prefixes to search for data files
var s3Prefixes = []string{
	"sparklend_additional_snapshots/", // Process first (older data)
	"sparklend/",                      // Process second (overwrites on conflict)
}

// Repositories holds all repository instances for the dataloader.
type Repositories struct {
	Token    outbound.TokenRepository
	User     outbound.UserRepository
	Position outbound.PositionRepository
	Protocol outbound.ProtocolRepository

	// migrator is the protocol repo which owns the schema
	migrator *postgres.ProtocolRepository
}

func main() {
	flag.Parse()

	// Setup logging
	logLevel := slog.LevelInfo
	if *verbose {
		logLevel = slog.LevelDebug
	}
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		logger.Error("dataloader failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	if *postgresURL == "" {
		return fmt.Errorf("--postgres-url is required")
	}

	// Setup context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		logger.Info("received shutdown signal")
		cancel()
	}()

	// Initialize AWS config
	logger.Info("initializing AWS config", "profile", *awsProfile)
	awsCfg, err := config.LoadDefaultConfig(ctx,
		config.WithSharedConfigProfile(*awsProfile),
	)
	if err != nil {
		return fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Initialize S3 reader
	s3Reader := s3.NewReader(awsCfg, logger)

	// Initialize database connection and repositories
	var repos *Repositories
	if !*dryRun {
		logger.Info("connecting to database")
		dbCfg := postgres.DefaultDBConfig(*postgresURL)
		db, err := postgres.OpenDB(ctx, dbCfg)
		if err != nil {
			return fmt.Errorf("failed to connect to database: %w", err)
		}
		defer db.Close()

		protocolRepo := postgres.NewProtocolRepository(db, logger)
		repos = &Repositories{
			Token:    postgres.NewTokenRepository(db, logger),
			User:     postgres.NewUserRepository(db, logger),
			Position: postgres.NewPositionRepository(db, logger),
			Protocol: protocolRepo,
			migrator: protocolRepo,
		}

		// Run migrations if requested
		if *migrate {
			logger.Info("running migrations")
			if err := repos.migrator.Migrate(ctx); err != nil {
				return fmt.Errorf("failed to run migrations: %w", err)
			}
		}

		// Insert default chains
		if err := seedChains(ctx, repos.Protocol); err != nil {
			return fmt.Errorf("failed to seed chains: %w", err)
		}
	}

	// Determine which tables to load
	tablesToLoad := loadOrder
	if *tables != "all" {
		requested := strings.Split(*tables, ",")
		tablesToLoad = nil
		for _, t := range requested {
			t = strings.TrimSpace(t)
			if t != "" {
				tablesToLoad = append(tablesToLoad, t)
			}
		}
	}

	// Process each table
	stats := &LoadStats{}
	for _, tableName := range tablesToLoad {
		if err := ctx.Err(); err != nil {
			return err
		}

		logger.Info("processing table", "table", tableName)
		if err := processTable(ctx, logger, s3Reader, repos, tableName, stats); err != nil {
			return fmt.Errorf("failed to process table %s: %w", tableName, err)
		}
	}

	// Print summary
	logger.Info("load complete",
		"tokens", stats.Tokens,
		"users", stats.Users,
		"protocols", stats.Protocols,
		"borrowers", stats.Borrowers,
		"collateral", stats.Collateral,
		"reserveData", stats.ReserveData,
		"errors", stats.Errors,
	)

	return nil
}

// LoadStats tracks loading progress
type LoadStats struct {
	Tokens      int64
	Users       int64
	Protocols   int64
	Borrowers   int64
	Collateral  int64
	ReserveData int64
	Errors      int64
}

func seedChains(ctx context.Context, repo outbound.ProtocolRepository) error {
	chains := []*entity.Chain{
		entity.NewChain(1, "mainnet"),
		entity.NewChain(5, "goerli"),
		entity.NewChain(11155111, "sepolia"),
		entity.NewChain(42161, "arbitrum"),
		entity.NewChain(10, "optimism"),
		entity.NewChain(137, "polygon"),
		entity.NewChain(8453, "base"),
		entity.NewChain(43114, "avalanche"),
	}
	return repo.UpsertChains(ctx, chains)
}

func processTable(ctx context.Context, logger *slog.Logger, s3Reader outbound.S3Reader, repos *Repositories, tableName string, stats *LoadStats) error {
	// Find matching files across all prefixes
	var allFiles []outbound.S3File
	for _, prefix := range s3Prefixes {
		fullPrefix := *s3Prefix + prefix
		files, err := s3Reader.ListFiles(ctx, *s3Bucket, fullPrefix)
		if err != nil {
			logger.Warn("failed to list files", "prefix", fullPrefix, "error", err)
			continue
		}

		// Filter files for this table
		for _, f := range files {
			if strings.Contains(f.Key, tableName) && strings.HasSuffix(f.Key, ".sql.gz") {
				allFiles = append(allFiles, f)
			}
		}
	}

	if len(allFiles) == 0 {
		logger.Info("no files found for table", "table", tableName)
		return nil
	}

	// Sort by modification time (oldest first for proper merge behavior)
	sort.Slice(allFiles, func(i, j int) bool {
		return allFiles[i].LastModified.Before(allFiles[j].LastModified)
	})

	logger.Info("found files", "table", tableName, "count", len(allFiles))

	// Process each file
	for _, file := range allFiles {
		if err := ctx.Err(); err != nil {
			return err
		}

		logger.Info("processing file", "key", file.Key, "size", file.Size)
		start := time.Now()

		if err := processFile(ctx, logger, s3Reader, repos, file.Key, tableName, stats); err != nil {
			logger.Error("failed to process file", "key", file.Key, "error", err)
			stats.Errors++
			continue
		}

		logger.Info("processed file", "key", file.Key, "duration", time.Since(start))
	}

	return nil
}

func processFile(ctx context.Context, logger *slog.Logger, s3Reader outbound.S3Reader, repos *Repositories, key, tableName string, stats *LoadStats) error {
	// Stream file from S3
	reader, err := s3Reader.StreamFile(ctx, *s3Bucket, key)
	if err != nil {
		return fmt.Errorf("failed to stream file: %w", err)
	}
	defer reader.Close()

	// Parse COPY blocks
	blocks, err := parser.ParseCopyBlocks(reader)
	if err != nil {
		return fmt.Errorf("failed to parse COPY blocks: %w", err)
	}

	logger.Debug("parsed blocks", "count", len(blocks))

	// Find the relevant block for this table
	for _, block := range blocks {
		if !strings.Contains(block.Table, tableName) {
			continue
		}

		logger.Debug("processing block", "table", block.Table, "rows", len(block.Rows))

		switch {
		case strings.Contains(block.Table, "Token"):
			if err := loadTokens(ctx, logger, repos, block, stats); err != nil {
				return err
			}
		case strings.Contains(block.Table, "User") && !strings.Contains(block.Table, "Snapshot"):
			if err := loadUsers(ctx, logger, repos, block, stats); err != nil {
				return err
			}
		case strings.Contains(block.Table, "UserReserveSnapshot"):
			if err := loadUserReserveSnapshots(ctx, logger, repos, block, stats); err != nil {
				return err
			}
		case strings.Contains(block.Table, "ReserveMarketSnapshot"):
			if err := loadReserveMarketSnapshots(ctx, logger, repos, block, stats); err != nil {
				return err
			}
		case strings.Contains(block.Table, "UserAccountSnapshot"):
			if err := loadUserAccountSnapshots(ctx, logger, repos, block, stats); err != nil {
				return err
			}
		default:
			logger.Debug("skipping unknown table", "table", block.Table)
		}
	}

	return nil
}

const batchSize = 5000

func loadTokens(ctx context.Context, logger *slog.Logger, repos *Repositories, block *parser.CopyBlock, stats *LoadStats) error {
	colIndex := block.ColumnIndex()
	tokens := make([]*entity.Token, 0, batchSize)

	for _, row := range block.Rows {
		tokenRow, err := transform.ParseTokenRow(row, colIndex)
		if err != nil {
			logger.Debug("failed to parse token row", "error", err)
			continue
		}

		token, err := transform.TransformToken(tokenRow)
		if err != nil {
			logger.Debug("failed to transform token", "error", err)
			continue
		}

		tokens = append(tokens, token)

		if len(tokens) >= batchSize {
			if repos != nil && !*dryRun {
				if err := repos.Token.UpsertTokens(ctx, tokens); err != nil {
					return err
				}
			}
			stats.Tokens += int64(len(tokens))
			tokens = tokens[:0]
		}
	}

	// Flush remaining
	if len(tokens) > 0 && repos != nil && !*dryRun {
		if err := repos.Token.UpsertTokens(ctx, tokens); err != nil {
			return err
		}
		stats.Tokens += int64(len(tokens))
	}

	logger.Info("loaded tokens", "count", stats.Tokens)
	return nil
}

func loadUsers(ctx context.Context, logger *slog.Logger, repos *Repositories, block *parser.CopyBlock, stats *LoadStats) error {
	colIndex := block.ColumnIndex()
	users := make([]*entity.User, 0, batchSize)

	for _, row := range block.Rows {
		userRow, err := transform.ParseUserRow(row, colIndex)
		if err != nil {
			logger.Debug("failed to parse user row", "error", err)
			continue
		}

		user, err := transform.TransformUser(userRow)
		if err != nil {
			logger.Debug("failed to transform user", "error", err)
			continue
		}

		users = append(users, user)

		if len(users) >= batchSize {
			if repos != nil && !*dryRun {
				if err := repos.User.UpsertUsers(ctx, users); err != nil {
					return err
				}
			}
			stats.Users += int64(len(users))
			users = users[:0]
		}
	}

	// Flush remaining
	if len(users) > 0 && repos != nil && !*dryRun {
		if err := repos.User.UpsertUsers(ctx, users); err != nil {
			return err
		}
		stats.Users += int64(len(users))
	}

	logger.Info("loaded users", "count", stats.Users)
	return nil
}

func loadUserReserveSnapshots(ctx context.Context, logger *slog.Logger, repos *Repositories, block *parser.CopyBlock, stats *LoadStats) error {
	colIndex := block.ColumnIndex()
	borrowers := make([]*entity.Borrower, 0, batchSize)
	collateral := make([]*entity.BorrowerCollateral, 0, batchSize)

	// SparkLend mainnet protocol ID
	protocolID := transform.GetSparkLendMainnetProtocolID()

	// Track users we've seen (for auto-creating missing users)
	seenUsers := make(map[string]*entity.User)

	for _, row := range block.Rows {
		snapshotRow, err := transform.ParseUserReserveSnapshotRow(row, colIndex)
		if err != nil {
			logger.Debug("failed to parse snapshot row", "error", err)
			continue
		}

		// Get or create user
		user, ok := seenUsers[snapshotRow.UserID]
		if !ok {
			user, err = transform.CreateUserFromUserID(snapshotRow.UserID)
			if err != nil {
				logger.Debug("failed to create user", "user_id", snapshotRow.UserID, "error", err)
				continue
			}
			seenUsers[snapshotRow.UserID] = user
		}

		// Get token ID
		chainID, _ := transform.ParseChainFromUserID(snapshotRow.UserID)
		tokenID := transform.GenerateTokenID(chainID, transform.NormalizeAddress(snapshotRow.UnderlyingAsset))

		// Transform snapshot
		b, c := transform.TransformUserReserveSnapshot(snapshotRow, user.ID, protocolID, tokenID)

		if b != nil {
			borrowers = append(borrowers, b)
		}
		if c != nil {
			collateral = append(collateral, c)
		}

		// Flush batches
		if len(borrowers) >= batchSize {
			if repos != nil && !*dryRun {
				if err := repos.Position.UpsertBorrowers(ctx, borrowers); err != nil {
					return err
				}
			}
			stats.Borrowers += int64(len(borrowers))
			borrowers = borrowers[:0]
		}
		if len(collateral) >= batchSize {
			if repos != nil && !*dryRun {
				if err := repos.Position.UpsertBorrowerCollateral(ctx, collateral); err != nil {
					return err
				}
			}
			stats.Collateral += int64(len(collateral))
			collateral = collateral[:0]
		}
	}

	// Upsert any new users first
	if repos != nil && !*dryRun && len(seenUsers) > 0 {
		users := make([]*entity.User, 0, len(seenUsers))
		for _, u := range seenUsers {
			users = append(users, u)
		}
		if err := repos.User.UpsertUsers(ctx, users); err != nil {
			return fmt.Errorf("failed to upsert users: %w", err)
		}
		stats.Users += int64(len(users))
	}

	// Flush remaining
	if len(borrowers) > 0 && repos != nil && !*dryRun {
		if err := repos.Position.UpsertBorrowers(ctx, borrowers); err != nil {
			return err
		}
		stats.Borrowers += int64(len(borrowers))
	}
	if len(collateral) > 0 && repos != nil && !*dryRun {
		if err := repos.Position.UpsertBorrowerCollateral(ctx, collateral); err != nil {
			return err
		}
		stats.Collateral += int64(len(collateral))
	}

	logger.Info("loaded snapshots", "borrowers", stats.Borrowers, "collateral", stats.Collateral)
	return nil
}

func loadReserveMarketSnapshots(ctx context.Context, logger *slog.Logger, repos *Repositories, block *parser.CopyBlock, stats *LoadStats) error {
	colIndex := block.ColumnIndex()
	reserveData := make([]*entity.SparkLendReserveData, 0, batchSize)

	protocolID := transform.GetSparkLendMainnetProtocolID()

	for _, row := range block.Rows {
		snapshotRow, err := transform.ParseReserveMarketSnapshotRow(row, colIndex)
		if err != nil {
			logger.Debug("failed to parse reserve snapshot row", "error", err)
			continue
		}

		// Get token ID from underlying asset
		tokenID := transform.GenerateTokenID(1, transform.NormalizeAddress(snapshotRow.UnderlyingAsset))

		data := transform.TransformReserveMarketSnapshot(snapshotRow, protocolID, tokenID)
		reserveData = append(reserveData, data)

		if len(reserveData) >= batchSize {
			if repos != nil && !*dryRun {
				if err := repos.Protocol.UpsertSparkLendReserveData(ctx, reserveData); err != nil {
					return err
				}
			}
			stats.ReserveData += int64(len(reserveData))
			reserveData = reserveData[:0]
		}
	}

	// Flush remaining
	if len(reserveData) > 0 && repos != nil && !*dryRun {
		if err := repos.Protocol.UpsertSparkLendReserveData(ctx, reserveData); err != nil {
			return err
		}
		stats.ReserveData += int64(len(reserveData))
	}

	logger.Info("loaded reserve data", "count", stats.ReserveData)
	return nil
}

func loadUserAccountSnapshots(ctx context.Context, logger *slog.Logger, repos *Repositories, block *parser.CopyBlock, stats *LoadStats) error {
	colIndex := block.ColumnIndex()
	metadata := make([]*entity.UserProtocolMetadata, 0, batchSize)

	protocolID := transform.GetSparkLendMainnetProtocolID()

	// Track users we've seen
	seenUsers := make(map[string]*entity.User)

	for _, row := range block.Rows {
		snapshotRow, err := transform.ParseUserAccountSnapshotRow(row, colIndex)
		if err != nil {
			logger.Debug("failed to parse account snapshot row", "error", err)
			continue
		}

		// Get or create user
		user, ok := seenUsers[snapshotRow.UserID]
		if !ok {
			user, err = transform.CreateUserFromUserID(snapshotRow.UserID)
			if err != nil {
				logger.Debug("failed to create user", "user_id", snapshotRow.UserID, "error", err)
				continue
			}
			seenUsers[snapshotRow.UserID] = user
		}

		meta := transform.TransformUserAccountSnapshotToMetadata(snapshotRow, user.ID, protocolID)
		metadata = append(metadata, meta)

		if len(metadata) >= batchSize {
			if repos != nil && !*dryRun {
				if err := repos.User.UpsertUserProtocolMetadata(ctx, metadata); err != nil {
					return err
				}
			}
			metadata = metadata[:0]
		}
	}

	// Upsert any new users first
	if repos != nil && !*dryRun && len(seenUsers) > 0 {
		users := make([]*entity.User, 0, len(seenUsers))
		for _, u := range seenUsers {
			users = append(users, u)
		}
		if err := repos.User.UpsertUsers(ctx, users); err != nil {
			return fmt.Errorf("failed to upsert users: %w", err)
		}
	}

	// Flush remaining
	if len(metadata) > 0 && repos != nil && !*dryRun {
		if err := repos.User.UpsertUserProtocolMetadata(ctx, metadata); err != nil {
			return err
		}
	}

	logger.Info("loaded account snapshots", "count", len(seenUsers))
	return nil
}

package anchorage_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// AnchorageClient defines the interface for fetching data from the Anchorage API.
// The concrete Client in client.go implements this. Define here (consumer side)
// so the service can be unit-tested with a mock.
type AnchorageClient interface {
	FetchPackages(ctx context.Context) ([]Package, error)
	ForEachOperationsPage(ctx context.Context, afterID string, fn func([]Operation) error) error
}

// Default retry settings for transient API errors.
const (
	defaultMaxRetries  = 3
	defaultBaseBackoff = 2 * time.Second
	defaultMaxBackoff  = 30 * time.Second
)

// Service polls the Anchorage API and persists package snapshots.
type Service struct {
	client        AnchorageClient
	snapshotRepo  outbound.AnchorageSnapshotRepository
	operationRepo outbound.AnchorageOperationRepository
	primeID       int64
	pollInterval  time.Duration
	logger        *slog.Logger

	// Retry config (defaults applied in NewService).
	maxRetries  int
	baseBackoff time.Duration
	maxBackoff  time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewService creates a new anchorage tracker service.
func NewService(
	client AnchorageClient,
	snapshotRepo outbound.AnchorageSnapshotRepository,
	operationRepo outbound.AnchorageOperationRepository,
	primeID int64,
	pollInterval time.Duration,
	logger *slog.Logger,
) *Service {
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{
		client:        client,
		snapshotRepo:  snapshotRepo,
		operationRepo: operationRepo,
		primeID:       primeID,
		pollInterval:  pollInterval,
		logger:        logger,
		maxRetries:    defaultMaxRetries,
		baseBackoff:   defaultBaseBackoff,
		maxBackoff:    defaultMaxBackoff,
	}
}

// Start begins the polling loop in a background goroutine.
func (s *Service) Start(ctx context.Context) error {
	if s.pollInterval <= 0 {
		return fmt.Errorf("poll interval must be positive, got %s", s.pollInterval)
	}

	ctx, s.cancel = context.WithCancel(ctx)

	// Initial sync before entering the loop.
	if _, err := s.poll(ctx); err != nil {
		return fmt.Errorf("initial poll: %w", err)
	}
	if _, err := s.syncOperations(ctx); err != nil {
		return fmt.Errorf("initial operations sync: %w", err)
	}

	s.wg.Add(1)
	go s.run(ctx)

	s.logger.Info("anchorage tracker started",
		"prime_id", s.primeID,
		"poll_interval", s.pollInterval,
	)

	return nil
}

// Stop cancels the polling loop and waits for it to finish.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("anchorage tracker stopped")
	return nil
}

// BackfillOperations fetches all operations from the Anchorage API and stores them.
// If operations already exist, it resumes from the last known cursor.
// Returns the number of operations stored.
func (s *Service) BackfillOperations(ctx context.Context) (int, error) {
	s.logger.Info("starting operations backfill")
	n, err := s.syncOperations(ctx)
	if err != nil {
		return 0, err
	}
	s.logger.Info("backfill complete", "stored", n)
	return n, nil
}

// syncOperations fetches new operations page by page, persisting each page
// immediately to avoid unbounded memory accumulation during large backfills.
func (s *Service) syncOperations(ctx context.Context) (int, error) {
	cursor, err := s.operationRepo.GetLastCursor(ctx, s.primeID)
	if err != nil {
		return 0, fmt.Errorf("get last cursor: %w", err)
	}

	if cursor != "" {
		s.logger.Debug("fetching operations after", "cursor", cursor)
	}

	var total int
	err = s.client.ForEachOperationsPage(ctx, cursor, func(ops []Operation) error {
		entities, err := toOperationEntities(ops, s.primeID)
		if err != nil {
			return fmt.Errorf("convert operations: %w", err)
		}

		if err := s.operationRepo.SaveOperations(ctx, entities); err != nil {
			return fmt.Errorf("save operations: %w", err)
		}

		total += len(entities)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("sync operations: %w", err)
	}

	if total > 0 {
		s.logger.Info("synced operations", "count", total)
	}
	return total, nil
}

func (s *Service) run(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runWithRetry(ctx, "poll", func() error {
				_, err := s.poll(ctx)
				return err
			})
			s.runWithRetry(ctx, "sync operations", func() error {
				_, err := s.syncOperations(ctx)
				return err
			})
		}
	}
}

// runWithRetry executes fn with exponential backoff on failure.
// It retries up to maxRetries times before logging the error and moving on.
func (s *Service) runWithRetry(ctx context.Context, name string, fn func() error) {
	for attempt := 0; attempt <= s.maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return
		}

		if attempt == s.maxRetries {
			s.logger.Error(name+" failed after retries",
				"error", err,
				"attempts", attempt+1,
			)
			return
		}

		backoff := time.Duration(math.Min(
			float64(s.baseBackoff)*math.Pow(2, float64(attempt)),
			float64(s.maxBackoff),
		))

		s.logger.Warn(name+" failed, retrying",
			"error", err,
			"attempt", attempt+1,
			"backoff", backoff,
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
	}
}

// poll fetches all packages from the Anchorage API and stores snapshots.
func (s *Service) poll(ctx context.Context) (int, error) {
	s.logger.Info("polling anchorage packages")

	packages, err := s.client.FetchPackages(ctx)
	if err != nil {
		return 0, fmt.Errorf("fetch packages: %w", err)
	}

	s.logger.Info("fetched packages", "count", len(packages))

	now := time.Now().UTC()
	snapshots, err := toSnapshots(packages, s.primeID, now)
	if err != nil {
		return 0, fmt.Errorf("convert packages: %w", err)
	}

	if len(snapshots) == 0 {
		s.logger.Info("no snapshots to store")
		return 0, nil
	}

	if err := s.snapshotRepo.SaveSnapshots(ctx, snapshots); err != nil {
		return 0, fmt.Errorf("save snapshots: %w", err)
	}

	s.logger.Info("stored snapshots", "count", len(snapshots))
	return len(snapshots), nil
}

// toSnapshots flattens packages into one snapshot row per collateral asset.
func toSnapshots(packages []Package, primeID int64, now time.Time) ([]entity.AnchoragePackageSnapshot, error) {
	var snapshots []entity.AnchoragePackageSnapshot

	for _, pkg := range packages {
		ltvTimestamp, err := time.Parse(time.RFC3339Nano, pkg.LTVTimestamp)
		if err != nil {
			return nil, fmt.Errorf("parse ltv_timestamp for package %s: %w", pkg.PackageID, err)
		}

		for _, asset := range pkg.CollateralAssets {
			snapshots = append(snapshots, entity.AnchoragePackageSnapshot{
				PrimeID:        primeID,
				PackageID:      pkg.PackageID,
				PledgorID:      pkg.PledgorID,
				SecuredPartyID: pkg.SecuredPartyID,
				Active:         pkg.Active,
				State:          pkg.State,

				CurrentLTV:    pkg.CurrentLTV,
				ExposureValue: pkg.ExposureValue,
				PackageValue:  pkg.PackageValue,

				MarginCallLTV:   pkg.MarginCall.LTV,
				CriticalLTV:     pkg.Critical.LTV,
				MarginReturnLTV: pkg.MarginReturn.LTV,

				AssetType:          asset.Asset.AssetType,
				CustodyType:        asset.Asset.Type,
				AssetPrice:         asset.Price,
				AssetQuantity:      asset.Quantity,
				AssetWeightedValue: asset.WeightedValue,

				LTVTimestamp: ltvTimestamp,
				SnapshotTime: now,
			})
		}
	}

	return snapshots, nil
}

// toOperationEntities converts API operations to domain entities.
func toOperationEntities(ops []Operation, primeID int64) ([]entity.AnchorageOperation, error) {
	entities := make([]entity.AnchorageOperation, 0, len(ops))

	for _, op := range ops {
		createdAt, err := time.Parse(time.RFC3339Nano, op.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("parse created_at for operation %s: %w", op.ID, err)
		}

		entities = append(entities, entity.AnchorageOperation{
			PrimeID:       primeID,
			OperationID:   op.ID,
			Action:        op.Action,
			OperationType: op.Type,
			TypeID:        op.TypeID,
			AssetType:     op.Asset.AssetType,
			CustodyType:   op.Asset.Type,
			Quantity:      op.Quantity,
			Notes:         op.Notes,
			CreatedAt:     createdAt,
		})
	}

	return entities, nil
}

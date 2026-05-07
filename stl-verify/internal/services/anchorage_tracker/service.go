package anchorage_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// AnchorageClient defines the interface for fetching data from the Anchorage API.
type AnchorageClient interface {
	FetchPackages(ctx context.Context) ([]Package, error)
	ForEachOperationsPage(ctx context.Context, afterID string, fn func([]Operation) error) error
}

// Service fetches Anchorage collateral data and persists it.
type Service struct {
	client        AnchorageClient
	snapshotRepo  outbound.AnchorageSnapshotRepository
	operationRepo outbound.AnchorageOperationRepository
	primeID       int64
	logger        *slog.Logger
}

// NewService creates a new anchorage tracker service.
func NewService(
	client AnchorageClient,
	snapshotRepo outbound.AnchorageSnapshotRepository,
	operationRepo outbound.AnchorageOperationRepository,
	primeID int64,
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
		logger:        logger,
	}
}

// Run fetches current package snapshots and syncs new operations, then returns.
func (s *Service) Run(ctx context.Context) error {
	if _, err := s.poll(ctx); err != nil {
		return fmt.Errorf("poll packages: %w", err)
	}
	if _, err := s.syncOperations(ctx); err != nil {
		return fmt.Errorf("sync operations: %w", err)
	}
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

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
	ForEachOperationsPage(ctx context.Context, fn func([]Operation) error) error
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

// BackfillOperations fetches all operations from the Anchorage API and stores
// the ones not already present. Returns the number of operations stored.
func (s *Service) BackfillOperations(ctx context.Context) (int, error) {
	s.logger.Info("starting operations backfill")
	n, err := s.syncOperations(ctx)
	if err != nil {
		return 0, err
	}
	s.logger.Info("backfill complete", "stored", n)
	return n, nil
}

// syncOperations fetches the full operation list page by page and persists
// only the operations whose id is not already stored for the prime. Each page
// is saved immediately so memory stays bounded on a large backfill.
//
// The full fetch is deliberate. Anchorage's operations endpoint takes an
// `afterId` cursor of the form `timestamp|id`, but the timestamp unit and the
// sort direction are undocumented; the previous implementation synthesised
// `unix_seconds|operation_id` and received an empty list on every run after
// the first backfill, so no operation after 2026-04-07 was ever stored
// (VEC-826). The feed is tens of rows per quarter, so re-reading it every run
// costs one or two requests. The id filter lives here rather than in the
// INSERT's ON CONFLICT clause because the processing_version trigger assigns
// a fresh version to a re-insert from a different build, which would make a
// naive re-insert append a duplicate row instead of being ignored.
func (s *Service) syncOperations(ctx context.Context) (int, error) {
	known, err := s.operationRepo.KnownOperationIDs(ctx, s.primeID)
	if err != nil {
		return 0, fmt.Errorf("list known operations: %w", err)
	}

	var fetched, stored int
	err = s.client.ForEachOperationsPage(ctx, func(ops []Operation) error {
		fetched += len(ops)

		fresh := make([]Operation, 0, len(ops))
		for _, op := range ops {
			if _, seen := known[op.ID]; seen {
				continue
			}
			known[op.ID] = struct{}{}
			fresh = append(fresh, op)
		}
		if len(fresh) == 0 {
			return nil
		}

		entities, err := toOperationEntities(fresh, s.primeID)
		if err != nil {
			return fmt.Errorf("convert operations: %w", err)
		}

		if err := s.operationRepo.SaveOperations(ctx, entities); err != nil {
			return fmt.Errorf("save operations: %w", err)
		}

		stored += len(entities)
		return nil
	})
	if err != nil {
		return 0, fmt.Errorf("sync operations: %w", err)
	}

	s.logger.Info("synced operations", "fetched", fetched, "stored", stored)
	return stored, nil
}

// poll fetches all packages from the Anchorage API and stores snapshots.
func (s *Service) poll(ctx context.Context) (int, error) {
	s.logger.Info("polling anchorage packages")

	packages, err := s.client.FetchPackages(ctx)
	if err != nil {
		return 0, fmt.Errorf("fetch packages: %w", err)
	}

	s.logger.Info("fetched packages", "count", len(packages))

	activePackages := s.filterActivePackages(packages)

	now := time.Now().UTC()
	snapshots, err := toSnapshots(activePackages, s.primeID, now)
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

// filterActivePackages drops inactive packages. Anchorage returns a null
// ltvTimestamp and currentLtv for them (no exposure → no LTV), which the
// schema (NOT NULL, NUMERIC) cannot represent.
func (s *Service) filterActivePackages(packages []Package) []Package {
	active := make([]Package, 0, len(packages))
	for _, pkg := range packages {
		if !pkg.Active {
			s.logger.Warn("skipping inactive anchorage package",
				"package_id", pkg.PackageID,
				"state", pkg.State,
				"exposure_value", pkg.ExposureValue,
			)
			continue
		}
		active = append(active, pkg)
	}
	return active
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

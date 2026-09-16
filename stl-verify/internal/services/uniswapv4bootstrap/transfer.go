package uniswapv4bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"slices"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockversion"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4indexer"
)

const transferScanSubject = "uniswap-v4 PositionManager ERC-721 Transfer logs"

// NFTTransferRecorder counts the rows this backfill queued and landed.
//
// Required, not optional: VectorUniswapV4NFTTransferGrowthHigh reads that
// counter as the table's growth, so a backfill that wrote through the repository
// without it would grow the table while the tripwire stayed flat.
type NFTTransferRecorder interface {
	RecordNFTTransferRows(ctx context.Context, attempted, written int)
}

// TransferVersions answers a scanned height's block_version from the raw archive —
// block_states is the watchers' operational table, off limits here, and retains far
// less than this range anyway — and reports what the run it served asked for.
type TransferVersions interface {
	uniswapv4indexer.BlockVersionResolver
	Summary() blockversion.RunSummary
}

type TransferDeps struct {
	PositionManager uniswapv4indexer.RegisteredPositionManager
	LogScan         outbound.LogScanClient
	// NewVersions is called once per run, because a Resolver memoises the heights it
	// proved against the archive: one shared between runs would hand a later run a
	// version it never proved, and its map is written without a mutex, which two
	// concurrent runs turn into a fatal `concurrent map writes`.
	NewVersions func() TransferVersions
	Repo        outbound.UniswapV4NFTTransferWriter
	TxManager   outbound.TxManager
	Progress    TransferProgressStore
	Telemetry   NFTTransferRecorder
	Logger      *slog.Logger
	Config      Config
}

// TransferService backfills the posm ERC-721 Transfer log from the
// PositionManager's deploy block, which the live indexer can never reach: a
// token minted before it went live and held ever since emits no Transfer for it
// to learn from, and uniswap_v4_position.owner names the PositionManager rather
// than the holder for every posm-managed position.
type TransferService struct {
	positionManager uniswapv4indexer.RegisteredPositionManager
	logScan         outbound.LogScanClient
	newVersions     func() TransferVersions
	repo            outbound.UniswapV4NFTTransferWriter
	txMgr           outbound.TxManager
	progress        TransferProgressStore
	telemetry       NFTTransferRecorder
	logger          *slog.Logger
	cfg             Config
}

type TransferSummary struct {
	PinnedBlock int64
	PinnedHash  common.Hash
	// FromBlock is where THIS attempt started scanning, so it is zero on an attempt
	// that found the scan already finished and scanned nothing.
	FromBlock        int64
	ResumedFromBlock int64
	ScanWindows      int
	ScanNarrowings   int
	ScanLogs         int
	TransfersDecoded int
	TransfersWritten int64
	Batches          int
	LowestBlockSeen  int64
	HighestBlockSeen int64
}

func NewTransferService(deps TransferDeps) (*TransferService, error) {
	if err := deps.validate(); err != nil {
		return nil, err
	}
	cfg := deps.Config.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return &TransferService{
		positionManager: deps.PositionManager,
		logScan:         deps.LogScan,
		newVersions:     deps.NewVersions,
		repo:            deps.Repo,
		txMgr:           deps.TxManager,
		progress:        deps.Progress,
		telemetry:       deps.Telemetry,
		logger:          deps.Logger,
		cfg:             cfg,
	}, nil
}

func (d TransferDeps) validate() error {
	switch {
	// address(0) matches no log, so a registry that lost the posm row would scan
	// the whole history and decode nothing, reporting success.
	case d.PositionManager.Address == (common.Address{}) || d.PositionManager.ID <= 0:
		return fmt.Errorf("a registered PositionManager with a non-zero address and a positive row id is required, got %+v", d.PositionManager)
	// The live indexer never reads it, so an unseeded deploy_block reaches here
	// rather than failing that worker's boot; scanning from genesis is not a
	// sensible fallback, so this refuses instead.
	case d.PositionManager.DeployBlock <= 0:
		return fmt.Errorf("uniswap_v4_position_manager.deploy_block for chain %d is %d: the scan would start at genesis, so the registry row needs a correcting version appended", d.Config.ChainID, d.PositionManager.DeployBlock)
	case d.LogScan == nil:
		return fmt.Errorf("log scan client is required")
	case d.NewVersions == nil:
		return fmt.Errorf("a block version resolver factory is required: a scanned log carries no version of its own")
	case d.Repo == nil:
		return fmt.Errorf("repo is required")
	case d.TxManager == nil:
		return fmt.Errorf("txManager is required")
	case d.Progress == nil:
		return fmt.Errorf("progress store is required")
	case d.Telemetry == nil:
		return fmt.Errorf("telemetry is required: the table's growth tripwire reads its counter, so a backfill without it grows the table invisibly")
	case d.Logger == nil:
		return fmt.Errorf("logger is required")
	}
	return nil
}

// Run replays the PositionManager's whole Transfer history up to one pinned
// finality-safe height, appending through the live indexer's insert.
//
// No chain STATE is read: a log carries its own block height, timestamp, token id
// and both parties, which is what makes this cheaper than the position bootstrap
// and why it needs no pinned multicall. Each row's block_version comes from the
// raw archive instead, since a log carries none. The pin is derived and held for
// two reasons: it keeps the scan below the reorg window, so the live indexer owns
// everything above it; and a pin that moves under the run means a reorg deeper
// than the finality depth, which the operator has to hear about.
//
// A run that dies part-way resumes on a later attempt through the
// TransferProgressStore, continuing from the first window it had not finished. No
// correctness depends on the record: a run that resumes from nothing simply
// rescans, and the write is the live path's, so a same-build rescan conflicts away.
func (s *TransferService) Run(ctx context.Context) (_ TransferSummary, runErr error) {
	// One Resolver per run, never per process: see TransferDeps.NewVersions.
	versions := s.newVersions()
	defer func() { s.logResolvedBlockVersions(ctx, versions.Summary(), runErr) }()

	pin, resumeFrom, err := s.resumePoint(ctx)
	if err != nil {
		return TransferSummary{}, err
	}
	summary := TransferSummary{PinnedBlock: pin.number, PinnedHash: pin.hash, ResumedFromBlock: resumeFrom}

	// A cursor past the pin means an earlier attempt finished the scan; resumePoint
	// has just re-read that pin, which is all the closing check would verify.
	if resumeFrom == pin.number+1 {
		s.logger.Info("uniswap-v4 posm transfer backfill already scanned to its pin on an earlier attempt",
			"chainId", s.cfg.ChainID, "pinnedBlock", pin.number, "nextBlock", resumeFrom)
		return summary, nil
	}

	from, err := s.scanStart(pin, resumeFrom)
	if err != nil {
		return summary, err
	}
	summary.FromBlock = from
	s.logStart(pin, from)

	if err := s.scanAndPersist(ctx, versions, from, pin, &summary); err != nil {
		return summary, err
	}
	// Every row is already committed, so this cannot prevent a bad write; it is
	// how the operator learns the scanned range was reorged under the run.
	if err := assertPinStable(ctx, s.logScan, pin); err != nil {
		return summary, err
	}
	s.warnIfHistoryLooksEmpty(summary)
	return summary, nil
}

// logResolvedBlockVersions reports what the run asked the archive for, the way
// morpho-v2-bootstrap does. A version above 0 across deep history is the archive's
// convention, not evidence of a reorg (see internal/pkg/blockversion).
func (s *TransferService) logResolvedBlockVersions(ctx context.Context, summary blockversion.RunSummary, runErr error) {
	if summary.HeightsResolved == 0 {
		return
	}
	outcome, level := "completed", slog.LevelInfo
	if runErr != nil {
		outcome, level = "aborted", slog.LevelError
	}
	attrs := []slog.Attr{
		slog.String("outcome", outcome),
		slog.Int64("chainId", s.cfg.ChainID),
		slog.Int("heights", summary.HeightsResolved),
	}
	for _, extent := range summary.Versions {
		attrs = append(attrs, slog.Group(fmt.Sprintf("version_%d", extent.Version),
			slog.Int("heights", extent.Heights),
			slog.Int64("from", extent.From),
			slog.Int64("to", extent.To)))
	}
	s.logger.LogAttrs(ctx, level, "uniswap-v4 posm transfer block versions resolved from the raw archive", attrs...)
}

// resumePoint is the pin this run scans up to and the height it resumes from.
func (s *TransferService) resumePoint(ctx context.Context) (pinnedBlock, int64, error) {
	recorded, found, err := s.progress.LoadProgress(ctx)
	if err != nil {
		return pinnedBlock{}, 0, fmt.Errorf("loading transfer backfill progress: %w", err)
	}
	if found && recorded.scopeMatches(s.cfg.ChainID, s.positionManager.ID) {
		pin, err := reReadPin(ctx, s.logScan, recorded.PinnedBlock, common.HexToHash(recorded.PinnedHash), "when this run's progress was recorded")
		if err != nil {
			return pinnedBlock{}, 0, fmt.Errorf("resuming the recorded transfer scan: %w", err)
		}
		s.logger.Info("resuming uniswap-v4 posm transfer backfill from recorded progress",
			"chainId", s.cfg.ChainID, "pinnedBlock", pin.number, "pinnedHash", pin.hash, "nextBlock", recorded.NextBlock)
		return pin, recorded.NextBlock, nil
	}
	if found {
		s.logger.Warn("recorded transfer backfill progress belongs to another chain or PositionManager, pinning afresh",
			"chainId", s.cfg.ChainID, "recordedChainId", recorded.ChainID,
			"positionManagerRowId", s.positionManager.ID, "recordedPositionManagerRowId", recorded.PositionManagerID)
	}

	pin, err := pinBlock(ctx, s.logScan, s.cfg.FinalityDepth, s.cfg.PinBlock)
	if err != nil {
		return pinnedBlock{}, 0, err
	}
	return pin, 0, nil
}

// scanStart is the deploy block, or an explicit FromBlock, or wherever an
// earlier attempt got to — whichever is highest.
func (s *TransferService) scanStart(pin pinnedBlock, resumeFrom int64) (int64, error) {
	from := s.cfg.FromBlock
	if from == 0 {
		from = s.positionManager.DeployBlock
	}
	from = max(from, resumeFrom)
	if from > pin.number {
		return 0, fmt.Errorf("scan start %d is above the pinned block %d: nothing to scan", from, pin.number)
	}
	return from, nil
}

func (s *TransferService) logStart(pin pinnedBlock, from int64) {
	s.logger.Info("starting uniswap-v4 posm transfer backfill",
		"chainId", s.cfg.ChainID, "positionManager", s.positionManager.Address,
		"positionManagerRowId", s.positionManager.ID, "deployBlock", s.positionManager.DeployBlock,
		"fromBlock", from, "pinnedBlock", pin.number, "pinnedHash", pin.hash, "pinnedTimestamp", pin.ts,
		"initialWindow", s.cfg.InitialWindow, "transferBatch", s.cfg.TransferBatch)
}

// scanAndPersist walks the range one adaptive window at a time, committing each
// window's rows before recording it as done. A window is the resume unit, so a
// kill mid-window costs one rescan and no rows.
func (s *TransferService) scanAndPersist(ctx context.Context, versions TransferVersions, from int64, pin pinnedBlock, summary *TransferSummary) error {
	scanner := &logWindowScanner{
		client:  s.logScan,
		filter:  s.baseFilter(),
		policy:  windowPolicy{initial: s.cfg.InitialWindow, min: s.cfg.MinWindow, max: s.cfg.MaxWindow},
		logger:  s.logger,
		subject: transferScanSubject,
	}

	stats, err := scanner.scan(ctx, from, pin.number, func(w logWindow) error {
		return s.persistWindow(ctx, versions, w, summary)
	})
	summary.ScanWindows, summary.ScanNarrowings, summary.ScanLogs = stats.windows, stats.narrowings, stats.logs
	if err != nil {
		return err
	}
	s.logger.Info("uniswap-v4 posm transfer scan complete",
		"chainId", s.cfg.ChainID, "fromBlock", from, "toBlock", pin.number,
		"windows", stats.windows, "narrowings", stats.narrowings, "logs", stats.logs,
		"transfersWritten", summary.TransfersWritten)
	return nil
}

func (s *TransferService) persistWindow(ctx context.Context, versions TransferVersions, w logWindow, summary *TransferSummary) error {
	transfers, err := uniswapv4indexer.NFTTransfersFromLogs(ctx, toSharedLogs(w.logs), s.positionManager, versions)
	if err != nil {
		return err
	}
	summary.TransfersDecoded += len(transfers)
	summary.recordBlockSpan(transfers)

	for chunk := range slices.Chunk(transfers, s.cfg.TransferBatch) {
		if err := ctx.Err(); err != nil {
			return err
		}
		written, err := s.persist(ctx, chunk)
		if err != nil {
			return fmt.Errorf("persisting %d transfers of blocks %d-%d: %w", len(chunk), w.from, w.to, err)
		}
		summary.TransfersWritten += written
		summary.Batches++
		s.telemetry.RecordNFTTransferRows(ctx, len(chunk), int(written))
	}

	return s.recordWindowDone(ctx, w, summary)
}

// recordWindowDone advances the resume point past a window whose rows have all
// committed.
func (s *TransferService) recordWindowDone(ctx context.Context, w logWindow, summary *TransferSummary) error {
	progress := TransferProgress{
		ChainID:           s.cfg.ChainID,
		PositionManagerID: s.positionManager.ID,
		PinnedBlock:       summary.PinnedBlock,
		PinnedHash:        summary.PinnedHash.Hex(),
		NextBlock:         w.to + 1,
	}
	if err := s.progress.SaveProgress(ctx, progress); err != nil {
		return fmt.Errorf("recording transfer backfill progress after blocks %d-%d: %w", w.from, w.to, err)
	}
	return nil
}

func (s *TransferService) persist(ctx context.Context, transfers []*entity.UniswapV4PositionNFTTransfer) (int64, error) {
	return persistInOneTransaction(ctx, s.txMgr, transfers, s.repo.SaveNFTTransfers)
}

// The emitting ADDRESS is what makes the result the posm's: this topic0 is shared
// by every ERC-20 and ERC-721 transfer on the chain.
func (s *TransferService) baseFilter() outbound.LogFilter {
	return outbound.LogFilter{
		Address: s.positionManager.Address,
		Topic0:  abis.TransferTopic0(),
	}
}

// A scan of the WHOLE history that decoded nothing is what a wrong PositionManager
// address looks like; it is also what a chain with no posm activity yet looks
// like, so it cannot be an error. Gated on having started at the deploy block: a
// resumed attempt covers only a tail, and a quiet tail says nothing about the
// address.
func (s *TransferService) warnIfHistoryLooksEmpty(summary TransferSummary) {
	if summary.ScanLogs > 0 || summary.FromBlock != s.positionManager.DeployBlock {
		return
	}
	s.logger.Warn("uniswap-v4 posm transfer backfill decoded no transfers at all",
		"chainId", s.cfg.ChainID, "positionManager", s.positionManager.Address,
		"fromBlock", summary.FromBlock, "toBlock", summary.PinnedBlock, "windows", summary.ScanWindows,
		"hint", "expected on a chain with no posm activity; otherwise the uniswap_v4_position_manager row's protocol address is wrong")
}

func (s *TransferSummary) recordBlockSpan(transfers []*entity.UniswapV4PositionNFTTransfer) {
	for _, t := range transfers {
		if s.LowestBlockSeen == 0 || t.BlockNumber < s.LowestBlockSeen {
			s.LowestBlockSeen = t.BlockNumber
		}
		s.HighestBlockSeen = max(s.HighestBlockSeen, t.BlockNumber)
	}
}

package allocation_tracker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/archiving"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type TransactionReceipt struct {
	TransactionHash string      `json:"transactionHash"`
	BlockNumber     string      `json:"blockNumber"`
	BlockHash       string      `json:"blockHash"`
	From            string      `json:"from"`
	To              string      `json:"to"`
	Status          string      `json:"status"`
	Logs            []types.Log `json:"logs"`
}

type Service struct {
	config      Config
	sqsConsumer outbound.SQSConsumer
	cache       outbound.BlockCacheReader
	extractor   *TransferExtractor
	registry    *SourceRegistry
	entryLookup map[EntryKey]*TokenEntry
	entries     []*TokenEntry
	// transferAliases routes a Transfer log to the entry it belongs to. Written and
	// read only from processBlock, which the SQS loop runs one block at a time.
	transferAliases  map[transferRouteKey]common.Address
	handler          AllocationHandler
	metrics          outbound.BackupMetricsRecorder
	ctx              context.Context
	cancel           context.CancelFunc
	wg               sync.WaitGroup // tracks the SQS run loop so Stop can drain it
	logger           *slog.Logger
	blocksSinceSweep int
}

func NewService(
	config Config,
	sqsConsumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	registry *SourceRegistry,
	entries []*TokenEntry,
	handler AllocationHandler,
	proxies []ProxyConfig,
) (*Service, error) {
	defaults := ConfigDefaults()
	if config.MaxMessages == 0 {
		config.MaxMessages = defaults.MaxMessages
	}
	if config.PollInterval == 0 {
		config.PollInterval = defaults.PollInterval
	}
	if config.SweepEveryNBlocks < 0 {
		return nil, fmt.Errorf("sweep every n blocks must not be negative, got %d", config.SweepEveryNBlocks)
	}
	if config.SweepEveryNBlocks == 0 {
		config.SweepEveryNBlocks = defaults.SweepEveryNBlocks
	}
	if config.Logger == nil {
		config.Logger = defaults.Logger
	}
	if config.ChainID == 0 {
		return nil, fmt.Errorf("chain ID is required")
	}
	if len(proxies) == 0 {
		return nil, fmt.Errorf("at least one proxy is required for chain ID %d", config.ChainID)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("at least one token entry is required for chain ID %d", config.ChainID)
	}
	if err := validateScopedEntriesAndProxies(entries, proxies, config.ChainID); err != nil {
		return nil, fmt.Errorf("validating scoped entries/proxies: %w", err)
	}

	return &Service{
		config:          config,
		sqsConsumer:     sqsConsumer,
		cache:           cache,
		extractor:       NewTransferExtractor(proxies),
		registry:        registry,
		entryLookup:     BuildEntryLookup(entries),
		entries:         entries,
		transferAliases: make(map[transferRouteKey]common.Address),
		handler:         handler,
		metrics:         config.Metrics,
		logger:          config.Logger.With("component", "allocation-tracker"),
	}, nil
}

func validateScopedEntriesAndProxies(entries []*TokenEntry, proxies []ProxyConfig, chainID int64) error {
	chainName, ok := entity.ChainIDToName[chainID]
	if !ok {
		return fmt.Errorf("unknown chain ID %d", chainID)
	}

	seenEntries := make(map[EntryKey]struct{}, len(entries))
	for i, entry := range entries {
		if entry == nil {
			return fmt.Errorf("entry at index %d is nil", i)
		}
		if entry.Chain != chainName {
			return fmt.Errorf(
				"entry %s/%s has chain %s, want %s",
				entry.ContractAddress.Hex(),
				entry.WalletAddress.Hex(),
				entry.Chain,
				chainName,
			)
		}
		key := entry.Key()
		if _, ok := seenEntries[key]; ok {
			return fmt.Errorf(
				"duplicate token entry for contract=%s wallet=%s chain=%s",
				entry.ContractAddress.Hex(),
				entry.WalletAddress.Hex(),
				entry.Chain,
			)
		}
		seenEntries[key] = struct{}{}
	}

	seenProxies := make(map[common.Address]struct{}, len(proxies))
	for _, proxy := range proxies {
		if proxy.Chain != chainName {
			return fmt.Errorf("proxy %s has chain %s, want %s", proxy.Address.Hex(), proxy.Chain, chainName)
		}
		if _, ok := seenProxies[proxy.Address]; ok {
			return fmt.Errorf("duplicate proxy address %s for chain %s", proxy.Address.Hex(), proxy.Chain)
		}
		seenProxies[proxy.Address] = struct{}{}
	}

	return nil
}

func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	loop := sqsutil.Config{
		Consumer:     s.sqsConsumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
		ChainID:      s.config.ChainID,
	}
	if err := loop.Validate(); err != nil {
		return err
	}

	s.wg.Go(func() {
		sqsutil.RunLoop(s.ctx, loop, s.processBlock)
	})

	s.logger.Info("started",
		"chainID", s.config.ChainID,
		"entries", len(s.entries),
		"sweepEveryNBlocks", s.config.SweepEveryNBlocks)
	return nil
}

// Stop cancels the SQS processing loop and waits for the loop goroutine to
// exit. A handler the drain abandoned can outlive it; archiving's drain gate is
// what refuses that handler's late archive write.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("stopped")
	return nil
}

func (s *Service) processBlock(
	ctx context.Context,
	event outbound.BlockEvent,
) (retErr error) {
	ctx = archiving.WithBlockVersion(ctx, event.Version)
	ctx = archiving.WithBlockNumber(ctx, event.BlockNumber)
	start := time.Now()
	defer func() { s.recordBlockMetrics(ctx, start, retErr) }()

	receiptsJSON, err := s.cache.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("fetching receipts from cache: %w", err)
	}
	if receiptsJSON == nil {
		return fmt.Errorf("receipts not found in cache for block %d (chain=%d, version=%d)", event.BlockNumber, event.ChainID, event.Version)
	}

	var receipts []TransactionReceipt
	if err := json.Unmarshal(receiptsJSON, &receipts); err != nil {
		return fmt.Errorf("parse receipts: %w", err)
	}

	var transfers []*TransferEvent
	for _, receipt := range receipts {
		transfers = append(transfers, s.extractor.Extract(receipt)...)
	}

	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()

	if len(transfers) > 0 {
		if err := s.processTransfers(ctx, event, transfers, blockTimestamp, start); err != nil {
			return err
		}
	}

	// Periodic sweep. VEC-188: a sweep failure must NOT reset the counter
	// (so the next block retries the sweep) and must propagate so SQS
	// redelivers — see TestProcessBlock_FailedSweepDoesNotResetCounter and
	// TestProcessBlock_SweepFetchFailure_ReturnsError.
	s.blocksSinceSweep++
	if s.blocksSinceSweep >= s.config.SweepEveryNBlocks {
		blockHash, err := event.ParsedBlockHash()
		if err != nil {
			return fmt.Errorf("parse block hash: %w", err)
		}
		if err := s.sweep(ctx, event.BlockNumber, blockHash, event.Version, blockTimestamp); err != nil {
			return fmt.Errorf("sweep block %d: %w", event.BlockNumber, err)
		}
		s.blocksSinceSweep = 0
	}

	return nil
}

// recordBlockMetrics records the per-block liveness + latency sample once per
// processBlock via defer (see Config.Metrics for why this is the liveness
// signal). Nil-safe: a service constructed without Config.Metrics (most unit
// tests) records nothing.
func (s *Service) recordBlockMetrics(ctx context.Context, start time.Time, err error) {
	if s.metrics == nil {
		return
	}
	status := outbound.StatusSuccess
	if err != nil {
		status = outbound.StatusError
	}
	s.metrics.RecordBlockProcessed(ctx, status)
	s.metrics.RecordProcessingLatency(ctx, time.Since(start), status)
}

// processTransfers snapshots the entries this block's Transfer logs touched.
// VEC-188 invariant: every failure here propagates so SQS NACKs the message —
// see TestProcessBlock_PartialFetchFailure_*.
func (s *Service) processTransfers(
	ctx context.Context,
	event outbound.BlockEvent,
	transfers []*TransferEvent,
	blockTimestamp time.Time,
	start time.Time,
) error {
	blockHash, err := event.ParsedBlockHash()
	if err != nil {
		return fmt.Errorf("parse block hash: %w", err)
	}
	if err := s.resolveMissingTransferAliases(ctx, blockHash, event.BlockNumber); err != nil {
		return fmt.Errorf("resolve transfer aliases for block %d: %w", event.BlockNumber, err)
	}

	affected := s.matchTransfers(transfers)
	if len(affected) == 0 {
		return nil
	}

	fetch, err := s.registry.FetchAll(ctx, affected, blockHash)
	if err != nil {
		return fmt.Errorf("fetch observations for block %d: %w", event.BlockNumber, err)
	}
	named, err := namedEmittersFromBalances(affected, fetch.Balances)
	if err != nil {
		return fmt.Errorf("read the share tokens of block %d: %w", event.BlockNumber, err)
	}
	routes, err := s.nextTransferRoutes(named, event.BlockNumber)
	if err != nil {
		return fmt.Errorf("route transfers for block %d: %w", event.BlockNumber, err)
	}

	snapshots := s.buildSnapshots(affected, fetch.Balances, transfers, event, blockTimestamp)
	supplies := buildSupplySnapshots(fetch.Supplies, event.ChainID, event.BlockNumber, event.Version, blockTimestamp, "event")
	if len(snapshots) > 0 || len(supplies) > 0 {
		if err := s.handler.HandleBatch(ctx, &SnapshotBatch{Snapshots: snapshots, Supplies: supplies}); err != nil {
			return fmt.Errorf("handler: %w", err)
		}
	}
	// Swapped only now: buildSnapshots keys its transfer lookup the same way
	// matchTransfers did, and a redelivery must repeat this block, not a later one.
	s.transferAliases = routes

	s.logger.Debug("block processed",
		"block", event.BlockNumber,
		"chain", event.ChainID,
		"transfers", len(transfers),
		"snapshots", len(snapshots),
		"supplies", len(supplies),
		"duration", time.Since(start))
	return nil
}

// entryKeyFor is the ONE place a transfer becomes an entry key, so a route can
// never be applied by one caller and forgotten by the other.
func (s *Service) entryKeyFor(t *TransferEvent) EntryKey {
	route := transferRouteKey{Emitter: t.TokenAddress, Wallet: t.ProxyAddress}
	if contract, ok := s.transferAliases[route]; ok {
		return EntryKey{ContractAddress: contract, WalletAddress: t.ProxyAddress}
	}
	return EntryKey{ContractAddress: t.TokenAddress, WalletAddress: t.ProxyAddress}
}

// namedEmitter pairs an entry with the token that emits its Transfer logs.
type namedEmitter struct {
	entry   *TokenEntry
	emitter common.Address
}

// resolveMissingTransferAliases names the emitting token of every entry that has
// no route yet, so its transfers stop going unmatched.
func (s *Service) resolveMissingTransferAliases(ctx context.Context, blockHash common.Hash, blockNumber int64) error {
	pending := s.entriesAwaitingTransferAlias()
	if len(pending) == 0 {
		return nil
	}
	grouped, err := s.registry.shareResolvers(pending)
	if err != nil {
		return err
	}

	var named []namedEmitter
	for source, sourceEntries := range grouped {
		shares, err := source.shareTokens(ctx, sourceEntries, blockHash)
		if err != nil {
			return fmt.Errorf("%s naming the share tokens of %d entries: %w", source.Name(), len(sourceEntries), err)
		}
		batch, err := namedEmittersFromShares(sourceEntries, shares)
		if err != nil {
			return err
		}
		named = append(named, batch...)
	}

	next, err := s.nextTransferRoutes(named, blockNumber)
	if err != nil {
		return err
	}
	s.transferAliases = next
	return nil
}

// entriesAwaitingTransferAlias reverse-scans transferAliases: an entry is named
// once some emitter routes to its own (contract, wallet), identity included.
func (s *Service) entriesAwaitingTransferAlias() []*TokenEntry {
	named := make(map[EntryKey]struct{}, len(s.transferAliases))
	for route, contract := range s.transferAliases {
		named[EntryKey{ContractAddress: contract, WalletAddress: route.Wallet}] = struct{}{}
	}

	var pending []*TokenEntry
	for _, entry := range s.entries {
		if entry.TokenType != TokenTypeCentrifuge {
			continue
		}
		if _, ok := named[entry.Key()]; ok {
			continue
		}
		pending = append(pending, entry)
	}
	return pending
}

// namedEmittersFromShares reads a resolver's answer. An entry it left out is a
// hard error: unnamed, it would be resolved again on every block and its
// transfers would never match.
func namedEmittersFromShares(entries []*TokenEntry, shares map[common.Address]common.Address) ([]namedEmitter, error) {
	named := make([]namedEmitter, 0, len(entries))
	for _, entry := range entries {
		share, ok := shares[entry.ContractAddress]
		if !ok {
			return nil, fmt.Errorf("no share token named for entry %s/%s",
				entry.ContractAddress.Hex(), entry.WalletAddress.Hex())
		}
		named = append(named, namedEmitter{entry: entry, emitter: share})
	}
	return named, nil
}

// namedEmittersFromBalances reads the shares a fetch already resolved, so a
// re-pointed share is current at no extra RPC cost.
func namedEmittersFromBalances(entries []*TokenEntry, balances map[EntryKey]*PositionBalance) ([]namedEmitter, error) {
	var named []namedEmitter
	for _, entry := range entries {
		bal, ok := balances[entry.Key()]
		if !ok || bal == nil || bal.ShareToken == nil {
			// Structural for every other type — they hold the token they are keyed
			// on — but a centrifuge entry with no share leaves its routes frozen at
			// whatever the last good fetch said, which is how a stale route persists.
			if entry.TokenType == TokenTypeCentrifuge {
				return nil, fmt.Errorf("centrifuge entry %s/%s came back with no share token",
					entry.ContractAddress.Hex(), entry.WalletAddress.Hex())
			}
			continue
		}
		named = append(named, namedEmitter{entry: entry, emitter: *bal.ShareToken})
	}
	return named, nil
}

// nextTransferRoutes returns the route set that replaces transferAliases once
// these entries have named their emitter. Built fresh rather than mutated so a
// reader mid-block keeps one consistent view (see processTransfers), and so two
// entries swapping shares in one batch release before either claims — mutating
// in place makes that swap fail or succeed on entry order.
func (s *Service) nextTransferRoutes(named []namedEmitter, blockNumber int64) (map[transferRouteKey]common.Address, error) {
	for _, n := range named {
		if err := s.checkShareRatchet(n, blockNumber); err != nil {
			return nil, err
		}
	}

	next := maps.Clone(s.transferAliases)
	if next == nil {
		next = make(map[transferRouteKey]common.Address, len(named))
	}
	for _, n := range named {
		maps.DeleteFunc(next, func(route transferRouteKey, contract common.Address) bool {
			if route.Wallet != n.entry.WalletAddress || contract != n.entry.ContractAddress || route.Emitter == n.emitter {
				return false
			}
			s.logger.Warn("share token re-pointed; event rows for this position stop until the next sweep",
				"entry", n.entry.ContractAddress.Hex(),
				"wallet", n.entry.WalletAddress.Hex(),
				"previousEmitter", route.Emitter.Hex(),
				"emitter", n.emitter.Hex(),
				"block", blockNumber)
			return true
		})
	}

	for _, n := range named {
		route := transferRouteKey{Emitter: n.emitter, Wallet: n.entry.WalletAddress}
		if prev, ok := next[route]; ok && prev != n.entry.ContractAddress {
			return nil, fmt.Errorf("%s and %s both claim transfers of %s into %s; tracking both would double count",
				prev.Hex(), n.entry.ContractAddress.Hex(), n.emitter.Hex(), n.entry.WalletAddress.Hex())
		}
		next[route] = n.entry.ContractAddress
	}
	return next, nil
}

// checkShareRatchet refuses to downgrade an entry that already named a share to
// holding itself: share() reverting is how a direct share is detected, so a
// transient failure would otherwise re-key the position onto the retired vault,
// where the cache trigger drops it and every health signal stays green.
func (s *Service) checkShareRatchet(n namedEmitter, blockNumber int64) error {
	if n.emitter != n.entry.ContractAddress {
		return nil
	}
	for route, contract := range s.transferAliases {
		if route.Wallet == n.entry.WalletAddress && contract == n.entry.ContractAddress && route.Emitter != n.emitter {
			return fmt.Errorf(
				"entry %s/%s named share %s before and now reports itself at block %d; a vault cannot become its own share",
				n.entry.ContractAddress.Hex(), n.entry.WalletAddress.Hex(), route.Emitter.Hex(), blockNumber)
		}
	}
	return nil
}

func (s *Service) matchTransfers(
	transfers []*TransferEvent,
) []*TokenEntry {
	seen := make(map[EntryKey]bool)
	var matched []*TokenEntry
	for _, t := range transfers {
		key := s.entryKeyFor(t)
		if seen[key] {
			continue
		}
		if entry, ok := s.entryLookup[key]; ok {
			matched = append(matched, entry)
			seen[key] = true
		}
	}
	return matched
}

func (s *Service) buildSnapshots(
	entries []*TokenEntry,
	balances map[EntryKey]*PositionBalance,
	transfers []*TransferEvent,
	event outbound.BlockEvent,
	blockTimestamp time.Time,
) []*PositionSnapshot {
	tLookup := make(map[EntryKey]*TransferEvent)
	for _, t := range transfers {
		key := s.entryKeyFor(t)
		if _, exists := tLookup[key]; !exists {
			tLookup[key] = t
		}
	}

	var snapshots []*PositionSnapshot
	for _, entry := range entries {
		bal, ok := balances[entry.Key()]
		if !ok {
			continue
		}

		snap := &PositionSnapshot{
			Entry:           entry,
			Balance:         bal.Balance,
			ScaledBalance:   bal.ScaledBalance,
			UnderlyingValue: bal.UnderlyingValue,
			PoolToken0:      bal.PoolToken0,
			PoolToken1:      bal.PoolToken1,
			ShareToken:      bal.ShareToken,
			ChainID:         event.ChainID,
			BlockNumber:     event.BlockNumber,
			BlockVersion:    event.Version,
			BlockTimestamp:  blockTimestamp,
		}
		if t, ok := tLookup[entry.Key()]; ok {
			snap.TxHash = t.TxHash
			snap.LogIndex = t.LogIndex
			snap.TxAmount = t.Amount
			snap.Direction = t.Direction
			snap.From = &t.From
			snap.To = &t.To
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots
}

// buildSupplySnapshots converts per-contract pool supplies read in one multicall
// into persistable snapshot records. Deduplicated by the map iteration itself
// (one entry per contract address).
func buildSupplySnapshots(
	supplies map[common.Address]*PoolSupply,
	chainID, blockNumber int64,
	blockVersion int,
	blockTimestamp time.Time,
	source string,
) []*TokenTotalSupplySnapshot {
	if len(supplies) == 0 {
		return nil
	}
	out := make([]*TokenTotalSupplySnapshot, 0, len(supplies))
	for addr, sup := range supplies {
		if sup == nil || sup.TotalSupply == nil {
			continue
		}
		out = append(out, &TokenTotalSupplySnapshot{
			ChainID:           chainID,
			TokenAddress:      addr,
			TotalSupply:       sup.TotalSupply,
			ScaledTotalSupply: sup.ScaledTotalSupply,
			BlockNumber:       blockNumber,
			BlockVersion:      blockVersion,
			BlockTimestamp:    blockTimestamp,
			Source:            source,
		})
	}
	return out
}

// sweep runs periodic reconciliation to capture balance changes that don't
// emit Transfer events — e.g. aToken interest accrual, ERC4626 yield compounding,
// and BUIDL rebases. Without this, positions would drift between transfer-triggered
// snapshots.
func (s *Service) sweep(ctx context.Context, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	start := time.Now()

	fetch, err := s.registry.FetchAll(ctx, s.entries, blockHash)
	if err != nil {
		return fmt.Errorf("fetch sweep observations for block %d: %w", blockNumber, err)
	}
	named, err := namedEmittersFromBalances(s.entries, fetch.Balances)
	if err != nil {
		return fmt.Errorf("read the sweep share tokens of block %d: %w", blockNumber, err)
	}
	routes, err := s.nextTransferRoutes(named, blockNumber)
	if err != nil {
		return fmt.Errorf("route transfers for sweep block %d: %w", blockNumber, err)
	}

	var snapshots []*PositionSnapshot
	for _, entry := range s.entries {
		bal, ok := fetch.Balances[entry.Key()]
		if !ok {
			continue
		}
		snapshots = append(snapshots, &PositionSnapshot{
			Entry:           entry,
			Balance:         bal.Balance,
			ScaledBalance:   bal.ScaledBalance,
			UnderlyingValue: bal.UnderlyingValue,
			PoolToken0:      bal.PoolToken0,
			PoolToken1:      bal.PoolToken1,
			ShareToken:      bal.ShareToken,
			ChainID:         s.config.ChainID,
			BlockNumber:     blockNumber,
			BlockVersion:    blockVersion,
			TxAmount:        big.NewInt(0),
			Direction:       DirectionSweep,
			BlockTimestamp:  blockTimestamp,
		})
	}

	supplies := buildSupplySnapshots(fetch.Supplies, s.config.ChainID, blockNumber, blockVersion, blockTimestamp, "sweep")

	if len(snapshots) == 0 && len(supplies) == 0 {
		s.transferAliases = routes
		return nil
	}

	if err := s.handler.HandleBatch(ctx, &SnapshotBatch{Snapshots: snapshots, Supplies: supplies}); err != nil {
		return fmt.Errorf("sweep handler: %w", err)
	}
	s.transferAliases = routes

	s.logger.Info("sweep complete",
		"block", blockNumber,
		"snapshots", len(snapshots),
		"supplies", len(supplies),
		"duration", time.Since(start))
	return nil
}

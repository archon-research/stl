// Package fluid_vault_indexer indexes Fluid (Instadapp) lending vaults whose
// debt is the targeted USDS-family token (sUSDS by default). It consumes
// BlockEvents from SQS, discovers vaults (startup reconcile via the resolver's
// getAllVaultsAddresses + per-block VaultFactory deploy events), and on any
// position-changing log from a known vault reads that vault's end-of-block
// aggregate state from Fluid's VaultResolver and appends one fluid_vault_state
// snapshot per touched vault.
//
// Events only trigger the read; the on-chain resolver read is the source of
// truth. State writes for a block are batched into a single transaction
// (ADR-0002), sorted by natural key by the repository.
package fluid_vault_indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// FluidVaultFactoryAddress is the deployed mainnet Fluid VaultFactory; it emits
// VaultDeployed when a new vault is created.
var FluidVaultFactoryAddress = common.HexToAddress("0x324c5Dc1fC42c7a4D43d92df1eBA58a54d13Bf2d")

// SUSDSAddress is the default targeted debt token (Savings USDS, the ERC-4626
// USDS wrapper). Fluid has no plain-USDS debt vaults; sUSDS is the USDS-family
// debt token in use. Overridable via Config.TargetDebtToken.
var SUSDSAddress = common.HexToAddress("0xa3931d71877C0E7a3148CB7Eb4463524FEc27fbD")

const (
	fluidProtocolName    = "fluid"
	fluidProtocolType    = "lending"
	fluidLiquidityNumber = 19239106 // Fluid Liquidity deploy block (matches the B1 protocol seed)
)

// FluidLiquidityAddress is the Fluid Liquidity contract recorded as the protocol
// address (matches the B1 migration seed row).
var FluidLiquidityAddress = common.HexToAddress("0x52Aa899454998Be5b000Ad077a46Bbe360F4e497")

// Config holds service configuration.
type Config struct {
	shared.SQSConsumerConfig

	// TargetDebtToken is the debt token a vault must use to be in scope. Zero
	// value defaults to sUSDS.
	TargetDebtToken common.Address

	// Metrics records one blocks_processed_total sample per consumed block so
	// liveness is observable even through periods where no in-scope vault is
	// touched (the multicall metric stays flat then). Optional; nil disables it.
	Metrics outbound.BackupMetricsRecorder
}

// Service is the Fluid vault indexer SQS consumer.
type Service struct {
	config       Config
	consumer     outbound.SQSConsumer
	cache        outbound.BlockCacheReader
	blockQuerier entity.BlockQuerier
	txManager    outbound.TxManager
	vaultRepo    outbound.FluidVaultRepository
	tokenRepo    outbound.TokenRepository
	protocolRepo outbound.ProtocolRepository

	metrics outbound.BackupMetricsRecorder

	blockchain    *blockchainService
	registry      *VaultRegistry
	deployedTopic common.Hash
	triggerTopics map[common.Hash]struct{} // LogOperate / LogLiquidate

	logger *slog.Logger
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewService constructs the Fluid vault indexer.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	blockQuerier entity.BlockQuerier,
	reader outbound.StateReader,
	txManager outbound.TxManager,
	vaultRepo outbound.FluidVaultRepository,
	tokenRepo outbound.TokenRepository,
	protocolRepo outbound.ProtocolRepository,
) (*Service, error) {
	if err := validateDependencies(consumer, cache, blockQuerier, reader, txManager, vaultRepo, tokenRepo, protocolRepo); err != nil {
		return nil, err
	}

	config.SQSConsumerConfig.ApplyDefaults()
	if err := config.SQSConsumerConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	if config.TargetDebtToken == (common.Address{}) {
		config.TargetDebtToken = SUSDSAddress
	}

	logger := config.Logger.With("component", "fluid-vault-indexer")
	blockchain, err := newBlockchainService(reader, logger)
	if err != nil {
		return nil, fmt.Errorf("creating blockchain service: %w", err)
	}

	eventsABI, err := abis.GetFluidVaultEventsABI()
	if err != nil {
		return nil, fmt.Errorf("loading Fluid vault events ABI: %w", err)
	}
	deployed, ok := eventsABI.Events["VaultDeployed"]
	if !ok {
		return nil, fmt.Errorf("VaultDeployed event missing from ABI")
	}
	triggerTopics := make(map[common.Hash]struct{}, 2)
	for _, name := range []string{"LogOperate", "LogLiquidate"} {
		ev, ok := eventsABI.Events[name]
		if !ok {
			return nil, fmt.Errorf("%s event missing from ABI", name)
		}
		triggerTopics[ev.ID] = struct{}{}
	}

	return &Service{
		config:        config,
		consumer:      consumer,
		cache:         cache,
		blockQuerier:  blockQuerier,
		txManager:     txManager,
		vaultRepo:     vaultRepo,
		tokenRepo:     tokenRepo,
		protocolRepo:  protocolRepo,
		metrics:       config.Metrics,
		blockchain:    blockchain,
		registry:      NewVaultRegistry(logger),
		deployedTopic: deployed.ID,
		triggerTopics: triggerTopics,
		logger:        logger,
	}, nil
}

func validateDependencies(
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	blockQuerier entity.BlockQuerier,
	reader outbound.StateReader,
	txManager outbound.TxManager,
	vaultRepo outbound.FluidVaultRepository,
	tokenRepo outbound.TokenRepository,
	protocolRepo outbound.ProtocolRepository,
) error {
	switch {
	case consumer == nil:
		return fmt.Errorf("sqs consumer is required")
	case cache == nil:
		return fmt.Errorf("block cache is required")
	case blockQuerier == nil:
		return fmt.Errorf("block querier is required")
	case reader == nil:
		return fmt.Errorf("state reader is required")
	case txManager == nil:
		return fmt.Errorf("tx manager is required")
	case vaultRepo == nil:
		return fmt.Errorf("fluid vault repository is required")
	case tokenRepo == nil:
		return fmt.Errorf("token repository is required")
	case protocolRepo == nil:
		return fmt.Errorf("protocol repository is required")
	}
	return nil
}

// Start loads the registry, reconciles all existing vaults against the resolver
// at the latest block (so vaults that predate the indexer are picked up — not
// just ones whose VaultDeployed event arrives later), then runs the SQS
// processing loop until ctx is cancelled.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.registry.LoadFromDB(ctx, s.vaultRepo, s.config.ChainID); err != nil {
		return fmt.Errorf("loading vault registry: %w", err)
	}

	head, err := s.blockQuerier.BlockNumber(ctx)
	if err != nil {
		return fmt.Errorf("getting latest block for reconcile: %w", err)
	}
	if err := s.ReconcileVaults(ctx, int64(head)); err != nil {
		return fmt.Errorf("startup vault reconcile: %w", err)
	}

	s.wg.Go(func() {
		sqsutil.RunLoop(s.ctx, sqsutil.Config{
			Consumer:     s.consumer,
			MaxMessages:  s.config.MaxMessages,
			PollInterval: s.config.PollInterval,
			Logger:       s.logger,
			ChainID:      s.config.ChainID,
		}, s.processBlockEvent)
	})

	s.logger.Info("fluid vault indexer started",
		"chainID", s.config.ChainID,
		"targetDebtToken", s.config.TargetDebtToken.Hex(),
		"knownVaults", s.registry.Count())
	return nil
}

// Stop cancels the SQS loop and waits for the goroutine to drain.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
	s.logger.Info("fluid vault indexer stopped")
	return nil
}

// ReconcileVaults enumerates all vaults via the resolver at blockNumber and
// registers any in-scope (targeted-debt, plain single) vault not already known.
// It must not run concurrently with the SQS consumer loop: discovery is
// check-then-act across registry reads and DB writes, so a concurrent
// discoverDeployedVault for the same address would double-process it. Start runs
// this synchronously before launching the consumer goroutine.
//
// Reconcile is registry-only: it does not write a fluid_vault_state row. The
// first snapshot for a reconciled vault is written on its next
// LogOperate/LogLiquidate (see snapshotVaults), which is also when a real block
// timestamp is available — BlockQuerier only exposes the head number, not its
// timestamp, so there is no correct ts to stamp a reconcile-time snapshot with.
// Backfilling historical states for pre-existing vaults is a separate concern.
func (s *Service) ReconcileVaults(ctx context.Context, blockNumber int64) error {
	pin := outbound.PinForStaticRead(blockNumber)
	addrs, err := s.blockchain.GetAllVaultAddresses(ctx, pin)
	if err != nil {
		return fmt.Errorf("enumerating vaults: %w", err)
	}

	var unknown []common.Address
	for _, a := range addrs {
		if s.registry.IsKnownVault(a) || s.registry.IsKnownNotVault(a) {
			continue
		}
		unknown = append(unknown, a)
	}
	if len(unknown) == 0 {
		return nil
	}

	data, err := s.blockchain.GetVaultsEntireDataBestEffort(ctx, unknown, pin)
	if err != nil {
		return fmt.Errorf("reading vault data for reconcile: %w", err)
	}
	for _, ved := range data {
		// nil entries are vaults the resolver could not serve (e.g. a vault type
		// deployed ahead of a resolver upgrade). Leave them unknown so a later
		// reconcile retries; do not MarkNotVault (the gap may be transient).
		if ved == nil {
			continue
		}
		if err := s.classifyAndRegister(ctx, ved, pin); err != nil {
			return fmt.Errorf("registering vault %s: %w", ved.Vault.Hex(), err)
		}
	}
	s.logger.Info("vault reconcile complete", "scanned", len(unknown), "known", s.registry.Count())
	return nil
}

// processBlockEvent handles one consumed block. It records a blocks_processed_total
// sample for every block — including blocks that touch no in-scope vault — so the
// counter is the honest per-block liveness signal (the multicall metric only moves
// when a tracked vault is touched, which can be quiet for days).
func (s *Service) processBlockEvent(ctx context.Context, event outbound.BlockEvent) (retErr error) {
	defer func() { s.recordBlockProcessed(ctx, retErr) }()

	pin, err := outbound.PinForEvent(event)
	if err != nil {
		return fmt.Errorf("pinning reads for block %d: %w", event.BlockNumber, err)
	}

	receipts, err := s.fetchReceipts(ctx, event)
	if err != nil {
		return err
	}

	touched, err := s.scanLogs(ctx, receipts, pin)
	if err != nil {
		return err
	}
	if len(touched) == 0 {
		return nil
	}

	return s.snapshotVaults(ctx, touched, event, pin)
}

func (s *Service) recordBlockProcessed(ctx context.Context, err error) {
	if s.metrics == nil {
		return
	}
	status := "success"
	if err != nil {
		status = "error"
	}
	s.metrics.RecordBlockProcessed(ctx, status)
}

func (s *Service) fetchReceipts(ctx context.Context, event outbound.BlockEvent) ([]shared.TransactionReceipt, error) {
	receiptsJSON, err := s.cache.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return nil, fmt.Errorf("fetching receipts from cache: %w", err)
	}
	if receiptsJSON == nil {
		return nil, fmt.Errorf("receipts not found in cache for block %d (chain=%d, version=%d)", event.BlockNumber, event.ChainID, event.Version)
	}
	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(receiptsJSON, &receipts); err != nil {
		return nil, fmt.Errorf("unmarshalling receipts: %w", err)
	}
	return receipts, nil
}

// scanLogs walks every log once. A VaultDeployed log from the factory discovers
// a new vault; any log from a known vault marks it for an end-of-block read.
// Returns the deduplicated set of known-vault addresses touched this block.
func (s *Service) scanLogs(ctx context.Context, receipts []shared.TransactionReceipt, pin outbound.BlockPin) ([]common.Address, error) {
	touchedSet := make(map[common.Address]struct{})
	for _, r := range receipts {
		for _, log := range r.Logs {
			if len(log.Topics) == 0 {
				continue
			}
			addr := common.HexToAddress(log.Address)
			topic0 := common.HexToHash(log.Topics[0])

			if addr == FluidVaultFactoryAddress && topic0 == s.deployedTopic {
				if err := s.discoverDeployedVault(ctx, log, pin); err != nil {
					return nil, err
				}
				continue
			}
			// A known vault's position-changing events (LogOperate / LogLiquidate)
			// trigger an end-of-block read; other logs it may emit (e.g. fToken
			// ERC-20 Transfer) are not position changes and are ignored.
			if _, isTrigger := s.triggerTopics[topic0]; isTrigger && s.registry.IsKnownVault(addr) {
				touchedSet[addr] = struct{}{}
			}
		}
	}

	touched := make([]common.Address, 0, len(touchedSet))
	for a := range touchedSet {
		touched = append(touched, a)
	}
	sort.Slice(touched, func(i, j int) bool { return touched[i].Cmp(touched[j]) < 0 })
	return touched, nil
}

// discoverDeployedVault reads the newly-deployed vault's data and registers it
// if in scope. The deployed vault address is the first indexed topic.
func (s *Service) discoverDeployedVault(ctx context.Context, log shared.Log, pin outbound.BlockPin) error {
	if len(log.Topics) < 2 {
		return fmt.Errorf("VaultDeployed log missing vault topic: %v", log.Topics)
	}
	vault := common.HexToAddress(log.Topics[1])
	if s.registry.IsKnownVault(vault) || s.registry.IsKnownNotVault(vault) {
		return nil
	}
	// Classification probes are static-intent (identity data, not versioned
	// per-block state) but issued mid-block, so they derive from the event pin
	// to preserve the block's version rather than resetting to 0.
	staticPin := pin.Static()
	data, err := s.blockchain.GetVaultsEntireDataBestEffort(ctx, []common.Address{vault}, staticPin)
	if err != nil {
		return fmt.Errorf("reading deployed vault %s: %w", vault.Hex(), err)
	}
	// A nil entry means the resolver could not serve this vault yet; leave it
	// unknown so it is retried at the next startup reconcile rather than caching
	// it as not-a-vault (reconcile only runs at startup, so this defers discovery
	// until the process next restarts).
	if data[0] == nil {
		return nil
	}
	return s.classifyAndRegister(ctx, data[0], staticPin)
}

// classifyAndRegister decides whether a vault is in scope (plain single-debt
// vault whose debt is the targeted token). Out-of-scope vaults are cached as
// not-vault so they are not re-read. In-scope vaults resolve their token ids
// and are persisted + registered.
func (s *Service) classifyAndRegister(ctx context.Context, ved *VaultEntireData, pin outbound.BlockPin) error {
	if !ved.IsPlainSingle() {
		s.logger.Info("skipping smart/DEX Fluid vault (out of scope)",
			"vault", ved.Vault.Hex(), "vaultType", ved.VaultType, "isSmartCol", ved.IsSmartCol, "isSmartDebt", ved.IsSmartDebt)
		s.registry.MarkNotVault(ved.Vault)
		return nil
	}
	if ved.DebtToken != s.config.TargetDebtToken {
		s.logger.Info("skipping Fluid vault with non-target debt token (out of scope)",
			"vault", ved.Vault.Hex(), "debtToken", ved.DebtToken.Hex(), "targetDebtToken", s.config.TargetDebtToken.Hex())
		s.registry.MarkNotVault(ved.Vault)
		return nil
	}

	collMD, err := s.blockchain.GetTokenMetadata(ctx, ved.CollateralToken, pin)
	if err != nil {
		return fmt.Errorf("collateral token metadata: %w", err)
	}
	debtMD, err := s.blockchain.GetTokenMetadata(ctx, ved.DebtToken, pin)
	if err != nil {
		return fmt.Errorf("debt token metadata: %w", err)
	}

	// The vault's created-at block and the token first-seen block are the pin's
	// block number regardless of pin mode.
	blockNumber := pin.Number()
	var registered *entity.FluidVault
	if err := s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, s.config.ChainID, FluidLiquidityAddress, fluidProtocolName, fluidProtocolType, fluidLiquidityNumber)
		if err != nil {
			return fmt.Errorf("getting protocol: %w", err)
		}
		collTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, s.config.ChainID, ved.CollateralToken, collMD.Symbol, collMD.Decimals, &blockNumber)
		if err != nil {
			return fmt.Errorf("getting collateral token: %w", err)
		}
		debtTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, s.config.ChainID, ved.DebtToken, debtMD.Symbol, debtMD.Decimals, &blockNumber)
		if err != nil {
			return fmt.Errorf("getting debt token: %w", err)
		}

		// VaultType is the resolver's raw numeric vaultType code (e.g. "10000"
		// for a plain single vault, "40000" for a smart/DEX vault), stored
		// verbatim rather than mapped to a "T1"-style label — the numeric code is
		// authoritative and the label mapping is not published in a verifiable form.
		vault, err := entity.NewFluidVault(s.config.ChainID, protocolID, ved.Vault.Bytes(), ved.VaultType.String(), collTokenID, debtTokenID, blockNumber)
		if err != nil {
			return fmt.Errorf("creating vault entity: %w", err)
		}
		ids, err := s.vaultRepo.RecordVaults(ctx, tx, []*entity.FluidVault{vault})
		if err != nil {
			return fmt.Errorf("upserting vault: %w", err)
		}
		id, ok := ids[ved.Vault]
		if !ok {
			return fmt.Errorf("upsert returned no id for vault %s", ved.Vault.Hex())
		}
		vault.ID = id
		registered = vault
		return nil
	}); err != nil {
		return err
	}

	s.registry.RegisterVault(registered)
	return nil
}

// snapshotVaults reads each touched vault's end-of-block state from the resolver
// and appends one snapshot per vault in a single transaction.
func (s *Service) snapshotVaults(ctx context.Context, touched []common.Address, event outbound.BlockEvent, pin outbound.BlockPin) error {
	data, err := s.blockchain.GetVaultsEntireData(ctx, touched, pin)
	if err != nil {
		return fmt.Errorf("reading touched vault state at block %d: %w", event.BlockNumber, err)
	}

	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()
	states := make([]*entity.FluidVaultState, 0, len(data))
	for i, ved := range data {
		vault := s.registry.GetVault(touched[i])
		if vault == nil {
			// Touched set is built from known vaults; a miss means the registry
			// changed under us, which must not silently drop a snapshot.
			return fmt.Errorf("touched vault %s no longer in registry", touched[i].Hex())
		}
		state, err := s.buildVaultState(vault.ID, ved, event.BlockNumber, event.Version, blockTimestamp)
		if err != nil {
			return fmt.Errorf("building state for vault %s: %w", touched[i].Hex(), err)
		}
		states = append(states, state)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		return s.vaultRepo.SaveVaultStates(ctx, tx, states)
	})
}

// buildVaultState maps decoded resolver data to a FluidVaultState. Exchange
// prices are uint256 (always non-negative). Rates are int256 and can genuinely
// be negative, so they are passed through verbatim — a negative rate is real
// data, not a capture error, and the entity stores it signed.
func (s *Service) buildVaultState(vaultID int64, ved *VaultEntireData, blockNumber int64, blockVersion int, ts time.Time) (*entity.FluidVaultState, error) {
	return entity.NewFluidVaultState(entity.FluidVaultStateParams{
		FluidVaultID:        vaultID,
		BlockNumber:         blockNumber,
		BlockVersion:        blockVersion,
		Timestamp:           ts,
		TotalCollateral:     ved.TotalSupplyVault,
		TotalDebt:           ved.TotalBorrowVault,
		SupplyExchangePrice: ved.SupplyExchangePrice,
		BorrowExchangePrice: ved.BorrowExchangePrice,
		SupplyRate:          ved.SupplyRate,
		BorrowRate:          ved.BorrowRate,
	})
}

package balancer_dex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/common/sqsutil"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/dextelemetry"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// BalancerVaultAddress is the canonical Balancer V2 Vault on mainnet. All
// Vault events (Swap, PoolBalanceChanged, PoolBalanceManaged, PoolRegistered)
// emit from this contract and the worker uses it as the contract_address for
// Vault-sourced protocol_event rows AND as the GetOrCreateProtocol key for
// the BalancerV2 protocol row seeded in
// db/migrations/20260521_100000_create_dex_prereqs.sql.
var BalancerVaultAddress = common.HexToAddress("0xBA12222222228d8Ba445958a75a0704d566BF2C8")

// balancerProtocolDeployBlock is the block at which the Balancer V2 Vault
// landed on mainnet. Reused as created_at_block when GetOrCreateProtocol has
// to insert the protocol row (it should already exist via the prereq
// migration).
const balancerProtocolDeployBlock = int64(12272146)

// Config holds service configuration.
type Config struct {
	shared.SQSConsumerConfig

	// Telemetry, when non-nil, is invoked once per processBlockEvent with the
	// terminal duration and error. Wired by the worker main.go to emit
	// balancer_blocks_processed_total / balancer_errors_total. Nil disables.
	Telemetry *dextelemetry.Telemetry
}

// ConfigDefaults returns default configuration values.
func ConfigDefaults() Config {
	return Config{
		SQSConsumerConfig: shared.SQSConsumerConfigDefaults(),
	}
}

// Service is the Balancer V2 DEX SQS consumer.
type Service struct {
	config Config

	consumer     outbound.SQSConsumer
	cache        outbound.BlockCacheReader
	txManager    outbound.TxManager
	balancerRepo outbound.BalancerPoolRepository
	tokenRepo    outbound.TokenRepository
	protocolRepo outbound.ProtocolRepository
	eventRepo    outbound.EventRepository

	blockchain *blockchainService
	extractor  *eventExtractor
	telemetry  *dextelemetry.Telemetry

	registry *poolRegistry

	// balancerProtocolID is the cached protocol_event.protocol_id for the
	// Balancer V2 protocol row, populated via a check-lock-check on first
	// call. Reads on the hot path are unsynchronised; the mutex is only
	// taken to serialise the I/O of the initial GetOrCreateProtocol upsert,
	// avoiding a transient DB stall blocking every subsequent event.
	protocolIDMu       sync.Mutex
	balancerProtocolID atomic.Int64

	ctx    context.Context
	cancel context.CancelFunc
	logger *slog.Logger
}

// NewService constructs a Balancer V2 DEX service. Dependencies follow
// hexagonal ports — no postgres/sqs adapter packages are imported here.
func NewService(
	config Config,
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	multicaller outbound.Multicaller,
	txManager outbound.TxManager,
	balancerRepo outbound.BalancerPoolRepository,
	tokenRepo outbound.TokenRepository,
	protocolRepo outbound.ProtocolRepository,
	eventRepo outbound.EventRepository,
) (*Service, error) {
	if err := validateDependencies(consumer, cache, multicaller, txManager, balancerRepo, tokenRepo, protocolRepo, eventRepo); err != nil {
		return nil, fmt.Errorf("validating dependencies: %w", err)
	}
	config.SQSConsumerConfig.ApplyDefaults()
	if err := config.SQSConsumerConfig.Validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	// BalancerVaultAddress is a mainnet constant. Indexing Balancer V2 on a
	// non-mainnet chain would register the protocol row under the wrong
	// chain_id with the mainnet Vault address — reject loudly here so the
	// operator catches the misconfiguration at boot instead of post-hoc.
	if config.SQSConsumerConfig.ChainID != 1 {
		return nil, fmt.Errorf("BalancerV2 worker only supports chain_id=1 (Ethereum mainnet), got %d", config.SQSConsumerConfig.ChainID)
	}

	extractor, err := newEventExtractor()
	if err != nil {
		return nil, fmt.Errorf("creating event extractor: %w", err)
	}
	bc, err := newBlockchainService(multicaller)
	if err != nil {
		return nil, fmt.Errorf("creating blockchain service: %w", err)
	}

	return &Service{
		config:       config,
		consumer:     consumer,
		cache:        cache,
		txManager:    txManager,
		balancerRepo: balancerRepo,
		tokenRepo:    tokenRepo,
		protocolRepo: protocolRepo,
		eventRepo:    eventRepo,
		blockchain:   bc,
		extractor:    extractor,
		telemetry:    config.Telemetry,
		registry:     newPoolRegistry(),
		logger:       config.Logger.With("component", "balancer-dex-worker"),
	}, nil
}

// Start primes the in-process pool registry from the database and begins the
// SQS poll loop.
func (s *Service) Start(ctx context.Context) error {
	s.ctx, s.cancel = context.WithCancel(ctx)

	if err := s.loadRegistry(ctx); err != nil {
		return fmt.Errorf("loading balancer pool registry: %w", err)
	}

	go sqsutil.RunLoop(s.ctx, sqsutil.Config{
		Consumer:     s.consumer,
		MaxMessages:  s.config.MaxMessages,
		PollInterval: s.config.PollInterval,
		Logger:       s.logger,
		ChainID:      s.config.ChainID,
	}, s.processBlockEvent)

	s.logger.Info("balancer dex worker started",
		"chainID", s.config.ChainID,
		"pools", s.registry.poolCount())
	return nil
}

// Stop cancels the background loop.
func (s *Service) Stop() error {
	if s.cancel != nil {
		s.cancel()
	}
	s.logger.Info("balancer dex worker stopped")
	return nil
}

// loadRegistry materialises the pool list + per-pool token slots from the
// database. Pools whose join-table rows are not yet populated will get
// hydrated lazily on first event via populatePoolTokens.
func (s *Service) loadRegistry(ctx context.Context) error {
	pools, err := s.balancerRepo.ListEnabledBalancerPools(ctx, s.config.ChainID)
	if err != nil {
		return fmt.Errorf("listing balancer pools: %w", err)
	}
	for _, p := range pools {
		tokens, addrs, err := s.balancerRepo.ListBalancerPoolTokens(ctx, p.ID)
		if err != nil {
			return fmt.Errorf("listing pool tokens for %d: %w", p.ID, err)
		}
		// Pass addrs so the address→index map is hydrated from the DB. Passing
		// nil here (the prior bug) left addrToIx empty on restart, breaking
		// Swap tokenIn/tokenOut resolution until the next getPoolTokens.
		s.registry.addPool(p, tokens, addrs)
	}
	return nil
}

// processBlockEvent is the per-message handler. Each receipt's logs are
// filtered to the Balancer scope and dispatched per category.
func (s *Service) processBlockEvent(ctx context.Context, event outbound.BlockEvent) (retErr error) {
	start := time.Now()
	defer func() {
		s.telemetry.RecordBlockProcessed(ctx, time.Since(start), retErr)
		if retErr != nil {
			s.telemetry.RecordError(ctx, "processBlockEvent", retErr)
		}
	}()
	receiptsJSON, err := s.cache.GetReceipts(ctx, event.ChainID, event.BlockNumber, event.Version)
	if err != nil {
		return fmt.Errorf("fetching receipts from cache: %w", err)
	}
	if receiptsJSON == nil {
		return fmt.Errorf("receipts not found in cache for block %d (chain=%d, version=%d)", event.BlockNumber, event.ChainID, event.Version)
	}

	var receipts []shared.TransactionReceipt
	if err := json.Unmarshal(receiptsJSON, &receipts); err != nil {
		return fmt.Errorf("unmarshalling receipts: %w", err)
	}

	blockTimestamp := time.Unix(event.BlockTimestamp, 0).UTC()

	var errs []error
	for _, r := range receipts {
		if err := s.processReceipt(ctx, r, event.ChainID, event.BlockNumber, event.Version, blockTimestamp); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *Service) processReceipt(ctx context.Context, receipt shared.TransactionReceipt, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	// Per-receipt dedup of pool-state writes — multiple matched logs for the
	// same pool share one multicall snapshot.
	statesWritten := make(map[int64]bool)

	var errs []error
	for _, log := range receipt.Logs {
		addr := common.HexToAddress(log.Address)

		// Branch 1: log was emitted by the Vault. Decode topic[1] poolId and
		// keep going only if that poolId is in our registry.
		if addr == BalancerVaultAddress {
			if name, ok := s.extractor.vaultEvent(log); ok {
				if err := s.handleVaultLog(ctx, log, name, chainID, blockNumber, blockVersion, blockTimestamp, statesWritten); err != nil {
					errs = append(errs, err)
				}
				continue
			}
		}

		// Branch 2: log was emitted by a known pool contract (BPT Transfer or
		// pool parameter event).
		if pool := s.registry.poolByAddress(addr); pool != nil {
			if name, ok := s.extractor.poolEvent(log); ok {
				if err := s.handlePoolLog(ctx, log, pool, name, chainID, blockNumber, blockVersion, blockTimestamp, statesWritten); err != nil {
					errs = append(errs, err)
				}
				continue
			}
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

// handleVaultLog processes a Vault-emitted Swap / PoolBalanceChanged /
// PoolBalanceManaged. Filters by poolId — events for poolIds outside our
// registry are dropped silently after the topic match.
func (s *Service) handleVaultLog(ctx context.Context, log shared.Log, name BalancerEventName, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time, statesWritten map[int64]bool) error {
	decoded, err := s.extractor.extractVaultEvent(log)
	if err != nil {
		return fmt.Errorf("decoding %s: %w", name, err)
	}
	if decoded == nil {
		return nil
	}
	// Lookup by poolId. Events for poolIds we don't track are not errors;
	// the Vault is shared by every Balancer pool and our registry is sparse.
	pool := s.registry.poolByPoolID(decoded.PoolID)
	if pool == nil {
		return nil
	}

	// Ensure the join-table is populated. First Vault event for a pool with no
	// known token slots triggers a Vault.getPoolTokens read and persists the
	// rows (including identifying the phantom BPT slot).
	if pool.tokenCount() == 0 {
		if err := s.populatePoolTokens(ctx, chainID, pool, blockNumber); err != nil {
			return fmt.Errorf("populating pool tokens for %s: %w", pool.entity.Label, err)
		}
	}

	// PoolBalanceManaged has no typed projection — just write the audit row.
	// State snapshot is still useful for completeness; skip it to keep the
	// path narrow (asset-manager flows are out of scope per the plan).
	if name == EventPoolBalanceManaged {
		return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
			return s.saveProtocolEvent(ctx, tx, decoded, BalancerVaultAddress, chainID, blockNumber, blockVersion, blockTimestamp)
		})
	}

	// All other Vault events trigger a multicall snapshot.
	tokenAddresses := pool.tokenAddresses()
	state, err := s.blockchain.readPoolState(ctx, pool.entity, pool.tokens(), tokenAddresses, blockNumber)
	if err != nil {
		return fmt.Errorf("reading pool state for %s at block %d: %w", pool.entity.Label, blockNumber, err)
	}

	var tokenUpdate *entity.BalancerPoolToken
	if err := s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveProtocolEvent(ctx, tx, decoded, BalancerVaultAddress, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		tu, err := s.saveBalancerPoolEventProjection(ctx, tx, decoded, pool, blockNumber, blockVersion, blockTimestamp)
		if err != nil {
			return err
		}
		tokenUpdate = tu
		if !statesWritten[pool.entity.ID] {
			if err := s.savePoolState(ctx, tx, pool, state, blockNumber, blockVersion, blockTimestamp); err != nil {
				return err
			}
			statesWritten[pool.entity.ID] = true
		}
		return nil
	}); err != nil {
		return err
	}
	// Apply the registry mutation only after the tx commits (see
	// saveBalancerPoolEventProjection).
	if tokenUpdate != nil {
		pool.replaceToken(tokenUpdate)
	}
	return nil
}

// handlePoolLog processes a pool-contract emitted event: BPT Transfer or
// pool-parameter event (AmpUpdate*, TokenRate*, SwapFee*, Paused).
func (s *Service) handlePoolLog(ctx context.Context, log shared.Log, pool *registeredPool, name BalancerEventName, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time, statesWritten map[int64]bool) error {
	decoded, err := s.extractor.extractPoolEvent(log)
	if err != nil {
		return fmt.Errorf("decoding %s for %s: %w", name, pool.entity.Label, err)
	}
	if decoded == nil {
		return nil
	}

	// Ensure token slots are populated before we attempt to snapshot state.
	if pool.tokenCount() == 0 {
		if err := s.populatePoolTokens(ctx, chainID, pool, blockNumber); err != nil {
			return fmt.Errorf("populating pool tokens for %s: %w", pool.entity.Label, err)
		}
	}

	// BPT Transfer events do not require a state snapshot — supply changes
	// land alongside Vault PoolBalanceChanged events that emit in the same
	// tx. We still want the per-user position rows though.
	if name == EventTransfer {
		return s.handleBPTTransfer(ctx, decoded, pool, chainID, blockNumber, blockVersion, blockTimestamp)
	}

	// Parameter events: snapshot state + write typed projection.
	tokenAddresses := pool.tokenAddresses()
	state, err := s.blockchain.readPoolState(ctx, pool.entity, pool.tokens(), tokenAddresses, blockNumber)
	if err != nil {
		return fmt.Errorf("reading pool state for %s at block %d: %w", pool.entity.Label, blockNumber, err)
	}

	var tokenUpdate *entity.BalancerPoolToken
	if err := s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveProtocolEvent(ctx, tx, decoded, pool.entity.Address, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		tu, err := s.saveBalancerPoolEventProjection(ctx, tx, decoded, pool, blockNumber, blockVersion, blockTimestamp)
		if err != nil {
			return err
		}
		tokenUpdate = tu
		if !statesWritten[pool.entity.ID] {
			if err := s.savePoolState(ctx, tx, pool, state, blockNumber, blockVersion, blockTimestamp); err != nil {
				return err
			}
			statesWritten[pool.entity.ID] = true
		}
		return nil
	}); err != nil {
		return err
	}
	if tokenUpdate != nil {
		pool.replaceToken(tokenUpdate)
	}
	return nil
}

// handleBPTTransfer writes the protocol_event audit row plus one
// balancer_user_bpt_position row per non-zero side (mint / burn / regular).
// The post-event BPT balance for each side is read via a single balanceOf
// multicall pinned to the event block.
func (s *Service) handleBPTTransfer(ctx context.Context, decoded *decodedEvent, pool *registeredPool, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	t := decoded.Transfer
	if t == nil {
		return nil
	}

	zero := common.Address{}
	users := make([]common.Address, 0, 2)
	if t.From != zero {
		users = append(users, t.From)
	}
	if t.To != zero {
		users = append(users, t.To)
	}
	balances, err := s.blockchain.readBPTBalances(ctx, pool.entity.Address, users, blockNumber)
	if err != nil {
		return fmt.Errorf("reading BPT balances for %s: %w", pool.entity.Label, err)
	}
	balanceFor := func(addr common.Address) *big.Int {
		for i, u := range users {
			if u == addr {
				return balances[i]
			}
		}
		return nil
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveProtocolEvent(ctx, tx, decoded, pool.entity.Address, chainID, blockNumber, blockVersion, blockTimestamp); err != nil {
			return err
		}
		if t.From != zero {
			pos := &entity.BalancerUserBptPosition{
				BalancerPoolID: pool.entity.ID,
				UserAddress:    t.From,
				BlockNumber:    blockNumber,
				BlockVersion:   int32(blockVersion),
				Timestamp:      blockTimestamp,
				TxHash:         decoded.TxHash,
				LogIndex:       decoded.LogIdx,
				BptBalance:     balanceFor(t.From),
				Delta:          negate(t.Value),
			}
			if err := s.balancerRepo.SaveBalancerUserBptPosition(ctx, tx, pos); err != nil {
				return fmt.Errorf("saving bpt position (from) for %s: %w", pool.entity.Label, err)
			}
		}
		if t.To != zero {
			pos := &entity.BalancerUserBptPosition{
				BalancerPoolID: pool.entity.ID,
				UserAddress:    t.To,
				BlockNumber:    blockNumber,
				BlockVersion:   int32(blockVersion),
				Timestamp:      blockTimestamp,
				TxHash:         decoded.TxHash,
				LogIndex:       decoded.LogIdx,
				BptBalance:     balanceFor(t.To),
				Delta:          bigIntCopy(t.Value),
			}
			if err := s.balancerRepo.SaveBalancerUserBptPosition(ctx, tx, pos); err != nil {
				return fmt.Errorf("saving bpt position (to) for %s: %w", pool.entity.Label, err)
			}
		}
		return nil
	})
}

// populatePoolTokens reads Vault.getPoolTokens(poolId) for a pool whose
// balancer_pool_token rows are empty, registers each token in the global
// `token` table (decimals defaulted to 18 — a follow-up backfill can refresh
// from the ERC-20 metadata), identifies the phantom BPT slot if any, and
// persists one balancer_pool_token row per slot.
func (s *Service) populatePoolTokens(ctx context.Context, chainID int64, pool *registeredPool, blockNumber int64) error {
	vt, err := s.blockchain.readVaultTokens(ctx, pool.entity.VaultAddress, pool.entity.PoolID, blockNumber)
	if err != nil {
		return err
	}
	if len(vt.Tokens) == 0 {
		return fmt.Errorf("Vault.getPoolTokens returned no tokens for %s", pool.entity.PoolID.Hex())
	}

	// Read symbol+decimals for every slot OUTSIDE the tx so the multicalls
	// don't hold a DB connection, and in ONE batched multicall (2N sub-calls)
	// instead of N sequential round-trips. Done once per pool — this method
	// is only called when balancer_pool_token rows are missing.
	syms, decs, err := s.blockchain.readERC20MetadataBatch(ctx, vt.Tokens, blockNumber)
	if err != nil {
		return fmt.Errorf("reading ERC20 metadata batch: %w", err)
	}

	persistedTokens := make([]*entity.BalancerPoolToken, 0, len(vt.Tokens))
	persistedAddresses := make([]common.Address, 0, len(vt.Tokens))
	err = s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		for i, addr := range vt.Tokens {
			isPhantom := addr == pool.entity.Address
			tokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, addr, syms[i], int(decs[i]), blockNumber)
			if err != nil {
				return fmt.Errorf("registering token %s: %w", addr.Hex(), err)
			}
			row := &entity.BalancerPoolToken{
				BalancerPoolID: pool.entity.ID,
				TokenIndex:     int16(i),
				TokenID:        tokenID,
				IsPhantom:      isPhantom,
			}
			if err := s.balancerRepo.UpsertBalancerPoolToken(ctx, tx, row); err != nil {
				return fmt.Errorf("upserting pool token slot %d: %w", i, err)
			}
			persistedTokens = append(persistedTokens, row)
			persistedAddresses = append(persistedAddresses, addr)
		}
		return nil
	})
	if err != nil {
		return err
	}

	pool.setTokens(persistedTokens, persistedAddresses)
	return nil
}

// resolveProtocolID returns the cached BalancerV2 protocol_id, populating it
// via GetOrCreateProtocol on first call. Check-lock-check: a fast unlocked
// load short-circuits the steady-state hot path; the mutex is only taken on
// the first (or post-error) call to serialise the I/O. The atomic store
// publishes to subsequent unsynchronised reads.
func (s *Service) resolveProtocolID(ctx context.Context, tx pgx.Tx, chainID int64) (int64, error) {
	if id := s.balancerProtocolID.Load(); id != 0 {
		return id, nil
	}
	s.protocolIDMu.Lock()
	defer s.protocolIDMu.Unlock()
	if id := s.balancerProtocolID.Load(); id != 0 {
		return id, nil
	}
	id, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, BalancerVaultAddress, "BalancerV2", "dex", balancerProtocolDeployBlock)
	if err != nil {
		return 0, fmt.Errorf("getting BalancerV2 protocol_id: %w", err)
	}
	s.balancerProtocolID.Store(id)
	return id, nil
}

// saveProtocolEvent writes the raw audit row for any decoded event.
func (s *Service) saveProtocolEvent(ctx context.Context, tx pgx.Tx, decoded *decodedEvent, contractAddress common.Address, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	protocolID, err := s.resolveProtocolID(ctx, tx, chainID)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(decoded)
	if err != nil {
		return fmt.Errorf("marshalling decoded event: %w", err)
	}
	evt, err := entity.NewProtocolEvent(
		int(chainID),
		protocolID,
		blockNumber,
		blockVersion,
		decoded.TxHash.Bytes(),
		int(decoded.LogIdx),
		contractAddress.Bytes(),
		string(decoded.Name),
		payload,
		blockTimestamp,
	)
	if err != nil {
		return fmt.Errorf("building protocol_event: %w", err)
	}
	return s.eventRepo.SaveEvent(ctx, tx, evt)
}

func validateDependencies(
	consumer outbound.SQSConsumer,
	cache outbound.BlockCacheReader,
	mc outbound.Multicaller,
	txm outbound.TxManager,
	balancer outbound.BalancerPoolRepository,
	tok outbound.TokenRepository,
	proto outbound.ProtocolRepository,
	evt outbound.EventRepository,
) error {
	if consumer == nil {
		return fmt.Errorf("consumer is required")
	}
	if cache == nil {
		return fmt.Errorf("cache is required")
	}
	if mc == nil {
		return fmt.Errorf("multicaller is required")
	}
	if txm == nil {
		return fmt.Errorf("txManager is required")
	}
	if balancer == nil {
		return fmt.Errorf("balancerRepo is required")
	}
	if tok == nil {
		return fmt.Errorf("tokenRepo is required")
	}
	if proto == nil {
		return fmt.Errorf("protocolRepo is required")
	}
	if evt == nil {
		return fmt.Errorf("eventRepo is required")
	}
	return nil
}

// -----------------------------------------------------------------------------
// poolRegistry — in-memory snapshot of balancer_pool + per-pool token slots.
// -----------------------------------------------------------------------------

// registeredPool bundles a pool entity with the latest known token-slot
// metadata. The token addresses parallel `slots` so callers can map between
// the on-chain Vault.tokens ordering and the join-table index without
// touching the token table again.
type registeredPool struct {
	entity   *entity.BalancerPool
	mu       sync.RWMutex
	slots    []*entity.BalancerPoolToken
	addrs    []common.Address
	addrToIx map[common.Address]int16
}

func newRegisteredPool(p *entity.BalancerPool, slots []*entity.BalancerPoolToken, addrs []common.Address) *registeredPool {
	rp := &registeredPool{
		entity:   p,
		slots:    append([]*entity.BalancerPoolToken(nil), slots...),
		addrs:    append([]common.Address(nil), addrs...),
		addrToIx: make(map[common.Address]int16, len(slots)),
	}
	for i, a := range addrs {
		rp.addrToIx[a] = int16(i)
	}
	return rp
}

func (p *registeredPool) tokenCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.slots)
}

func (p *registeredPool) tokens() []*entity.BalancerPoolToken {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]*entity.BalancerPoolToken, len(p.slots))
	copy(out, p.slots)
	return out
}

func (p *registeredPool) tokenAddresses() []common.Address {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]common.Address, len(p.addrs))
	copy(out, p.addrs)
	return out
}

func (p *registeredPool) tokenAddressAt(index int) (common.Address, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if index < 0 || index >= len(p.addrs) {
		return common.Address{}, false
	}
	return p.addrs[index], true
}

func (p *registeredPool) tokenAt(index int) (*entity.BalancerPoolToken, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if index < 0 || index >= len(p.slots) {
		return nil, false
	}
	return p.slots[index], true
}

func (p *registeredPool) setTokens(slots []*entity.BalancerPoolToken, addrs []common.Address) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.slots = append([]*entity.BalancerPoolToken(nil), slots...)
	p.addrs = append([]common.Address(nil), addrs...)
	p.addrToIx = make(map[common.Address]int16, len(slots))
	for i, a := range addrs {
		p.addrToIx[a] = int16(i)
	}
}

func (p *registeredPool) replaceToken(t *entity.BalancerPoolToken) {
	p.mu.Lock()
	defer p.mu.Unlock()
	idx := int(t.TokenIndex)
	if idx >= 0 && idx < len(p.slots) {
		p.slots[idx] = t
	}
}

// tokenIndices resolves the (in, out) token addresses from a Swap to their
// balancer_pool_token.token_index. Returns an error if either address is not
// known to the pool — that shouldn't happen for a pool we registered with
// proper getPoolTokens-derived slots, so surfacing the mismatch is loud.
func (p *registeredPool) tokenIndices(in, out common.Address) (int16, int16, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	inIdx, ok := p.addrToIx[in]
	if !ok {
		return 0, 0, fmt.Errorf("tokenIn %s not in pool slots", in.Hex())
	}
	outIdx, ok := p.addrToIx[out]
	if !ok {
		return 0, 0, fmt.Errorf("tokenOut %s not in pool slots", out.Hex())
	}
	return inIdx, outIdx, nil
}

// projectByToken maps a Vault-tokens-ordered slice onto
// balancer_pool_token.token_index order, returning a fresh slice in the
// registry's slot order. Missing addresses get nil entries.
//
// Phantom BPT slots (slot.IsPhantom == true on ComposableStable pools) are
// always projected as nil. The Vault returns the pool's own BPT supply in
// the corresponding tokens-array position, which is internal accounting,
// NOT real liquidity; summing it into TVL or treating it as a swap-side
// reserve would double-count the pool's own supply against itself.
func (p *registeredPool) projectByToken(tokens []common.Address, values []*big.Int) []*big.Int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if len(p.addrs) == 0 {
		// No slot info — fall back to the input order so callers still see data.
		out := make([]*big.Int, len(values))
		for i, v := range values {
			if v != nil {
				out[i] = new(big.Int).Set(v)
			}
		}
		return out
	}
	out := make([]*big.Int, len(p.addrs))
	for i, addr := range tokens {
		if i >= len(values) {
			continue
		}
		slot, ok := p.addrToIx[addr]
		if !ok {
			continue
		}
		if int(slot) < len(p.slots) && p.slots[slot] != nil && p.slots[slot].IsPhantom {
			continue
		}
		if values[i] == nil {
			continue
		}
		out[slot] = new(big.Int).Set(values[i])
	}
	return out
}

// -----------------------------------------------------------------------------
// poolRegistry: thin index over registeredPool.
// -----------------------------------------------------------------------------

type poolRegistry struct {
	mu sync.RWMutex

	poolsByAddress map[common.Address]*registeredPool
	poolsByPoolID  map[common.Hash]*registeredPool
}

func newPoolRegistry() *poolRegistry {
	return &poolRegistry{
		poolsByAddress: make(map[common.Address]*registeredPool),
		poolsByPoolID:  make(map[common.Hash]*registeredPool),
	}
}

func (r *poolRegistry) addPool(p *entity.BalancerPool, slots []*entity.BalancerPoolToken, addrs []common.Address) {
	r.mu.Lock()
	defer r.mu.Unlock()
	rp := newRegisteredPool(p, slots, addrs)
	r.poolsByAddress[p.Address] = rp
	r.poolsByPoolID[p.PoolID] = rp
}

func (r *poolRegistry) poolByAddress(a common.Address) *registeredPool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.poolsByAddress[a]
}

func (r *poolRegistry) poolByPoolID(id common.Hash) *registeredPool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.poolsByPoolID[id]
}

func (r *poolRegistry) poolCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.poolsByAddress)
}

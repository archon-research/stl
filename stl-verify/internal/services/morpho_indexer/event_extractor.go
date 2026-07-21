package morpho_indexer

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// ---------------------------------------------------------------------------
// EventExtractor
// ---------------------------------------------------------------------------

// EventExtractor parses Morpho Blue and MetaMorpho events from transaction logs.
type EventExtractor struct {
	morphoBlueABI        *abi.ABI
	morphoBlueSignatures map[common.Hash]*abi.Event
	metaMorphoABI        *abi.ABI
	metaMorphoSignatures map[common.Hash]*abi.Event

	// vaultActivitySignatures is the discovery-trigger topic set: only the
	// Morpho VaultV2 4-field AccrueInterest topic. See IsVaultActivityEvent
	// for the rationale on narrowing — V1/V1.1 are discovered via Morpho
	// Blue, V2 always emits AccrueInterest first in any state-changing
	// transaction. Same set is consumed by the live indexer
	// (service.go's processReceipt default branch and hasRelevantEvents)
	// and by the morpho-vault-indexer backfiller, so changes apply
	// uniformly.
	vaultActivitySignatures map[common.Hash]*abi.Event
}

// NewEventExtractor creates a new EventExtractor with loaded ABIs.
func NewEventExtractor() (*EventExtractor, error) {
	e := &EventExtractor{
		morphoBlueSignatures:    make(map[common.Hash]*abi.Event),
		metaMorphoSignatures:    make(map[common.Hash]*abi.Event),
		vaultActivitySignatures: make(map[common.Hash]*abi.Event),
	}
	if err := e.loadABIs(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *EventExtractor) loadABIs() error {
	morphoABI, err := abis.GetMorphoBlueEventsABI()
	if err != nil {
		return fmt.Errorf("failed to parse Morpho Blue events ABI: %w", err)
	}
	e.morphoBlueABI = morphoABI

	morphoEventNames := []string{
		"CreateMarket", "Supply", "Withdraw", "Borrow", "Repay",
		"SupplyCollateral", "WithdrawCollateral", "Liquidate", "AccrueInterest", "SetFee",
	}
	for _, name := range morphoEventNames {
		event, ok := e.morphoBlueABI.Events[name]
		if !ok {
			return fmt.Errorf("morpho Blue %s event not found in ABI", name)
		}
		e.morphoBlueSignatures[event.ID] = &event
	}

	metaMorphoABI, err := abis.GetMetaMorphoV1EventsABI()
	if err != nil {
		return fmt.Errorf("failed to parse MetaMorpho events ABI: %w", err)
	}
	e.metaMorphoABI = metaMorphoABI

	metaMorphoEventNames := []string{"Deposit", "Withdraw", "Transfer", "AccrueInterest"}
	for _, name := range metaMorphoEventNames {
		event, ok := e.metaMorphoABI.Events[name]
		if !ok {
			return fmt.Errorf("metaMorpho %s event not found in ABI", name)
		}
		e.metaMorphoSignatures[event.ID] = &event
	}

	// Register VaultV2 AccrueInterest (different topic hash from the V1/V1.1
	// 2-field variant because of the 4-field signature). V1.1 shares its
	// 2-field AccrueInterest with V1 by topic and is already covered above.
	v2ABI, err := abis.GetMetaMorphoV2AccrueInterestABI()
	if err != nil {
		return fmt.Errorf("failed to parse VaultV2 AccrueInterest ABI: %w", err)
	}
	v2Event, ok := v2ABI.Events["AccrueInterest"]
	if !ok {
		return fmt.Errorf("VaultV2 AccrueInterest event not found in ABI")
	}
	e.metaMorphoSignatures[v2Event.ID] = &v2Event
	// Discovery-trigger set: V2 4-field AccrueInterest only. See
	// IsVaultActivityEvent for the why; deliberately not including V1/V1.1
	// Deposit/Withdraw/AccrueInterest here.
	e.vaultActivitySignatures[v2Event.ID] = &v2Event

	// Register the full VaultV2 governance / allocation / cap / fee / role /
	// timelock event surface. Every one is persisted as a protocol_event
	// audit-log row when emitted by a known vault; the adapter / allocation /
	// cap / fee subset additionally has typed extraction and structured
	// handlers, while the rest stay audit-log-only. Without this
	// registration the events would silently fall through IsMetaMorphoEvent's
	// filter.
	v2EventsABI, err := abis.GetVaultV2EventsABI()
	if err != nil {
		return fmt.Errorf("failed to parse VaultV2 events ABI: %w", err)
	}
	for _, event := range v2EventsABI.Events {
		// Skip event signatures (topic hashes) already registered above to
		// avoid clobbering the existing MetaMorpho V1 / V1.1 entries —
		// Deposit, Withdraw, Transfer, AccrueInterest are inherited
		// ERC20/ERC4626 surface and share their event.ID with V1/V1.1.
		if _, present := e.metaMorphoSignatures[event.ID]; present {
			continue
		}
		ev := event
		e.metaMorphoSignatures[ev.ID] = &ev
	}

	return nil
}

// IsMorphoBlueEvent returns true if the log matches a known Morpho Blue event.
func (e *EventExtractor) IsMorphoBlueEvent(log shared.Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	_, ok := e.morphoBlueSignatures[common.HexToHash(log.Topics[0])]
	return ok
}

// IsMetaMorphoEvent returns true if the log matches a known MetaMorpho event.
func (e *EventExtractor) IsMetaMorphoEvent(log shared.Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	_, ok := e.metaMorphoSignatures[common.HexToHash(log.Topics[0])]
	return ok
}

// IsVaultActivityEvent returns true if the log matches the discovery-trigger
// event — Morpho VaultV2's 4-field AccrueInterest. An unknown contract emitting
// this event becomes a candidate vault.
//
// Why narrow to V2-only:
//
//   - V1/V1.1 vaults are discovered via the Morpho Blue path: their address
//     appears as caller / onBehalf in Morpho Blue Supply / Withdraw / Borrow /
//     Repay events when they allocate, and the live indexer (service.go's
//     discoverV1V11VaultsInReceipt) plus the morpho-vault-indexer
//     backfiller (emitMorphoBlueCandidates) probe those addresses. The V1/V1.1
//     factories remain deployed and callable; the live indexer's coverage
//     does not assume otherwise.
//   - V2 contracts call _accrueInterest() unconditionally at the top of every
//     state-changing entry point (deposit, withdraw, redeem, mint, allocate,
//     deallocate, forceDeallocate). AccrueInterest fires before any other event
//     in the same transaction's log_index order, so discovery on this single
//     topic catches the vault before any subsequent log it might emit.
//   - The 4-field AccrueInterest topic is unique to V2 (V1/V1.1 use a 2-field
//     variant with a different keccak). The narrow gate keeps the live
//     indexer's probe well clear of unrelated ERC4626 noise and legacy
//     ERC20s whose fallback runs `INVALID` (0xfe) and trips Multicall3's
//     gas budget — see VEC-198 multicall-gas-cap fix.
//   - Same gate is used by both the live indexer (service.go's processReceipt
//     default branch and hasRelevantEvents) and the morpho-vault-indexer
//     backfiller's extractCandidatesFromReceipts. Single source of truth for
//     "what makes an unknown contract worth probing" — narrowing here narrows
//     both code paths uniformly.
//
// Trade-off: a V2 vault that's been deployed but has not yet taken its first
// state-changing call (so AccrueInterest never fired) won't be discovered until
// it does. Vaults without deposits have no positions to track, so this is
// acceptable for VEC-198's TVL-finding goal.
func (e *EventExtractor) IsVaultActivityEvent(log shared.Log) bool {
	if len(log.Topics) == 0 {
		return false
	}
	_, ok := e.vaultActivitySignatures[common.HexToHash(log.Topics[0])]
	return ok
}

// MetaMorphoEventName returns the registered event name for the log's topic[0].
// Returns "", false if the topic is not registered. Used by audit-log saving
// in service.go to label every MetaMorpho event row with its event_type
// without requiring per-event typed extraction.
func (e *EventExtractor) MetaMorphoEventName(log shared.Log) (string, bool) {
	if len(log.Topics) == 0 {
		return "", false
	}
	ev, ok := e.metaMorphoSignatures[common.HexToHash(log.Topics[0])]
	if !ok {
		return "", false
	}
	return ev.Name, true
}

// ExtractMorphoBlueEvent parses a Morpho Blue event from a log entry.
func (e *EventExtractor) ExtractMorphoBlueEvent(log shared.Log) (MorphoBlueEvent, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("no topics")
	}

	eventSig := common.HexToHash(log.Topics[0])
	event, ok := e.morphoBlueSignatures[eventSig]
	if !ok {
		return nil, fmt.Errorf("not a tracked Morpho Blue event")
	}

	eventData := make(map[string]any)

	if err := parseTopics(event, log.Topics, eventData); err != nil {
		return nil, fmt.Errorf("parsing indexed params for %s: %w", event.Name, err)
	}
	if err := parseData(event, log.Data, eventData); err != nil {
		return nil, fmt.Errorf("parsing non-indexed params for %s: %w", event.Name, err)
	}

	switch event.Name {
	case "CreateMarket":
		return extractCreateMarket(eventData, log.TransactionHash)
	case "Supply":
		return extractSupply(eventData, log.TransactionHash)
	case "Withdraw":
		return extractWithdrawBlue(eventData, log.TransactionHash)
	case "Borrow":
		return extractBorrow(eventData, log.TransactionHash)
	case "Repay":
		return extractRepay(eventData, log.TransactionHash)
	case "SupplyCollateral":
		return extractSupplyCollateral(eventData, log.TransactionHash)
	case "WithdrawCollateral":
		return extractWithdrawCollateral(eventData, log.TransactionHash)
	case "Liquidate":
		return extractLiquidate(eventData, log.TransactionHash)
	case "AccrueInterest":
		return extractAccrueInterest(eventData, log.TransactionHash)
	case "SetFee":
		return extractSetFee(eventData, log.TransactionHash)
	default:
		return nil, fmt.Errorf("unknown Morpho Blue event: %s", event.Name)
	}
}

// ExtractMetaMorphoEvent parses a MetaMorpho event from a log entry.
func (e *EventExtractor) ExtractMetaMorphoEvent(log shared.Log) (MetaMorphoEvent, error) {
	if len(log.Topics) == 0 {
		return nil, fmt.Errorf("no topics")
	}

	eventSig := common.HexToHash(log.Topics[0])
	event, ok := e.metaMorphoSignatures[eventSig]
	if !ok {
		return nil, fmt.Errorf("not a tracked MetaMorpho event")
	}

	// Only events with structured handlers are extracted into strongly-typed
	// structs: the ERC4626/ERC20 state events (Deposit / Withdraw / Transfer /
	// AccrueInterest) plus the VaultV2 adapter / allocation / cap / fee surface
	// tracked structurally. The remaining registered V2 events
	// (role / timelock / gate / metadata setters) are recognised so the indexer
	// audit-logs them, but have no typed handler: they return (nil, nil), which
	// signals "registered topic, no typed extraction" so the caller saves the
	// audit-log row without aborting the receipt.
	switch event.Name {
	case "Deposit", "Withdraw", "Transfer", "AccrueInterest",
		"AddAdapter", "RemoveAdapter", "Allocate", "Deallocate", "ForceDeallocate",
		"IncreaseAbsoluteCap", "DecreaseAbsoluteCap", "IncreaseRelativeCap", "DecreaseRelativeCap",
		"SetPerformanceFee", "SetManagementFee", "SetPerformanceFeeRecipient", "SetManagementFeeRecipient":
		// fall through to typed extraction below
	default:
		return nil, nil
	}

	eventData := make(map[string]any)

	if err := parseTopics(event, log.Topics, eventData); err != nil {
		return nil, fmt.Errorf("parsing indexed params for %s: %w", event.Name, err)
	}
	if err := parseData(event, log.Data, eventData); err != nil {
		return nil, fmt.Errorf("parsing non-indexed params for %s: %w", event.Name, err)
	}

	switch event.Name {
	case "Deposit":
		return extractVaultDeposit(eventData, log.TransactionHash)
	case "Withdraw":
		return extractVaultWithdraw(eventData, log.TransactionHash)
	case "Transfer":
		return extractVaultTransfer(eventData, log.TransactionHash)
	case "AccrueInterest":
		return extractVaultAccrueInterest(eventData, log.TransactionHash)
	case "AddAdapter":
		return extractAddAdapter(eventData, log.TransactionHash)
	case "RemoveAdapter":
		return extractRemoveAdapter(eventData, log.TransactionHash)
	case "Allocate":
		return extractAllocate(eventData, log.TransactionHash)
	case "Deallocate":
		return extractDeallocate(eventData, log.TransactionHash)
	case "ForceDeallocate":
		return extractForceDeallocate(eventData, log.TransactionHash)
	case "IncreaseAbsoluteCap":
		return extractIncreaseAbsoluteCap(eventData, log.TransactionHash)
	case "DecreaseAbsoluteCap":
		return extractDecreaseAbsoluteCap(eventData, log.TransactionHash)
	case "IncreaseRelativeCap":
		return extractIncreaseRelativeCap(eventData, log.TransactionHash)
	case "DecreaseRelativeCap":
		return extractDecreaseRelativeCap(eventData, log.TransactionHash)
	case "SetPerformanceFee":
		return extractSetPerformanceFee(eventData, log.TransactionHash)
	case "SetManagementFee":
		return extractSetManagementFee(eventData, log.TransactionHash)
	case "SetPerformanceFeeRecipient":
		return extractSetPerformanceFeeRecipient(eventData, log.TransactionHash)
	case "SetManagementFeeRecipient":
		return extractSetManagementFeeRecipient(eventData, log.TransactionHash)
	default:
		// Unreachable — gated by the switch above.
		return nil, nil
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// parseTopics extracts indexed parameters from log topics into eventData.
func parseTopics(event *abi.Event, topics []string, eventData map[string]any) error {
	var indexed abi.Arguments
	for _, arg := range event.Inputs {
		if arg.Indexed {
			indexed = append(indexed, arg)
		}
	}

	if len(indexed) == 0 {
		return nil
	}

	// abi.ParseTopicsIntoMap indexes hashes[i] for each indexed arg and panics
	// (index out of range) if the log carries fewer topics than the event
	// declares indexed params. A registered topic0 on a malformed/truncated log
	// is data drift we return as an error, not a crash.
	hashes := make([]common.Hash, 0, len(topics)-1)
	for i := 1; i < len(topics); i++ {
		hashes = append(hashes, common.HexToHash(topics[i]))
	}
	if len(hashes) != len(indexed) {
		return fmt.Errorf("event %s declares %d indexed params but log carries %d topic(s) after topic0", event.Name, len(indexed), len(hashes))
	}

	return abi.ParseTopicsIntoMap(eventData, indexed, hashes)
}

// parseData extracts non-indexed parameters from log data into eventData.
func parseData(event *abi.Event, data string, eventData map[string]any) error {
	var nonIndexed abi.Arguments
	for _, arg := range event.Inputs {
		if !arg.Indexed {
			nonIndexed = append(nonIndexed, arg)
		}
	}

	if len(nonIndexed) == 0 || len(data) <= 2 {
		return nil
	}

	raw := common.FromHex(data)
	return nonIndexed.UnpackIntoMap(eventData, raw)
}

func getBytes32(eventData map[string]any, key string) ([32]byte, error) {
	v, ok := eventData[key]
	if !ok {
		return [32]byte{}, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.([32]byte)
	if !ok {
		return [32]byte{}, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return b, nil
}

func getAddress(eventData map[string]any, key string) (common.Address, error) {
	v, ok := eventData[key]
	if !ok {
		return common.Address{}, fmt.Errorf("missing field: %s", key)
	}
	addr, ok := v.(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return addr, nil
}

func getBigInt(eventData map[string]any, key string) (*big.Int, error) {
	v, ok := eventData[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.(*big.Int)
	if !ok {
		return nil, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return b, nil
}

// getOptionalBigInt reads a *big.Int field that may legitimately be absent (a
// field present only on some ABI variants). A missing key yields (nil, nil); a
// present-but-wrong-type value is a real decode error and propagates rather than
// being silently dropped to NULL.
func getOptionalBigInt(eventData map[string]any, key string) (*big.Int, error) {
	if _, ok := eventData[key]; !ok {
		return nil, nil
	}
	return getBigInt(eventData, key)
}

func getBytes(eventData map[string]any, key string) ([]byte, error) {
	v, ok := eventData[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return b, nil
}

// getHashSlice reads a bytes32[] field, which the ABI decoder yields as
// [][32]byte, and returns it as []common.Hash for typed events.
func getHashSlice(eventData map[string]any, key string) ([]common.Hash, error) {
	v, ok := eventData[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	raw, ok := v.([][32]byte)
	if !ok {
		return nil, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	hashes := make([]common.Hash, len(raw))
	for i, b := range raw {
		hashes[i] = common.Hash(b)
	}
	return hashes, nil
}

// ---------------------------------------------------------------------------
// Morpho Blue event extractors
// ---------------------------------------------------------------------------

func extractCreateMarket(eventData map[string]any, txHash string) (*CreateMarketEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}

	// marketParams is a struct (tuple)
	mp, ok := eventData["marketParams"]
	if !ok {
		return nil, fmt.Errorf("missing field: marketParams")
	}

	// Use abi.ConvertType for safe conversion from the ABI-generated anonymous struct.
	converted := abi.ConvertType(mp, abiMarketParams{})
	mps, ok := converted.(abiMarketParams)
	if !ok {
		return nil, fmt.Errorf("failed to convert marketParams: %T", mp)
	}

	return &CreateMarketEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Params: &MarketParams{
			LoanToken:       mps.LoanToken,
			CollateralToken: mps.CollateralToken,
			Oracle:          mps.Oracle,
			Irm:             mps.Irm,
			LLTV:            mps.Lltv,
		},
	}, nil
}

func extractSupply(eventData map[string]any, txHash string) (*SupplyEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &SupplyEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractWithdrawBlue(eventData map[string]any, txHash string) (*WithdrawEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	receiver, err := getAddress(eventData, "receiver")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &WithdrawEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Receiver:       receiver,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractBorrow(eventData map[string]any, txHash string) (*BorrowEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	receiver, err := getAddress(eventData, "receiver")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &BorrowEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Receiver:       receiver,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractRepay(eventData map[string]any, txHash string) (*RepayEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &RepayEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractSupplyCollateral(eventData map[string]any, txHash string) (*SupplyCollateralEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}

	return &SupplyCollateralEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Assets:         assets,
	}, nil
}

func extractWithdrawCollateral(eventData map[string]any, txHash string) (*WithdrawCollateralEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	receiver, err := getAddress(eventData, "receiver")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}

	return &WithdrawCollateralEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		OnBehalf:       onBehalf,
		Receiver:       receiver,
		Assets:         assets,
	}, nil
}

func extractLiquidate(eventData map[string]any, txHash string) (*LiquidateEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	caller, err := getAddress(eventData, "caller")
	if err != nil {
		return nil, err
	}
	borrower, err := getAddress(eventData, "borrower")
	if err != nil {
		return nil, err
	}
	repaidAssets, err := getBigInt(eventData, "repaidAssets")
	if err != nil {
		return nil, err
	}
	repaidShares, err := getBigInt(eventData, "repaidShares")
	if err != nil {
		return nil, err
	}
	seizedAssets, err := getBigInt(eventData, "seizedAssets")
	if err != nil {
		return nil, err
	}
	badDebtAssets, err := getBigInt(eventData, "badDebtAssets")
	if err != nil {
		return nil, err
	}
	badDebtShares, err := getBigInt(eventData, "badDebtShares")
	if err != nil {
		return nil, err
	}

	return &LiquidateEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		Caller:         caller,
		Borrower:       borrower,
		RepaidAssets:   repaidAssets,
		RepaidShares:   repaidShares,
		SeizedAssets:   seizedAssets,
		BadDebtAssets:  badDebtAssets,
		BadDebtShares:  badDebtShares,
	}, nil
}

func extractAccrueInterest(eventData map[string]any, txHash string) (*AccrueInterestEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	prevBorrowRate, err := getBigInt(eventData, "prevBorrowRate")
	if err != nil {
		return nil, err
	}
	interest, err := getBigInt(eventData, "interest")
	if err != nil {
		return nil, err
	}
	feeShares, err := getBigInt(eventData, "feeShares")
	if err != nil {
		return nil, err
	}

	return &AccrueInterestEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		PrevBorrowRate: prevBorrowRate,
		Interest:       interest,
		FeeShares:      feeShares,
	}, nil
}

func extractSetFee(eventData map[string]any, txHash string) (*SetFeeEvent, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return nil, err
	}
	newFee, err := getBigInt(eventData, "newFee")
	if err != nil {
		return nil, err
	}

	return &SetFeeEvent{
		morphoBlueBase: morphoBlueBase{marketID: id, txHash: txHash},
		NewFee:         newFee,
	}, nil
}

// ---------------------------------------------------------------------------
// MetaMorpho event extractors
// ---------------------------------------------------------------------------

func extractVaultDeposit(eventData map[string]any, txHash string) (*VaultDepositEvent, error) {
	sender, err := getAddress(eventData, "sender")
	if err != nil {
		return nil, err
	}
	owner, err := getAddress(eventData, "owner")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &VaultDepositEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		Sender:         sender,
		Owner:          owner,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractVaultWithdraw(eventData map[string]any, txHash string) (*VaultWithdrawEvent, error) {
	sender, err := getAddress(eventData, "sender")
	if err != nil {
		return nil, err
	}
	receiver, err := getAddress(eventData, "receiver")
	if err != nil {
		return nil, err
	}
	owner, err := getAddress(eventData, "owner")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	shares, err := getBigInt(eventData, "shares")
	if err != nil {
		return nil, err
	}

	return &VaultWithdrawEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		Sender:         sender,
		Receiver:       receiver,
		Owner:          owner,
		Assets:         assets,
		Shares:         shares,
	}, nil
}

func extractVaultTransfer(eventData map[string]any, txHash string) (*VaultTransferEvent, error) {
	from, err := getAddress(eventData, "from")
	if err != nil {
		return nil, err
	}
	to, err := getAddress(eventData, "to")
	if err != nil {
		return nil, err
	}
	value, err := getBigInt(eventData, "value")
	if err != nil {
		return nil, err
	}

	return &VaultTransferEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		From:           from,
		To:             to,
		Value:          value,
	}, nil
}

func extractVaultAccrueInterest(eventData map[string]any, txHash string) (*VaultAccrueInterestEvent, error) {
	newTotalAssets, err := getBigInt(eventData, "newTotalAssets")
	if err != nil {
		return nil, err
	}

	// V1 uses "feeShares", V2 uses "performanceFeeShares" for the same concept.
	feeShares, err := getBigInt(eventData, "feeShares")
	if err != nil {
		feeShares, err = getBigInt(eventData, "performanceFeeShares")
		if err != nil {
			return nil, fmt.Errorf("missing both feeShares and performanceFeeShares")
		}
	}

	result := &VaultAccrueInterestEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		NewTotalAssets: newTotalAssets,
		FeeShares:      feeShares,
	}

	// V2-only fields: absent on the V1/V1.1 2-field variant (left nil), but a
	// present-but-mistyped value must surface, not silently persist NULL.
	prev, err := getOptionalBigInt(eventData, "previousTotalAssets")
	if err != nil {
		return nil, err
	}
	result.PreviousTotalAssets = prev

	mgmt, err := getOptionalBigInt(eventData, "managementFeeShares")
	if err != nil {
		return nil, err
	}
	result.ManagementFeeShares = mgmt

	return result, nil
}

// ---------------------------------------------------------------------------
// Morpho VaultV2 adapter / allocation / cap / fee extractors
// ---------------------------------------------------------------------------

func extractAddAdapter(eventData map[string]any, txHash string) (*AddAdapterEvent, error) {
	account, err := getAddress(eventData, "account")
	if err != nil {
		return nil, err
	}
	return &AddAdapterEvent{metaMorphoBase: metaMorphoBase{txHash: txHash}, Account: account}, nil
}

func extractRemoveAdapter(eventData map[string]any, txHash string) (*RemoveAdapterEvent, error) {
	account, err := getAddress(eventData, "account")
	if err != nil {
		return nil, err
	}
	return &RemoveAdapterEvent{metaMorphoBase: metaMorphoBase{txHash: txHash}, Account: account}, nil
}

// allocationFields holds the fields shared by the identically-shaped Allocate
// and Deallocate events.
type allocationFields struct {
	sender  common.Address
	adapter common.Address
	assets  *big.Int
	ids     []common.Hash
	change  *big.Int
}

func extractAllocationFields(eventData map[string]any) (allocationFields, error) {
	sender, err := getAddress(eventData, "sender")
	if err != nil {
		return allocationFields{}, err
	}
	adapter, err := getAddress(eventData, "adapter")
	if err != nil {
		return allocationFields{}, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return allocationFields{}, err
	}
	ids, err := getHashSlice(eventData, "ids")
	if err != nil {
		return allocationFields{}, err
	}
	change, err := getBigInt(eventData, "change")
	if err != nil {
		return allocationFields{}, err
	}
	return allocationFields{sender: sender, adapter: adapter, assets: assets, ids: ids, change: change}, nil
}

func extractAllocate(eventData map[string]any, txHash string) (*AllocateEvent, error) {
	f, err := extractAllocationFields(eventData)
	if err != nil {
		return nil, err
	}
	return &AllocateEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		Sender:         f.sender,
		Adapter:        f.adapter,
		Assets:         f.assets,
		IDs:            f.ids,
		Change:         f.change,
	}, nil
}

func extractDeallocate(eventData map[string]any, txHash string) (*DeallocateEvent, error) {
	f, err := extractAllocationFields(eventData)
	if err != nil {
		return nil, err
	}
	return &DeallocateEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		Sender:         f.sender,
		Adapter:        f.adapter,
		Assets:         f.assets,
		IDs:            f.ids,
		Change:         f.change,
	}, nil
}

func extractForceDeallocate(eventData map[string]any, txHash string) (*ForceDeallocateEvent, error) {
	sender, err := getAddress(eventData, "sender")
	if err != nil {
		return nil, err
	}
	adapter, err := getAddress(eventData, "adapter")
	if err != nil {
		return nil, err
	}
	assets, err := getBigInt(eventData, "assets")
	if err != nil {
		return nil, err
	}
	onBehalf, err := getAddress(eventData, "onBehalf")
	if err != nil {
		return nil, err
	}
	ids, err := getHashSlice(eventData, "ids")
	if err != nil {
		return nil, err
	}
	penaltyAssets, err := getBigInt(eventData, "penaltyAssets")
	if err != nil {
		return nil, err
	}
	return &ForceDeallocateEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		Sender:         sender,
		Adapter:        adapter,
		Assets:         assets,
		OnBehalf:       onBehalf,
		IDs:            ids,
		PenaltyAssets:  penaltyAssets,
	}, nil
}

// capIdentity holds the (id, idData) pair carried by every cap event; id is
// keccak256(idData) and is decoded from the indexed topic.
type capIdentity struct {
	id     common.Hash
	idData []byte
}

func extractCapIdentity(eventData map[string]any) (capIdentity, error) {
	id, err := getBytes32(eventData, "id")
	if err != nil {
		return capIdentity{}, err
	}
	idData, err := getBytes(eventData, "idData")
	if err != nil {
		return capIdentity{}, err
	}
	return capIdentity{id: common.Hash(id), idData: idData}, nil
}

func extractIncreaseAbsoluteCap(eventData map[string]any, txHash string) (*IncreaseAbsoluteCapEvent, error) {
	c, err := extractCapIdentity(eventData)
	if err != nil {
		return nil, err
	}
	newCap, err := getBigInt(eventData, "newAbsoluteCap")
	if err != nil {
		return nil, err
	}
	return &IncreaseAbsoluteCapEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		ID:             c.id,
		IDData:         c.idData,
		NewAbsoluteCap: newCap,
	}, nil
}

func extractDecreaseAbsoluteCap(eventData map[string]any, txHash string) (*DecreaseAbsoluteCapEvent, error) {
	c, err := extractCapIdentity(eventData)
	if err != nil {
		return nil, err
	}
	sender, err := getAddress(eventData, "sender")
	if err != nil {
		return nil, err
	}
	newCap, err := getBigInt(eventData, "newAbsoluteCap")
	if err != nil {
		return nil, err
	}
	return &DecreaseAbsoluteCapEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		Sender:         sender,
		ID:             c.id,
		IDData:         c.idData,
		NewAbsoluteCap: newCap,
	}, nil
}

func extractIncreaseRelativeCap(eventData map[string]any, txHash string) (*IncreaseRelativeCapEvent, error) {
	c, err := extractCapIdentity(eventData)
	if err != nil {
		return nil, err
	}
	newCap, err := getBigInt(eventData, "newRelativeCap")
	if err != nil {
		return nil, err
	}
	return &IncreaseRelativeCapEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		ID:             c.id,
		IDData:         c.idData,
		NewRelativeCap: newCap,
	}, nil
}

func extractDecreaseRelativeCap(eventData map[string]any, txHash string) (*DecreaseRelativeCapEvent, error) {
	c, err := extractCapIdentity(eventData)
	if err != nil {
		return nil, err
	}
	sender, err := getAddress(eventData, "sender")
	if err != nil {
		return nil, err
	}
	newCap, err := getBigInt(eventData, "newRelativeCap")
	if err != nil {
		return nil, err
	}
	return &DecreaseRelativeCapEvent{
		metaMorphoBase: metaMorphoBase{txHash: txHash},
		Sender:         sender,
		ID:             c.id,
		IDData:         c.idData,
		NewRelativeCap: newCap,
	}, nil
}

func extractSetPerformanceFee(eventData map[string]any, txHash string) (*SetPerformanceFeeEvent, error) {
	fee, err := getBigInt(eventData, "newPerformanceFee")
	if err != nil {
		return nil, err
	}
	return &SetPerformanceFeeEvent{metaMorphoBase: metaMorphoBase{txHash: txHash}, NewPerformanceFee: fee}, nil
}

func extractSetManagementFee(eventData map[string]any, txHash string) (*SetManagementFeeEvent, error) {
	fee, err := getBigInt(eventData, "newManagementFee")
	if err != nil {
		return nil, err
	}
	return &SetManagementFeeEvent{metaMorphoBase: metaMorphoBase{txHash: txHash}, NewManagementFee: fee}, nil
}

func extractSetPerformanceFeeRecipient(eventData map[string]any, txHash string) (*SetPerformanceFeeRecipientEvent, error) {
	recipient, err := getAddress(eventData, "newPerformanceFeeRecipient")
	if err != nil {
		return nil, err
	}
	return &SetPerformanceFeeRecipientEvent{metaMorphoBase: metaMorphoBase{txHash: txHash}, NewPerformanceFeeRecipient: recipient}, nil
}

func extractSetManagementFeeRecipient(eventData map[string]any, txHash string) (*SetManagementFeeRecipientEvent, error) {
	recipient, err := getAddress(eventData, "newManagementFeeRecipient")
	if err != nil {
		return nil, err
	}
	return &SetManagementFeeRecipientEvent{metaMorphoBase: metaMorphoBase{txHash: txHash}, NewManagementFeeRecipient: recipient}, nil
}

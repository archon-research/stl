package uniswap_v3_dex

import (
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// eventExtractor decodes Uniswap V3 pool and NFPM logs by topic[0]. The two
// ABIs share no topic hashes (pool and NFPM events have distinct signatures
// including ERC-721 Transfer which is itself unique).
type eventExtractor struct {
	poolByTopic map[common.Hash]*abi.Event // pool: Swap/Mint/Burn/Collect/Initialize/IOCN/SetFeeProtocol/CollectProtocol/Flash.
	nfpmByTopic map[common.Hash]*abi.Event // NFPM: IncreaseLiquidity/DecreaseLiquidity/Collect/Transfer.

	poolNames map[common.Hash]uniswapV3EventName
	nfpmNames map[common.Hash]uniswapV3EventName
}

func newEventExtractor() (*eventExtractor, error) {
	e := &eventExtractor{
		poolByTopic: make(map[common.Hash]*abi.Event),
		nfpmByTopic: make(map[common.Hash]*abi.Event),
		poolNames:   make(map[common.Hash]uniswapV3EventName),
		nfpmNames:   make(map[common.Hash]uniswapV3EventName),
	}
	if err := e.load(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *eventExtractor) load() error {
	poolABI, err := abis.GetUniswapV3PoolEventsABI()
	if err != nil {
		return fmt.Errorf("loading Uniswap V3 pool events ABI: %w", err)
	}
	poolMap := map[string]uniswapV3EventName{
		"Swap":                               eventSwap,
		"Mint":                               eventMint,
		"Burn":                               eventBurn,
		"Collect":                            eventCollect,
		"Initialize":                         eventInitialize,
		"IncreaseObservationCardinalityNext": eventIncreaseObservationCardinalityNext,
		"SetFeeProtocol":                     eventSetFeeProtocol,
		"CollectProtocol":                    eventCollectProtocol,
		"Flash":                              eventFlash,
	}
	for sol, n := range poolMap {
		ev, ok := poolABI.Events[sol]
		if !ok {
			return fmt.Errorf("pool event %s not in ABI", sol)
		}
		evCopy := ev
		e.poolByTopic[ev.ID] = &evCopy
		e.poolNames[ev.ID] = n
	}

	nfpmABI, err := abis.GetUniswapV3NFPMEventsABI()
	if err != nil {
		return fmt.Errorf("loading Uniswap V3 NFPM events ABI: %w", err)
	}
	nfpmMap := map[string]uniswapV3EventName{
		"IncreaseLiquidity": eventNFPMIncreaseLiquidity,
		"DecreaseLiquidity": eventNFPMDecreaseLiquidity,
		"Collect":           eventNFPMCollect,
		"Transfer":          eventNFPMTransfer,
	}
	for sol, n := range nfpmMap {
		ev, ok := nfpmABI.Events[sol]
		if !ok {
			return fmt.Errorf("nfpm event %s not in ABI", sol)
		}
		evCopy := ev
		e.nfpmByTopic[ev.ID] = &evCopy
		e.nfpmNames[ev.ID] = n
	}
	return nil
}

// poolTopic returns the decoded pool event name for the log's topic[0],
// or ("", false) if no pool ABI matches.
func (e *eventExtractor) poolTopic(log shared.Log) (uniswapV3EventName, bool) {
	if len(log.Topics) == 0 {
		return "", false
	}
	n, ok := e.poolNames[common.HexToHash(log.Topics[0])]
	return n, ok
}

// nfpmTopic returns the decoded NFPM event name for the log's topic[0],
// or ("", false) if no NFPM ABI matches.
func (e *eventExtractor) nfpmTopic(log shared.Log) (uniswapV3EventName, bool) {
	if len(log.Topics) == 0 {
		return "", false
	}
	n, ok := e.nfpmNames[common.HexToHash(log.Topics[0])]
	return n, ok
}

// extractPoolEvent decodes a pool-emitted log into a typed decodedEvent.
// Flash and unknown topics return (nil, nil) so callers can skip.
func (e *eventExtractor) extractPoolEvent(log shared.Log) (*decodedEvent, error) {
	name, ok := e.poolTopic(log)
	if !ok {
		return nil, nil
	}
	ev := e.poolByTopic[common.HexToHash(log.Topics[0])]
	data := make(map[string]any)
	if err := parseTopics(ev, log.Topics, data); err != nil {
		return nil, fmt.Errorf("decoding topics for %s: %w", name, err)
	}
	if err := parseData(ev, log.Data, data); err != nil {
		return nil, fmt.Errorf("decoding data for %s: %w", name, err)
	}
	logIdx, err := parseLogIndex(log.LogIndex)
	if err != nil {
		return nil, err
	}
	out := &decodedEvent{
		Name:    name,
		Address: common.HexToAddress(log.Address),
		LogIdx:  logIdx,
		TxHash:  common.HexToHash(log.TransactionHash),
	}
	switch name {
	case eventSwap:
		s, err := extractSwap(data)
		if err != nil {
			return nil, fmt.Errorf("decoding Swap: %w", err)
		}
		out.Swap = s
	case eventMint:
		l, err := extractMint(data)
		if err != nil {
			return nil, fmt.Errorf("decoding Mint: %w", err)
		}
		out.Liquidity = l
	case eventBurn:
		l, err := extractBurn(data)
		if err != nil {
			return nil, fmt.Errorf("decoding Burn: %w", err)
		}
		out.Liquidity = l
	case eventCollect:
		l, err := extractCollect(data)
		if err != nil {
			return nil, fmt.Errorf("decoding Collect: %w", err)
		}
		out.Liquidity = l
	case eventInitialize:
		p, err := extractInitialize(data)
		if err != nil {
			return nil, fmt.Errorf("decoding Initialize: %w", err)
		}
		out.Parameter = p
	case eventIncreaseObservationCardinalityNext:
		p, err := extractIOCN(data)
		if err != nil {
			return nil, fmt.Errorf("decoding IOCN: %w", err)
		}
		out.Parameter = p
	case eventSetFeeProtocol:
		p, err := extractSetFeeProtocol(data)
		if err != nil {
			return nil, fmt.Errorf("decoding SetFeeProtocol: %w", err)
		}
		out.Parameter = p
	case eventCollectProtocol:
		p, err := extractCollectProtocol(data)
		if err != nil {
			return nil, fmt.Errorf("decoding CollectProtocol: %w", err)
		}
		out.Parameter = p
	case eventFlash:
		// Intentionally a no-op — plan does not require a typed projection.
		return nil, nil
	default:
		return nil, fmt.Errorf("unhandled pool event %s", name)
	}
	return out, nil
}

// extractNFPMEvent decodes an NFPM-emitted log into a typed decodedEvent.
func (e *eventExtractor) extractNFPMEvent(log shared.Log) (*decodedEvent, error) {
	name, ok := e.nfpmTopic(log)
	if !ok {
		return nil, nil
	}
	ev := e.nfpmByTopic[common.HexToHash(log.Topics[0])]
	data := make(map[string]any)
	if err := parseTopics(ev, log.Topics, data); err != nil {
		return nil, fmt.Errorf("decoding NFPM topics for %s: %w", name, err)
	}
	if err := parseData(ev, log.Data, data); err != nil {
		return nil, fmt.Errorf("decoding NFPM data for %s: %w", name, err)
	}
	logIdx, err := parseLogIndex(log.LogIndex)
	if err != nil {
		return nil, err
	}
	out := &decodedEvent{
		Name:    name,
		Address: common.HexToAddress(log.Address),
		LogIdx:  logIdx,
		TxHash:  common.HexToHash(log.TransactionHash),
	}
	switch name {
	case eventNFPMIncreaseLiquidity, eventNFPMDecreaseLiquidity:
		nl, err := extractNFPMLiquidity(data)
		if err != nil {
			return nil, fmt.Errorf("decoding %s: %w", name, err)
		}
		if name == eventNFPMIncreaseLiquidity {
			out.NFPMIncrease = nl
		} else {
			out.NFPMDecrease = nl
		}
	case eventNFPMCollect:
		nc, err := extractNFPMCollect(data)
		if err != nil {
			return nil, fmt.Errorf("decoding NFPM Collect: %w", err)
		}
		out.NFPMCollect = nc
	case eventNFPMTransfer:
		nt, err := extractNFPMTransfer(data)
		if err != nil {
			return nil, fmt.Errorf("decoding NFPM Transfer: %w", err)
		}
		out.NFPMTransfer = nt
	default:
		return nil, fmt.Errorf("unhandled NFPM event %s", name)
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// Per-event extractors
// -----------------------------------------------------------------------------

func extractSwap(d map[string]any) (*swapEvent, error) {
	sender, err := getAddress(d, "sender")
	if err != nil {
		return nil, err
	}
	recipient, err := getAddress(d, "recipient")
	if err != nil {
		return nil, err
	}
	a0, err := getBigInt(d, "amount0")
	if err != nil {
		return nil, err
	}
	a1, err := getBigInt(d, "amount1")
	if err != nil {
		return nil, err
	}
	sp, err := getBigInt(d, "sqrtPriceX96")
	if err != nil {
		return nil, err
	}
	liq, err := getBigInt(d, "liquidity")
	if err != nil {
		return nil, err
	}
	tick, err := getInt32(d, "tick")
	if err != nil {
		return nil, err
	}
	return &swapEvent{
		Sender:       sender,
		Recipient:    recipient,
		Amount0:      a0,
		Amount1:      a1,
		SqrtPriceX96: sp,
		Liquidity:    liq,
		Tick:         tick,
	}, nil
}

func extractMint(d map[string]any) (*liquidityEvent, error) {
	sender, err := getAddress(d, "sender")
	if err != nil {
		return nil, err
	}
	owner, err := getAddress(d, "owner")
	if err != nil {
		return nil, err
	}
	tl, err := getInt32(d, "tickLower")
	if err != nil {
		return nil, err
	}
	tu, err := getInt32(d, "tickUpper")
	if err != nil {
		return nil, err
	}
	amt, err := getBigInt(d, "amount")
	if err != nil {
		return nil, err
	}
	a0, err := getBigInt(d, "amount0")
	if err != nil {
		return nil, err
	}
	a1, err := getBigInt(d, "amount1")
	if err != nil {
		return nil, err
	}
	s := sender
	return &liquidityEvent{
		Kind:      entity.UniswapV3PoolLiquidityEventMint,
		Owner:     owner,
		TickLower: tl,
		TickUpper: tu,
		Amount:    amt,
		Amount0:   a0,
		Amount1:   a1,
		Sender:    &s,
	}, nil
}

func extractBurn(d map[string]any) (*liquidityEvent, error) {
	owner, err := getAddress(d, "owner")
	if err != nil {
		return nil, err
	}
	tl, err := getInt32(d, "tickLower")
	if err != nil {
		return nil, err
	}
	tu, err := getInt32(d, "tickUpper")
	if err != nil {
		return nil, err
	}
	amt, err := getBigInt(d, "amount")
	if err != nil {
		return nil, err
	}
	a0, err := getBigInt(d, "amount0")
	if err != nil {
		return nil, err
	}
	a1, err := getBigInt(d, "amount1")
	if err != nil {
		return nil, err
	}
	return &liquidityEvent{
		Kind:      entity.UniswapV3PoolLiquidityEventBurn,
		Owner:     owner,
		TickLower: tl,
		TickUpper: tu,
		Amount:    amt,
		Amount0:   a0,
		Amount1:   a1,
	}, nil
}

func extractCollect(d map[string]any) (*liquidityEvent, error) {
	owner, err := getAddress(d, "owner")
	if err != nil {
		return nil, err
	}
	recipient, err := getAddress(d, "recipient")
	if err != nil {
		return nil, err
	}
	tl, err := getInt32(d, "tickLower")
	if err != nil {
		return nil, err
	}
	tu, err := getInt32(d, "tickUpper")
	if err != nil {
		return nil, err
	}
	a0, err := getBigInt(d, "amount0")
	if err != nil {
		return nil, err
	}
	a1, err := getBigInt(d, "amount1")
	if err != nil {
		return nil, err
	}
	r := recipient
	return &liquidityEvent{
		Kind:      entity.UniswapV3PoolLiquidityEventCollect,
		Owner:     owner,
		TickLower: tl,
		TickUpper: tu,
		Amount0:   a0,
		Amount1:   a1,
		Recipient: &r,
	}, nil
}

func extractInitialize(d map[string]any) (*parameterEvent, error) {
	sp, err := getBigInt(d, "sqrtPriceX96")
	if err != nil {
		return nil, err
	}
	tick, err := getInt32(d, "tick")
	if err != nil {
		return nil, err
	}
	return &parameterEvent{
		Kind:         entity.UniswapV3PoolParameterEventInitialize,
		SqrtPriceX96: sp,
		Tick:         &tick,
	}, nil
}

func extractIOCN(d map[string]any) (*parameterEvent, error) {
	oldC, err := getInt32(d, "observationCardinalityNextOld")
	if err != nil {
		return nil, err
	}
	newC, err := getInt32(d, "observationCardinalityNextNew")
	if err != nil {
		return nil, err
	}
	return &parameterEvent{
		Kind:                      entity.UniswapV3PoolParameterEventIncreaseObservationCardinalityNext,
		ObservationCardinalityOld: &oldC,
		ObservationCardinalityNew: &newC,
	}, nil
}

func extractSetFeeProtocol(d map[string]any) (*parameterEvent, error) {
	f0o, err := getInt32(d, "feeProtocol0Old")
	if err != nil {
		return nil, err
	}
	f1o, err := getInt32(d, "feeProtocol1Old")
	if err != nil {
		return nil, err
	}
	f0n, err := getInt32(d, "feeProtocol0New")
	if err != nil {
		return nil, err
	}
	f1n, err := getInt32(d, "feeProtocol1New")
	if err != nil {
		return nil, err
	}
	return &parameterEvent{
		Kind:            entity.UniswapV3PoolParameterEventSetFeeProtocol,
		FeeProtocol0Old: &f0o,
		FeeProtocol0New: &f0n,
		FeeProtocol1Old: &f1o,
		FeeProtocol1New: &f1n,
	}, nil
}

func extractCollectProtocol(d map[string]any) (*parameterEvent, error) {
	sender, err := getAddress(d, "sender")
	if err != nil {
		return nil, err
	}
	recipient, err := getAddress(d, "recipient")
	if err != nil {
		return nil, err
	}
	a0, err := getBigInt(d, "amount0")
	if err != nil {
		return nil, err
	}
	a1, err := getBigInt(d, "amount1")
	if err != nil {
		return nil, err
	}
	s := sender
	r := recipient
	return &parameterEvent{
		Kind:      entity.UniswapV3PoolParameterEventCollectProtocol,
		Sender:    &s,
		Recipient: &r,
		Amount0:   a0,
		Amount1:   a1,
	}, nil
}

func extractNFPMLiquidity(d map[string]any) (*nfpmLiquidityEvent, error) {
	tokenID, err := getBigInt(d, "tokenId")
	if err != nil {
		return nil, err
	}
	liq, err := getBigInt(d, "liquidity")
	if err != nil {
		return nil, err
	}
	a0, err := getBigInt(d, "amount0")
	if err != nil {
		return nil, err
	}
	a1, err := getBigInt(d, "amount1")
	if err != nil {
		return nil, err
	}
	return &nfpmLiquidityEvent{
		TokenID:   tokenID,
		Liquidity: liq,
		Amount0:   a0,
		Amount1:   a1,
	}, nil
}

func extractNFPMCollect(d map[string]any) (*nfpmCollectEvent, error) {
	tokenID, err := getBigInt(d, "tokenId")
	if err != nil {
		return nil, err
	}
	recipient, err := getAddress(d, "recipient")
	if err != nil {
		return nil, err
	}
	a0, err := getBigInt(d, "amount0")
	if err != nil {
		return nil, err
	}
	a1, err := getBigInt(d, "amount1")
	if err != nil {
		return nil, err
	}
	return &nfpmCollectEvent{
		TokenID:   tokenID,
		Recipient: recipient,
		Amount0:   a0,
		Amount1:   a1,
	}, nil
}

func extractNFPMTransfer(d map[string]any) (*nfpmTransferEvent, error) {
	from, err := getAddress(d, "from")
	if err != nil {
		return nil, err
	}
	to, err := getAddress(d, "to")
	if err != nil {
		return nil, err
	}
	tokenID, err := getBigInt(d, "tokenId")
	if err != nil {
		return nil, err
	}
	return &nfpmTransferEvent{From: from, To: to, TokenID: tokenID}, nil
}

// -----------------------------------------------------------------------------
// ABI helpers (mirrors curve_dex/event_extractor.go).
// -----------------------------------------------------------------------------

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
	hashes := make([]common.Hash, 0, len(topics)-1)
	for i := 1; i < len(topics); i++ {
		hashes = append(hashes, common.HexToHash(topics[i]))
	}
	return abi.ParseTopicsIntoMap(eventData, indexed, hashes)
}

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

func getAddress(d map[string]any, key string) (common.Address, error) {
	v, ok := d[key]
	if !ok {
		return common.Address{}, fmt.Errorf("missing field: %s", key)
	}
	addr, ok := v.(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("invalid type for %s: %T", key, v)
	}
	return addr, nil
}

func getBigInt(d map[string]any, key string) (*big.Int, error) {
	v, ok := d[key]
	if !ok {
		return nil, fmt.Errorf("missing field: %s", key)
	}
	// uint8 / uint16 are decoded by go-ethereum's ABI as those native types; we
	// accept them too so the small-int fields on V3 events (e.g. feeProtocol0Old)
	// can be promoted through this helper if the caller asks for a *big.Int.
	switch b := v.(type) {
	case *big.Int:
		return b, nil
	case uint16:
		return new(big.Int).SetUint64(uint64(b)), nil
	case uint8:
		return new(big.Int).SetUint64(uint64(b)), nil
	}
	return nil, fmt.Errorf("invalid type for %s: %T", key, v)
}

// getInt32 reads int24 / uint16 / uint8 / *big.Int into an int32. V3 events
// use int24 for ticks, uint16 for cardinalities, and uint8 for feeProtocol —
// go-ethereum's ABI maps int24 to *big.Int and uint16/uint8 to native types.
func getInt32(d map[string]any, key string) (int32, error) {
	v, ok := d[key]
	if !ok {
		return 0, fmt.Errorf("missing field: %s", key)
	}
	switch n := v.(type) {
	case *big.Int:
		return int32(n.Int64()), nil
	case int32:
		return n, nil
	case int16:
		return int32(n), nil
	case int8:
		return int32(n), nil
	case uint32:
		return int32(n), nil
	case uint16:
		return int32(n), nil
	case uint8:
		return int32(n), nil
	}
	return 0, fmt.Errorf("invalid type for %s (want int32-like): %T", key, v)
}

func parseLogIndex(s string) (int32, error) {
	if s == "" {
		return 0, nil
	}
	v, ok := new(big.Int).SetString(s, 0)
	if !ok {
		return 0, fmt.Errorf("parsing log index %q", s)
	}
	return int32(v.Int64()), nil
}

package curve_dex

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// savePoolEventProjection writes the typed projection row for a decoded pool
// event. Swap → curve_pool_swap; AddLiquidity / RemoveLiquidity* →
// curve_pool_liquidity_event; RampA / StopRampA / NewFee / ApplyNewFee →
// curve_pool_parameter_event.
func (s *Service) savePoolEventProjection(ctx context.Context, tx pgx.Tx, decoded *DecodedEvent, pool *entity.CurvePool, _ int64, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	switch {
	case decoded.Swap != nil:
		swap := &entity.CurvePoolSwap{
			CurvePoolID:  pool.ID,
			BlockNumber:  blockNumber,
			BlockVersion: int32(blockVersion),
			Timestamp:    blockTimestamp,
			TxHash:       decoded.TxHash,
			LogIndex:     decoded.LogIdx,
			Buyer:        decoded.Swap.Buyer,
			SoldID:       int16(decoded.Swap.SoldID),
			TokensSold:   decoded.Swap.TokensSold,
			BoughtID:     int16(decoded.Swap.BoughtID),
			TokensBought: decoded.Swap.TokensBought,
			IsUnderlying: decoded.Swap.IsUnderlying,
		}
		if err := s.curveRepo.SaveCurvePoolSwap(ctx, tx, swap); err != nil {
			return fmt.Errorf("saving swap for %s: %w", pool.Label, err)
		}
		return nil
	case decoded.Liquidity != nil:
		l := decoded.Liquidity
		evt := &entity.CurvePoolLiquidityEvent{
			CurvePoolID:  pool.ID,
			BlockNumber:  blockNumber,
			BlockVersion: int32(blockVersion),
			Timestamp:    blockTimestamp,
			TxHash:       decoded.TxHash,
			LogIndex:     decoded.LogIdx,
			EventKind:    l.Kind,
			Provider:     l.Provider,
			TokenAmounts: l.TokenAmounts,
			Fees:         l.Fees,
			InvariantD:   l.Invariant,
			TokenSupply:  l.TokenSupply,
			TokenAmount:  l.TokenAmount,
			CoinIndex:    l.CoinIndex,
			CoinAmount:   l.CoinAmount,
		}
		if err := s.curveRepo.SaveCurvePoolLiquidityEvent(ctx, tx, evt); err != nil {
			return fmt.Errorf("saving %s for %s: %w", l.Kind, pool.Label, err)
		}
		return nil
	case decoded.Parameter != nil:
		p := decoded.Parameter
		var extra json.RawMessage
		if p.Extra != nil {
			b, err := json.Marshal(p.Extra)
			if err != nil {
				return fmt.Errorf("marshalling parameter extra: %w", err)
			}
			extra = b
		}
		evt := &entity.CurvePoolParameterEvent{
			CurvePoolID:  pool.ID,
			BlockNumber:  blockNumber,
			BlockVersion: int32(blockVersion),
			Timestamp:    blockTimestamp,
			TxHash:       decoded.TxHash,
			LogIndex:     decoded.LogIdx,
			EventKind:    p.Kind,
			OldA:         p.OldA,
			NewA:         p.NewA,
			InitialTime:  bigIntToTimePtr(p.InitialTime),
			FutureTime:   bigIntToTimePtr(p.FutureTime),
			NewFee:       p.NewFee,
			NewAdminFee:  p.NewAdminFee,
			Extra:        extra,
		}
		if err := s.curveRepo.SaveCurvePoolParameterEvent(ctx, tx, evt); err != nil {
			return fmt.Errorf("saving parameter %s for %s: %w", p.Kind, pool.Label, err)
		}
		return nil
	}
	return nil
}

// savePoolState writes one curve_pool_state row for the snapshot taken at
// blockNumber. source = "event" because we got here from a decoded log.
func (s *Service) savePoolState(ctx context.Context, tx pgx.Tx, pool *entity.CurvePool, mc *poolMulticallResult, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	state := &entity.CurvePoolState{
		CurvePoolID:  pool.ID,
		BlockNumber:  blockNumber,
		BlockVersion: int32(blockVersion),
		Timestamp:    blockTimestamp,
		Source:       entity.CurvePoolStateSourceEvent,
		Balances:     mc.Balances,
		TotalSupply:  mc.TotalSupply,
		VirtualPrice: mc.VirtualPrice,
		AFactor:      mc.AFactor,
		Fee:          mc.Fee,
		PriceOracle:  mc.PriceOracle,
		LastPrice:    mc.LastPrice,
	}
	if err := s.curveRepo.SaveCurvePoolState(ctx, tx, state); err != nil {
		return fmt.Errorf("saving pool state for %s: %w", pool.Label, err)
	}
	return nil
}

// saveExchangeRates writes one curve_pool_exchange_rate row per directional pair.
func (s *Service) saveExchangeRates(ctx context.Context, tx pgx.Tx, pool *entity.CurvePool, mc *poolMulticallResult, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	for _, e := range mc.ExchangeDy {
		r := &entity.CurvePoolExchangeRate{
			CurvePoolID:  pool.ID,
			BlockNumber:  blockNumber,
			BlockVersion: int32(blockVersion),
			Timestamp:    blockTimestamp,
			I:            e.I,
			J:            e.J,
			Dx:           e.Dx,
			Dy:           e.Dy,
		}
		if err := s.curveRepo.SaveCurvePoolExchangeRate(ctx, tx, r); err != nil {
			return fmt.Errorf("saving exchange rate (%d,%d) for %s: %w", e.I, e.J, pool.Label, err)
		}
	}
	return nil
}

// saveGaugeState writes one curve_gauge_state row plus, where new reward
// tokens appear, registers them in the `token` table on-the-fly so downstream
// queries can join.
//
// Returns the list of newly-registered reward token addresses. The caller
// must commit the surrounding tx successfully BEFORE calling
// markTokensRegistered with this list — otherwise a tx rollback would
// leave the in-process cache thinking the token row exists when it doesn't,
// permanently skipping the metadata read on subsequent gauge events.
func (s *Service) saveGaugeState(ctx context.Context, tx pgx.Tx, gauge *entity.CurveGauge, gs *gaugeMulticallResult, chainID, blockNumber int64, blockVersion int, blockTimestamp time.Time) ([]common.Address, error) {
	finishes := make([]time.Time, len(gs.RewardPeriodFinish))
	for i, pf := range gs.RewardPeriodFinish {
		if pf == nil {
			finishes[i] = time.Time{}
			continue
		}
		finishes[i] = time.Unix(pf.Int64(), 0).UTC()
	}

	var newlyRegistered []common.Address
	for _, tok := range gs.RewardTokens {
		if isZeroAddress(tok) {
			continue
		}
		// Cheap fast-path: if we've registered this token in-process already,
		// skip both the on-chain metadata read and the repo round-trip.
		s.registeredTokensMu.Lock()
		_, seen := s.registeredTokens[tok]
		s.registeredTokensMu.Unlock()
		if seen {
			continue
		}
		// First-time registration: read symbol() + decimals() on-chain so a
		// non-18-decimal token (USDC, WBTC, …) isn't silently saved with the
		// 18 placeholder, which would break every downstream USD conversion.
		symbol, decimals, err := s.blockchain.readERC20Metadata(ctx, tok, blockNumber)
		if err != nil {
			return nil, fmt.Errorf("reading ERC20 metadata for reward token %s: %w", tok.Hex(), err)
		}
		if _, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, tok, symbol, int(decimals), blockNumber); err != nil {
			return nil, fmt.Errorf("registering reward token %s: %w", tok.Hex(), err)
		}
		// Cache update is DEFERRED to post-commit (see method doc-comment).
		newlyRegistered = append(newlyRegistered, tok)
	}

	state := &entity.CurveGaugeState{
		CurveGaugeID:       gauge.ID,
		BlockNumber:        blockNumber,
		BlockVersion:       int32(blockVersion),
		Timestamp:          blockTimestamp,
		Source:             entity.CurveGaugeStateSourceEvent,
		InflationRate:      gs.InflationRate,
		WorkingSupply:      gs.WorkingSupply,
		TotalSupply:        gs.TotalSupply,
		IsKilled:           gs.IsKilled,
		RewardCount:        gs.RewardCount,
		RewardTokens:       gs.RewardTokens,
		RewardRates:        gs.RewardRates,
		RewardPeriodFinish: finishes,
	}
	if err := s.curveRepo.SaveCurveGaugeState(ctx, tx, state); err != nil {
		return nil, fmt.Errorf("saving gauge state for %s: %w", gauge.Address.Hex(), err)
	}
	return newlyRegistered, nil
}

// markTokensRegistered inserts the addresses into the in-process cache after
// the surrounding tx has committed successfully. Safe to call with an empty
// or nil slice.
func (s *Service) markTokensRegistered(tokens []common.Address) {
	if len(tokens) == 0 {
		return
	}
	s.registeredTokensMu.Lock()
	defer s.registeredTokensMu.Unlock()
	for _, t := range tokens {
		s.registeredTokens[t] = struct{}{}
	}
}

// -----------------------------------------------------------------------------
// small math helpers
// -----------------------------------------------------------------------------

func negate(b *big.Int) *big.Int {
	if b == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Neg(b)
}

func bigIntCopy(b *big.Int) *big.Int {
	if b == nil {
		return nil
	}
	return new(big.Int).Set(b)
}

func bigIntToTimePtr(b *big.Int) *time.Time {
	if b == nil || b.Sign() == 0 {
		return nil
	}
	t := time.Unix(b.Int64(), 0).UTC()
	return &t
}

func isZeroAddress(a [20]byte) bool {
	for _, b := range a {
		if b != 0 {
			return false
		}
	}
	return true
}

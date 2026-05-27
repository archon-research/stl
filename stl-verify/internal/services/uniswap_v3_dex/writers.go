package uniswap_v3_dex

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// savePoolEventProjection writes the typed projection row for a decoded pool
// event. Swap → uniswap_v3_pool_swap; Mint/Burn/Collect →
// uniswap_v3_pool_liquidity_event; Initialize / IOCN / SetFeeProtocol /
// CollectProtocol → uniswap_v3_pool_parameter_event.
func (s *Service) savePoolEventProjection(ctx context.Context, tx pgx.Tx, decoded *decodedEvent, pool *entity.UniswapV3Pool, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	switch {
	case decoded.Swap != nil:
		sw := decoded.Swap
		swap := &entity.UniswapV3PoolSwap{
			UniswapV3PoolID:   pool.ID,
			BlockNumber:       blockNumber,
			BlockVersion:      int32(blockVersion),
			Timestamp:         blockTimestamp,
			TxHash:            decoded.TxHash,
			LogIndex:          decoded.LogIdx,
			Sender:            sw.Sender,
			Recipient:         sw.Recipient,
			Amount0:           sw.Amount0,
			Amount1:           sw.Amount1,
			SqrtPriceX96After: sw.SqrtPriceX96,
			LiquidityAfter:    sw.Liquidity,
			TickAfter:         sw.Tick,
		}
		if err := s.uniswapRepo.SaveUniswapV3PoolSwap(ctx, tx, swap); err != nil {
			return fmt.Errorf("saving swap for %s: %w", pool.Label, err)
		}
		return nil
	case decoded.Liquidity != nil:
		l := decoded.Liquidity
		evt := &entity.UniswapV3PoolLiquidityEvent{
			UniswapV3PoolID: pool.ID,
			BlockNumber:     blockNumber,
			BlockVersion:    int32(blockVersion),
			Timestamp:       blockTimestamp,
			TxHash:          decoded.TxHash,
			LogIndex:        decoded.LogIdx,
			EventKind:       l.Kind,
			Owner:           l.Owner,
			TickLower:       l.TickLower,
			TickUpper:       l.TickUpper,
			Amount:          l.Amount,
			Amount0:         l.Amount0,
			Amount1:         l.Amount1,
			Sender:          l.Sender,
			Recipient:       l.Recipient,
		}
		if err := s.uniswapRepo.SaveUniswapV3PoolLiquidityEvent(ctx, tx, evt); err != nil {
			return fmt.Errorf("saving %s for %s: %w", l.Kind, pool.Label, err)
		}
		return nil
	case decoded.Parameter != nil:
		p := decoded.Parameter
		evt := &entity.UniswapV3PoolParameterEvent{
			UniswapV3PoolID:           pool.ID,
			BlockNumber:               blockNumber,
			BlockVersion:              int32(blockVersion),
			Timestamp:                 blockTimestamp,
			TxHash:                    decoded.TxHash,
			LogIndex:                  decoded.LogIdx,
			EventKind:                 p.Kind,
			SqrtPriceX96:              p.SqrtPriceX96,
			Tick:                      p.Tick,
			ObservationCardinalityOld: p.ObservationCardinalityOld,
			ObservationCardinalityNew: p.ObservationCardinalityNew,
			FeeProtocol0Old:           p.FeeProtocol0Old,
			FeeProtocol0New:           p.FeeProtocol0New,
			FeeProtocol1Old:           p.FeeProtocol1Old,
			FeeProtocol1New:           p.FeeProtocol1New,
			Amount0:                   p.Amount0,
			Amount1:                   p.Amount1,
			Sender:                    p.Sender,
			Recipient:                 p.Recipient,
		}
		if err := s.uniswapRepo.SaveUniswapV3PoolParameterEvent(ctx, tx, evt); err != nil {
			return fmt.Errorf("saving parameter %s for %s: %w", p.Kind, pool.Label, err)
		}
		return nil
	}
	return nil
}

// savePoolState writes one uniswap_v3_pool_state row using the multicall
// result. The observe([0]) cumulatives are required on every row (plan §12.4
// #12) so downstream TWAP queries can self-join. source = "event" because we
// got here from a decoded log.
func (s *Service) savePoolState(ctx context.Context, tx pgx.Tx, pool *entity.UniswapV3Pool, mc *poolMulticallResult, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	obsIdx := mc.ObservationIndex
	obsCard := mc.ObservationCardinality
	obsCardNext := mc.ObservationCardinalityNext
	feeProto := mc.FeeProtocol
	unlocked := mc.Unlocked
	state := &entity.UniswapV3PoolState{
		UniswapV3PoolID:                pool.ID,
		BlockNumber:                    blockNumber,
		BlockVersion:                   int32(blockVersion),
		Timestamp:                      blockTimestamp,
		Source:                         entity.UniswapV3PoolStateSourceEvent,
		SqrtPriceX96:                   mc.SqrtPriceX96,
		Tick:                           mc.Tick,
		Liquidity:                      mc.Liquidity,
		ObservationIndex:               &obsIdx,
		ObservationCardinality:         &obsCard,
		ObservationCardinalityNext:     &obsCardNext,
		FeeProtocol:                    &feeProto,
		Unlocked:                       &unlocked,
		TickCumulative:                 mc.TickCumulative,
		SecsPerLiquidityCumulativeX128: mc.SecsPerLiquidityCumulativeX128,
		Balance0:                       mc.Balance0,
		Balance1:                       mc.Balance1,
	}
	if err := s.uniswapRepo.SaveUniswapV3PoolState(ctx, tx, state); err != nil {
		return fmt.Errorf("saving pool state for %s: %w", pool.Label, err)
	}
	return nil
}

// savePositionState writes one uniswap_v3_position_state row using the NFPM
// positions(tokenId) multicall result. Source = "event".
func (s *Service) savePositionState(ctx context.Context, tx pgx.Tx, positionID int64, p *nfpmPositionResult, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	state := &entity.UniswapV3PositionState{
		UniswapV3PositionID:      positionID,
		BlockNumber:              blockNumber,
		BlockVersion:             int32(blockVersion),
		Timestamp:                blockTimestamp,
		Source:                   entity.UniswapV3PositionStateSourceEvent,
		Liquidity:                p.Liquidity,
		TokensOwed0:              p.TokensOwed0,
		TokensOwed1:              p.TokensOwed1,
		FeeGrowthInside0LastX128: p.FeeGrowthInside0LastX128,
		FeeGrowthInside1LastX128: p.FeeGrowthInside1LastX128,
	}
	if err := s.uniswapRepo.SaveUniswapV3PositionState(ctx, tx, state); err != nil {
		return fmt.Errorf("saving position state for id %d: %w", positionID, err)
	}
	return nil
}

package balancer_dex

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// saveBalancerPoolEventProjection writes the typed projection row for a
// decoded Vault or pool-contract event.
//
//	Swap                            → balancer_pool_swap
//	PoolBalanceChanged              → balancer_pool_liquidity_event
//	AmpUpdate* / TokenRate* / etc.  → balancer_pool_parameter_event
//	PoolBalanceManaged              → no typed projection (protocol_event only)
//
// Returns a non-nil *BalancerPoolToken when the caller must refresh the
// in-memory registry slot (TokenRateProviderSet). The caller MUST apply it via
// pool.replaceToken ONLY after the surrounding transaction commits — mutating
// the registry inside the tx callback (the prior bug) desyncs the cache from
// the DB if the commit later rolls back.
func (s *Service) saveBalancerPoolEventProjection(ctx context.Context, tx pgx.Tx, decoded *decodedEvent, pool *registeredPool, blockNumber int64, blockVersion int, blockTimestamp time.Time) (*entity.BalancerPoolToken, error) {
	switch {
	case decoded.Swap != nil:
		inIdx, outIdx, err := pool.tokenIndices(decoded.Swap.TokenIn, decoded.Swap.TokenOut)
		if err != nil {
			return nil, fmt.Errorf("resolving swap token indices for %s: %w", pool.entity.Label, err)
		}
		swap := &entity.BalancerPoolSwap{
			BalancerPoolID: pool.entity.ID,
			BlockNumber:    blockNumber,
			BlockVersion:   int32(blockVersion),
			Timestamp:      blockTimestamp,
			TxHash:         decoded.TxHash,
			LogIndex:       decoded.LogIdx,
			TokenInIdx:     inIdx,
			TokenOutIdx:    outIdx,
			AmountIn:       decoded.Swap.AmountIn,
			AmountOut:      decoded.Swap.AmountOut,
		}
		if err := s.balancerRepo.SaveBalancerPoolSwap(ctx, tx, swap); err != nil {
			return nil, fmt.Errorf("saving swap for %s: %w", pool.entity.Label, err)
		}
		return nil, nil

	case decoded.Liquidity != nil:
		l := decoded.Liquidity
		// Project Vault.tokens-ordered deltas onto balancer_pool_token.token_index
		// ordering — Vault's tokens array is typically the same as our slot
		// ordering, but project explicitly so downstream consumers can index
		// straight against balancer_pool_token rows.
		deltas := pool.projectByToken(l.Tokens, l.Deltas)
		fees := pool.projectByToken(l.Tokens, l.ProtocolFeeAmounts)
		evt := &entity.BalancerPoolLiquidityEvent{
			BalancerPoolID:     pool.entity.ID,
			BlockNumber:        blockNumber,
			BlockVersion:       int32(blockVersion),
			Timestamp:          blockTimestamp,
			TxHash:             decoded.TxHash,
			LogIndex:           decoded.LogIdx,
			LiquidityProvider:  l.LiquidityProvider,
			Deltas:             deltas,
			ProtocolFeeAmounts: fees,
		}
		if err := s.balancerRepo.SaveBalancerPoolLiquidityEvent(ctx, tx, evt); err != nil {
			return nil, fmt.Errorf("saving liquidity event for %s: %w", pool.entity.Label, err)
		}
		return nil, nil

	case decoded.Parameter != nil:
		p := decoded.Parameter
		var extra json.RawMessage
		if p.Extra != nil {
			b, err := json.Marshal(p.Extra)
			if err != nil {
				return nil, fmt.Errorf("marshalling parameter extra: %w", err)
			}
			extra = b
		}
		evt := &entity.BalancerPoolParameterEvent{
			BalancerPoolID:    pool.entity.ID,
			BlockNumber:       blockNumber,
			BlockVersion:      int32(blockVersion),
			Timestamp:         blockTimestamp,
			TxHash:            decoded.TxHash,
			LogIndex:          decoded.LogIdx,
			EventKind:         p.Kind,
			StartValue:        p.StartValue,
			EndValue:          p.EndValue,
			StartTime:         bigIntToTimePtr(p.StartTime),
			EndTime:           bigIntToTimePtr(p.EndTime),
			CurrentValue:      p.CurrentValue,
			RateProvider:      p.RateProvider,
			CacheDuration:     p.CacheDuration,
			Rate:              p.Rate,
			SwapFeePercentage: p.SwapFeePercentage,
			Extra:             extra,
		}
		// TokenRate* events carry a tokenIndex; resolve that to a token address
		// for the typed column so downstream queries can join straight to
		// balancer_pool_token without a separate index lookup.
		if p.TokenIndex != nil {
			if addr, ok := pool.tokenAddressAt(int(*p.TokenIndex)); ok {
				a := addr
				evt.TokenAddress = &a
			}
		}
		if err := s.balancerRepo.SaveBalancerPoolParameterEvent(ctx, tx, evt); err != nil {
			return nil, fmt.Errorf("saving parameter %s for %s: %w", p.Kind, pool.entity.Label, err)
		}
		// TokenRateProviderSet refreshes the persistent join-table row so the
		// registry stays in sync with the latest provider address. Persist
		// inside the tx, but hand the updated slot back to the caller to apply
		// to the in-memory registry AFTER commit.
		if p.Kind == entity.BalancerParameterEventTokenRateProviderSet && p.TokenIndex != nil && p.RateProvider != nil {
			if existing, ok := pool.tokenAt(int(*p.TokenIndex)); ok {
				updated := *existing
				updated.RateProvider = p.RateProvider
				if err := s.balancerRepo.UpsertBalancerPoolToken(ctx, tx, &updated); err != nil {
					return nil, fmt.Errorf("updating rate_provider for %s slot %d: %w", pool.entity.Label, *p.TokenIndex, err)
				}
				return &updated, nil
			}
		}
		return nil, nil
	}
	return nil, nil
}

// savePoolState writes one balancer_pool_state row.
func (s *Service) savePoolState(ctx context.Context, tx pgx.Tx, pool *registeredPool, mc *poolMulticallResult, blockNumber int64, blockVersion int, blockTimestamp time.Time) error {
	// Order the multicall balances onto balancer_pool_token.token_index so the
	// NUMERIC[] column lines up with the registry view.
	balances := pool.projectByToken(mc.Vault.Tokens, mc.Vault.Balances)
	var lastChange *int64
	if mc.Vault.LastChangeBlock != nil {
		v := mc.Vault.LastChangeBlock.Int64()
		lastChange = &v
	}
	state := &entity.BalancerPoolState{
		BalancerPoolID:  pool.entity.ID,
		BlockNumber:     blockNumber,
		BlockVersion:    int32(blockVersion),
		Timestamp:       blockTimestamp,
		Source:          entity.BalancerPoolStateSourceEvent,
		Balances:        balances,
		AmpFactor:       mc.AmpFactor,
		BptRate:         mc.BptRate,
		ActualSupply:    mc.ActualSupply,
		TotalSupply:     mc.TotalSupply,
		TokenRates:      mc.TokenRates,
		ScalingFactors:  mc.ScalingFactors,
		LastChangeBlock: lastChange,
	}
	if err := s.balancerRepo.SaveBalancerPoolState(ctx, tx, state); err != nil {
		return fmt.Errorf("saving pool state for %s: %w", pool.entity.Label, err)
	}
	return nil
}

// -----------------------------------------------------------------------------
// small helpers
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

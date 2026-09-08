package morpho_indexer

import (
	"bytes"
	"context"
	"fmt"
	"math/big"
	"slices"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// This file holds the Morpho Blue market handlers: market creation, the
// per-user position events, liquidations, and interest accrual, plus the
// market-state and position snapshots they persist.

// handleCreateMarket handles a CreateMarket event.
func (s *Service) handleCreateMarket(ctx context.Context, e *CreateMarketEvent, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	mp := e.Params
	if mp == nil {
		return fmt.Errorf("CreateMarket event missing marketParams")
	}

	// Fetch token metadata and initial market state.
	loanMetadata, collMetadata, err := s.blockchainSvc.getTokenPairMetadata(ctx, mp.LoanToken, mp.CollateralToken, blockNumber)
	if err != nil {
		return fmt.Errorf("getting token pair metadata: %w", err)
	}

	ms, err := s.blockchainSvc.getMarketState(ctx, e.MarketID(), blockHash)
	if err != nil {
		return fmt.Errorf("fetching initial market state: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", s.deployBlock)
		if err != nil {
			return fmt.Errorf("getting protocol: %w", err)
		}

		loanTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, mp.LoanToken, loanMetadata.Symbol, loanMetadata.Decimals, &blockNumber)
		if err != nil {
			return fmt.Errorf("getting loan token: %w", err)
		}

		collTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, mp.CollateralToken, collMetadata.Symbol, collMetadata.Decimals, &blockNumber)
		if err != nil {
			return fmt.Errorf("getting collateral token: %w", err)
		}

		market, err := entity.NewMorphoMarket(chainID, protocolID, common.Hash(e.MarketID()), loanTokenID, collTokenID, mp.Oracle, mp.Irm, mp.LLTV, blockNumber)
		if err != nil {
			return fmt.Errorf("creating market entity: %w", err)
		}

		marketID, err := s.morphoRepo.GetOrCreateMarket(ctx, tx, market)
		if err != nil {
			return fmt.Errorf("creating market: %w", err)
		}

		return s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, blockTimestamp, ms, nil)
	})
}

// handlePositionEvent handles Supply, Withdraw, Borrow, Repay, SupplyCollateral, WithdrawCollateral events.
func (s *Service) handlePositionEvent(ctx context.Context, mktID [32]byte, user common.Address, eventType entity.MorphoEventType, txHash string, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	ms, ps, err := s.blockchainSvc.getMarketAndPositionState(ctx, mktID, user, blockHash)
	if err != nil {
		return fmt.Errorf("fetching on-chain state: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, mktID, chainID, blockNumber)
		if err != nil {
			return fmt.Errorf("ensuring market: %w", err)
		}

		// Save market state snapshot
		if err := s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, blockTimestamp, ms, nil); err != nil {
			return fmt.Errorf("saving market state: %w", err)
		}

		// Save user position snapshot
		return s.savePositionSnapshot(ctx, tx, user, marketID, blockNumber, blockVersion, blockTimestamp, ps, ms, eventType, txHash, chainID)
	})
}

// handleLiquidateEvent handles Liquidate events by snapshotting both borrower and liquidator.
func (s *Service) handleLiquidateEvent(ctx context.Context, e *LiquidateEvent, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	borrower := e.Borrower
	liquidator := e.Caller

	// Fetch market state + both positions in a single RPC call.
	ms, borrowerPos, liquidatorPos, err := s.blockchainSvc.getMarketAndTwoPositionStates(ctx, e.MarketID(), borrower, liquidator, blockHash)
	if err != nil {
		return fmt.Errorf("fetching on-chain state: %w", err)
	}

	// Save the borrower and liquidator positions in user-address order so the
	// per-row mmp advisory locks are acquired in a transaction-stable order.
	// Today the mss lock taken inside saveMarketStateSnapshot serializes all
	// concurrent transactions on this market, which means mmp ordering between
	// the two positions can't actually deadlock — but this defensive sort
	// closes the door on a future refactor that batches events into a shared
	// transaction or otherwise re-orders the per-event tx scope. See ADR-0002 §3.
	positions := []userPosition{
		{borrower, borrowerPos, "borrower"},
		{liquidator, liquidatorPos, "liquidator"},
	}
	slices.SortFunc(positions, func(a, b userPosition) int {
		return bytes.Compare(a.user.Bytes(), b.user.Bytes())
	})

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, e.MarketID(), chainID, blockNumber)
		if err != nil {
			return fmt.Errorf("ensuring market: %w", err)
		}

		if err := s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, blockTimestamp, ms, nil); err != nil {
			return fmt.Errorf("saving market state: %w", err)
		}

		for _, p := range positions {
			if err := s.savePositionSnapshot(ctx, tx, p.user, marketID, blockNumber, blockVersion, blockTimestamp, p.pos, ms, e.Type(), e.TxHash(), chainID); err != nil {
				return fmt.Errorf("saving %s position: %w", p.role, err)
			}
		}
		return nil
	})
}

// userPosition pairs a user address with their position state and a role
// label, for ordered per-user mmp/mvp lock acquisition (see
// handleLiquidateEvent / handleVaultTransfer). The role survives the sort so
// failures still surface as "saving <borrower|liquidator|sender|receiver>
// position: ..." regardless of which user got sorted first.
type userPosition struct {
	user common.Address
	pos  *PositionState
	role string
}

// handleAccrueInterest handles AccrueInterest events.
func (s *Service) handleAccrueInterest(ctx context.Context, e *AccrueInterestEvent, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	ms, err := s.blockchainSvc.getMarketState(ctx, e.MarketID(), blockHash)
	if err != nil {
		return fmt.Errorf("fetching market state: %w", err)
	}

	accrueData := &accrueInterestData{
		PrevBorrowRate: e.PrevBorrowRate,
		Interest:       e.Interest,
		FeeShares:      e.FeeShares,
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		marketID, err := s.ensureMarket(ctx, tx, e.MarketID(), chainID, blockNumber)
		if err != nil {
			return fmt.Errorf("ensuring market: %w", err)
		}
		return s.saveMarketStateSnapshot(ctx, tx, marketID, blockNumber, blockVersion, blockTimestamp, ms, accrueData)
	})
}

type accrueInterestData struct {
	PrevBorrowRate *big.Int
	Interest       *big.Int
	FeeShares      *big.Int
}

// ensureMarket ensures the market exists in the database and returns its ID.
func (s *Service) ensureMarket(ctx context.Context, tx pgx.Tx, marketID [32]byte, chainID, blockNumber int64) (int64, error) {
	// Check if market already exists
	existing, err := s.morphoRepo.GetMarketByMarketID(ctx, chainID, common.Hash(marketID))
	if err != nil {
		return 0, fmt.Errorf("checking market existence: %w", err)
	}
	if existing != nil {
		return existing.ID, nil
	}

	// Market doesn't exist yet, fetch params from chain and create it
	params, err := s.blockchainSvc.getMarketParams(ctx, marketID, blockNumber)
	if err != nil {
		return 0, fmt.Errorf("fetching market params: %w", err)
	}

	// Fetch both token metadata in a single RPC call.
	loanMd, collMd, err := s.blockchainSvc.getTokenPairMetadata(ctx, params.LoanToken, params.CollateralToken, blockNumber)
	if err != nil {
		return 0, fmt.Errorf("getting token pair metadata: %w", err)
	}

	protocolID, err := s.protocolRepo.GetOrCreateProtocol(ctx, tx, chainID, MorphoBlueAddress, "Morpho Blue", "lending", s.deployBlock)
	if err != nil {
		return 0, fmt.Errorf("getting protocol: %w", err)
	}

	loanTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, params.LoanToken, loanMd.Symbol, loanMd.Decimals, &blockNumber)
	if err != nil {
		return 0, fmt.Errorf("getting loan token: %w", err)
	}

	collTokenID, err := s.tokenRepo.GetOrCreateToken(ctx, tx, chainID, params.CollateralToken, collMd.Symbol, collMd.Decimals, &blockNumber)
	if err != nil {
		return 0, fmt.Errorf("getting collateral token: %w", err)
	}

	market, err := entity.NewMorphoMarket(chainID, protocolID, common.Hash(marketID), loanTokenID, collTokenID, params.Oracle, params.Irm, params.LLTV, blockNumber)
	if err != nil {
		return 0, fmt.Errorf("creating market entity: %w", err)
	}

	return s.morphoRepo.GetOrCreateMarket(ctx, tx, market)
}

// Contract for the four snapshot helpers — saveMarketStateSnapshot /
// savePositionSnapshot here, saveVaultStateSnapshotInTx / saveVaultPositionInTx
// on the vault side. Read this before adding a new event handler or batching
// existing ones into a shared transaction.
//
// Each event handler today opens its own WithTransaction scope and touches at
// most one market/vault. That bounds per-tx lock acquisition to:
//   - 0 or 1 mss + 0..N mmp at a single market_id (handlePositionEvent,
//     handleLiquidateEvent), or
//   - 0 or 1 mvs + 0..N mvp at a single vault_id (handleVaultTransfer,
//     saveVaultEventSnapshot).
//
// Two invariants prevent deadlocks under cross-build contention:
//
//  1. STATE-FIRST: every handler that writes mmp MUST first write mss for
//     the same (market_id, block_number, block_version, timestamp), and
//     likewise mvs before mvp. The state lock then serialises every other
//     concurrent tx on the same market/vault, so the trailing per-user mmp
//     /mvp locks can never be held by two txs at once for that (market, …)
//     tuple.
//
//  2. SORTED-USERS: handlers that write more than one mmp/mvp in a single tx
//     (handleLiquidateEvent: borrower + liquidator; handleVaultTransfer:
//     sender + receiver) sort their per-user saves by user address before
//     iterating. That's defence-in-depth: invariant 1 already prevents
//     deadlock today, but the sort survives a future refactor that loosens
//     it (e.g. event batching across markets in one tx, or removal of the
//     state save).
//
// If you add a handler that writes mmp/mvp without first writing mss/mvs for
// the same key, OR that batches multiple markets/vaults into a shared tx,
// you MUST extend the sort to cover the per-tx lock acquisition order across
// all keys.
//
// See ADR-0002 §3 and VEC-194 PR.

func (s *Service) saveMarketStateSnapshot(ctx context.Context, tx pgx.Tx, morphoMarketID, blockNumber int64, blockVersion int, blockTimestamp time.Time, ms *MarketState, accrueData *accrueInterestData) error {
	state, err := entity.NewMorphoMarketState(morphoMarketID, blockNumber, blockVersion, blockTimestamp, ms.TotalSupplyAssets, ms.TotalSupplyShares, ms.TotalBorrowAssets, ms.TotalBorrowShares, ms.LastUpdate.Int64(), ms.Fee)
	if err != nil {
		return fmt.Errorf("creating market state entity: %w", err)
	}

	if accrueData != nil {
		state.WithAccrueInterest(accrueData.PrevBorrowRate, accrueData.Interest, accrueData.FeeShares)
	}

	return s.morphoRepo.SaveMarketState(ctx, tx, state)
}

func (s *Service) savePositionSnapshot(ctx context.Context, tx pgx.Tx, user common.Address, morphoMarketID, blockNumber int64, blockVersion int, blockTimestamp time.Time, ps *PositionState, ms *MarketState, eventType entity.MorphoEventType, txHash string, chainID int64) error {
	userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
		ChainID:        chainID,
		Address:        user,
		FirstSeenBlock: &blockNumber,
	})
	if err != nil {
		return fmt.Errorf("ensuring user: %w", err)
	}

	supplyAssets := entity.ComputeSupplyAssets(ps.SupplyShares, ms.TotalSupplyAssets, ms.TotalSupplyShares)
	borrowAssets := entity.ComputeBorrowAssets(ps.BorrowShares, ms.TotalBorrowAssets, ms.TotalBorrowShares)

	position, err := entity.NewMorphoMarketPosition(userID, morphoMarketID, blockNumber, blockVersion, blockTimestamp, ps.SupplyShares, ps.BorrowShares, ps.Collateral, supplyAssets, borrowAssets)
	if err != nil {
		return fmt.Errorf("creating position entity: %w", err)
	}

	return s.morphoRepo.SaveMarketPosition(ctx, tx, position)
}

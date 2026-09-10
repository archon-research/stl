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

// This file holds the MetaMorpho share-accounting handlers: deposit, withdraw,
// transfer, and interest accrual, plus the vault-state and user-position
// snapshots they persist. Every vault flavour (V1 / V1.1 / V2) emits them.

// userVaultBalance is the vault analogue of userPosition.
type userVaultBalance struct {
	user    common.Address
	balance *big.Int
	role    string
}

// handleVaultTransfer handles vault Transfer events.
func (s *Service) handleVaultTransfer(ctx context.Context, e *VaultTransferEvent, vaultAddress common.Address, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	vault := s.vaultRegistry.GetVault(vaultAddress)
	if vault == nil {
		return fmt.Errorf("vault not found in registry: %s", vaultAddress.Hex())
	}

	// Filter out mint/burn (zero address) and internal vault accounting (vault address).
	// Mints and burns are already covered by Deposit/Withdraw handlers.
	hasFrom := e.From != (common.Address{}) && e.From != vaultAddress
	hasTo := e.To != (common.Address{}) && e.To != vaultAddress

	// Fetch vault state + both balances in a single RPC call when both addresses are present.
	var vs *VaultState
	var senderBalance, receiverBalance *big.Int
	var err error

	switch {
	case hasFrom && hasTo:
		vs, senderBalance, receiverBalance, err = s.blockchainSvc.getVaultStateAndTwoBalances(ctx, vaultAddress, e.From, e.To, blockHash)
	case hasFrom:
		vs, senderBalance, err = s.blockchainSvc.getVaultStateAndBalance(ctx, vaultAddress, e.From, blockHash)
	case hasTo:
		vs, receiverBalance, err = s.blockchainSvc.getVaultStateAndBalance(ctx, vaultAddress, e.To, blockHash)
	default:
		vs, err = s.blockchainSvc.getVaultState(ctx, vaultAddress, blockHash)
	}
	if err != nil {
		return fmt.Errorf("fetching vault state and balances for vault=%s from=%s to=%s block=%d: %w",
			vaultAddress.Hex(), e.From.Hex(), e.To.Hex(), blockNumber, err)
	}

	// Save sender and receiver vault positions in user-address order so the
	// per-row mvp advisory locks are acquired in a transaction-stable order;
	// same defense-in-depth rationale as handleLiquidateEvent. See ADR-0002 §3.
	balances := make([]userVaultBalance, 0, 2)
	if hasFrom {
		balances = append(balances, userVaultBalance{e.From, senderBalance, "sender"})
	}
	if hasTo {
		balances = append(balances, userVaultBalance{e.To, receiverBalance, "receiver"})
	}
	slices.SortFunc(balances, func(a, b userVaultBalance) int {
		return bytes.Compare(a.user.Bytes(), b.user.Bytes())
	})

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveVaultStateSnapshotInTx(ctx, tx, vault.ID, blockNumber, blockVersion, blockTimestamp, vs, nil); err != nil {
			return fmt.Errorf("saving vault state: %w", err)
		}

		for _, b := range balances {
			if err := s.saveVaultPositionInTx(ctx, tx, b.user, vault.ID, blockNumber, blockVersion, blockTimestamp, b.balance, vs, e.Type(), e.TxHash(), chainID); err != nil {
				return fmt.Errorf("saving %s position: %w", b.role, err)
			}
		}

		return nil
	})
}

// handleVaultAccrueInterest handles vault AccrueInterest events.
func (s *Service) handleVaultAccrueInterest(ctx context.Context, e *VaultAccrueInterestEvent, vaultAddress common.Address, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time) error {
	vault := s.vaultRegistry.GetVault(vaultAddress)
	if vault == nil {
		return fmt.Errorf("vault not found in registry: %s", vaultAddress.Hex())
	}

	vs, err := s.blockchainSvc.getVaultState(ctx, vaultAddress, blockHash)
	if err != nil {
		return fmt.Errorf("fetching vault state: %w", err)
	}

	accrueData := &vaultAccrueData{
		FeeShares:           e.FeeShares,
		NewTotalAssets:      e.NewTotalAssets,
		PreviousTotalAssets: e.PreviousTotalAssets,
		ManagementFeeShares: e.ManagementFeeShares,
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		return s.saveVaultStateSnapshotInTx(ctx, tx, vault.ID, blockNumber, blockVersion, blockTimestamp, vs, accrueData)
	})
}

// saveVaultEventSnapshot handles deposit/withdraw by saving vault state + user position.
func (s *Service) saveVaultEventSnapshot(ctx context.Context, user common.Address, vaultAddress common.Address, chainID, blockNumber int64, blockHash common.Hash, blockVersion int, blockTimestamp time.Time, eventType entity.MorphoEventType, txHash string) error {
	vault := s.vaultRegistry.GetVault(vaultAddress)
	if vault == nil {
		return fmt.Errorf("vault not found in registry: %s", vaultAddress.Hex())
	}

	// Fetch vault state + user balance in a single RPC call.
	vs, balance, err := s.blockchainSvc.getVaultStateAndBalance(ctx, vaultAddress, user, blockHash)
	if err != nil {
		return fmt.Errorf("fetching vault state and balance: %w", err)
	}

	return s.txManager.WithTransaction(ctx, func(tx pgx.Tx) error {
		if err := s.saveVaultStateSnapshotInTx(ctx, tx, vault.ID, blockNumber, blockVersion, blockTimestamp, vs, nil); err != nil {
			return fmt.Errorf("saving vault state: %w", err)
		}
		return s.saveVaultPositionInTx(ctx, tx, user, vault.ID, blockNumber, blockVersion, blockTimestamp, balance, vs, eventType, txHash, chainID)
	})
}

type vaultAccrueData struct {
	FeeShares           *big.Int // V1: single fee, V2: performanceFeeShares
	NewTotalAssets      *big.Int
	PreviousTotalAssets *big.Int // V2 only
	ManagementFeeShares *big.Int // V2 only
}

func (s *Service) saveVaultStateSnapshotInTx(ctx context.Context, tx pgx.Tx, vaultID, blockNumber int64, blockVersion int, blockTimestamp time.Time, vs *VaultState, accrueData *vaultAccrueData) error {
	state, err := entity.NewMorphoVaultState(vaultID, blockNumber, blockVersion, blockTimestamp, vs.TotalAssets, vs.TotalSupply)
	if err != nil {
		return fmt.Errorf("creating vault state entity: %w", err)
	}

	if accrueData != nil {
		state.WithAccrueInterest(accrueData.FeeShares, accrueData.NewTotalAssets, accrueData.PreviousTotalAssets, accrueData.ManagementFeeShares)
	}

	return s.morphoRepo.SaveVaultState(ctx, tx, state)
}

func (s *Service) saveVaultPositionInTx(ctx context.Context, tx pgx.Tx, user common.Address, vaultID, blockNumber int64, blockVersion int, blockTimestamp time.Time, shares *big.Int, vs *VaultState, eventType entity.MorphoEventType, txHash string, chainID int64) error {
	userID, err := s.userRepo.GetOrCreateUser(ctx, tx, entity.User{
		ChainID:        chainID,
		Address:        user,
		FirstSeenBlock: &blockNumber,
	})
	if err != nil {
		return fmt.Errorf("ensuring user: %w", err)
	}

	assets := entity.ComputeVaultAssets(shares, vs.TotalAssets, vs.TotalSupply)

	position, err := entity.NewMorphoVaultPosition(userID, vaultID, blockNumber, blockVersion, blockTimestamp, shares, assets)
	if err != nil {
		return fmt.Errorf("creating vault position entity: %w", err)
	}

	return s.morphoRepo.SaveVaultPosition(ctx, tx, position)
}

package uniswapv4indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// BlockVersionResolver answers which block_version a scanned height was indexed
// under. The hash comes from the log itself, so the answer can be proved to speak
// for the block being decoded rather than for a fork kept past its reorg.
type BlockVersionResolver interface {
	ResolveBlockVersion(ctx context.Context, blockNumber int64, blockHash common.Hash) (int, error)
}

// NFTTransfersFromLogs decodes a window of scanned posm logs into transfer rows.
//
// Unlike the live path, which meets these logs inside a receipt whose block
// coordinates it already knows, a scanned log carries its own: the height and
// the timestamp come from the log itself, which is what makes this backfill need
// no chain read at all. A log the filter should not have returned — another
// contract's, another event's — fails the window rather than being skipped: the
// address filter is the only thing separating a posm transfer from any ERC-20
// one, so a foreign log means the filter or the registry is wrong, and skipping
// it would leave a hole no rerun would look for again.
func NFTTransfersFromLogs(
	ctx context.Context,
	logs []shared.Log,
	positionManager RegisteredPositionManager,
	versions BlockVersionResolver,
) ([]*entity.UniswapV4PositionNFTTransfer, error) {
	ev, err := PositionManagerTransferEvent()
	if err != nil {
		return nil, fmt.Errorf("loading the PositionManager Transfer fragment: %w", err)
	}

	transfers := make([]*entity.UniswapV4PositionNFTTransfer, 0, len(logs))
	for _, log := range logs {
		transfer, err := decodeScannedNFTTransfer(ctx, *ev, log, positionManager, versions)
		if err != nil {
			return nil, err
		}
		transfers = append(transfers, transfer)
	}
	return transfers, nil
}

func decodeScannedNFTTransfer(
	ctx context.Context,
	ev abi.Event,
	log shared.Log,
	positionManager RegisteredPositionManager,
	versions BlockVersionResolver,
) (*entity.UniswapV4PositionNFTTransfer, error) {
	if err := assertScannedTransferSite(ev, log, positionManager.Address); err != nil {
		return nil, err
	}

	blockNumber, err := shared.ParseHexUint(log.BlockNumber)
	if err != nil {
		return nil, fmt.Errorf("parsing block number %q of PositionManager Transfer (tx %s, index %s): %w", log.BlockNumber, log.TransactionHash, log.LogIndex, err)
	}
	blockTimestamp, err := scannedBlockTimestamp(log)
	if err != nil {
		return nil, err
	}
	logIndex, err := shared.ParseHexUint(log.LogIndex)
	if err != nil {
		return nil, fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}

	if !shared.IsHexWord(log.BlockHash) {
		return nil, fmt.Errorf("PositionManager Transfer (tx %s, index %s) has block hash %q, which is not a 32-byte hex word", log.TransactionHash, log.LogIndex, log.BlockHash)
	}
	blockVersion, err := versions.ResolveBlockVersion(ctx, int64(blockNumber), common.HexToHash(log.BlockHash))
	if err != nil {
		return nil, fmt.Errorf("resolving the block version of PositionManager Transfer (tx %s, index %s) at block %d: %w", log.TransactionHash, log.LogIndex, blockNumber, err)
	}

	transfer, err := newNFTTransferRow(ev, log, positionManager.ID, blockCoords{
		number: int64(blockNumber), version: blockVersion, ts: blockTimestamp,
	}, int(logIndex))
	if err != nil {
		return nil, fmt.Errorf("PositionManager Transfer (tx %s, index %s): %w", log.TransactionHash, log.LogIndex, err)
	}
	return transfer, nil
}

// Alchemy returns a timestamp per log where the JSON-RPC spec carries none, which
// is why no header read is needed. A missing one is refused: 1970 lands the row
// outside the band every sibling read prunes chunks with.
func scannedBlockTimestamp(log shared.Log) (time.Time, error) {
	if log.BlockTimestamp == "" {
		return time.Time{}, fmt.Errorf("PositionManager Transfer (tx %s, index %s) carries no blockTimestamp: the provider must return it on eth_getLogs, or this scan needs a per-block header read", log.TransactionHash, log.LogIndex)
	}
	seconds, err := shared.ParseHexUint(log.BlockTimestamp)
	if err != nil {
		return time.Time{}, fmt.Errorf("parsing blockTimestamp %q of PositionManager Transfer (tx %s, index %s): %w", log.BlockTimestamp, log.TransactionHash, log.LogIndex, err)
	}
	if seconds == 0 {
		return time.Time{}, fmt.Errorf("PositionManager Transfer (tx %s, index %s) has blockTimestamp 0", log.TransactionHash, log.LogIndex)
	}
	return time.Unix(int64(seconds), 0).UTC(), nil
}

func assertScannedTransferSite(ev abi.Event, log shared.Log, positionManager common.Address) error {
	if !common.IsHexAddress(log.Address) {
		return fmt.Errorf("scanned log (index %s) has invalid address %q", log.LogIndex, log.Address)
	}
	if addr := common.HexToAddress(log.Address); !shared.LogBelongsTo(addr, positionManager) {
		return fmt.Errorf("scanned log (tx %s, index %s) was emitted by %s, not the PositionManager %s", log.TransactionHash, log.LogIndex, addr, positionManager)
	}
	if err := assertHexWords(log); err != nil {
		return err
	}
	if len(log.Topics) == 0 || common.HexToHash(log.Topics[0]) != ev.ID {
		return fmt.Errorf("scanned log (tx %s, index %s) topic0 is not %s", log.TransactionHash, log.LogIndex, ev.Name)
	}
	if len(log.Topics) != erc721TransferTopics {
		return fmt.Errorf("scanned PositionManager Transfer (tx %s, index %s) carries %d topics, want %d: an ERC-20 Transfer shares this topic0, so the filter's address must be wrong",
			log.TransactionHash, log.LogIndex, len(log.Topics), erc721TransferTopics)
	}
	// A scan bounded below the reorg window is answered from the canonical chain, so a removed log
	// means the provider is serving a fork, and the resolver would prove its version against it.
	if log.Removed {
		return fmt.Errorf("scanned PositionManager Transfer (tx %s, index %s) is flagged removed: the provider answered from a fork below the finality depth", log.TransactionHash, log.LogIndex)
	}
	return nil
}

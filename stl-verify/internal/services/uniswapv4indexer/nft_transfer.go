package uniswapv4indexer

import (
	"fmt"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// ERC-20's Transfer has the same topic0 with three topics, so arity is the only
// discriminator left once a log has passed the address filter.
const erc721TransferTopics = 4

// The posm's other events (Approval, subscriptions) are skipped, not an error:
// only Transfer names a holder.
func (d *receiptDecoder) decodePositionManagerLog(log shared.Log) error {
	if err := assertHexWords(log); err != nil {
		return err
	}
	ev, err := PositionManagerTransferEvent()
	if err != nil {
		return fmt.Errorf("loading the PositionManager Transfer fragment: %w", err)
	}
	if len(log.Topics) == 0 || common.HexToHash(log.Topics[0]) != ev.ID {
		return nil
	}
	if len(log.Topics) != erc721TransferTopics {
		return fmt.Errorf("PositionManager Transfer log (index %s) carries %d topics, want %d: an ERC-20 Transfer shares this topic0, so the address must be wrong",
			log.LogIndex, len(log.Topics), erc721TransferTopics)
	}

	logIndex, err := shared.ParseHexUint(log.LogIndex)
	if err != nil {
		return fmt.Errorf("parsing log index %q: %w", log.LogIndex, err)
	}
	transfer, err := d.buildNFTTransfer(*ev, log, int(logIndex))
	if err != nil {
		return err
	}
	d.out.NFTTransfers = append(d.out.NFTTransfers, transfer)
	return nil
}

func (d *receiptDecoder) buildNFTTransfer(ev abi.Event, log shared.Log, logIndex int) (*entity.UniswapV4PositionNFTTransfer, error) {
	return newNFTTransferRow(ev, log, d.positionManager.ID, blockCoords{
		number: d.blockNumber, version: d.version, ts: d.ts,
	}, logIndex)
}

// newNFTTransferRow turns one Transfer log into a row. Shared by the live
// indexer and the historical scan so a change to the field names, the entity or
// the validation cannot reach one path without the other; coords is the only
// thing they disagree about, the live path taking it from the receipt's block and
// the scan from the log itself. coords.hash goes unread here.
func newNFTTransferRow(
	ev abi.Event,
	log shared.Log,
	positionManagerID int64,
	coords blockCoords,
	logIndex int,
) (*entity.UniswapV4PositionNFTTransfer, error) {
	data, err := shared.DecodeLog(ev, log)
	if err != nil {
		return nil, fmt.Errorf("decoding PositionManager Transfer log (index %s): %w", log.LogIndex, err)
	}
	from, err := shared.GetAddrField(data, "from")
	if err != nil {
		return nil, err
	}
	to, err := shared.GetAddrField(data, "to")
	if err != nil {
		return nil, err
	}
	tokenID, err := shared.GetBigIntField(data, "tokenId")
	if err != nil {
		return nil, err
	}

	transfer := &entity.UniswapV4PositionNFTTransfer{
		PositionManagerID: positionManagerID,
		TokenID:           tokenID,
		BlockNumber:       coords.number,
		BlockVersion:      coords.version,
		BlockTimestamp:    coords.ts,
		TxHash:            common.HexToHash(log.TransactionHash),
		LogIndex:          logIndex,
		From:              from,
		To:                to,
	}
	if err := transfer.Validate(); err != nil {
		return nil, fmt.Errorf("validating PositionManager Transfer: %w", err)
	}
	return transfer, nil
}

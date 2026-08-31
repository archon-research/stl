package uniswapv4indexer

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

const erc721TransferEventName = "Transfer"

// positionManagerTransferEvent is kept apart from eventsByID on purpose: the
// two ABIs share no address and must share no topic0 map. An absent event is an
// error rather than a nil, which would silently drop every transfer.
var positionManagerTransferEvent = sync.OnceValues(func() (*abi.Event, error) {
	positionManagerABI, err := PositionManagerABI()
	if err != nil {
		return nil, err
	}
	ev, ok := positionManagerABI.Events[erc721TransferEventName]
	if !ok {
		return nil, fmt.Errorf("the PositionManager ABI does not define %s", erc721TransferEventName)
	}
	return &ev, nil
})

// ERC-20's Transfer hashes to the same topic0 with only three topics, so the
// arity is the sole discriminator once a log has passed the address filter.
const erc721TransferTopics = 4

// decodePositionManagerLog turns one PositionManager log into a transfer row.
// The contract's other events (Approval, subscriptions) are skipped rather than
// captured: protocol_event is scoped to the PoolManager's protocol_id.
func (d *receiptDecoder) decodePositionManagerLog(log shared.Log) error {
	if err := assertHexWords(log); err != nil {
		return err
	}
	ev, err := positionManagerTransferEvent()
	if err != nil {
		return fmt.Errorf("loading PositionManager ABI: %w", err)
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
		PositionManagerID: d.positionManager.ID,
		TokenID:           tokenID,
		BlockNumber:       d.blockNumber,
		BlockVersion:      d.version,
		BlockTimestamp:    d.ts,
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

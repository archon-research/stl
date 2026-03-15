package transfer_user_discovery

import (
	"math/big"
	"strconv"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	gethhexutil "github.com/ethereum/go-ethereum/common/hexutil"
)

var transferEventTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

var transferAmountArgs abi.Arguments

func init() {
	uint256Type, _ := abi.NewType("uint256", "", nil)
	transferAmountArgs = abi.Arguments{{Type: uint256Type}}
}

type TransferEvent struct {
	TokenAddress    common.Address
	ProtocolAddress common.Address
	From            common.Address
	To              common.Address
	Amount          *big.Int
	TxHash          []byte
	LogIndex        int
	BlockNumber     int64
	BlockVersion    int
}

type TransferExtractor struct {
	protocolByToken map[common.Address]common.Address
}

func NewTransferExtractor(tokens []outbound.TrackedReceiptToken) *TransferExtractor {
	protocolByToken := make(map[common.Address]common.Address, len(tokens))
	for _, token := range tokens {
		protocolByToken[token.ReceiptTokenAddress] = token.ProtocolAddress
	}

	return &TransferExtractor{protocolByToken: protocolByToken}
}

func (e *TransferExtractor) Extract(receipts []shared.TransactionReceipt, blockNumber int64, blockVersion int) []*TransferEvent {
	var events []*TransferEvent

	for _, receipt := range receipts {
		txHash := common.HexToHash(receipt.TransactionHash).Bytes()

		for _, log := range receipt.Logs {
			transferEvent, ok := e.extractTransferLog(log, txHash, blockNumber, blockVersion)
			if !ok {
				continue
			}

			events = append(events, transferEvent)
		}
	}

	return events
}

func (e *TransferExtractor) extractTransferLog(log shared.Log, txHash []byte, blockNumber int64, blockVersion int) (*TransferEvent, bool) {
	if !isTransferLog(log) {
		return nil, false
	}

	tokenAddress := common.HexToAddress(log.Address)
	protocolAddress, ok := e.protocolByToken[tokenAddress]
	if !ok {
		return nil, false
	}

	amount, ok := decodeTransferAmount(log.Data)
	if !ok {
		return nil, false
	}

	logIndex, ok := parseLogIndex(log.LogIndex)
	if !ok {
		return nil, false
	}

	return &TransferEvent{
		TokenAddress:    tokenAddress,
		ProtocolAddress: protocolAddress,
		From:            topicAddress(log.Topics[1]),
		To:              topicAddress(log.Topics[2]),
		Amount:          amount,
		TxHash:          txHash,
		LogIndex:        logIndex,
		BlockNumber:     blockNumber,
		BlockVersion:    blockVersion,
	}, true
}

func isTransferLog(log shared.Log) bool {
	return len(log.Topics) >= 3 && common.HexToHash(log.Topics[0]) == transferEventTopic
}

func parseLogIndex(logIndex string) (int, bool) {
	index, err := strconv.ParseInt(logIndex, 0, 64)
	if err != nil {
		return 0, false
	}

	return int(index), true
}

func topicAddress(topic string) common.Address {
	return common.BytesToAddress(common.HexToHash(topic).Bytes())
}

func decodeTransferAmount(data string) (*big.Int, bool) {
	decoded, err := gethhexutil.Decode(data)
	if err != nil {
		return nil, false
	}

	values, err := transferAmountArgs.Unpack(decoded)
	if err != nil || len(values) != 1 {
		return nil, false
	}

	amount, ok := values[0].(*big.Int)
	if !ok {
		return nil, false
	}

	return amount, true
}

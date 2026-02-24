package allocation_tracker

import (
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

var transferEventTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

var transferAmountArgs abi.Arguments

func init() {
	uint256Type, _ := abi.NewType("uint256", "", nil)
	transferAmountArgs = abi.Arguments{{Type: uint256Type}}
}

type TransferEvent struct {
	TokenAddress common.Address
	From         common.Address
	To           common.Address
	Amount       *big.Int
	Direction    Direction
	Star         string
	ProxyAddress common.Address
	TxHash       string
	LogIndex     int
}

type TransferExtractor struct {
	proxyLookup map[common.Address]ProxyConfig
}

func NewTransferExtractor(proxies []ProxyConfig) *TransferExtractor {
	return &TransferExtractor{
		proxyLookup: BuildProxyLookup(proxies),
	}
}

func (e *TransferExtractor) Extract(receipt TransactionReceipt) []*TransferEvent {
	var events []*TransferEvent

	for _, log := range receipt.Logs {
		if len(log.Topics) < 3 {
			continue
		}
		if log.Topics[0] != transferEventTopic {
			continue
		}

		from := common.BytesToAddress(log.Topics[1].Bytes())
		to := common.BytesToAddress(log.Topics[2].Bytes())

		proxyFrom, fromIsProxy := e.proxyLookup[from]
		proxyTo, toIsProxy := e.proxyLookup[to]

		if !fromIsProxy && !toIsProxy {
			continue
		}

		amount := new(big.Int)
		if len(log.Data) >= 32 {
			values, err := transferAmountArgs.Unpack(log.Data)
			if err == nil && len(values) > 0 {
				if v, ok := values[0].(*big.Int); ok {
					amount = v
				}
			}
		}

		if fromIsProxy {
			events = append(events, &TransferEvent{
				TokenAddress: log.Address,
				From:         from,
				To:           to,
				Amount:       new(big.Int).Set(amount),
				Direction:    DirectionOut,
				Star:         proxyFrom.Star,
				ProxyAddress: proxyFrom.Address,
				TxHash:       log.TxHash.Hex(),
				LogIndex:     int(log.Index),
			})
		}
		if toIsProxy {
			events = append(events, &TransferEvent{
				TokenAddress: log.Address,
				From:         from,
				To:           to,
				Amount:       new(big.Int).Set(amount),
				Direction:    DirectionIn,
				Star:         proxyTo.Star,
				ProxyAddress: proxyTo.Address,
				TxHash:       log.TxHash.Hex(),
				LogIndex:     int(log.Index),
			})
		}
	}

	return events
}

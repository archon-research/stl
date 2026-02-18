package allocation_tracker

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

var transferEventTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

// TransferEvent is a parsed ERC20 Transfer involving a known proxy.
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
	return &TransferExtractor{proxyLookup: BuildProxyLookup(proxies)}
}

func (e *TransferExtractor) Extract(receipt TransactionReceipt) []*TransferEvent {
	var events []*TransferEvent

	for _, log := range receipt.Logs {
		if len(log.Topics) < 3 {
			continue
		}
		if common.HexToHash(log.Topics[0]) != transferEventTopic {
			continue
		}

		from := common.HexToAddress(log.Topics[1])
		to := common.HexToAddress(log.Topics[2])

		proxyFrom, fromIsProxy := e.proxyLookup[from]
		proxyTo, toIsProxy := e.proxyLookup[to]

		if !fromIsProxy && !toIsProxy {
			continue
		}

		amount := new(big.Int)
		if data := common.FromHex(log.Data); len(data) >= 32 {
			amount.SetBytes(data[:32])
		}

		token := common.HexToAddress(log.Address)
		idx := parseHexInt(log.LogIndex)

		if fromIsProxy {
			events = append(events, &TransferEvent{
				TokenAddress: token, From: from, To: to, Amount: new(big.Int).Set(amount),
				Direction: DirectionOut, Star: proxyFrom.Star, ProxyAddress: proxyFrom.Address,
				TxHash: receipt.TransactionHash, LogIndex: idx,
			})
		}
		if toIsProxy {
			events = append(events, &TransferEvent{
				TokenAddress: token, From: from, To: to, Amount: new(big.Int).Set(amount),
				Direction: DirectionIn, Star: proxyTo.Star, ProxyAddress: proxyTo.Address,
				TxHash: receipt.TransactionHash, LogIndex: idx,
			})
		}
	}

	return events
}

func parseHexInt(s string) int {
	s = strings.TrimPrefix(s, "0x")
	if s == "" {
		return 0
	}
	v := new(big.Int)
	v.SetString(s, 16)
	return int(v.Int64())
}

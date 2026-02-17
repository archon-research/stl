package allocation_tracker

import (
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// ERC20 Transfer(address,address,uint256) event signature
var transferEventTopic = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

// TransferExtractor finds ERC20 Transfer events involving known ALM proxy addresses.
type TransferExtractor struct {
	proxyLookup map[common.Address]ProxyConfig
}

func NewTransferExtractor(proxies []ProxyConfig) *TransferExtractor {
	return &TransferExtractor{
		proxyLookup: BuildProxyLookup(proxies),
	}
}

// ExtractFromReceipt scans all logs in a receipt for Transfer events involving a proxy.
func (e *TransferExtractor) ExtractFromReceipt(receipt TransactionReceipt, chainID int64, blockNumber int64, blockVersion int) []*AllocationEvent {
	var events []*AllocationEvent

	for _, log := range receipt.Logs {
		if len(log.Topics) < 3 {
			continue
		}

		topic0 := common.HexToHash(log.Topics[0])
		if topic0 != transferEventTopic {
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
		dataBytes := common.FromHex(log.Data)
		if len(dataBytes) >= 32 {
			amount.SetBytes(dataBytes[:32])
		}

		tokenAddress := common.HexToAddress(log.Address)
		logIndex := parseHexInt(log.LogIndex)

		if fromIsProxy {
			events = append(events, &AllocationEvent{
				ChainID:      chainID,
				BlockNumber:  blockNumber,
				BlockVersion: blockVersion,
				TxHash:       receipt.TransactionHash,
				LogIndex:     logIndex,
				TokenAddress: tokenAddress,
				From:         from,
				To:           to,
				Amount:       new(big.Int).Set(amount),
				Direction:    DirectionOut,
				Star:         proxyFrom.Star,
				ProxyAddress: proxyFrom.Address,
			})
		}

		if toIsProxy {
			events = append(events, &AllocationEvent{
				ChainID:      chainID,
				BlockNumber:  blockNumber,
				BlockVersion: blockVersion,
				TxHash:       receipt.TransactionHash,
				LogIndex:     logIndex,
				TokenAddress: tokenAddress,
				From:         from,
				To:           to,
				Amount:       new(big.Int).Set(amount),
				Direction:    DirectionIn,
				Star:         proxyTo.Star,
				ProxyAddress: proxyTo.Address,
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
	val := new(big.Int)
	val.SetString(s, 16)
	return int(val.Int64())
}

package abis

import (
	"fmt"
	"sync"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
)

// TransferTopic0 is keccak256("Transfer(address,address,uint256)"), the topic0
// of BOTH the ERC-20 and the ERC-721 Transfer log: the signatures are identical
// and indexed flags are not hashed into it. So it never tells which standard —
// or which token — emitted a log, and a decoder that keys on it alone will read
// an ERC-20 transfer as an ERC-721 one. Two things do discriminate: the emitting
// address, and arity, because ERC-721 indexes tokenId (four topics, empty data)
// where ERC-20 carries the value in data (three topics, 32 bytes).
var TransferTopic0 = common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")

const transferEventName = "Transfer"

// tokenId is indexed, which puts it in topics[3] and leaves data empty.
const erc721TransferJSON = `[
	{
		"anonymous": false,
		"inputs": [
			{"indexed": true, "name": "from",    "type": "address"},
			{"indexed": true, "name": "to",      "type": "address"},
			{"indexed": true, "name": "tokenId", "type": "uint256"}
		],
		"name": "Transfer",
		"type": "event"
	}
]`

var erc721TransferEventOnce = sync.OnceValues(func() (*abi.Event, error) {
	parsed, err := ParseABI(erc721TransferJSON)
	if err != nil {
		return nil, fmt.Errorf("parsing the ERC-721 %s fragment: %w", transferEventName, err)
	}
	ev, ok := parsed.Events[transferEventName]
	if !ok {
		return nil, fmt.Errorf("the ERC-721 fragment does not define %s", transferEventName)
	}
	return &ev, nil
})

// ERC721TransferEvent is the one shared ERC-721 Transfer fragment, so the
// indexed flags cannot drift between the services that decode a token id.
func ERC721TransferEvent() (*abi.Event, error) {
	return erc721TransferEventOnce()
}

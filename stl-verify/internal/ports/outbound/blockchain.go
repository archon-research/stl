package outbound

import "context"

// BlockHeader represents an Ethereum block header from eth_newHeads subscription.
type BlockHeader struct {
	Number           string `json:"number"`
	Hash             string `json:"hash"`
	ParentHash       string `json:"parentHash"`
	Nonce            string `json:"nonce"`
	SHA3Uncles       string `json:"sha3Uncles"`
	LogsBloom        string `json:"logsBloom"`
	TransactionsRoot string `json:"transactionsRoot"`
	StateRoot        string `json:"stateRoot"`
	ReceiptsRoot     string `json:"receiptsRoot"`
	Miner            string `json:"miner"`
	Difficulty       string `json:"difficulty"`
	ExtraData        string `json:"extraData"`
	GasLimit         string `json:"gasLimit"`
	GasUsed          string `json:"gasUsed"`
	Timestamp        string `json:"timestamp"`
	BaseFeePerGas    string `json:"baseFeePerGas,omitempty"`
}

// BlockSubscriber defines the interface for subscribing to new blockchain blocks.
// This port is designed for WebSocket-based subscriptions like Alchemy's eth_newHeads.
type BlockSubscriber interface {
	// Subscribe starts listening for new block headers.
	// The returned channel emits BlockHeader events as new blocks are mined.
	// The caller should call Unsubscribe to stop receiving events and clean up resources.
	Subscribe(ctx context.Context) (<-chan BlockHeader, error)

	// Unsubscribe stops the subscription and closes the block header channel.
	// This should be called when the subscriber is no longer needed.
	Unsubscribe() error

	// HealthCheck verifies the connection to the blockchain node is operational.
	HealthCheck(ctx context.Context) error
}

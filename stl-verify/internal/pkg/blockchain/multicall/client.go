package multicall

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type Client struct {
	ethClient *ethclient.Client
	address   common.Address
	abi       *abi.ABI
	telemetry *Telemetry // optional; nil disables recording
}

var _ outbound.Multicaller = (*Client)(nil)

// Option configures a Client at construction, letting callers add optional behaviour such as telemetry without changing NewClient's signature.
type Option func(*Client)

// WithTelemetry attaches metrics recording to the client.
func WithTelemetry(t *Telemetry) Option {
	return func(c *Client) { c.telemetry = t }
}

// NewClient creates a new Multicall3 client.
//
// Multicall3 contract deployment blocks by chain:
//   - Ethereum Mainnet: 14,353,601 (December 2021)
//   - Other chains: varies by deployment
//
// For historical queries before the deployment block, calls will fail with
// "execution reverted" as the contract does not exist at those blocks.
//
// Address: 0xcA11bde05977b3631167028862bE2a173976CA11 (same on all chains)
func NewClient(ethClient *ethclient.Client, multicall3Address common.Address, opts ...Option) (outbound.Multicaller, error) {
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load multicall3 ABI: %w", err)
	}

	c := &Client{
		ethClient: ethClient,
		address:   multicall3Address,
		abi:       multicallABI,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// recordBatch records the batch size when telemetry is attached. nil telemetry
// is a no-op so non-instrumented callers pay nothing.
func (c *Client) recordBatch(ctx context.Context, size int) {
	if c.telemetry != nil {
		c.telemetry.RecordBatch(ctx, size)
	}
}

func (c *Client) Address() common.Address {
	return c.address
}

func (c *Client) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	if blockNumber == nil {
		return nil, fmt.Errorf("block number is required")
	}
	if blockNumber.Sign() < 0 {
		return nil, fmt.Errorf("negative block number: %s", blockNumber)
	}
	if len(calls) == 0 {
		return []outbound.Result{}, nil
	}

	// Records the attempted batch size (before Pack/CallContract can fail), so
	// _count/_sum reflect attempted multicalls, not only successful ones.
	c.recordBatch(ctx, len(calls))

	data, err := c.packAggregate3(calls)
	if err != nil {
		return nil, err
	}

	result, err := c.ethClient.CallContract(ctx, c.callMsg(data), blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to call multicall contract at address=%s block=%s calls=%d: %w",
			c.address.Hex(), blockNumberString(blockNumber), len(calls), err)
	}

	return c.unpackAggregate3(result, blockNumberString(blockNumber))
}

// ExecuteAtHash pins the read to blockHash instead of a block number. See the
// outbound.Multicaller doc comment for why hash-pinning matters for reorg
// correctness.
func (c *Client) ExecuteAtHash(ctx context.Context, calls []outbound.Call, blockHash common.Hash) ([]outbound.Result, error) {
	if blockHash == (common.Hash{}) {
		return nil, fmt.Errorf("block hash is required")
	}
	if len(calls) == 0 {
		return []outbound.Result{}, nil
	}

	c.recordBatch(ctx, len(calls))

	data, err := c.packAggregate3(calls)
	if err != nil {
		return nil, err
	}

	result, err := c.ethClient.CallContractAtHash(ctx, c.callMsg(data), blockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to call multicall contract at address=%s blockHash=%s calls=%d: %w",
			c.address.Hex(), blockHash.Hex(), len(calls), err)
	}

	return c.unpackAggregate3(result, blockHash.Hex())
}

func (c *Client) callMsg(data []byte) ethereum.CallMsg {
	return ethereum.CallMsg{
		To:   &c.address,
		Data: data,
	}
}

func (c *Client) packAggregate3(calls []outbound.Call) ([]byte, error) {
	data, err := c.abi.Pack("aggregate3", calls)
	if err != nil {
		return nil, fmt.Errorf("failed to pack multicall: %w", err)
	}
	return data, nil
}

func (c *Client) unpackAggregate3(result []byte, blockDesc string) ([]outbound.Result, error) {
	unpacked, err := c.abi.Unpack("aggregate3", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack multicall response at block=%s: %w", blockDesc, err)
	}

	resultsRaw := unpacked[0].([]struct {
		Success    bool   `json:"success"`
		ReturnData []byte `json:"returnData"`
	})

	results := make([]outbound.Result, len(resultsRaw))
	for i, r := range resultsRaw {
		results[i] = outbound.Result{
			Success:    r.Success,
			ReturnData: r.ReturnData,
		}
	}

	return results, nil
}

func blockNumberString(blockNumber *big.Int) string {
	if blockNumber == nil {
		return "latest"
	}
	return blockNumber.String()
}

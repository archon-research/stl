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
)

type Multicaller interface {
	Execute(ctx context.Context, calls []Call, blockNumber *big.Int) ([]Result, error)
	Address() common.Address
}

type Client struct {
	ethClient *ethclient.Client
	address   common.Address
	abi       *abi.ABI
}

func NewClient(ethClient *ethclient.Client, multicall3Address common.Address) (Multicaller, error) {
	multicallABI, err := abis.GetMulticall3ABI()
	if err != nil {
		return nil, fmt.Errorf("failed to load multicall3 ABI: %w", err)
	}

	return &Client{
		ethClient: ethClient,
		address:   multicall3Address,
		abi:       multicallABI,
	}, nil
}

func (c *Client) Address() common.Address {
	return c.address
}

func (c *Client) Execute(ctx context.Context, calls []Call, blockNumber *big.Int) ([]Result, error) {
	if len(calls) == 0 {
		return []Result{}, nil
	}

	data, err := c.abi.Pack("aggregate3", calls)
	if err != nil {
		return nil, fmt.Errorf("failed to pack multicall: %w", err)
	}

	msg := ethereum.CallMsg{
		To:   &c.address,
		Data: data,
	}

	result, err := c.ethClient.CallContract(ctx, msg, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to call multicall contract at address=%s block=%s calls=%d: %w",
			c.address.Hex(), blockNumberString(blockNumber), len(calls), err)
	}

	unpacked, err := c.abi.Unpack("aggregate3", result)
	if err != nil {
		return nil, fmt.Errorf("failed to unpack multicall response at block=%s: %w",
			blockNumberString(blockNumber), err)
	}

	resultsRaw := unpacked[0].([]struct {
		Success    bool   `json:"success"`
		ReturnData []byte `json:"returnData"`
	})

	results := make([]Result, len(resultsRaw))
	for i, r := range resultsRaw {
		results[i] = Result{
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

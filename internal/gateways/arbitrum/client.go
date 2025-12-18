package arbitrum

import "fmt"

type Client struct {
	url string
}

func NewClient(url string) *Client {
	return &Client{url: url}
}

func (c *Client) GetLatestBlock() (int64, error) {
	fmt.Println("Fetching latest block from Arbitrum...")
	return 1000, nil
}

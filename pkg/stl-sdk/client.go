package stlsdk

// Client is the main entry point for the SDK
type Client struct {
	APIKey string
}

func NewClient(apiKey string) *Client {
	return &Client{APIKey: apiKey}
}

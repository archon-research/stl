package etherscan

// proxyResponse represents the generic response from Etherscan's proxy module.
// Example response:
//
//	{
//	  "jsonrpc": "2.0",
//	  "id": 1,
//	  "result": {...}
//	}
type proxyResponse struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Result  any    `json:"result"`
	Error   *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// blockResponse represents the result from eth_getBlockByNumber/eth_getBlockByHash.
// Example response:
//
//	{
//	  "number": "0x1400000",
//	  "hash": "0x...",
//	  "timestamp": "0x65a1234"
//	}
type blockResponse struct {
	Number    string `json:"number"`
	Hash      string `json:"hash"`
	Timestamp string `json:"timestamp"`
}

// etherscanError represents an error response from the Etherscan API.
// Example response:
//
//	{
//	  "status": "0",
//	  "message": "NOTOK",
//	  "result": "Invalid API Key"
//	}
type etherscanError struct {
	Status  string `json:"status"`
	Message string `json:"message"`
	Result  string `json:"result"`
}

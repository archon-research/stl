package main

import "math/big"

// FailedTransaction represents a failed transaction that attempted to interact with SparkLend
type FailedTransaction struct {
	BlockNumber     uint64 `json:"block_number"`
	BlockTimestamp  uint64 `json:"block_timestamp"`
	TransactionHash string `json:"transaction_hash"`
	FromAddress     string `json:"from_address"`
	ToAddress       string `json:"to_address"`
	FunctionName    string `json:"function_name"`
	FunctionArgs    string `json:"function_args"` // JSON encoded
	GasUsed         string `json:"gas_used"`
	RevertReason    string `json:"revert_reason"`
}

// SyncState represents the checkpoint for resuming scans
type SyncState struct {
	LastProcessedBlock uint64
	UpdatedAt          int64
}

// SparkLendFunctions defines the function selectors we're interested in
var SparkLendFunctions = map[string]string{
	"0x617ba037": "supply",          // supply(address,uint256,address,uint16)
	"0xa415bcad": "borrow",          // borrow(address,uint256,uint256,uint16,address)
	"0x573ade81": "repay",           // repay(address,uint256,uint256,address)
	"0x69328dec": "withdraw",        // withdraw(address,uint256,address)
	"0x00a718a9": "liquidationCall", // liquidationCall(address,address,address,uint256,bool)
}

// FunctionABI defines the ABI for each SparkLend function for decoding
type FunctionABI struct {
	Name   string
	Inputs []ABIInput
}

type ABIInput struct {
	Name string
	Type string
}

// SparkLendABIs contains the ABI definitions for decoding function arguments
var SparkLendABIs = map[string]FunctionABI{
	"supply": {
		Name: "supply",
		Inputs: []ABIInput{
			{Name: "asset", Type: "address"},
			{Name: "amount", Type: "uint256"},
			{Name: "onBehalfOf", Type: "address"},
			{Name: "referralCode", Type: "uint16"},
		},
	},
	"borrow": {
		Name: "borrow",
		Inputs: []ABIInput{
			{Name: "asset", Type: "address"},
			{Name: "amount", Type: "uint256"},
			{Name: "interestRateMode", Type: "uint256"},
			{Name: "referralCode", Type: "uint16"},
			{Name: "onBehalfOf", Type: "address"},
		},
	},
	"repay": {
		Name: "repay",
		Inputs: []ABIInput{
			{Name: "asset", Type: "address"},
			{Name: "amount", Type: "uint256"},
			{Name: "interestRateMode", Type: "uint256"},
			{Name: "onBehalfOf", Type: "address"},
		},
	},
	"withdraw": {
		Name: "withdraw",
		Inputs: []ABIInput{
			{Name: "asset", Type: "address"},
			{Name: "amount", Type: "uint256"},
			{Name: "to", Type: "address"},
		},
	},
	"liquidationCall": {
		Name: "liquidationCall",
		Inputs: []ABIInput{
			{Name: "collateralAsset", Type: "address"},
			{Name: "debtAsset", Type: "address"},
			{Name: "user", Type: "address"},
			{Name: "debtToCover", Type: "uint256"},
			{Name: "receiveAToken", Type: "bool"},
		},
	},
}

// DecodedArgs holds the decoded function arguments
type DecodedArgs struct {
	Asset            string   `json:"asset,omitempty"`
	Amount           *big.Int `json:"amount,omitempty"`
	OnBehalfOf       string   `json:"onBehalfOf,omitempty"`
	ReferralCode     uint16   `json:"referralCode,omitempty"`
	InterestRateMode *big.Int `json:"interestRateMode,omitempty"`
	To               string   `json:"to,omitempty"`
	CollateralAsset  string   `json:"collateralAsset,omitempty"`
	DebtAsset        string   `json:"debtAsset,omitempty"`
	User             string   `json:"user,omitempty"`
	DebtToCover      *big.Int `json:"debtToCover,omitempty"`
	ReceiveAToken    bool     `json:"receiveAToken,omitempty"`
}

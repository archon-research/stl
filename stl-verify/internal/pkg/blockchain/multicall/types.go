package multicall

import "github.com/ethereum/go-ethereum/common"

type Call struct {
	Target       common.Address
	AllowFailure bool
	CallData     []byte
}

type Result struct {
	Success    bool
	ReturnData []byte
}

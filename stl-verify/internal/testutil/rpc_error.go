package testutil

// RPCError is a JSON-RPC error double implementing go-ethereum's rpc.Error and
// rpc.DataError, so the same value drives every classifier in rpcerr. It is a
// comparable value: errors.Is finds it through any %w chain.
type RPCError struct {
	Code int
	Msg  string
	Data string
}

func (e RPCError) Error() string  { return e.Msg }
func (e RPCError) ErrorCode() int { return e.Code }
func (e RPCError) ErrorData() any {
	if e.Data == "" {
		return nil
	}
	return e.Data
}

// GasExhaustedRPCError is the node's answer to an eth_call that ran past its
// gas cap, in the wording Alchemy uses for mainnet.
func GasExhaustedRPCError() RPCError {
	return RPCError{Code: -32000, Msg: "out of gas: gas required exceeds: 550000000"}
}

// ThrottledRPCError is Alchemy's compute-unit throttle: a transport fault that
// no classifier may read as a contract answer.
func ThrottledRPCError() RPCError {
	return RPCError{Code: 429, Msg: "Your app has exceeded its compute units per second capacity"}
}

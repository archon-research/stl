package alchemy

import "errors"

// ErrUpstreamNullResult signals that the JSON-RPC endpoint returned a literal
// `null` result for a request that, in normal operation, is expected to carry
// a body (block, receipts, traces, blobs). On Alchemy this typically appears
// during a propagation race: the new-head WebSocket announces a hash before
// the corresponding body has propagated through every HTTP RPC backend, so
// `eth_getBlockByHash` for that hash transiently resolves to `null` even
// though the chain has actually produced the block. The retry loop and the
// adapter-level retry policy treat this as a recoverable error so the next
// fetch — once propagation completes — gets the real payload.
var ErrUpstreamNullResult = errors.New("upstream returned null result")

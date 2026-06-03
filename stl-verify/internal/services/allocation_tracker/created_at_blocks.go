package allocation_tracker

import "github.com/ethereum/go-ethereum/common"

// createdAtBlockKey identifies a tracked position contract on a specific chain.
type createdAtBlockKey struct {
	Chain    string
	Contract common.Address
}

// knownCreatedAtBlocks records the on-chain block at which each tracked position
// contract was created, keyed by (chain, contract).
//
// These are chain-observed values owned by the allocation tracker. They were
// previously hard-coded inline on each entry in this file, before entries were
// sourced from the axis-synome contract; the axis-synome contract intentionally
// does not carry created_at_block, because it is observed on-chain rather than
// sourced from the Sky Atlas. Positions absent from this map fall back to the
// observation block in the prime-position handler (self-correcting downward via
// the token upsert's LEAST(existing, new)).
var knownCreatedAtBlocks = map[createdAtBlockKey]int64{
	{Chain: "mainnet", Contract: common.HexToAddress("0x779224df1c756b4edd899854f32a53e8c2b2ce5d")}:     23118264,
	{Chain: "mainnet", Contract: common.HexToAddress("0x56a76b428244a50513ec81e225a293d128fd581d")}:     23319630,
	{Chain: "mainnet", Contract: common.HexToAddress("0xe41a0583334f0dc4e023acd0bfef3667f6fe0597")}:     22932160,
	{Chain: "mainnet", Contract: common.HexToAddress("0x38464507e02c983f20428a6e8566693fe9e422a9")}:     23896879,
	{Chain: "mainnet", Contract: common.HexToAddress("0xa632d59b9b804a956bfaa9b48af3a1b74808fc1f")}:     23390887,
	{Chain: "mainnet", Contract: common.HexToAddress("0x49506c3aa028693458d6ee816b2ec28522946872")}:     24019368,
	{Chain: "mainnet", Contract: common.HexToAddress("0x6c3ea9036406852006290770bedfcaba0e23a0e8")}:     23118264,
	{Chain: "mainnet", Contract: common.HexToAddress("0xfa82580c16a31d0c1bc632a36f82e83efef3eec0")}:     22319334,
	{Chain: "mainnet", Contract: common.HexToAddress("0xe3190143eb552456f88464662f0c0c4ac67a77eb")}:     23132230,
	{Chain: "mainnet", Contract: common.HexToAddress("0x68215b6533c47ff9f7125ac95adf00fe4a62f79e")}:     23132230,
	{Chain: "mainnet", Contract: common.HexToAddress("0x51c2d74017390cbbd30550179a16a1c28f7210fc")}:     24041058,
	{Chain: "mainnet", Contract: common.HexToAddress("0xbeef2b5fd3d94469b7782aebe6364e6e6fb1b709")}:     24148141,
	{Chain: "avalanche-c", Contract: common.HexToAddress("0x28b3a8fb53b741a8fd78c0fb9a6b2393d896a43d")}: 69983672,
	{Chain: "avalanche-c", Contract: common.HexToAddress("0x2c0adff8e114f3ca106051144353ac703d24b901")}: 72633046,
	{Chain: "plume", Contract: common.HexToAddress("0xa5d465251fbcc907f5dd6bb2145488dfc6a2627b")}:       41311106,
	{Chain: "monad", Contract: common.HexToAddress("0x00000000efe302beaa2b3e6e1b18d08d69a9012a")}:       36870131,
	{Chain: "monad", Contract: common.HexToAddress("0x6b405dca74897c9442d369dcf6c0ec230f7e1c7c")}:       39598901,
}

// lookupCreatedAtBlock returns the known on-chain creation block for a position
// contract on a chain, or nil when it is not recorded (the handler then falls
// back to the observation block).
func lookupCreatedAtBlock(chain string, contract common.Address) *int64 {
	block, ok := knownCreatedAtBlocks[createdAtBlockKey{Chain: chain, Contract: contract}]
	if !ok {
		return nil
	}

	value := block
	return &value
}

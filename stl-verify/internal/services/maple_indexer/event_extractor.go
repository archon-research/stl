package maple_indexer

import (
	"fmt"
	"log/slog"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// EventExtractor recognises Syrup ERC-4626 vault events
// (Deposit, Withdraw, Transfer) and extracts the user addresses that need a
// per-block position refresh as a result of each log.
//
// Unlike morpho_indexer.EventExtractor, this extractor does not decode the
// full event payload — the service only needs to know which user addresses
// were touched at a given block so it can fetch their on-chain balanceOf via
// multicall. ABI args (assets/shares/value) are not consumed, so we don't
// pay the cost of parsing them.
type EventExtractor struct {
	abi        *abi.ABI
	logger     *slog.Logger
	depositID  common.Hash
	withdrawID common.Hash
	transferID common.Hash
}

// NewEventExtractor loads the Syrup events ABI and caches the three
// topic-hashes the extractor recognises.
func NewEventExtractor(logger *slog.Logger) (*EventExtractor, error) {
	if logger == nil {
		logger = slog.Default()
	}
	a, err := abis.GetSyrupVaultEventsABI()
	if err != nil {
		return nil, fmt.Errorf("loading Syrup events ABI: %w", err)
	}
	idFor := func(name string) (common.Hash, error) {
		ev, ok := a.Events[name]
		if !ok {
			return common.Hash{}, fmt.Errorf("event %s missing from Syrup ABI", name)
		}
		return ev.ID, nil
	}
	d, err := idFor("Deposit")
	if err != nil {
		return nil, err
	}
	w, err := idFor("Withdraw")
	if err != nil {
		return nil, err
	}
	t, err := idFor("Transfer")
	if err != nil {
		return nil, err
	}
	return &EventExtractor{abi: a, logger: logger, depositID: d, withdrawID: w, transferID: t}, nil
}

// DepositTopic returns the keccak topic hash for the Syrup Deposit event.
func (e *EventExtractor) DepositTopic() common.Hash { return e.depositID }

// WithdrawTopic returns the keccak topic hash for the Syrup Withdraw event.
func (e *EventExtractor) WithdrawTopic() common.Hash { return e.withdrawID }

// TransferTopic returns the keccak topic hash for the Syrup Transfer event.
func (e *EventExtractor) TransferTopic() common.Hash { return e.transferID }

// IsRelevant returns true if the log was emitted by the given vault address
// AND has a recognised Syrup topic (Deposit/Withdraw/Transfer).
func (e *EventExtractor) IsRelevant(vault common.Address, log shared.Log) bool {
	if !addressMatches(log.Address, vault) {
		return false
	}
	if len(log.Topics) == 0 {
		return false
	}
	topic := common.HexToHash(log.Topics[0])
	switch topic {
	case e.depositID, e.withdrawID, e.transferID:
		return true
	}
	return false
}

// ExtractTouchedAddresses returns the set of user addresses that need a
// position refresh as a result of the given logs for `vault`, plus a bool
// indicating whether any log was relevant at all.
//
// Topic layouts:
//
//   - Deposit:  topics = [sig, sender, owner]
//     Refresh both — sender funds, owner receives shares.
//   - Withdraw: topics = [sig, sender, receiver, owner]
//     Refresh owner (shares burned) and receiver (assets recipient — usually
//     same as owner, but kept distinct for completeness).
//   - Transfer: topics = [sig, from, to]
//     Refresh both. The zero address is filtered out because mint/burn
//     pseudo-transfers have one side at 0x0; the *real* user side is the
//     non-zero address.
//
// The returned map deduplicates addresses across logs, so multiple events
// touching the same user in the same block produce a single balanceOf call.
func (e *EventExtractor) ExtractTouchedAddresses(
	vault common.Address,
	logs []shared.Log,
) (map[common.Address]struct{}, bool) {
	users := make(map[common.Address]struct{})
	relevant := false
	for _, log := range logs {
		if !e.IsRelevant(vault, log) {
			continue
		}
		relevant = true
		topic := common.HexToHash(log.Topics[0])
		switch topic {
		case e.depositID:
			if len(log.Topics) >= 3 {
				addUser(users, log.Topics[1])
				addUser(users, log.Topics[2])
			} else {
				e.logger.Warn("malformed Deposit log: expected 3 topics", "got", len(log.Topics), "tx", log.TransactionHash)
			}
		case e.withdrawID:
			if len(log.Topics) >= 4 {
				// receiver and owner — the actual position holders.
				// sender (Topics[1]) is the msg.sender, which on
				// router-style withdrawals is a contract, not a user.
				addUser(users, log.Topics[2])
				addUser(users, log.Topics[3])
			} else {
				e.logger.Warn("malformed Withdraw log: expected 4 topics", "got", len(log.Topics), "tx", log.TransactionHash)
			}
		case e.transferID:
			if len(log.Topics) >= 3 {
				addUser(users, log.Topics[1])
				addUser(users, log.Topics[2])
			} else {
				e.logger.Warn("malformed Transfer log: expected 3 topics", "got", len(log.Topics), "tx", log.TransactionHash)
			}
		}
	}
	return users, relevant
}

// addUser inserts a topic-encoded address into users, skipping the zero
// address (used as the counter-party for mint/burn pseudo-transfers).
func addUser(users map[common.Address]struct{}, topicHex string) {
	addr := common.BytesToAddress(common.HexToHash(topicHex).Bytes())
	if addr == (common.Address{}) {
		return
	}
	users[addr] = struct{}{}
}

// addressMatches compares a hex-encoded log address (possibly without 0x or
// with mixed case) against a canonical common.Address.
func addressMatches(logAddress string, vault common.Address) bool {
	// HexToAddress silently coerces malformed input (truncating/padding), which
	// could falsely match the vault. Reject anything that is not a valid hex
	// address first.
	if !common.IsHexAddress(logAddress) {
		return false
	}
	return common.HexToAddress(logAddress) == vault
}

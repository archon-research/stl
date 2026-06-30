package curveindexer

import (
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// appendRawCaptured appends a capture-net entry holding the raw {topics, data} of
// a log. Used for logs that are not ABI-decoded into a typed payload (unknown
// topic0, zero-topic logs, and word-sliced fixed-array liquidity events), so
// protocol_event stays a complete mirror of the on-chain log surface. eventName
// is the log's topic0 hex (or "" for a zero-topic log).
func appendRawCaptured(
	captured []CapturedEvent,
	pool RegisteredPool,
	logIndex uint,
	txHash common.Hash,
	eventName string,
	log shared.Log,
) ([]CapturedEvent, error) {
	payload, err := json.Marshal(map[string]any{"topics": log.Topics, "data": log.Data})
	if err != nil {
		return nil, fmt.Errorf("marshalling captured event payload (log index %s): %w", log.LogIndex, err)
	}
	return append(captured, CapturedEvent{
		Pool:      pool,
		LogIndex:  logIndex,
		TxHash:    txHash,
		EventName: eventName,
		Payload:   payload,
	}), nil
}

// ERC-20 LP-token event names as persisted on curve_lp_token_event.event_name.
const (
	lpEventTransfer = "transfer"
	lpEventApproval = "approval"
)

// extractLpTokenEvent decodes an ERC-20 Transfer or Approval log into an
// LpTokenEventRecord. It is signature-driven and address-agnostic so both
// stableswap (pool == LP token for NG; separate LP contract for pre-NG, routed
// by the coordinator) and cryptoswap reuse it unchanged.
//
// Transfer(from, to, value) maps from/to directly. Approval(owner, spender,
// value) maps owner->From, spender->To (the on-chain field order is identical;
// both events have two indexed address topics then a uint256 value).
//
// Reuse depends on the ABI naming the fields exactly: Transfer ->
// sender/receiver/value, Approval -> owner/spender/value. Any handler's ABI that
// adds these events (e.g. the cryptoswap ABI) MUST use those field names.
func extractLpTokenEvent(
	data map[string]any,
	eventName string,
	pool RegisteredPool,
	logIndex uint,
	txHash common.Hash,
) (LpTokenEventRecord, error) {
	var fromKey, toKey, name string
	switch eventName {
	case "Transfer":
		fromKey, toKey, name = "sender", "receiver", lpEventTransfer
	case "Approval":
		fromKey, toKey, name = "owner", "spender", lpEventApproval
	default:
		return LpTokenEventRecord{}, fmt.Errorf("not an LP-token event: %s", eventName)
	}

	from, err := getAddrField(data, fromKey)
	if err != nil {
		return LpTokenEventRecord{}, err
	}
	to, err := getAddrField(data, toKey)
	if err != nil {
		return LpTokenEventRecord{}, err
	}
	value, err := getBigIntField(data, "value")
	if err != nil {
		return LpTokenEventRecord{}, err
	}

	return LpTokenEventRecord{
		Pool:      pool,
		LogIndex:  logIndex,
		TxHash:    txHash,
		EventName: name,
		From:      from,
		To:        to,
		Value:     value,
	}, nil
}

// isLpTokenEvent reports whether an ABI event name is an ERC-20 LP-token event.
func isLpTokenEvent(eventName string) bool {
	return eventName == "Transfer" || eventName == "Approval"
}

// parameterEventName maps an ABI event name to the curve_parameter_event.event_name
// value, returning ("", false) for events that are not parameter events.
func parameterEventName(abiEventName string) (string, bool) {
	name, ok := abiParamEventNames[abiEventName]
	return name, ok
}

var abiParamEventNames = map[string]string{
	"RampA":          "ramp_a",
	"StopRampA":      "stop_ramp_a",
	"NewFee":         "new_fee",
	"CommitNewFee":   "commit_new_fee",
	"ApplyNewFee":    "apply_new_fee",
	"NewAdmin":       "new_admin",
	"CommitNewAdmin": "commit_new_admin",
	// Cryptoswap-only admin/governance events.
	"RampAgamma":          "ramp_a_gamma",
	"NewParameters":       "new_parameters",
	"CommitNewParameters": "commit_new_parameters",
	"ClaimAdminFee":       "claim_admin_fee",
}

// marshalParameterParams builds the params JSONB for a parameter event using the
// exact keys documented on curve_parameter_event.params. The decoded ABI fields
// arrive in `data`; pairs lists the (jsonKey, abiField) entries to emit. All
// values are stringified decimals to preserve full uint256 precision in JSON.
func marshalParameterParams(data map[string]any, pairs [][2]string) (json.RawMessage, error) {
	out := make(map[string]string, len(pairs))
	for _, p := range pairs {
		jsonKey, abiField := p[0], p[1]
		v, err := getBigIntField(data, abiField)
		if err != nil {
			return nil, fmt.Errorf("parameter event field %q: %w", abiField, err)
		}
		out[jsonKey] = v.String()
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshalling parameter event params: %w", err)
	}
	return payload, nil
}

// marshalAddressParams builds the params JSONB for an address-carrying parameter
// event (NewAdmin/CommitNewAdmin), emitting lowercase 0x-hex addresses and any
// big.Int fields. addrPairs and bigIntPairs are (jsonKey, abiField) lists.
func marshalAddressParams(data map[string]any, addrPairs, bigIntPairs [][2]string) (json.RawMessage, error) {
	out := make(map[string]string, len(addrPairs)+len(bigIntPairs))
	for _, p := range addrPairs {
		jsonKey, abiField := p[0], p[1]
		a, err := getAddrField(data, abiField)
		if err != nil {
			return nil, fmt.Errorf("parameter event field %q: %w", abiField, err)
		}
		out[jsonKey] = a.Hex()
	}
	for _, p := range bigIntPairs {
		jsonKey, abiField := p[0], p[1]
		v, err := getBigIntField(data, abiField)
		if err != nil {
			return nil, fmt.Errorf("parameter event field %q: %w", abiField, err)
		}
		out[jsonKey] = v.String()
	}
	payload, err := json.Marshal(out)
	if err != nil {
		return nil, fmt.Errorf("marshalling parameter event params: %w", err)
	}
	return payload, nil
}

// extractParameterEvent decodes a parameter/admin event into a ParameterEventRecord
// using the documented JSONB keys for its event_name. It is shared between the
// stableswap and cryptoswap handlers; only the events present in a handler's ABI
// ever reach it.
func extractParameterEvent(
	data map[string]any,
	abiEventName, paramName string,
	pool RegisteredPool,
	logIndex uint,
	txHash common.Hash,
) (ParameterEventRecord, error) {
	var (
		params json.RawMessage
		err    error
	)
	switch abiEventName {
	case "RampA":
		params, err = marshalParameterParams(data, [][2]string{
			{"old_a", "old_A"}, {"new_a", "new_A"},
			{"initial_time", "initial_time"}, {"future_time", "future_time"},
		})
	case "StopRampA":
		params, err = marshalParameterParams(data, [][2]string{
			{"a", "A"}, {"t", "t"},
		})
	case "NewFee":
		params, err = marshalParameterParams(data, [][2]string{
			{"fee", "fee"}, {"admin_fee", "admin_fee"},
		})
	case "CommitNewFee":
		params, err = marshalParameterParams(data, [][2]string{
			{"deadline", "deadline"}, {"fee", "fee"}, {"admin_fee", "admin_fee"},
		})
	case "ApplyNewFee":
		params, err = marshalParameterParams(data, [][2]string{
			{"fee", "fee"}, {"offpeg_fee_multiplier", "offpeg_fee_multiplier"},
		})
	case "NewAdmin":
		params, err = marshalAddressParams(data, [][2]string{{"admin", "admin"}}, nil)
	case "CommitNewAdmin":
		params, err = marshalAddressParams(data,
			[][2]string{{"admin", "admin"}},
			[][2]string{{"deadline", "deadline"}},
		)
	case "RampAgamma":
		params, err = marshalParameterParams(data, [][2]string{
			{"initial_a", "initial_A"}, {"future_a", "future_A"},
			{"initial_gamma", "initial_gamma"}, {"future_gamma", "future_gamma"},
			{"initial_time", "initial_time"}, {"future_time", "future_time"},
		})
	case "NewParameters":
		// Cryptoswap NewParameters carries no admin_fee (the catalogue comment is
		// stale on this point); admin_fee lives only on the ADMIN_FEE constant.
		params, err = marshalParameterParams(data, [][2]string{
			{"mid_fee", "mid_fee"}, {"out_fee", "out_fee"}, {"fee_gamma", "fee_gamma"},
			{"allowed_extra_profit", "allowed_extra_profit"},
			{"adjustment_step", "adjustment_step"}, {"ma_time", "ma_time"},
		})
	case "CommitNewParameters":
		// deadline is the indexed pending-change deadline, decoded from Topics[1].
		params, err = marshalParameterParams(data, [][2]string{
			{"deadline", "deadline"},
			{"mid_fee", "mid_fee"}, {"out_fee", "out_fee"}, {"fee_gamma", "fee_gamma"},
			{"allowed_extra_profit", "allowed_extra_profit"},
			{"adjustment_step", "adjustment_step"}, {"ma_time", "ma_time"},
		})
	case "ClaimAdminFee":
		params, err = marshalAddressParams(data,
			[][2]string{{"admin", "admin"}},
			[][2]string{{"tokens", "tokens"}},
		)
	default:
		return ParameterEventRecord{}, fmt.Errorf("unhandled parameter event %s", abiEventName)
	}
	if err != nil {
		return ParameterEventRecord{}, err
	}

	return ParameterEventRecord{
		Pool:      pool,
		LogIndex:  logIndex,
		TxHash:    txHash,
		EventName: paramName,
		Params:    params,
	}, nil
}

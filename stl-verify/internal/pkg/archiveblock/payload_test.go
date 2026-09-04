package archiveblock

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestHashFromPayload(t *testing.T) {
	tests := []struct {
		name      string
		payload   string
		want      string
		wantFound bool
	}{
		{name: "a header", payload: `{"number":"0x1","hash":"0xabc"}`, want: "0xabc", wantFound: true},
		{name: "a payload carrying no hash", payload: `{"number":"0x1"}`},
		{name: "a null payload", payload: `null`},
		{name: "a nested hash is not the block's", payload: `{"transactions":[{"hash":"0xdead"}]}`},
		{name: "a hash that is not a string", payload: `{"hash":123}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := HashFromPayload([]byte(tt.payload))

			if found != tt.wantFound {
				t.Fatalf("HashFromPayload found = %v, want %v", found, tt.wantFound)
			}
			if got != tt.want {
				t.Errorf("HashFromPayload = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestListBlockHash_ReadsTheBlockAReceiptOrTraceListDescribes(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "receipts", payload: fmt.Sprintf(`[{"blockHash":%q,"status":"0x1"},{"blockHash":%q}]`, testHash, testHash)},
		{name: "traces", payload: fmt.Sprintf(`[{"action":{"from":"0xabc"},"blockHash":%q,"type":"call"}]`, testHash)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ListBlockHash([]byte(tt.payload))
			if err != nil {
				t.Fatalf("ListBlockHash: %v", err)
			}

			if got != testHash {
				t.Errorf("ListBlockHash = %q, want %q", got, testHash)
			}
		})
	}
}

// A block with no transactions has no receipts and no traces, so an empty list
// is the only answer that may be accepted without a hash.
func TestListBlockHash_ReportsAnEmptyListAsSuch(t *testing.T) {
	_, err := ListBlockHash([]byte(` [ ] `))

	if !errors.Is(err, ErrEmptyList) {
		t.Fatalf("error = %v, want ErrEmptyList", err)
	}
}

func TestListBlockHash_ErrorsOnAListThatNamesNoBlock(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "elements carrying no blockHash", payload: `[{"status":"0x1"}]`},
		{name: "a truncated prefix", payload: fmt.Sprintf(`[{"logsBloom":%q`, strings.Repeat("0", 64))},
		{name: "a blockHash that is not a string", payload: `[{"blockHash":123}]`},
		{name: "a nested blockHash is not the element's", payload: `[{"logs":[{"blockHash":"0xdead"}]}]`},
		{name: "not a list at all", payload: `{"blockHash":"0xdead"}`},
		{name: "a null payload", payload: `null`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ListBlockHash([]byte(tt.payload))

			if err == nil {
				t.Fatalf("ListBlockHash = %q, want an error from a list that names no block", got)
			}
			if errors.Is(err, ErrEmptyList) {
				t.Errorf("error = %v, want it told apart from a list with no elements", err)
			}
		})
	}
}

func TestHasTransactions_ReadsWhetherABlockCarriedAny(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		want    bool
	}{
		{name: "a full-transaction block", payload: `{"hash":"0xabc","transactions":[{"hash":"0xaa"}],"uncles":[]}`, want: true},
		{name: "a hashes-only block", payload: `{"hash":"0xabc","transactions":["0xaa"]}`, want: true},
		{name: "a block with no transactions", payload: `{"hash":"0xabc","transactions":[],"uncles":[]}`},
		{name: "a transaction list behind a nested object", payload: `{"withdrawals":[{"index":"0x1"}],"transactions":[]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := HasTransactions([]byte(tt.payload))
			if err != nil {
				t.Fatalf("HasTransactions: %v", err)
			}

			if got != tt.want {
				t.Errorf("HasTransactions = %v, want %v", got, tt.want)
			}
		})
	}
}

// Whether the block carried transactions is the only thing that makes an empty
// receipt or trace list legitimate, so a payload that cannot answer must not
// read as "no transactions".
func TestHasTransactions_ErrorsOnAPayloadThatCannotAnswer(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{name: "no transaction list at all", payload: `{"hash":"0xabc"}`},
		{name: "a truncated payload", payload: `{"hash":"0xabc","logsBloom":"0x00`},
		{name: "a transaction list that is not a list", payload: `{"transactions":"0xaa"}`},
		{name: "not an object", payload: `[{"transactions":[]}]`},
		{name: "a null payload", payload: `null`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := HasTransactions([]byte(tt.payload))

			if err == nil {
				t.Fatalf("HasTransactions = %v, want an error from a payload that cannot answer", got)
			}
		})
	}
}

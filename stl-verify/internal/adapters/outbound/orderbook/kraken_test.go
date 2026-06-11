package orderbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// newTestKrakenHandler builds a handler subscribed to the pair the tests use,
// with a quiet logger.
func newTestKrakenHandler() *krakenHandler {
	return (&krakenExchange{}).newHandler([]string{"XBT/USD"}, testLogger()).(*krakenHandler)
}

func TestKrakenHandlerSnapshotThenUpdate(t *testing.T) {
	h := newTestKrakenHandler()

	// Snapshot frame: [channelID, {as,bs}, channelName, pair].
	snapshot := `[0,{"as":[["101.0","3.0","t"],["102.0","1.0","t"]],"bs":[["100.0","2.0","t"],["99.0","4.0","t"]]},"book-500","XBT/USD"]`
	sigs, err := h.handle([]byte(snapshot))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if len(sigs) != 1 || !sigs[0].isSnapshot {
		t.Fatalf("expected snapshot signal, got %+v", sigs)
	}
	book := sigs[0].book
	if sz, ok := sizeAt(book.Bids(), "100.0"); !ok || sz != "2.0" {
		t.Errorf("bid 100.0 = %q (ok=%v), want 2.0", sz, ok)
	}
	if sz, ok := sizeAt(book.Asks(), "101.0"); !ok || sz != "3.0" {
		t.Errorf("ask 101.0 = %q (ok=%v), want 3.0", sz, ok)
	}

	// Update frame: [channelID, {b}, channelName, pair]; zero volume removes.
	// No checksum field here so this case stays focused on apply/remove behavior;
	// checksum validation is covered separately.
	update := `[0,{"b":[["100.0","0.00000000","t"],["98.5","5.0","t"]]},"book-500","XBT/USD"]`
	sigs, err = h.handle([]byte(update))
	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if len(sigs) != 1 || sigs[0].isSnapshot {
		t.Fatalf("expected non-snapshot signal, got %+v", sigs)
	}
	book = sigs[0].book
	if _, ok := sizeAt(book.Bids(), "100.0"); ok {
		t.Error("bid 100.0 should be removed by zero volume")
	}
	if sz, ok := sizeAt(book.Bids(), "99.0"); !ok || sz != "4.0" {
		t.Errorf("bid 99.0 after update = %q (ok=%v), want 4.0", sz, ok)
	}
	if sz, ok := sizeAt(book.Bids(), "98.5"); !ok || sz != "5.0" {
		t.Errorf("bid 98.5 = %q (ok=%v), want 5.0", sz, ok)
	}
}

func TestKrakenHandlerCombinedUpdate(t *testing.T) {
	h := newTestKrakenHandler()
	snapshot := `[0,{"as":[["101","3","t"]],"bs":[["100","2","t"]]},"book-500","XBT/USD"]`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// Two data objects (a and b) in one frame.
	combined := `[0,{"a":[["101","0","t"],["103","1","t"]]},{"b":[["100","9","t"]]},"book-500","XBT/USD"]`
	sigs, err := h.handle([]byte(combined))
	if err != nil {
		t.Fatalf("combined: %v", err)
	}
	if len(sigs) != 1 {
		t.Fatalf("expected 1 signal, got %d", len(sigs))
	}
	book := sigs[0].book
	if _, ok := sizeAt(book.Asks(), "101"); ok {
		t.Error("ask 101 should be removed")
	}
	if sz, ok := sizeAt(book.Asks(), "103"); !ok || sz != "1" {
		t.Errorf("ask 103 = %q (ok=%v), want 1", sz, ok)
	}
	if sz, ok := sizeAt(book.Bids(), "100"); !ok || sz != "9" {
		t.Errorf("bid 100 = %q (ok=%v), want 9", sz, ok)
	}
}

func TestKrakenHandlerEvents(t *testing.T) {
	h := newTestKrakenHandler()
	for _, raw := range []string{
		`{"event":"systemStatus","status":"online"}`,
		`{"event":"heartbeat"}`,
		`{"event":"pong"}`,
		`{"event":"subscriptionStatus","status":"subscribed","pair":"XBT/USD"}`,
	} {
		if sigs, err := h.handle([]byte(raw)); err != nil || sigs != nil {
			t.Errorf("event %s: sigs=%v err=%v", raw, sigs, err)
		}
	}
}

// TestKrakenHandlerSubscriptionErrorKeepsConnection covers the poisoned-group
// bug: a subscriptionStatus error is scoped to one pair; Kraken keeps the
// connection and the other subscriptions alive. Treating it as connection-fatal
// would reconnect with the same subscribe payload and permanently cycle every
// healthy pair in the group. The handler must surface it in the log and carry on.
func TestKrakenHandlerSubscriptionErrorKeepsConnection(t *testing.T) {
	logger, sb := captureLogger()
	h := (&krakenExchange{}).newHandler([]string{"XBT/USD"}, logger).(*krakenHandler)
	frame := `{"event":"subscriptionStatus","status":"error","pair":"NOPE/USD","errorMessage":"Currency pair not supported"}`
	sigs, err := h.handle([]byte(frame))
	if err != nil {
		t.Fatalf("subscription error must not kill the connection, got %v", err)
	}
	if sigs != nil {
		t.Errorf("subscription error must emit no book change, got %+v", sigs)
	}
	// Swallowing the error silently is not acceptable; it must be loud in the log.
	if got := sb.String(); !strings.Contains(got, "ERROR") || !strings.Contains(got, "NOPE/USD") {
		t.Errorf("subscription error must be logged at error level with the pair, got %q", got)
	}
}

// TestKrakenHandlerUpdateBeforeSnapshotIsRejected: an update with no prior
// snapshot would be applied to an empty book and emitted as a partial (wrong)
// book. Kraken v1 has no sequence numbers and the checksum is absent on some
// frames, so this must force a re-sync, exactly like the OKX/Coinbase guards.
func TestKrakenHandlerUpdateBeforeSnapshotIsRejected(t *testing.T) {
	h := newTestKrakenHandler()
	update := `[0,{"b":[["100.0","1.0","t"]]},"book-500","XBT/USD"]`
	if _, err := h.handle([]byte(update)); !errors.Is(err, errSequenceGap) {
		t.Fatalf("err = %v, want errSequenceGap", err)
	}
}

func TestKrakenHandlerMalformedDataIsFatal(t *testing.T) {
	h := newTestKrakenHandler()
	// A non-object where a data object is expected must error (forcing reconnect),
	// since Kraken v1 has no sequence backstop to detect a silently dropped frame.
	frame := `[0,"not-an-object","book-500","XBT/USD"]`
	if _, err := h.handle([]byte(frame)); err == nil {
		t.Fatal("expected an error for a malformed data object, got nil")
	}
}

func TestKrakenChecksumToken(t *testing.T) {
	cases := []struct{ in, want string }{
		{"101.5", "1015"},
		{"0.50000", "50000"},
		{"3.00000000", "300000000"},
		{"1000.0", "10000"},
		{"0.0", "0"}, // all-zero token collapses to "0"
		{"0", "0"},
	}
	for _, c := range cases {
		if got := krakenChecksumToken(c.in); got != c.want {
			t.Errorf("krakenChecksumToken(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestKrakenChecksum(t *testing.T) {
	book := entity.NewOrderbook(exchangeKraken, "XBT/USD")
	book.ApplyLevel(entity.Ask, "102.0", "3.5")
	book.ApplyLevel(entity.Ask, "101.5", "2.0")
	book.ApplyLevel(entity.Bid, "99.5", "4.25")
	book.ApplyLevel(entity.Bid, "100.0", "1.0")

	// Top 10 asks (ascending) then bids (descending), each price+volume with the
	// decimal point and leading zeros stripped:
	//   asks: 101.5/2.0 -> "1015"+"20"; 102.0/3.5 -> "1020"+"35"
	//   bids: 100.0/1.0 -> "1000"+"10"; 99.5/4.25 -> "995"+"425"
	want := crc32.ChecksumIEEE([]byte("101520102035100010995425"))
	if got := krakenChecksum(book); got != want {
		t.Errorf("krakenChecksum = %d, want %d", got, want)
	}
}

func TestKrakenChecksumMismatchResyncs(t *testing.T) {
	h := newTestKrakenHandler()
	snapshot := `[0,{"as":[["101.0","2.0"]],"bs":[["100.0","1.0"]]},"book-10","XBT/USD"]`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	// A deliberately wrong checksum must force a re-sync.
	bad := `[0,{"b":[["99.0","5.0"]],"c":"999999999"},"book-10","XBT/USD"]`
	if _, err := h.handle([]byte(bad)); !errors.Is(err, errSequenceGap) {
		t.Fatalf("err = %v, want errSequenceGap", err)
	}
}

func TestKrakenChecksumValidPasses(t *testing.T) {
	// Derive the expected checksum from the book state the handler will reach.
	expect := entity.NewOrderbook(exchangeKraken, "XBT/USD")
	expect.ApplyLevel(entity.Ask, "101.0", "2.0")
	expect.ApplyLevel(entity.Bid, "100.0", "1.0")
	expect.ApplyLevel(entity.Bid, "99.0", "5.0")
	c := strconv.FormatUint(uint64(krakenChecksum(expect)), 10)

	h := newTestKrakenHandler()
	snapshot := `[0,{"as":[["101.0","2.0"]],"bs":[["100.0","1.0"]]},"book-10","XBT/USD"]`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	update := `[0,{"b":[["99.0","5.0"]],"c":"` + c + `"},"book-10","XBT/USD"]`
	if _, err := h.handle([]byte(update)); err != nil {
		t.Fatalf("valid checksum should pass, got %v", err)
	}
}

func TestKrakenTrimsToDepth(t *testing.T) {
	h := newTestKrakenHandler()
	asks := make([][]string, 0, krakenDepth+2)
	for i := 1; i <= krakenDepth+2; i++ {
		asks = append(asks, []string{fmt.Sprintf("%d.0", i), "1.0"})
	}
	dataObj, err := json.Marshal(map[string]any{"as": asks, "bs": [][]string{{"0.5", "1.0"}}})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := json.Marshal([]any{0, json.RawMessage(dataObj), "book-500", "XBT/USD"})
	if err != nil {
		t.Fatal(err)
	}
	sigs, err := h.handle(frame)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	book := sigs[0].book
	if got := len(book.Asks()); got != krakenDepth {
		t.Errorf("ask depth = %d, want %d (trimmed to top-N)", got, krakenDepth)
	}
	// The highest-priced asks beyond the top krakenDepth must be trimmed away.
	if _, ok := sizeAt(book.Asks(), fmt.Sprintf("%d.0", krakenDepth+2)); ok {
		t.Error("highest ask beyond depth should be trimmed")
	}
}

func TestKrakenHandlerRejectsUnsubscribedSymbol(t *testing.T) {
	// Build through the exchange so newHandler's symbolSet seeding is covered too.
	h := (&krakenExchange{}).newHandler([]string{"XBT/USD"}, testLogger())
	frame := `[0,{"as":[["101.0","3.0","t"]],"bs":[["100.0","2.0","t"]]},"book-500","ETH/USD"]`
	if _, err := h.handle([]byte(frame)); !errors.Is(err, errUnexpectedSymbol) {
		t.Fatalf("err = %v, want errUnexpectedSymbol", err)
	}
}

func TestKrakenHandlerRejectsMalformedLevel(t *testing.T) {
	// A non-canonical price/size must fail the frame so the engine resyncs.
	tests := []struct{ name, frame string }{
		{"trailing dot price", `[0,{"as":[["100.","1","t"]],"bs":[]},"book-500","XBT/USD"]`},
		{"exponent size", `[0,{"as":[],"bs":[["100","1e3","t"]]},"book-500","XBT/USD"]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newTestKrakenHandler()
			if _, err := h.handle([]byte(tt.frame)); err == nil {
				t.Error("expected an error for a malformed level (forces resync)")
			}
		})
	}
}

// TestKrakenTrimUsesExactDecimalOrdering pins the reason CompareDecimal exists: at
// the depth boundary, two levels that float64 cannot tell apart must be ordered by
// exact decimal value, so the better-priced one is kept and the worse trimmed.
func TestKrakenTrimUsesExactDecimalOrdering(t *testing.T) {
	h := newTestKrakenHandler()
	asks := make([][]string, 0, krakenDepth+1)
	for i := 1; i <= krakenDepth-1; i++ {
		asks = append(asks, []string{fmt.Sprintf("%d.0", i), "1.0"})
	}
	// These two differ only in the 17th significant digit; as float64 they are equal,
	// so a float sort could trim either. The lower must be kept, the higher dropped.
	keep := "500.00000000000001"
	drop := "500.00000000000002"
	asks = append(asks, []string{drop, "1.0"}, []string{keep, "1.0"}) // total krakenDepth+1
	dataObj, err := json.Marshal(map[string]any{"as": asks, "bs": [][]string{{"0.5", "1.0"}}})
	if err != nil {
		t.Fatal(err)
	}
	frame, err := json.Marshal([]any{0, json.RawMessage(dataObj), "book-500", "XBT/USD"})
	if err != nil {
		t.Fatal(err)
	}
	sigs, err := h.handle(frame)
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	book := sigs[0].book
	if got := len(book.Asks()); got != krakenDepth {
		t.Fatalf("ask depth = %d, want %d", got, krakenDepth)
	}
	if _, ok := sizeAt(book.Asks(), keep); !ok {
		t.Errorf("better-priced boundary level %s should be kept", keep)
	}
	if _, ok := sizeAt(book.Asks(), drop); ok {
		t.Errorf("worse-priced boundary level %s should be trimmed", drop)
	}
}

// TestKrakenHandlerChecksumOnlyFrame: a frame carrying only a checksum (no
// level changes) emits nothing, but the checksum is still verified; in a quiet
// market it is the only signal that the local book has drifted.
func TestKrakenHandlerChecksumOnlyFrame(t *testing.T) {
	expect := entity.NewOrderbook(exchangeKraken, "XBT/USD")
	expect.ApplyLevel(entity.Ask, "101.0", "2.0")
	expect.ApplyLevel(entity.Bid, "100.0", "1.0")
	c := strconv.FormatUint(uint64(krakenChecksum(expect)), 10)

	h := newTestKrakenHandler()
	snapshot := `[0,{"as":[["101.0","2.0"]],"bs":[["100.0","1.0"]]},"book-500","XBT/USD"]`
	if _, err := h.handle([]byte(snapshot)); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Matching checksum: no change to emit, no error.
	sigs, err := h.handle([]byte(`[0,{"c":"` + c + `"},"book-500","XBT/USD"]`))
	if err != nil {
		t.Fatalf("checksum-only frame with matching checksum: %v", err)
	}
	if sigs != nil {
		t.Errorf("checksum-only frame should emit nothing, got %+v", sigs)
	}

	// Mismatching checksum: the book has drifted, force a re-sync.
	if _, err := h.handle([]byte(`[0,{"c":"123"},"book-500","XBT/USD"]`)); !errors.Is(err, errSequenceGap) {
		t.Fatalf("err = %v, want errSequenceGap for a drifted checksum-only frame", err)
	}
}

func TestKrakenHandlerParsesLevelTime(t *testing.T) {
	h := newTestKrakenHandler()
	// Per-level timestamps are Unix "seconds.fraction"; the emitted event time is
	// the max across the levels applied in the frame.
	snapshot := `[0,{"as":[["101.0","3.0","1700000000.5"]],"bs":[["100.0","2.0","1700000001.250000"]]},"book-500","XBT/USD"]`
	sigs, err := h.handle([]byte(snapshot))
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	want := time.Unix(1700000001, 250000000).UTC()
	if !sigs[0].t.Equal(want) {
		t.Errorf("event time = %v, want %v (max per-level timestamp)", sigs[0].t, want)
	}
}

func TestParseKrakenLevelTime(t *testing.T) {
	tests := []struct {
		in   string
		want time.Time
	}{
		{"1700000000", time.Unix(1700000000, 0).UTC()},
		{"1700000000.5", time.Unix(1700000000, 500000000).UTC()},
		{"1700000000.123456789", time.Unix(1700000000, 123456789).UTC()},
		{"", time.Time{}},
		{"t", time.Time{}},
		{"abc.def", time.Time{}},
		{"100.xyz", time.Time{}}, // valid seconds, unparseable fraction -> zero
	}
	for _, tt := range tests {
		if got := parseKrakenLevelTime(tt.in); !got.Equal(tt.want) {
			t.Errorf("parseKrakenLevelTime(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestKrakenSubscribeAndPing(t *testing.T) {
	e := &krakenExchange{}
	msgs := e.subscribeMessages([]string{"XBT/USD"})
	if len(msgs) != 1 {
		t.Fatalf("subscribeMessages = %v", msgs)
	}
	m := msgs[0].(map[string]any)
	if m["event"] != "subscribe" {
		t.Errorf("event = %v", m["event"])
	}
	sub := m["subscription"].(map[string]any)
	if sub["name"] != "book" || sub["depth"] != krakenDepth {
		t.Errorf("subscription = %v", sub)
	}
	if frame, interval := e.appPing(); string(frame) != `{"event":"ping"}` || interval <= 0 {
		t.Errorf("appPing = %q %v", frame, interval)
	}
}

func TestKrakenProviderNameAndValidation(t *testing.T) {
	p := NewKrakenProvider(testConfig())
	if p.Name() != "kraken" {
		t.Errorf("Name = %q", p.Name())
	}
	if _, err := p.Watch(context.Background(), nil); err == nil {
		t.Error("Watch with no symbols should error")
	}
}

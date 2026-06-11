package rawsckey

import "testing"

func TestSelector(t *testing.T) {
	if got := Selector([]byte{0xfe, 0xaf, 0x96, 0x8c, 0x01}); got != "0xfeaf968c" {
		t.Fatalf("Selector = %q, want 0xfeaf968c", got)
	}
	if got := Selector([]byte{0x01, 0x02}); got != "0x" {
		t.Fatalf("Selector short input = %q, want 0x", got)
	}
}

func TestBatchHashDeterministic(t *testing.T) {
	inputs := []BatchHashInput{
		{Target: []byte{0x01}, CallData: []byte{0xfe, 0xaf, 0x96, 0x8c}},
		{Target: []byte{0x02}, CallData: []byte{0x18, 0x16, 0x0d, 0xdd}},
	}
	h1, h2 := BatchHash(inputs), BatchHash(inputs)
	if h1 != h2 {
		t.Fatalf("BatchHash not deterministic: %q vs %q", h1, h2)
	}
	if len(h1) != 16 {
		t.Fatalf("BatchHash len = %d, want 16", len(h1))
	}
}

func TestBatchHashOrderSensitive(t *testing.T) {
	a := BatchHashInput{Target: []byte{0x01}, CallData: []byte{0xaa}}
	b := BatchHashInput{Target: []byte{0x02}, CallData: []byte{0xbb}}
	if BatchHash([]BatchHashInput{a, b}) == BatchHash([]BatchHashInput{b, a}) {
		t.Fatal("BatchHash should differ when call order differs")
	}
}

func TestBatchHashDistinctByContent(t *testing.T) {
	base := BatchHashInput{Target: []byte{0x01}, CallData: []byte{0xaa}}
	mut := BatchHashInput{Target: []byte{0x01}, CallData: []byte{0xab}} // one byte differs
	if BatchHash([]BatchHashInput{base}) == BatchHash([]BatchHashInput{mut}) {
		t.Fatal("BatchHash should differ when call data differs")
	}
}

func TestBatchHashEmpty(t *testing.T) {
	if got := BatchHash(nil); len(got) != 16 {
		t.Fatalf("BatchHash(nil) len = %d, want 16", len(got))
	}
}

func TestBuild(t *testing.T) {
	got := Build(1, 21500042, 0, "oracle-price", "a3f2c1d4e5b6f7c8")
	want := "raw-sc-calls/chain_id=1/block=21500000-21500999/21500042_0_oracle-price_a3f2c1d4e5b6f7c8.jsonl.zst"
	if got != want {
		t.Fatalf("Build = %q, want %q", got, want)
	}
}

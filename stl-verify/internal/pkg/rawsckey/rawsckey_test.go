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

func TestHashInputDeterministic(t *testing.T) {
	data := []byte("some-call-data")
	h1, h2 := HashInput(data), HashInput(data)
	if h1 != h2 {
		t.Fatalf("HashInput not deterministic: %q vs %q", h1, h2)
	}
	if len(h1) != 16 {
		t.Fatalf("HashInput len = %d, want 16", len(h1))
	}
}

func TestBuild(t *testing.T) {
	callData := []byte{0xfe, 0xaf, 0x96, 0x8c, 0xaa, 0xbb}
	got := Build(21500042, 0, "oracle-price", callData)
	want := "raw-sc-calls/block=21500000-21500999/bv=0/21500042_oracle-price_0xfeaf968c_" + HashInput(callData) + ".jsonl.zst"
	if got != want {
		t.Fatalf("Build = %q, want %q", got, want)
	}
}

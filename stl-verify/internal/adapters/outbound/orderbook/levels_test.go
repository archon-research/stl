package orderbook

import (
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func TestParseLevel(t *testing.T) {
	tests := []struct {
		name      string
		price     string
		size      string
		wantPrice string
		wantSize  string
		wantErr   bool
	}{
		{name: "ok", price: "100.5", size: "2.25", wantPrice: "100.5", wantSize: "2.25"},
		{name: "zero size ok", price: "100", size: "0", wantPrice: "100", wantSize: "0"},
		{name: "high precision preserved verbatim", price: "0.000000000000001234", size: "12345.678901234567890", wantPrice: "0.000000000000001234", wantSize: "12345.678901234567890"},
		{name: "bad price", price: "abc", size: "1", wantErr: true},
		{name: "bad size", price: "1", size: "xyz", wantErr: true},
		{name: "negative price rejected", price: "-1", size: "1", wantErr: true},
		{name: "zero price rejected", price: "0", size: "1", wantErr: true},
		{name: "negative size rejected", price: "100", size: "-1", wantErr: true},
		{name: "NaN price rejected", price: "NaN", size: "1", wantErr: true},
		{name: "Inf price rejected", price: "Inf", size: "1", wantErr: true},
		{name: "NaN size rejected", price: "100", size: "NaN", wantErr: true},
		{name: "Inf size rejected", price: "100", size: "+Inf", wantErr: true},
		// Scientific notation parses as a float but breaks delete-by-key matching,
		// so it must be rejected (kept verbatim would strand a ghost level).
		{name: "exponent price rejected", price: "1e5", size: "1", wantErr: true},
		{name: "exponent size rejected", price: "100", size: "1E3", wantErr: true},
		{name: "signed price rejected", price: "+100", size: "1", wantErr: true},
		{name: "trailing dot price rejected", price: "100.", size: "1", wantErr: true},
		{name: "leading dot price rejected", price: ".5", size: "1", wantErr: true},
		{name: "empty size rejected", price: "100", size: "", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lvl, err := parseLevel(tt.price, tt.size)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseLevel err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if lvl.Price != tt.wantPrice || lvl.Size != tt.wantSize {
				t.Errorf("parseLevel = %+v, want {%q %q}", lvl, tt.wantPrice, tt.wantSize)
			}
		})
	}
}

func TestApplyDeltaLevels(t *testing.T) {
	ob := entity.NewOrderbook("test", "X")
	ob.ApplyLevel(entity.Bid, "100", "5")
	// Update 100 -> 7, add 99 -> 3, remove a non-existent level (no-op).
	err := applyDeltaLevels(ob, entity.Bid, [][]string{{"100", "7"}, {"99", "3"}, {"50", "0"}})
	if err != nil {
		t.Fatalf("applyDeltaLevels err = %v", err)
	}
	if len(ob.Bids()) != 2 {
		t.Errorf("bid depth = %d, want 2", len(ob.Bids()))
	}
	if sz, ok := sizeAt(ob.Bids(), "100"); !ok || sz != "7" {
		t.Errorf("size at 100 = %q (ok=%v), want 7", sz, ok)
	}

	if err := applyDeltaLevels(ob, entity.Bid, [][]string{{"bad"}}); err == nil {
		t.Error("applyDeltaLevels with malformed level should error")
	}
}

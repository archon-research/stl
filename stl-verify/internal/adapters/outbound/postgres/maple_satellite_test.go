package postgres

import (
	"encoding/hex"
	"testing"
)

// Golden vectors pin the frozen hashdiff recipe so the SQL backfill
// (md5 over the same canonical encoding) produces byte-identical hashes.
// Recipe: fields joined by 0x1f; SQL NULL rendered as 0x1e; md5 -> 16 bytes.
func TestMetaHashdiff(t *testing.T) {
	s := func(v string) *string { return &v }

	tests := []struct {
		name    string
		fields  []*string
		wantHex string
	}{
		{
			name:    "pool name and is_syrup",
			fields:  []*string{s("Maple USDC"), s("false")},
			wantHex: "7d2346613ce754bdf490750ec390b0ac",
		},
		{
			name:    "loan OTL all meta null",
			fields:  []*string{s("OTL"), nil, nil, nil, nil, nil, nil},
			wantHex: "1450044b1760280cf5bc207dfa37b15d",
		},
		{
			name:    "loan OTL amm",
			fields:  []*string{s("OTL"), s("amm"), nil, nil, nil, nil, nil},
			wantHex: "2695022cc4f7f384470297fd8624d6a0",
		},
		{
			name:    "single int field (sky version)",
			fields:  []*string{s("5")},
			wantHex: "e4da3b7fbbce2345d7772b0674a318d5",
		},
		{
			// Pins the helper's nil -> NULL-sentinel encoding (used by the
			// nullable loan_meta_* columns and the SQL backfill COALESCE
			// sentinel); the live sky writer renders a non-pointer int so it
			// never sends nil here.
			name:    "single null field",
			fields:  []*string{nil},
			wantHex: "7bc72a0767d237be4da30ace191acdc2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hex.EncodeToString(metaHashdiff(tt.fields))
			if got != tt.wantHex {
				t.Errorf("metaHashdiff = %s, want %s", got, tt.wantHex)
			}
		})
	}
}

func TestMetaHashdiffDistinguishesNullFromEmpty(t *testing.T) {
	empty := ""
	withEmpty := metaHashdiff([]*string{&empty})
	withNull := metaHashdiff([]*string{nil})
	if hex.EncodeToString(withEmpty) == hex.EncodeToString(withNull) {
		t.Error("empty string and NULL must hash differently")
	}
}

func TestMetaHashdiffOrderSensitive(t *testing.T) {
	a := "a"
	b := "b"
	ab := metaHashdiff([]*string{&a, &b})
	ba := metaHashdiff([]*string{&b, &a})
	if hex.EncodeToString(ab) == hex.EncodeToString(ba) {
		t.Error("field order must change the hash")
	}
}

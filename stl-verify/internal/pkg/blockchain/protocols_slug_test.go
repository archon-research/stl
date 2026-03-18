package blockchain

import "testing"

func TestGetProtocolBySlug(t *testing.T) {
	tests := []struct {
		slug     string
		wantName string
		wantOK   bool
	}{
		{"spark_ethereum", "Sparklend", true},
		{"aave_v3_ethereum", "Aave V3", true},
		{"aave_v2_ethereum", "Aave V2", true},
		{"aave_v3_lido_ethereum", "Aave V3 Lido", true},
		{"aave_v3_rwa_ethereum", "Aave V3 RWA", true},
		{"aave_v3_avalanche", "Aave V3 Avalanche", true},
		{"unknown_protocol", "", false},
		{"", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.slug, func(t *testing.T) {
			key, config, ok := GetProtocolBySlug(tt.slug)
			if ok != tt.wantOK {
				t.Fatalf("GetProtocolBySlug(%q) ok = %v, want %v", tt.slug, ok, tt.wantOK)
			}
			if !ok {
				return
			}
			if config.Name != tt.wantName {
				t.Errorf("GetProtocolBySlug(%q) name = %q, want %q", tt.slug, config.Name, tt.wantName)
			}
			if config.Slug != tt.slug {
				t.Errorf("GetProtocolBySlug(%q) slug = %q, want %q", tt.slug, config.Slug, tt.slug)
			}
			if key.ChainID == 0 {
				t.Errorf("GetProtocolBySlug(%q) returned zero ChainID", tt.slug)
			}
		})
	}
}

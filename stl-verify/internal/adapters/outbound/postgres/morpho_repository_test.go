package postgres

import (
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func TestReclassifies(t *testing.T) {
	unknown := entity.MorphoAdapterTypeUnknown
	marketV1 := entity.MorphoAdapterTypeMarketV1
	merkl := entity.MorphoAdapterTypeERC4626Merkl

	tests := []struct {
		name     string
		known    *entity.MorphoAdapterType
		asserted *entity.MorphoAdapterType
		want     bool
	}{
		{name: "log says nothing, assertion probed nothing", known: nil, asserted: nil, want: false},
		{name: "log says nothing, assertion probed Unknown", known: nil, asserted: &unknown, want: false},
		{name: "log says nothing, assertion probed MarketV1", known: nil, asserted: &marketV1, want: true},
		{name: "log says nothing, assertion probed ERC4626Merkl", known: nil, asserted: &merkl, want: true},

		{name: "log holds Unknown, assertion probed nothing", known: &unknown, asserted: nil, want: false},
		{name: "log holds Unknown, assertion probed Unknown", known: &unknown, asserted: &unknown, want: false},
		{name: "log holds Unknown, assertion probed MarketV1", known: &unknown, asserted: &marketV1, want: true},
		{name: "log holds Unknown, assertion probed ERC4626Merkl", known: &unknown, asserted: &merkl, want: true},

		{name: "log holds MarketV1, assertion probed nothing", known: &marketV1, asserted: nil, want: false},
		{name: "log holds MarketV1, assertion probed Unknown", known: &marketV1, asserted: &unknown, want: false},
		{name: "log holds MarketV1, assertion probed MarketV1", known: &marketV1, asserted: &marketV1, want: false},
		{name: "log holds MarketV1, assertion probed ERC4626Merkl", known: &marketV1, asserted: &merkl, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := reclassifies(tt.known, tt.asserted); got != tt.want {
				t.Errorf("reclassifies(%s, %s) = %v, want %v",
					describeAdapterType(tt.known), describeAdapterType(tt.asserted), got, tt.want)
			}
		})
	}
}

func describeAdapterType(t *entity.MorphoAdapterType) string {
	if t == nil {
		return "nil"
	}
	switch *t {
	case entity.MorphoAdapterTypeUnknown:
		return "Unknown"
	case entity.MorphoAdapterTypeMarketV1:
		return "MarketV1"
	case entity.MorphoAdapterTypeERC4626Merkl:
		return "ERC4626Merkl"
	default:
		return "unmodelled"
	}
}

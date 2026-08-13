package oracle_pricing

import (
	"reflect"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
)

func TestValidationFeeds(t *testing.T) {
	feeds := []blockchain.FeedConfig{
		{TokenID: 1, FeedAddress: common.HexToAddress("0x0000000000000000000000000000000000000F01"), FeedDecimals: 8, QuoteCurrency: "USD"},
	}
	vaults := []blockchain.ERC4626VaultConfig{
		{TokenID: 2, UnderlyingFeed: common.HexToAddress("0x0000000000000000000000000000000000000F02"), FeedDecimals: 8},
	}
	coinFeeds := []blockchain.FeedConfig{
		{TokenID: 3, FeedAddress: common.HexToAddress("0x0000000000000000000000000000000000000F03"), FeedDecimals: 8, QuoteCurrency: "USD"},
		{TokenID: 3, FeedAddress: common.HexToAddress("0x0000000000000000000000000000000000000F04"), FeedDecimals: 18, QuoteCurrency: "USD"},
	}

	tests := []struct {
		name   string
		unit   *OracleUnit
		want   []blockchain.FeedConfig
		wantOK bool
	}{
		{
			name:   "feed oracle returns its feeds",
			unit:   &OracleUnit{Oracle: &entity.Oracle{OracleType: entity.OracleTypeChainlinkFeed}, Feeds: feeds},
			want:   feeds,
			wantOK: true,
		},
		{
			name:   "erc4626 oracle returns underlying feeds",
			unit:   &OracleUnit{Oracle: &entity.Oracle{OracleType: entity.OracleTypeERC4626Share}, ERC4626Vaults: vaults},
			want:   blockchain.ERC4626UnderlyingFeeds(vaults),
			wantOK: true,
		},
		{
			name: "curve lp ng oracle returns coin feeds",
			unit: &OracleUnit{
				Oracle:        &entity.Oracle{OracleType: entity.OracleTypeCurveLPNG},
				CurveLPNGPool: &blockchain.CurveLPNGPoolConfig{TokenID: 3, CoinFeeds: coinFeeds},
			},
			want:   coinFeeds,
			wantOK: true,
		},
		{
			name:   "aave oracle has no feeds to validate",
			unit:   &OracleUnit{Oracle: &entity.Oracle{OracleType: entity.OracleTypeAave}},
			want:   nil,
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ValidationFeeds(tt.unit)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("feeds = %+v, want %+v", got, tt.want)
			}
		})
	}
}

package oracle_pricing

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Mock repo
// ---------------------------------------------------------------------------

type mockRepo struct {
	getOracleFn                    func(ctx context.Context, name string) (*entity.Oracle, error)
	getEnabledAssetsFn             func(ctx context.Context, oracleID int64) ([]*entity.OracleAsset, error)
	getLatestPricesFn              func(ctx context.Context, oracleID int64) (map[int64]float64, error)
	getLatestBlockFn               func(ctx context.Context, oracleID int64) (int64, error)
	getTokenAddressesFn            func(ctx context.Context, oracleID int64) (map[int64][]byte, error)
	upsertPricesFn                 func(ctx context.Context, prices []*entity.OnchainTokenPrice) error
	getAllEnabledOraclesFn         func(ctx context.Context) ([]*entity.Oracle, error)
	getOracleByAddressFn           func(ctx context.Context, chainID int, address []byte) (*entity.Oracle, error)
	insertOracleFn                 func(ctx context.Context, oracle *entity.Oracle) (*entity.Oracle, error)
	getAllActiveProtocolOraclesFn  func(ctx context.Context) ([]*entity.ProtocolOracle, error)
	insertProtocolOracleBindingFn  func(ctx context.Context, binding *entity.ProtocolOracle) (*entity.ProtocolOracle, error)
	copyOracleAssetsFn             func(ctx context.Context, fromOracleID, toOracleID int64) error
	getAllProtocolOracleBindingsFn func(ctx context.Context) ([]*entity.ProtocolOracle, error)
}

func (m *mockRepo) GetOracle(ctx context.Context, name string) (*entity.Oracle, error) {
	if m.getOracleFn != nil {
		return m.getOracleFn(ctx, name)
	}
	return nil, errors.New("not mocked")
}
func (m *mockRepo) GetEnabledAssets(ctx context.Context, oracleID int64) ([]*entity.OracleAsset, error) {
	if m.getEnabledAssetsFn != nil {
		return m.getEnabledAssetsFn(ctx, oracleID)
	}
	return nil, errors.New("not mocked")
}
func (m *mockRepo) GetLatestPrices(ctx context.Context, oracleID int64) (map[int64]float64, error) {
	if m.getLatestPricesFn != nil {
		return m.getLatestPricesFn(ctx, oracleID)
	}
	return nil, errors.New("not mocked")
}
func (m *mockRepo) GetLatestBlock(ctx context.Context, oracleID int64) (int64, error) {
	if m.getLatestBlockFn != nil {
		return m.getLatestBlockFn(ctx, oracleID)
	}
	return 0, nil
}
func (m *mockRepo) GetTokenAddresses(ctx context.Context, oracleID int64) (map[int64][]byte, error) {
	if m.getTokenAddressesFn != nil {
		return m.getTokenAddressesFn(ctx, oracleID)
	}
	return nil, errors.New("not mocked")
}
func (m *mockRepo) UpsertPrices(ctx context.Context, prices []*entity.OnchainTokenPrice) error {
	if m.upsertPricesFn != nil {
		return m.upsertPricesFn(ctx, prices)
	}
	return nil
}
func (m *mockRepo) GetAllEnabledOracles(ctx context.Context) ([]*entity.Oracle, error) {
	if m.getAllEnabledOraclesFn != nil {
		return m.getAllEnabledOraclesFn(ctx)
	}
	return nil, errors.New("not mocked")
}
func (m *mockRepo) GetOracleByAddress(ctx context.Context, chainID int, address []byte) (*entity.Oracle, error) {
	if m.getOracleByAddressFn != nil {
		return m.getOracleByAddressFn(ctx, chainID, address)
	}
	return nil, errors.New("not mocked")
}
func (m *mockRepo) InsertOracle(ctx context.Context, oracle *entity.Oracle) (*entity.Oracle, error) {
	if m.insertOracleFn != nil {
		return m.insertOracleFn(ctx, oracle)
	}
	return nil, errors.New("not mocked")
}
func (m *mockRepo) GetAllActiveProtocolOracles(ctx context.Context) ([]*entity.ProtocolOracle, error) {
	if m.getAllActiveProtocolOraclesFn != nil {
		return m.getAllActiveProtocolOraclesFn(ctx)
	}
	return nil, errors.New("not mocked")
}
func (m *mockRepo) InsertProtocolOracleBinding(ctx context.Context, binding *entity.ProtocolOracle) (*entity.ProtocolOracle, error) {
	if m.insertProtocolOracleBindingFn != nil {
		return m.insertProtocolOracleBindingFn(ctx, binding)
	}
	return nil, errors.New("not mocked")
}
func (m *mockRepo) CopyOracleAssets(ctx context.Context, fromOracleID, toOracleID int64) error {
	if m.copyOracleAssetsFn != nil {
		return m.copyOracleAssetsFn(ctx, fromOracleID, toOracleID)
	}
	return errors.New("not mocked")
}
func (m *mockRepo) GetAllProtocolOracleBindings(ctx context.Context) ([]*entity.ProtocolOracle, error) {
	if m.getAllProtocolOracleBindingsFn != nil {
		return m.getAllProtocolOracleBindingsFn(ctx)
	}
	return nil, nil
}

// ---------------------------------------------------------------------------
// TestLoadOracleUnits
// ---------------------------------------------------------------------------

func TestLoadOracleUnits(t *testing.T) {
	wethAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	daiAddr := common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")
	feedAddr := common.HexToAddress("0x0000000000000000000000000000000000000F01")

	tests := []struct {
		name        string
		setupRepo   func() *mockRepo
		wantCount   int
		wantErr     bool
		errContains string
		checkUnits  func(t *testing.T, units []*OracleUnit)
	}{
		{
			name: "success with aave oracle",
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{{
							ID: 1, Name: "sparklend", Address: [20]byte{0xBB},
							Enabled: true, OracleType: "aave_oracle",
						}}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{
							{ID: 1, OracleID: 1, TokenID: 1, Enabled: true},
							{ID: 2, OracleID: 1, TokenID: 2, Enabled: true},
						}, nil
					},
					getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
						return map[int64][]byte{
							1: wethAddr.Bytes(),
							2: daiAddr.Bytes(),
						}, nil
					},
				}
			},
			wantCount: 1,
			checkUnits: func(t *testing.T, units []*OracleUnit) {
				t.Helper()
				u := units[0]
				if len(u.TokenAddrs) != 2 {
					t.Errorf("TokenAddrs len = %d, want 2", len(u.TokenAddrs))
				}
				if len(u.TokenIDs) != 2 {
					t.Errorf("TokenIDs len = %d, want 2", len(u.TokenIDs))
				}
				if len(u.Feeds) != 0 {
					t.Errorf("Feeds len = %d, want 0 for aave oracle", len(u.Feeds))
				}
			},
		},
		{
			name: "success with feed oracle",
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{{
							ID: 1, Name: "chainlink", Enabled: true,
							OracleType: "chainlink_feed", PriceDecimals: 8,
						}}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{{
							ID: 1, OracleID: 1, TokenID: 1, Enabled: true,
							FeedAddress: feedAddr.Bytes(), FeedDecimals: intPtr(8), QuoteCurrency: "USD",
						}}, nil
					},
					getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
						return map[int64][]byte{1: wethAddr.Bytes()}, nil
					},
				}
			},
			wantCount: 1,
			checkUnits: func(t *testing.T, units []*OracleUnit) {
				t.Helper()
				u := units[0]
				if len(u.Feeds) != 1 {
					t.Errorf("Feeds len = %d, want 1", len(u.Feeds))
				}
				if u.Feeds[0].FeedAddress != feedAddr {
					t.Errorf("FeedAddress = %s, want %s", u.Feeds[0].FeedAddress, feedAddr)
				}
			},
		},
		{
			name: "success with chronicle oracle (uses feed path)",
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{{
							ID: 1, Name: "chronicle", Enabled: true,
							OracleType: "chronicle", PriceDecimals: 18,
						}}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{{
							ID: 1, OracleID: 1, TokenID: 1, Enabled: true,
							FeedAddress: feedAddr.Bytes(), FeedDecimals: intPtr(18), QuoteCurrency: "USD",
						}}, nil
					},
					getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
						return map[int64][]byte{1: wethAddr.Bytes()}, nil
					},
				}
			},
			wantCount: 1,
			checkUnits: func(t *testing.T, units []*OracleUnit) {
				t.Helper()
				u := units[0]
				if len(u.Feeds) != 1 {
					t.Errorf("Feeds len = %d, want 1", len(u.Feeds))
				}
				if u.Feeds[0].FeedDecimals != 18 {
					t.Errorf("FeedDecimals = %d, want 18", u.Feeds[0].FeedDecimals)
				}
			},
		},
		{
			name: "deduplicates by oracle ID",
			setupRepo: func() *mockRepo {
				oracle := &entity.Oracle{ID: 1, Name: "test", Enabled: true}
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{oracle, oracle}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{{ID: 1, OracleID: 1, TokenID: 1, Enabled: true}}, nil
					},
					getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
						return map[int64][]byte{1: wethAddr.Bytes()}, nil
					},
				}
			},
			wantCount: 1,
		},
		{
			name: "skips oracle with no enabled assets",
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{{ID: 1, Name: "empty", Enabled: true}}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{}, nil
					},
				}
			},
			wantCount: 0,
		},
		{
			name: "error from GetAllEnabledOracles",
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return nil, errors.New("db error")
					},
				}
			},
			wantErr:     true,
			errContains: "getting enabled oracles",
		},
		{
			name: "skips oracle with build error",
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{{ID: 1, Name: "bad", Enabled: true}}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return nil, errors.New("query failed")
					},
				}
			},
			wantCount: 0,
		},
		{
			name: "feed oracle missing feed address returns error and skips",
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{{
							ID: 1, Name: "feed", Enabled: true,
							OracleType: "chainlink_feed", PriceDecimals: 8,
						}}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{{
							ID: 1, OracleID: 1, TokenID: 1, Enabled: true,
							FeedAddress: nil, // missing
						}}, nil
					},
					getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
						return map[int64][]byte{1: wethAddr.Bytes()}, nil
					},
				}
			},
			wantCount: 0,
		},
		{
			name: "feed oracle defaults quote currency to USD",
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{{
							ID: 1, Name: "feed", Enabled: true,
							OracleType: "chainlink_feed", PriceDecimals: 8,
						}}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{{
							ID: 1, OracleID: 1, TokenID: 1, Enabled: true,
							FeedAddress: feedAddr.Bytes(), FeedDecimals: nil, QuoteCurrency: "",
						}}, nil
					},
					getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
						return map[int64][]byte{1: wethAddr.Bytes()}, nil
					},
				}
			},
			wantCount: 1,
			checkUnits: func(t *testing.T, units []*OracleUnit) {
				t.Helper()
				if units[0].Feeds[0].QuoteCurrency != "USD" {
					t.Errorf("QuoteCurrency = %q, want USD", units[0].Feeds[0].QuoteCurrency)
				}
				if units[0].Feeds[0].FeedDecimals != 8 {
					t.Errorf("FeedDecimals = %d, want 8 (inherited from oracle)", units[0].Feeds[0].FeedDecimals)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.setupRepo()
			logger := testutil.DiscardLogger()

			units, err := LoadOracleUnits(context.Background(), repo, logger)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tt.errContains != "" && !contains(err.Error(), tt.errContains) {
					t.Errorf("error %q does not contain %q", err, tt.errContains)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(units) != tt.wantCount {
				t.Fatalf("units count = %d, want %d", len(units), tt.wantCount)
			}
			if tt.checkUnits != nil {
				tt.checkUnits(t, units)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestBuildRefFeedIdx
// ---------------------------------------------------------------------------

func TestBuildRefFeedIdx(t *testing.T) {
	wethAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	wbtcAddr := common.HexToAddress("0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599")
	daiAddr := common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")

	tests := []struct {
		name           string
		feeds          []blockchain.FeedConfig
		tokenAddrBytes map[int64][]byte
		wantRefFeedIdx map[string]int
		wantNonUSD     map[int]string
	}{
		{
			name:           "empty feeds",
			feeds:          nil,
			tokenAddrBytes: nil,
			wantRefFeedIdx: map[string]int{},
			wantNonUSD:     map[int]string{},
		},
		{
			name: "all USD feeds with WETH reference",
			feeds: []blockchain.FeedConfig{
				{TokenID: 1, QuoteCurrency: "USD"}, // WETH/USD
				{TokenID: 2, QuoteCurrency: "USD"}, // DAI/USD
			},
			tokenAddrBytes: map[int64][]byte{
				1: wethAddr.Bytes(),
				2: daiAddr.Bytes(),
			},
			wantRefFeedIdx: map[string]int{"ETH": 0},
			wantNonUSD:     map[int]string{},
		},
		{
			name: "non-USD feed with ETH reference",
			feeds: []blockchain.FeedConfig{
				{TokenID: 1, QuoteCurrency: "USD"}, // WETH/USD -> ref for ETH
				{TokenID: 2, QuoteCurrency: "ETH"}, // some/ETH -> non-USD
				{TokenID: 3, QuoteCurrency: "USD"}, // DAI/USD
			},
			tokenAddrBytes: map[int64][]byte{
				1: wethAddr.Bytes(),
				2: daiAddr.Bytes(),
				3: daiAddr.Bytes(),
			},
			wantRefFeedIdx: map[string]int{"ETH": 0},
			wantNonUSD:     map[int]string{1: "ETH"},
		},
		{
			name: "both ETH and BTC references",
			feeds: []blockchain.FeedConfig{
				{TokenID: 1, QuoteCurrency: "USD"}, // WETH/USD
				{TokenID: 2, QuoteCurrency: "USD"}, // WBTC/USD
				{TokenID: 3, QuoteCurrency: "ETH"}, // X/ETH
				{TokenID: 4, QuoteCurrency: "BTC"}, // Y/BTC
			},
			tokenAddrBytes: map[int64][]byte{
				1: wethAddr.Bytes(),
				2: wbtcAddr.Bytes(),
				3: daiAddr.Bytes(),
				4: daiAddr.Bytes(),
			},
			wantRefFeedIdx: map[string]int{"ETH": 0, "BTC": 1},
			wantNonUSD:     map[int]string{2: "ETH", 3: "BTC"},
		},
		{
			name: "token address not found is skipped",
			feeds: []blockchain.FeedConfig{
				{TokenID: 999, QuoteCurrency: "USD"},
			},
			tokenAddrBytes: map[int64][]byte{},
			wantRefFeedIdx: map[string]int{},
			wantNonUSD:     map[int]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			refFeedIdx, nonUSD := BuildRefFeedIdx(tt.feeds, tt.tokenAddrBytes)

			if len(refFeedIdx) != len(tt.wantRefFeedIdx) {
				t.Fatalf("refFeedIdx len = %d, want %d", len(refFeedIdx), len(tt.wantRefFeedIdx))
			}
			for k, v := range tt.wantRefFeedIdx {
				if refFeedIdx[k] != v {
					t.Errorf("refFeedIdx[%s] = %d, want %d", k, refFeedIdx[k], v)
				}
			}

			if len(nonUSD) != len(tt.wantNonUSD) {
				t.Fatalf("nonUSDFeeds len = %d, want %d", len(nonUSD), len(tt.wantNonUSD))
			}
			for k, v := range tt.wantNonUSD {
				if nonUSD[k] != v {
					t.Errorf("nonUSDFeeds[%d] = %q, want %q", k, nonUSD[k], v)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestConvertNonUSDPrices
// ---------------------------------------------------------------------------

func TestConvertNonUSDPrices(t *testing.T) {
	tests := []struct {
		name    string
		results []blockchain.FeedPriceResult
		unit    *OracleUnit
		want    []blockchain.FeedPriceResult
	}{
		{
			name: "no non-USD feeds is a no-op",
			results: []blockchain.FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true},
			},
			unit: &OracleUnit{
				Oracle:      &entity.Oracle{Name: "test"},
				Feeds:       []blockchain.FeedConfig{{TokenID: 1}},
				NonUSDFeeds: map[int]string{},
				RefFeedIdx:  map[string]int{},
			},
			want: []blockchain.FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true},
			},
		},
		{
			name: "converts ETH-quoted feed to USD",
			results: []blockchain.FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true}, // WETH/USD reference
				{TokenID: 2, Price: 0.5, Success: true},    // X/ETH
			},
			unit: &OracleUnit{
				Oracle:      &entity.Oracle{Name: "test"},
				Feeds:       []blockchain.FeedConfig{{TokenID: 1}, {TokenID: 2}},
				NonUSDFeeds: map[int]string{1: "ETH"},
				RefFeedIdx:  map[string]int{"ETH": 0},
			},
			want: []blockchain.FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true},
				{TokenID: 2, Price: 1000.0, Success: true}, // 0.5 * 2000
			},
		},
		{
			name: "reference feed failed marks non-USD feed as failed",
			results: []blockchain.FeedPriceResult{
				{TokenID: 1, Price: 0, Success: false},  // WETH/USD failed
				{TokenID: 2, Price: 0.5, Success: true}, // X/ETH
			},
			unit: &OracleUnit{
				Oracle:      &entity.Oracle{Name: "test"},
				Feeds:       []blockchain.FeedConfig{{TokenID: 1}, {TokenID: 2}},
				NonUSDFeeds: map[int]string{1: "ETH"},
				RefFeedIdx:  map[string]int{"ETH": 0},
			},
			want: []blockchain.FeedPriceResult{
				{TokenID: 1, Price: 0, Success: false},
				{TokenID: 2, Price: 0.5, Success: false}, // marked failed
			},
		},
		{
			name: "no reference feed available marks non-USD feed as failed",
			results: []blockchain.FeedPriceResult{
				{TokenID: 1, Price: 0.5, Success: true},
			},
			unit: &OracleUnit{
				Oracle:      &entity.Oracle{Name: "test"},
				Feeds:       []blockchain.FeedConfig{{TokenID: 1}},
				NonUSDFeeds: map[int]string{0: "ETH"},
				RefFeedIdx:  map[string]int{}, // no ETH ref
			},
			want: []blockchain.FeedPriceResult{
				{TokenID: 1, Price: 0.5, Success: false},
			},
		},
		{
			name: "non-USD feed already failed is skipped",
			results: []blockchain.FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true},
				{TokenID: 2, Price: 0, Success: false}, // already failed
			},
			unit: &OracleUnit{
				Oracle:      &entity.Oracle{Name: "test"},
				Feeds:       []blockchain.FeedConfig{{TokenID: 1}, {TokenID: 2}},
				NonUSDFeeds: map[int]string{1: "ETH"},
				RefFeedIdx:  map[string]int{"ETH": 0},
			},
			want: []blockchain.FeedPriceResult{
				{TokenID: 1, Price: 2000.0, Success: true},
				{TokenID: 2, Price: 0, Success: false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := testutil.DiscardLogger()
			ConvertNonUSDPrices(tt.results, tt.unit, logger, 12345)

			for i, got := range tt.results {
				want := tt.want[i]
				if got.Success != want.Success {
					t.Errorf("result[%d].Success = %v, want %v", i, got.Success, want.Success)
				}
				if got.Price != want.Price {
					t.Errorf("result[%d].Price = %f, want %f", i, got.Price, want.Price)
				}
			}
		})
	}
}

func intPtr(v int) *int { return &v }

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsHelper(s, substr))
}

func containsHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

package oracle_price_worker

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const (
	unitLastSuccessMetric   = "oracle.unit.last_success_timestamp_seconds"
	unitPricesFetchedMetric = "oracle.unit.prices_fetched"
)

// nowSeconds mirrors the gauge's fractional-second resolution so tests can
// bracket a recording between two captures without colliding on whole-second
// boundaries.
func nowSeconds() float64 {
	return float64(time.Now().UnixNano()) / 1e9
}

// newFreshnessService builds a started service wired to an in-memory metric
// reader so tests can assert on the per-unit freshness instruments. factory
// lets multi-unit tests hand different multicallers to different oracle types.
func newFreshnessService(t *testing.T, repo *mockRepo, factory MulticallerFactory) (*Service, sdkmetric.Reader) {
	t.Helper()

	tel, reader := newRecordingTelemetry(t)

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	svc, err := NewService(validConfig(), consumer, defaultBlockCacheReader(), repo, factory)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	svc.WithTelemetry(tel)

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		if err := svc.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})
	svc.decimalsValidated = true

	return svc, reader
}

func freshnessBlockEvent(blockNumber int64) outbound.BlockEvent {
	return outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        0,
		BlockHash:      "0x00000000000000000000000000000000000000000000000000c0ffee00000010",
		BlockTimestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix(),
	}
}

// findMetricData collects from reader and returns the named metric's data
// asserted to aggregation type D, reporting whether the metric was recorded
// at all.
func findMetricData[D metricdata.Aggregation](t *testing.T, reader sdkmetric.Reader, name string) (D, bool) {
	t.Helper()
	var zero D
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("collecting metrics: %v", err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != name {
				continue
			}
			d, ok := m.Data.(D)
			if !ok {
				t.Fatalf("metric %q is %T, want %T", name, m.Data, zero)
			}
			return d, true
		}
	}
	return zero, false
}

func gaugePoints(t *testing.T, reader sdkmetric.Reader, name string) []metricdata.DataPoint[float64] {
	t.Helper()
	g, ok := findMetricData[metricdata.Gauge[float64]](t, reader, name)
	if !ok {
		return nil
	}
	return g.DataPoints
}

func counterPoints(t *testing.T, reader sdkmetric.Reader, name string) []metricdata.DataPoint[int64] {
	t.Helper()
	sum, ok := findMetricData[metricdata.Sum[int64]](t, reader, name)
	if !ok {
		return nil
	}
	return sum.DataPoints
}

// gaugePointFor returns the single gauge data point labelled with oracleName,
// failing the test if it is absent or duplicated.
func gaugePointFor(t *testing.T, reader sdkmetric.Reader, oracleName string) metricdata.DataPoint[float64] {
	t.Helper()
	var found []metricdata.DataPoint[float64]
	for _, p := range gaugePoints(t, reader, unitLastSuccessMetric) {
		if v, ok := p.Attributes.Value("oracle.name"); ok && v.AsString() == oracleName {
			found = append(found, p)
		}
	}
	if len(found) != 1 {
		t.Fatalf("%s points for oracle %q = %d, want 1", unitLastSuccessMetric, oracleName, len(found))
	}
	return found[0]
}

// TestStart_BaselinesFreshnessForLoadedUnits pins the startup baseline: a unit
// that never completes a successful pass would otherwise never create its
// series, and the staleness alert cannot age an absent series (a restart
// would also silently resolve a firing alert forever).
func TestStart_BaselinesFreshnessForLoadedUnits(t *testing.T) {
	repo := &mockRepo{}
	feedOracleSetup(repo)
	mc := newFeedMulticaller(t, []*big.Int{big.NewInt(200_000_000_000)})

	before := nowSeconds()
	_, reader := newFreshnessService(t, repo, multicallFactoryFor(mc))
	after := nowSeconds()

	p := gaugePointFor(t, reader, "chainlink")
	if v, ok := p.Attributes.Value("chain"); !ok || v.AsString() != "mainnet" {
		t.Errorf("chain attribute = %q (present=%v), want %q", v.AsString(), ok, "mainnet")
	}
	if p.Value < before || p.Value > after {
		t.Errorf("baseline gauge value = %f, want within [%f, %f]", p.Value, before, after)
	}
}

func TestProcessBlock_UnitSuccessAdvancesFreshnessGauge(t *testing.T) {
	repo := &mockRepo{}
	feedOracleSetup(repo)
	mc := newFeedMulticaller(t, []*big.Int{big.NewInt(200_000_000_000)})
	svc, reader := newFreshnessService(t, repo, multicallFactoryFor(mc))

	before := nowSeconds()
	if err := svc.processBlock(context.Background(), freshnessBlockEvent(18000000)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	after := nowSeconds()

	p := gaugePointFor(t, reader, "chainlink")
	if v, ok := p.Attributes.Value("chain"); !ok || v.AsString() != "mainnet" {
		t.Errorf("chain attribute = %q (present=%v), want %q", v.AsString(), ok, "mainnet")
	}
	if p.Value < before || p.Value > after {
		t.Errorf("gauge value = %f, want within [%f, %f] (advanced past the startup baseline)", p.Value, before, after)
	}
}

// TestProcessBlock_UnchangedPricesStillAdvanceFreshness pins the change-only
// semantics: a pass whose prices all match the cache writes no rows but must
// still refresh the unit's staleness gauge, otherwise a frozen-but-healthy
// upstream would look identical to a dead unit.
func TestProcessBlock_UnchangedPricesStillAdvanceFreshness(t *testing.T) {
	repo := &mockRepo{}
	feedOracleSetup(repo)
	// Cache already holds the exact price the feed answers with, so change
	// detection suppresses every row.
	repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
		return map[int64]float64{1: 2000.0}, nil
	}
	mc := newFeedMulticaller(t, []*big.Int{big.NewInt(200_000_000_000)})
	svc, reader := newFreshnessService(t, repo, multicallFactoryFor(mc))

	before := nowSeconds()
	if err := svc.processBlock(context.Background(), freshnessBlockEvent(18000000)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}
	after := nowSeconds()

	repo.mu.Lock()
	upserts := repo.upsertPricesCalls
	repo.mu.Unlock()
	if upserts != 0 {
		t.Fatalf("UpsertPrices calls = %d, want 0 (prices unchanged)", upserts)
	}

	p := gaugePointFor(t, reader, "chainlink")
	if p.Value < before || p.Value > after {
		t.Errorf("gauge value = %f, want within [%f, %f]", p.Value, before, after)
	}
}

func TestProcessBlock_FailingUnitDoesNotAdvanceFreshness(t *testing.T) {
	repo := &mockRepo{}
	feedOracleSetup(repo)
	mc := &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			return nil, errors.New("rpc down")
		},
	}
	svc, reader := newFreshnessService(t, repo, multicallFactoryFor(mc))
	afterStart := nowSeconds()

	if err := svc.processBlock(context.Background(), freshnessBlockEvent(18000000)); err == nil {
		t.Fatal("processBlock should fail when the unit's fetch fails")
	}

	p := gaugePointFor(t, reader, "chainlink")
	if p.Value > afterStart {
		t.Errorf("gauge value = %f, want <= %f (startup baseline; a failed pass must not advance it)", p.Value, afterStart)
	}
}

// TestProcessBlock_UpsertFailureDoesNotAdvanceFreshness guards the DB-write
// half of "successful pass": freshness recorded before or inside the store
// path would make hours of failing writes read as fresh.
func TestProcessBlock_UpsertFailureDoesNotAdvanceFreshness(t *testing.T) {
	repo := &mockRepo{}
	feedOracleSetup(repo)
	repo.upsertPricesFn = func(_ context.Context, _ []*entity.OnchainTokenPrice) error {
		return errors.New("db down")
	}
	mc := newFeedMulticaller(t, []*big.Int{big.NewInt(200_000_000_000)})
	svc, reader := newFreshnessService(t, repo, multicallFactoryFor(mc))
	afterStart := nowSeconds()

	if err := svc.processBlock(context.Background(), freshnessBlockEvent(18000000)); err == nil {
		t.Fatal("processBlock should fail when the upsert fails")
	}

	p := gaugePointFor(t, reader, "chainlink")
	if p.Value > afterStart {
		t.Errorf("gauge value = %f, want <= %f (startup baseline; a failed upsert must not advance it)", p.Value, afterStart)
	}
}

// multiUnitRepoSetup seeds two units: the chainlink feed oracle (ID 1) as in
// feedOracleSetup plus the sparklend aave oracle (ID 2) with one asset.
func multiUnitRepoSetup(repo *mockRepo) {
	feedAddr := common.HexToAddress("0x0000000000000000000000000000000000000F01")
	wethAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	daiAddr := common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")

	repo.getEnabledOraclesByChainFn = func(_ context.Context, _ int64) ([]*entity.Oracle, error) {
		return []*entity.Oracle{
			{
				ID: 1, Name: "chainlink", Enabled: true,
				OracleType: entity.OracleTypeChainlinkFeed, PriceDecimals: 8,
			},
			{
				ID: 2, Name: "sparklend", Enabled: true, ChainID: 1,
				Address:    common.HexToAddress("0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD9"),
				OracleType: entity.OracleTypeAave, PriceDecimals: 8,
			},
		}, nil
	}
	repo.getEnabledAssetsFn = func(_ context.Context, oracleID int64) ([]*entity.OracleAsset, error) {
		if oracleID == 1 {
			return []*entity.OracleAsset{{
				ID: 1, OracleID: 1, TokenID: 1, Enabled: true,
				FeedAddress: feedAddr, FeedDecimals: 8, QuoteCurrency: "USD",
			}}, nil
		}
		return []*entity.OracleAsset{{ID: 2, OracleID: 2, TokenID: 2, Enabled: true}}, nil
	}
	repo.getTokenInfosFn = func(_ context.Context, oracleID int64) (map[int64]outbound.TokenInfo, error) {
		if oracleID == 1 {
			return map[int64]outbound.TokenInfo{1: {Address: wethAddr.Bytes()}}, nil
		}
		return map[int64]outbound.TokenInfo{2: {Address: daiAddr.Bytes()}}, nil
	}
	repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
		return map[int64]float64{}, nil
	}
}

// TestProcessBlock_PartialFailureAdvancesOnlyHealthyUnit pins per-unit
// attribution under partial failure: one broken unit fails the block (so SQS
// redelivers) but must not stop the healthy sibling's freshness, and must not
// borrow it either.
func TestProcessBlock_PartialFailureAdvancesOnlyHealthyUnit(t *testing.T) {
	repo := &mockRepo{}
	multiUnitRepoSetup(repo)

	feedMC := newFeedMulticaller(t, []*big.Int{big.NewInt(200_000_000_000)})
	failingMC := &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			return nil, errors.New("rpc down")
		},
	}
	factory := func(ot entity.OracleType) (outbound.Multicaller, error) {
		if ot == entity.OracleTypeAave {
			return failingMC, nil
		}
		return feedMC, nil
	}

	svc, reader := newFreshnessService(t, repo, factory)
	afterStart := nowSeconds()

	if err := svc.processBlock(context.Background(), freshnessBlockEvent(18000000)); err == nil {
		t.Fatal("processBlock should fail while one unit is broken")
	}

	healthy := gaugePointFor(t, reader, "chainlink")
	if healthy.Value < afterStart {
		t.Errorf("healthy unit gauge = %f, want >= %f (must advance despite the broken sibling)", healthy.Value, afterStart)
	}
	broken := gaugePointFor(t, reader, "sparklend")
	if broken.Value > afterStart {
		t.Errorf("broken unit gauge = %f, want <= %f (startup baseline only)", broken.Value, afterStart)
	}
}

func TestProcessBlock_FeedOracle_RecordsPricesFetched(t *testing.T) {
	repo := &mockRepo{}
	feedOracleSetup(repo)
	mc := newFeedMulticaller(t, []*big.Int{big.NewInt(200_000_000_000)})
	svc, reader := newFreshnessService(t, repo, multicallFactoryFor(mc))

	if err := svc.processBlock(context.Background(), freshnessBlockEvent(18000000)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	pts := counterPoints(t, reader, unitPricesFetchedMetric)
	if len(pts) != 1 {
		t.Fatalf("%s data points = %d, want 1", unitPricesFetchedMetric, len(pts))
	}
	p := pts[0]
	if v, ok := p.Attributes.Value("oracle.name"); !ok || v.AsString() != "chainlink" {
		t.Errorf("oracle.name attribute = %q (present=%v), want %q", v.AsString(), ok, "chainlink")
	}
	if p.Value != 1 {
		t.Errorf("counter value = %d, want 1", p.Value)
	}
}

// TestProcessBlock_MixedFeedResults_CountsOnlySuccessfulFetches pins the
// counting predicate itself: with one healthy and one reverting feed in the
// same unit, only the healthy read counts.
func TestProcessBlock_MixedFeedResults_CountsOnlySuccessfulFetches(t *testing.T) {
	repo := &mockRepo{}
	feedOracleSetup(repo)
	// Two feeds, but the multicaller only answers the first; the second comes
	// back Success=false (a reverting feed).
	repo.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
		return []*entity.OracleAsset{
			{
				ID: 1, OracleID: 1, TokenID: 1, Enabled: true,
				FeedAddress:  common.HexToAddress("0x0000000000000000000000000000000000000F01"),
				FeedDecimals: 8, QuoteCurrency: "USD",
			},
			{
				ID: 2, OracleID: 1, TokenID: 2, Enabled: true,
				FeedAddress:  common.HexToAddress("0x0000000000000000000000000000000000000F02"),
				FeedDecimals: 8, QuoteCurrency: "USD",
			},
		}, nil
	}
	repo.getTokenInfosFn = func(_ context.Context, _ int64) (map[int64]outbound.TokenInfo, error) {
		return map[int64]outbound.TokenInfo{
			1: {Address: common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").Bytes()},
			2: {Address: common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F").Bytes()},
		}, nil
	}
	// Discriminate by call target, not batch index: FetchFeedPrices retries a
	// failed feed via latestAnswer in a second batch where it sits at index 0,
	// so an index-keyed mock would let the dark feed "recover" on retry.
	healthyFeed := common.HexToAddress("0x0000000000000000000000000000000000000F01")
	mc := &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			results := make([]outbound.Result, len(calls))
			for i, c := range calls {
				if c.Target == healthyFeed {
					results[i] = outbound.Result{
						Success: true,
						ReturnData: testutil.PackLatestRoundData(t,
							big.NewInt(1), big.NewInt(200_000_000_000), big.NewInt(1000), big.NewInt(1000), big.NewInt(1)),
					}
				} else {
					results[i] = outbound.Result{Success: false}
				}
			}
			return results, nil
		},
	}
	svc, reader := newFreshnessService(t, repo, multicallFactoryFor(mc))

	if err := svc.processBlock(context.Background(), freshnessBlockEvent(18000000)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	pts := counterPoints(t, reader, unitPricesFetchedMetric)
	if len(pts) != 1 {
		t.Fatalf("%s data points = %d, want 1", unitPricesFetchedMetric, len(pts))
	}
	if pts[0].Value != 1 {
		t.Errorf("counter value = %d, want 1 (only the successful feed counts)", pts[0].Value)
	}
}

// TestProcessBlock_AllFeedsFailing_RecordsZeroPricesFetched pins the signal
// for the silent failure mode where every feed of a unit reverts forever:
// the pass "succeeds" (reverting feeds are guard-skipped, not errored), so
// the only honest evidence is a fetched count of zero. Zero must be recorded
// (not skipped) so the series exists and a zero rate is queryable.
func TestProcessBlock_AllFeedsFailing_RecordsZeroPricesFetched(t *testing.T) {
	repo := &mockRepo{}
	feedOracleSetup(repo)
	mc := newFeedMulticaller(t, nil) // every feed result comes back Success=false
	svc, reader := newFreshnessService(t, repo, multicallFactoryFor(mc))

	if err := svc.processBlock(context.Background(), freshnessBlockEvent(18000000)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	pts := counterPoints(t, reader, unitPricesFetchedMetric)
	if len(pts) != 1 {
		t.Fatalf("%s data points = %d, want 1", unitPricesFetchedMetric, len(pts))
	}
	if pts[0].Value != 0 {
		t.Errorf("counter value = %d, want 0", pts[0].Value)
	}
}

// TestProcessBlock_AllFeedsFailing_StillAdvancesFreshness documents the
// per-ORACLE granularity decision: the freshness gauge tracks "the worker
// successfully processed this unit", so a unit whose individual feeds are all
// dark still reads fresh. Feed-level darkness is the fetched counter's job
// (see TestProcessBlock_AllFeedsFailing_RecordsZeroPricesFetched).
func TestProcessBlock_AllFeedsFailing_StillAdvancesFreshness(t *testing.T) {
	repo := &mockRepo{}
	feedOracleSetup(repo)
	mc := newFeedMulticaller(t, nil)
	svc, reader := newFreshnessService(t, repo, multicallFactoryFor(mc))

	before := nowSeconds()
	if err := svc.processBlock(context.Background(), freshnessBlockEvent(18000000)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	if p := gaugePointFor(t, reader, "chainlink"); p.Value < before {
		t.Errorf("gauge value = %f, want >= %f (advanced past the startup baseline)", p.Value, before)
	}
}

// TestProcessBlock_AaveOracle_RecordsPricesFetched covers the aave path's
// counting: usable quotes count, the zero-price guard's skips do not.
func TestProcessBlock_AaveOracle_RecordsPricesFetched(t *testing.T) {
	repo := &mockRepo{}
	defaultRepoSetup(repo)
	mc := newOracleMulticallerWithT(t, []*big.Int{big.NewInt(250_000_000_000), big.NewInt(0)})
	svc, reader := newFreshnessService(t, repo, multicallFactoryFor(mc))

	if err := svc.processBlock(context.Background(), freshnessBlockEvent(18000000)); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	pts := counterPoints(t, reader, unitPricesFetchedMetric)
	if len(pts) != 1 {
		t.Fatalf("%s data points = %d, want 1", unitPricesFetchedMetric, len(pts))
	}
	p := pts[0]
	if v, ok := p.Attributes.Value("oracle.name"); !ok || v.AsString() != "sparklend" {
		t.Errorf("oracle.name attribute = %q (present=%v), want %q", v.AsString(), ok, "sparklend")
	}
	if p.Value != 1 {
		t.Errorf("counter value = %d, want 1 (zero-price quote must not count)", p.Value)
	}
}

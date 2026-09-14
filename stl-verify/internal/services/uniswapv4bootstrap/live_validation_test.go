//go:build livevalidation

// Manual, Alchemy-backed gate. Never compiled into CI; run it by hand with
//
//	ALCHEMY_API_KEY=… go test -tags=livevalidation -run TestLiveValidation ./internal/services/uniswapv4bootstrap

package uniswapv4bootstrap

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/alchemy"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4indexer"
)

const poolManagerDeployBlock = int64(21688329)

func liveClient(t *testing.T) *alchemy.Client {
	t.Helper()
	key := os.Getenv("ALCHEMY_API_KEY")
	if key == "" {
		t.Fatal("ALCHEMY_API_KEY must be set to run TestLiveValidation")
	}
	client, err := alchemy.NewClient(alchemy.ClientConfig{
		HTTPURL: "https://eth-mainnet.g.alchemy.com/v2/" + key,
		Timeout: 60 * time.Second,
		Logger:  testLogger(),
	})
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return client
}

// Empty poolIDs leaves topic1 unfiltered: one seeded pool emits only a few
// hundred logs across all of V4 history, far under any provider's cap.
func liveFilter(t *testing.T, poolIDs ...common.Hash) outbound.LogFilter {
	t.Helper()
	topic0, err := uniswapv4indexer.ModifyLiquidityTopic0()
	if err != nil {
		t.Fatalf("ModifyLiquidityTopic0: %v", err)
	}
	return outbound.LogFilter{
		Address: common.HexToAddress(poolManagerAddr),
		Topic0:  topic0,
		Topic1:  poolIDs,
	}
}

func TestLiveValidation_GetLogsReturnsDecodableModifyLiquidityLogs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	pool := testPool(poolAFixture, true)
	filter := liveFilter(t, pool.PoolIDHash)
	filter.FromBlock = pool.DeployBlock
	filter.ToBlock = pool.DeployBlock + 200_000

	logs, err := liveClient(t).GetLogs(ctx, filter)
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) == 0 {
		t.Fatal("no ModifyLiquidity logs in the pool's first 200k blocks; the filter is wrong")
	}

	poolsByHash := map[common.Hash]uniswapv4indexer.RegisteredPool{pool.PoolIDHash: pool}
	keys, err := uniswapv4indexer.PositionKeysFromLogs(toSharedLogs(logs), poolsByHash, common.HexToAddress(poolManagerAddr))
	if err != nil {
		t.Fatalf("decoding %d live logs: %v", len(logs), err)
	}
	if len(keys[pool.ID]) == 0 {
		t.Fatalf("%d live logs decoded into no position keys", len(logs))
	}
	t.Logf("decoded %d live logs into %d distinct position keys", len(logs), len(keys[pool.ID]))
}

// The bisect only works while the adapter still recognises the provider's
// refusal wording, which is prose it can change at any time.
func TestLiveValidation_OversizedRangeIsClassifiedAsARangeRefusal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	client := liveClient(t)
	head, err := client.GetCurrentBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetCurrentBlockNumber: %v", err)
	}

	filter := liveFilter(t)
	filter.FromBlock = poolManagerDeployBlock
	filter.ToBlock = head - DefaultFinalityDepth

	_, err = client.GetLogs(ctx, filter)
	if !errors.Is(err, outbound.ErrLogRangeTooLarge) {
		t.Fatalf("error = %v, want it to wrap ErrLogRangeTooLarge: the provider's refusal wording has drifted from rangeRefusalPhrases", err)
	}
	t.Logf("provider refused %d blocks as expected: %v", filter.ToBlock-filter.FromBlock+1, err)
}

// Enough for several windows, few enough to stay a bounded number of requests.
const liveScanBlocks = int64(100_000)

// Deliberately asserts nothing about narrowings: how often the provider refuses
// depends on chain density in the fortnight it runs.
func TestLiveValidation_AdaptiveScanCoversTheRangeAgainstTheRealProvider(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	head, err := liveClient(t).GetCurrentBlockNumber(ctx)
	if err != nil {
		t.Fatalf("GetCurrentBlockNumber: %v", err)
	}
	to := head - DefaultFinalityDepth
	from := to - liveScanBlocks + 1

	scanner := &logWindowScanner{
		client: liveClient(t),
		filter: liveFilter(t),
		policy: windowPolicy{initial: liveScanBlocks, min: DefaultMinWindow, max: liveScanBlocks},
		logger: testLogger(),
	}

	next := from
	stats, err := scanner.scan(ctx, from, to, func(w logWindow) error {
		if w.from != next {
			t.Errorf("window starts at %d, want %d: the scan left a gap", w.from, next)
		}
		next = w.to + 1
		return nil
	})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if next != to+1 {
		t.Errorf("scan stopped at %d, want %d", next-1, to)
	}
	t.Logf("scanned %d blocks in %d windows with %d narrowings, %d logs", to-from+1, stats.windows, stats.narrowings, stats.logs)
}

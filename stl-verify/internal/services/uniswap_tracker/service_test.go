package uniswap_tracker

import (
	"io"
	"log/slog"
	"math/big"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/uniswapv3"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ── DefaultPools ──

func TestDefaultPools(t *testing.T) {
	pools := TrackedPools()

	if len(pools) != 1 {
		t.Fatalf("expected 1 pool, got %d", len(pools))
	}

	p := pools[0]
	if p.Address != common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d") {
		t.Errorf("unexpected pool address: %s", p.Address.Hex())
	}
	if p.ChainID != 1 {
		t.Errorf("expected chainID 1, got %d", p.ChainID)
	}
	if p.Name != "AUSD/USDC 0.01%" {
		t.Errorf("unexpected name: %s", p.Name)
	}
}

// ── PoolsForChainID ──

func TestPoolsForChainID(t *testing.T) {
	pools := []PoolConfig{
		{Address: common.HexToAddress("0xaaa"), ChainID: 1, Name: "eth-pool"},
		{Address: common.HexToAddress("0xbbb"), ChainID: 10143, Name: "monad-pool"},
	}

	ethPools := PoolsForChainID(pools, 1)
	if len(ethPools) != 1 {
		t.Fatalf("expected 1 Ethereum pool, got %d", len(ethPools))
	}

	monadPools := PoolsForChainID(pools, 10143)
	if len(monadPools) != 1 {
		t.Fatalf("expected 1 Monad pool, got %d", len(monadPools))
	}

	noPools := PoolsForChainID(pools, 999)
	if len(noPools) != 0 {
		t.Fatalf("expected 0 pools for chain 999, got %d", len(noPools))
	}
}

// ── Price Computation ──

func TestComputePrice_OneToOne(t *testing.T) {
	// sqrtPriceX96 for a 1:1 pool = 2^96
	q96 := new(big.Int).Lsh(big.NewInt(1), 96)
	price := uniswapv3.ComputePrice(q96)

	if price[:3] != "1.0" {
		t.Errorf("expected price ~1.0 for sqrtPriceX96=2^96, got %s", price)
	}
}

func TestComputePriceFromTick_Zero(t *testing.T) {
	price := uniswapv3.ComputePriceFromTick(0)
	if price[:3] != "1.0" {
		t.Errorf("expected price 1.0 for tick 0, got %s", price)
	}
}

func TestComputePriceFromTick_Positive(t *testing.T) {
	price := uniswapv3.ComputePriceFromTick(100)
	if price[:4] != "1.01" {
		t.Errorf("expected price ~1.01 for tick 100, got %s", price)
	}
}

// ── TVL Calculation ──

func TestCalculateTVL_StablecoinPool(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	snap := &PoolSnapshot{
		Token0: TokenBalance{Balance: "12000000000000", Decimals: 6, Symbol: "AUSD"},
		Token1: TokenBalance{Balance: "12000000000000", Decimals: 6, Symbol: "USDC"},
	}

	tvl := svc.calculateTVL(snap)
	if tvl == nil {
		t.Fatal("expected non-nil TVL")
	}

	tvlFloat, _ := tvl.Float64()
	if tvlFloat < 23_000_000 || tvlFloat > 25_000_000 {
		t.Errorf("expected TVL ~24M, got %.2f", tvlFloat)
	}
}

func TestCalculateTVL_Empty(t *testing.T) {
	svc := &Service{logger: slog.New(slog.NewTextHandler(io.Discard, nil))}

	tvl := svc.calculateTVL(&PoolSnapshot{})
	if tvl != nil {
		t.Error("expected nil TVL for empty pool")
	}
}

// ── Entity Validation ──

func TestUniswapPoolSnapshot_Validate(t *testing.T) {
	validAddr := common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d").Bytes()
	now := time.Now()

	tests := []struct {
		name    string
		snap    entity.UniswapPoolSnapshot
		wantErr bool
	}{
		{
			name: "valid",
			snap: entity.UniswapPoolSnapshot{
				PoolAddress:  validAddr,
				ChainID:      1,
				BlockNumber:  100,
				Fee:          100,
				SnapshotTime: now,
			},
			wantErr: false,
		},
		{
			name: "invalid pool address",
			snap: entity.UniswapPoolSnapshot{
				PoolAddress:  []byte{0x01},
				ChainID:      1,
				BlockNumber:  100,
				Fee:          100,
				SnapshotTime: now,
			},
			wantErr: true,
		},
		{
			name: "missing chain ID",
			snap: entity.UniswapPoolSnapshot{
				PoolAddress:  validAddr,
				BlockNumber:  100,
				Fee:          100,
				SnapshotTime: now,
			},
			wantErr: true,
		},
		{
			name: "missing block number",
			snap: entity.UniswapPoolSnapshot{
				PoolAddress:  validAddr,
				ChainID:      1,
				Fee:          100,
				SnapshotTime: now,
			},
			wantErr: true,
		},
		{
			name: "missing fee",
			snap: entity.UniswapPoolSnapshot{
				PoolAddress:  validAddr,
				ChainID:      1,
				BlockNumber:  100,
				SnapshotTime: now,
			},
			wantErr: true,
		},
		{
			name: "zero snapshot_time",
			snap: entity.UniswapPoolSnapshot{
				PoolAddress: validAddr,
				ChainID:     1,
				BlockNumber: 100,
				Fee:         100,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.snap.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

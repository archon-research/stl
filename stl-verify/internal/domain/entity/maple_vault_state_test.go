package entity

import (
	"math/big"
	"testing"
	"time"
)

func TestNewMapleVaultState_Valid(t *testing.T) {
	ts := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	s, err := NewMapleVaultState(
		1, 18_500_000, 0, ts,
		big.NewInt(1_000_000_000_000),
		big.NewInt(900_000_000_000),
		big.NewInt(1_100_000),
	)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if s.MapleVaultID != 1 || s.BlockNumber != 18_500_000 || s.BlockVersion != 0 {
		t.Fatalf("fields mis-set: %+v", s)
	}
	if s.UnderlyingPriceUSD != nil || s.SyrupPriceUSD != nil {
		t.Fatal("optional price fields should default to nil")
	}
}

func TestMapleVaultState_WithPrices(t *testing.T) {
	ts := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	s, err := NewMapleVaultState(1, 1, 0, ts,
		big.NewInt(1), big.NewInt(1), big.NewInt(1))
	if err != nil {
		t.Fatal(err)
	}
	s.WithPrices(big.NewInt(99_900_000), big.NewInt(110_000_000))
	if s.UnderlyingPriceUSD.Cmp(big.NewInt(99_900_000)) != 0 {
		t.Fatal("underlying USD mis-set")
	}
	if s.SyrupPriceUSD.Cmp(big.NewInt(110_000_000)) != 0 {
		t.Fatal("syrup USD mis-set")
	}
}

func TestNewMapleVaultState_RejectsNegativeBlock(t *testing.T) {
	if _, err := NewMapleVaultState(1, -1, 0, time.Now(),
		big.NewInt(1), big.NewInt(1), big.NewInt(1)); err == nil {
		t.Fatal("expected error on negative block")
	}
}

func TestNewMapleVaultState_RejectsZeroTimestamp(t *testing.T) {
	if _, err := NewMapleVaultState(1, 1, 0, time.Time{},
		big.NewInt(1), big.NewInt(1), big.NewInt(1)); err == nil {
		t.Fatal("expected error on zero timestamp")
	}
}

func TestNewMapleVaultState_RejectsNilNumerics(t *testing.T) {
	cases := []struct {
		name    string
		a, b, c *big.Int
	}{
		{"nil totalAssets", nil, big.NewInt(1), big.NewInt(1)},
		{"nil totalSupply", big.NewInt(1), nil, big.NewInt(1)},
		{"nil sharePrice", big.NewInt(1), big.NewInt(1), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewMapleVaultState(1, 1, 0, time.Now(), c.a, c.b, c.c); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

func TestNewMapleVaultState_RejectsZeroVaultID(t *testing.T) {
	if _, err := NewMapleVaultState(0, 1, 0, time.Now(),
		big.NewInt(1), big.NewInt(1), big.NewInt(1)); err == nil {
		t.Fatal("expected error on zero mapleVaultID")
	}
}

func TestNewMapleVaultState_RejectsNegativeBlockVersion(t *testing.T) {
	if _, err := NewMapleVaultState(1, 1, -1, time.Now(),
		big.NewInt(1), big.NewInt(1), big.NewInt(1)); err == nil {
		t.Fatal("expected error on negative blockVersion")
	}
}

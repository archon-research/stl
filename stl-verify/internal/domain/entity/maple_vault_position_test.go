package entity

import (
	"math/big"
	"testing"
	"time"
)

func TestNewMapleVaultPosition_Valid(t *testing.T) {
	ts := time.Date(2026, 5, 27, 12, 0, 0, 0, time.UTC)
	p, err := NewMapleVaultPosition(77, 1, 18_500_000, 0, ts,
		big.NewInt(1_000), big.NewInt(1_100))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if p.UserID != 77 || p.MapleVaultID != 1 || p.Shares.Int64() != 1000 || p.Assets.Int64() != 1100 {
		t.Fatalf("fields mis-set: %+v", p)
	}
}

func TestNewMapleVaultPosition_RejectsZeroUser(t *testing.T) {
	if _, err := NewMapleVaultPosition(0, 1, 1, 0, time.Now(),
		big.NewInt(1), big.NewInt(1)); err == nil {
		t.Fatal("expected error on zero userID")
	}
}

func TestNewMapleVaultPosition_RejectsZeroVault(t *testing.T) {
	if _, err := NewMapleVaultPosition(1, 0, 1, 0, time.Now(),
		big.NewInt(1), big.NewInt(1)); err == nil {
		t.Fatal("expected error on zero mapleVaultID")
	}
}

func TestNewMapleVaultPosition_RejectsNegativeBlock(t *testing.T) {
	if _, err := NewMapleVaultPosition(1, 1, -1, 0, time.Now(),
		big.NewInt(1), big.NewInt(1)); err == nil {
		t.Fatal("expected error on negative blockNumber")
	}
}

func TestNewMapleVaultPosition_RejectsZeroTimestamp(t *testing.T) {
	if _, err := NewMapleVaultPosition(1, 1, 1, 0, time.Time{},
		big.NewInt(1), big.NewInt(1)); err == nil {
		t.Fatal("expected error on zero timestamp")
	}
}

func TestNewMapleVaultPosition_RejectsNilShares(t *testing.T) {
	if _, err := NewMapleVaultPosition(1, 1, 1, 0, time.Now(),
		nil, big.NewInt(1)); err == nil {
		t.Fatal("expected error on nil shares")
	}
}

func TestNewMapleVaultPosition_RejectsNilAssets(t *testing.T) {
	if _, err := NewMapleVaultPosition(1, 1, 1, 0, time.Now(),
		big.NewInt(1), nil); err == nil {
		t.Fatal("expected error on nil assets")
	}
}

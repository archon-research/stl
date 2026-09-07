package entity

import (
	"bytes"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

// MorphoVaultCap is a snapshot of one allocation cap on a Morpho VaultV2 at a
// single block. VaultV2 caps are keyed on-chain by an opaque id (bytes32);
// CapID is that key and IDData is its decodable pre-image (id = keccak256(idData)).
//
// AbsoluteCap and RelativeCap are the two limits carried by every cap, stored as
// raw on-chain uint128 values, unscaled. AbsoluteCap is an asset-amount ceiling
// in the vault's underlying base units; RelativeCap is a WAD fraction of total
// assets (1e18 = 100%). Each row is an end-of-block snapshot: when a cap event
// fires, the indexer reads both limits off the vault at the block hash and
// writes them together, so the latest row per (vault, cap_id) is the full
// current cap state. Same-block sibling cap events read identical values and
// dedupe to one row.
type MorphoVaultCap struct {
	MorphoVaultID int64
	CapID         []byte // 32 bytes, the bytes32 cap id (keccak256 of IDData)
	IDData        []byte // ABI-encoded pre-image of CapID
	AbsoluteCap   *big.Int
	RelativeCap   *big.Int
	BlockNumber   int64
	BlockVersion  int
	Timestamp     time.Time
}

// NewMorphoVaultCap creates a new MorphoVaultCap entity with validation.
func NewMorphoVaultCap(morphoVaultID int64, capID, idData []byte, absoluteCap, relativeCap *big.Int, blockNumber int64, blockVersion int, timestamp time.Time) (*MorphoVaultCap, error) {
	c := &MorphoVaultCap{
		MorphoVaultID: morphoVaultID,
		CapID:         capID,
		IDData:        idData,
		AbsoluteCap:   absoluteCap,
		RelativeCap:   relativeCap,
		BlockNumber:   blockNumber,
		BlockVersion:  blockVersion,
		Timestamp:     timestamp,
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("NewMorphoVaultCap: %w", err)
	}
	return c, nil
}

func (c *MorphoVaultCap) Validate() error {
	if c.MorphoVaultID <= 0 {
		return fmt.Errorf("morphoVaultID must be positive, got %d", c.MorphoVaultID)
	}
	if len(c.CapID) != 32 {
		return fmt.Errorf("capID must be 32 bytes, got %d", len(c.CapID))
	}
	if len(c.IDData) == 0 {
		return fmt.Errorf("idData must not be empty")
	}
	// The cap id is keccak256(idData) on-chain; enforce it so a row can never
	// carry an id/pre-image pair that doesn't hash together (id and idData both
	// come straight from the triggering event).
	if !bytes.Equal(crypto.Keccak256(c.IDData), c.CapID) {
		return fmt.Errorf("capID must equal keccak256(idData): capID=%x keccak256(idData)=%x", c.CapID, crypto.Keccak256(c.IDData))
	}
	if err := requireNonNegativeBigInt("absoluteCap", c.AbsoluteCap); err != nil {
		return err
	}
	if err := requireNonNegativeBigInt("relativeCap", c.RelativeCap); err != nil {
		return err
	}
	if c.BlockNumber <= 0 {
		return fmt.Errorf("blockNumber must be positive, got %d", c.BlockNumber)
	}
	if c.BlockVersion < 0 {
		return fmt.Errorf("blockVersion must be non-negative, got %d", c.BlockVersion)
	}
	if c.Timestamp.IsZero() {
		return fmt.Errorf("timestamp must not be zero")
	}
	return nil
}

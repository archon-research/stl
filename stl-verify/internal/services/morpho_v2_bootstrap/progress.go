package morpho_v2_bootstrap

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/ethereum/go-ethereum/common"
)

// SweepProgress is how far a run's log sweep got, together with what that fact
// is true FOR. A later attempt resumes from a record only when every scope field
// still describes its own sweep (see Service.resumeBlock).
type SweepProgress struct {
	ChainID int64 `json:"chain_id"`
	// FromBlock is the sweep's first block: the chain's VaultV2 factory deploy
	// block.
	FromBlock int64 `json:"from_block"`
	// VaultsDigest identifies the V2 vault set the completed chunks were fetched
	// for. A record from a different set licenses no skip: its chunks were read
	// through an address filter that never mentioned the vaults it lacks.
	VaultsDigest string `json:"vaults_digest"`
	// LastCompletedTo is the last block of the last chunk whose every log
	// replayed. Recording whole chunks only is what keeps a resumed run in
	// chain order: a mid-chunk restart could replay a RemoveAdapter without the
	// AddAdapter that opened the incarnation.
	LastCompletedTo int64 `json:"last_completed_to"`
}

// sameScope reports whether both records describe the same sweep.
// LastCompletedTo is the progress, not the scope, so it is excluded.
func (p SweepProgress) sameScope(other SweepProgress) bool {
	return p.ChainID == other.ChainID &&
		p.FromBlock == other.FromBlock &&
		p.VaultsDigest == other.VaultsDigest
}

// ProgressStore persists a run's sweep progress somewhere that outlives the
// process, so a run killed mid-sweep (a deploy rolls this Deployment like any
// other) resumes instead of restarting at the factory deploy block.
//
// LoadProgress reports absence as (zero, false, nil); an error means the record
// could not be read, which is not the same thing and must not be read as "start
// from the beginning".
type ProgressStore interface {
	SaveProgress(ctx context.Context, progress SweepProgress) error
	LoadProgress(ctx context.Context) (SweepProgress, bool, error)
}

// vaultSetDigest is a deterministic sha256 over the V2 vault set, computed from
// the sorted lowercase addresses so it identifies set membership alone — not the
// order the registry returned them in, nor address checksum casing.
func vaultSetDigest(vaults []common.Address) string {
	addresses := make([]string, 0, len(vaults))
	for _, vault := range vaults {
		addresses = append(addresses, strings.ToLower(vault.Hex()))
	}
	sort.Strings(addresses)

	digest := sha256.New()
	for _, address := range addresses {
		digest.Write([]byte(address))
		digest.Write([]byte{'\n'})
	}
	return hex.EncodeToString(digest.Sum(nil))
}

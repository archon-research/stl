package curveindexer

import (
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func TestStateSnapshot_ExactlyOneClass(t *testing.T) {
	ss := StateSnapshot{Stableswap: &entity.CurveStableswapState{}}
	if ss.Stableswap == nil || ss.Cryptoswap != nil {
		t.Fatal("expected stableswap-only snapshot")
	}
}

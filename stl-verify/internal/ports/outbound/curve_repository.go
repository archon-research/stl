package outbound

import (
	"context"
	"errors"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ErrTokenNotFound is returned when a token is not in the token table.
var ErrTokenNotFound = errors.New("token not found")

type CurveRepository interface {
	SaveSnapshots(ctx context.Context, snapshots []*entity.CurvePoolSnapshot) error
	LookupTokenID(ctx context.Context, chainID int64, address common.Address) (int64, error)
}

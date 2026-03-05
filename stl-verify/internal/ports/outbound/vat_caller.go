package outbound

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

// VatCaller is the port for reading debt data from the MakerDAO/Sky Vat contract.
// *ethereum.VatCaller implements this interface.
type VatCaller interface {
	GetIlk(ctx context.Context, vaultAddress common.Address) ([32]byte, error)
	GetRate(ctx context.Context, ilk [32]byte) (*big.Int, error)
	GetNormalizedDebt(ctx context.Context, ilk [32]byte, urnAddress common.Address) (*big.Int, error)
}

package maple_indexer

import (
	"context"
	"maps"
	"math/big"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// -----------------------------------------------------------------------------
// MapleRepository stub
// -----------------------------------------------------------------------------

// mapleRepoStub is the package-local stub for outbound.MapleRepository.
// Use newRepoStub() to construct; the fields below are publicly read/written
// inside the maple_indexer test package only.
type mapleRepoStub struct {
	vaults    map[common.Address]*entity.MapleVault
	getErr    error
	stateRows []*entity.MapleVaultState
	posRows   []*entity.MapleVaultPosition
	stateErr  error
	posErr    error
}

func newRepoStub(vaults map[common.Address]*entity.MapleVault, getErr error) *mapleRepoStub {
	return &mapleRepoStub{vaults: vaults, getErr: getErr}
}

func (s *mapleRepoStub) GetAllVaults(_ context.Context, _ int64) (map[common.Address]*entity.MapleVault, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	out := make(map[common.Address]*entity.MapleVault, len(s.vaults))
	maps.Copy(out, s.vaults)
	return out, nil
}

func (s *mapleRepoStub) SaveVaultState(_ context.Context, _ pgx.Tx, state *entity.MapleVaultState) error {
	if s.stateErr != nil {
		return s.stateErr
	}
	s.stateRows = append(s.stateRows, state)
	return nil
}

func (s *mapleRepoStub) SaveVaultPositions(_ context.Context, _ pgx.Tx, positions []*entity.MapleVaultPosition) error {
	if s.posErr != nil {
		return s.posErr
	}
	s.posRows = append(s.posRows, positions...)
	return nil
}

// Compile-time check that mapleRepoStub satisfies the outbound interface.
var _ outbound.MapleRepository = (*mapleRepoStub)(nil)

// -----------------------------------------------------------------------------
// Multicaller stub — for blockchain_service tests
// -----------------------------------------------------------------------------

// multicallStub returns a queued list of pre-encoded results in receive order.
// Each call to Execute pops len(calls) responses off the head of Responses.
type multicallStub struct {
	Responses [][]byte
	Err       error
	cursor    int
	// Calls captures every call list the stub received, in call order.
	Calls [][]outbound.Call
	// BlockNumbers captures the block argument passed on each Execute.
	BlockNumbers []*big.Int
}

func (m *multicallStub) Execute(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	if m.Err != nil {
		return nil, m.Err
	}
	m.Calls = append(m.Calls, calls)
	m.BlockNumbers = append(m.BlockNumbers, blockNumber)

	out := make([]outbound.Result, len(calls))
	for i := range calls {
		if m.cursor >= len(m.Responses) {
			out[i] = outbound.Result{Success: false}
			continue
		}
		out[i] = outbound.Result{Success: true, ReturnData: m.Responses[m.cursor]}
		m.cursor++
	}
	return out, nil
}

func (m *multicallStub) Address() common.Address {
	return common.HexToAddress("0xcA11bde05977b3631167028862bE2a173976CA11")
}

// Compile-time check that multicallStub satisfies the outbound interface.
var _ outbound.Multicaller = (*multicallStub)(nil)

// -----------------------------------------------------------------------------
// ABI encoding helpers
// -----------------------------------------------------------------------------

// encodeUint256 produces an ABI-encoded uint256 return value.
func encodeUint256(n *big.Int) []byte {
	ty, _ := abi.NewType("uint256", "", nil)
	args := abi.Arguments{{Type: ty}}
	out, err := args.Pack(n)
	if err != nil {
		panic(err) // test helper — input always known-good
	}
	return out
}

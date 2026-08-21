package main

import (
	"context"
	"reflect"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// Only a write that reported a row appended may be counted: a deduped write is what a
// re-run of an already-replayed range does on every row, and counting those would report
// a run that persisted nothing as a run that wrote everything.
func TestCountingMorphoRepository_CountsOnlyAppendedRows(t *testing.T) {
	tests := []struct {
		name     string
		appended bool
		write    func(context.Context, *countingMorphoRepository) error
		want     appendedRows
	}{
		{
			name:     "appended adapter state",
			appended: true,
			write:    saveAdapterStateThrough,
			want:     appendedRows{AdapterStates: 1},
		},
		{
			name:     "deduped adapter state",
			appended: false,
			write:    saveAdapterStateThrough,
			want:     appendedRows{},
		},
		{
			name:     "appended vault cap",
			appended: true,
			write:    saveVaultCapThrough,
			want:     appendedRows{VaultCaps: 1},
		},
		{
			name:     "deduped vault cap",
			appended: false,
			write:    saveVaultCapThrough,
			want:     appendedRows{},
		},
		{
			name:     "appended vault fee",
			appended: true,
			write:    saveVaultFeeThrough,
			want:     appendedRows{VaultFees: 1},
		},
		{
			name:     "deduped vault fee",
			appended: false,
			write:    saveVaultFeeThrough,
			want:     appendedRows{},
		},
		{
			name:     "appended membership observation",
			appended: true,
			write:    observeMembershipThrough,
			want:     appendedRows{MembershipObservations: 1},
		},
		{
			name:     "deduped membership observation",
			appended: false,
			write:    observeMembershipThrough,
			want:     appendedRows{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := newCountingMorphoRepository(appendingMock(tt.appended))
			if err := tt.write(context.Background(), repo); err != nil {
				t.Fatalf("write through the counting repository: %v", err)
			}
			if repo.counts != tt.want {
				t.Errorf("counts = %+v, want %+v", repo.counts, tt.want)
			}
		})
	}
}

// appendingMock is a morpho repository whose every append-reporting write answers with
// the same verdict.
func appendingMock(appended bool) *testutil.MockMorphoRepository {
	return &testutil.MockMorphoRepository{
		SaveAdapterStateFn: func(context.Context, pgx.Tx, *entity.MorphoAdapterState) (bool, error) {
			return appended, nil
		},
		SaveVaultCapFn: func(context.Context, pgx.Tx, *entity.MorphoVaultCap) (bool, error) {
			return appended, nil
		},
		SaveVaultFeeFn: func(context.Context, pgx.Tx, *entity.MorphoVaultFee) (bool, error) {
			return appended, nil
		},
		ObserveAdapterMembershipFn: func(context.Context, pgx.Tx, *entity.MorphoAdapterObservation) (int64, bool, error) {
			return 7, appended, nil
		},
	}
}

func saveAdapterStateThrough(ctx context.Context, repo *countingMorphoRepository) error {
	_, err := repo.SaveAdapterState(ctx, nil, &entity.MorphoAdapterState{})
	return err
}

func saveVaultCapThrough(ctx context.Context, repo *countingMorphoRepository) error {
	_, err := repo.SaveVaultCap(ctx, nil, &entity.MorphoVaultCap{})
	return err
}

func saveVaultFeeThrough(ctx context.Context, repo *countingMorphoRepository) error {
	_, err := repo.SaveVaultFee(ctx, nil, &entity.MorphoVaultFee{})
	return err
}

func observeMembershipThrough(ctx context.Context, repo *countingMorphoRepository) error {
	_, _, err := repo.ObserveAdapterMembership(ctx, nil, &entity.MorphoAdapterObservation{})
	return err
}

// The counter promotes every port method it does not override, so a port method that
// starts reporting an append would pass through uncounted with no compile error — and the
// symptom is the under-report the count exists to expose. Enumerated from the port rather
// than from a list, so the method added tomorrow is covered.
func TestCountingMorphoRepository_InterceptsEveryAppendingWrite(t *testing.T) {
	port := reflect.TypeFor[outbound.MorphoRepository]()
	appending := 0

	for method := range port.Methods() {
		if !reportsAnAppend(method.Type) {
			continue
		}
		appending++
		t.Run(method.Name, func(t *testing.T) {
			repo := newCountingMorphoRepository(appendingMock(true))
			callWithZeroArgs(t, repo, method.Name)
			if repo.counts.total() != 1 {
				t.Errorf("%s reported an append and the tally stayed %+v: the counter does not intercept it",
					method.Name, repo.counts)
			}
		})
	}

	if appending < 4 {
		t.Fatalf("found %d append-reporting port methods, want >= 4: the bool-result filter stopped matching them", appending)
	}
}

// reportsAnAppend recognises the port's write shape: a bool among the results, which is
// how every one of them says whether a row was appended.
func reportsAnAppend(signature reflect.Type) bool {
	for out := range signature.Outs() {
		if out.Kind() == reflect.Bool {
			return true
		}
	}
	return false
}

// callWithZeroArgs invokes one method with zero values throughout: the mock behind it
// answers from its configured verdict and reads none of them.
func callWithZeroArgs(t *testing.T, repo *countingMorphoRepository, name string) {
	t.Helper()
	method := reflect.ValueOf(repo).MethodByName(name)
	args := make([]reflect.Value, method.Type().NumIn())
	for i := range args {
		args[i] = reflect.New(method.Type().In(i)).Elem()
	}
	for _, result := range method.Call(args) {
		if err, ok := result.Interface().(error); ok && err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// Each partition's tally accumulates into the run's, per table.
func TestAppendedRows_AddAccumulatesPerTable(t *testing.T) {
	run := appendedRows{AdapterStates: 2, VaultCaps: 1}
	run.add(appendedRows{AdapterStates: 3, VaultFees: 4, MembershipObservations: 1})

	want := appendedRows{AdapterStates: 5, VaultCaps: 1, VaultFees: 4, MembershipObservations: 1}
	if run != want {
		t.Errorf("accumulated = %+v, want %+v", run, want)
	}
	if got := run.total(); got != 11 {
		t.Errorf("total() = %d, want 11", got)
	}
}

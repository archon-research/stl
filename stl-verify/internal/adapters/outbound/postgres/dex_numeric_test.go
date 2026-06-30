package postgres

import (
	"math/big"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestNumericToNullableBigInt(t *testing.T) {
	tests := []struct {
		name    string
		in      pgtype.Numeric
		want    *big.Int // nil means expect a nil result
		wantErr bool
	}{
		{
			name: "sql null maps to nil",
			in:   pgtype.Numeric{Valid: false},
			want: nil,
		},
		{
			name: "exp zero returns the mantissa",
			in:   pgtype.Numeric{Int: big.NewInt(42), Exp: 0, Valid: true},
			want: big.NewInt(42),
		},
		{
			name: "positive exp scales up",
			in:   pgtype.Numeric{Int: big.NewInt(5), Exp: 3, Valid: true},
			want: big.NewInt(5000),
		},
		{
			name: "negative exp on a whole number scales down exactly",
			in:   pgtype.Numeric{Int: big.NewInt(7000), Exp: -3, Valid: true},
			want: big.NewInt(7),
		},
		{
			name:    "negative exp on a non-whole value errors",
			in:      pgtype.Numeric{Int: big.NewInt(7001), Exp: -3, Valid: true},
			wantErr: true,
		},
		{
			name:    "valid with nil mantissa errors",
			in:      pgtype.Numeric{Int: nil, Valid: true},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NumericToNullableBigInt(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got nil (result %v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch {
			case tt.want == nil:
				if got != nil {
					t.Errorf("got %v, want nil", got)
				}
			case got == nil:
				t.Errorf("got nil, want %v", tt.want)
			case got.Cmp(tt.want) != 0:
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBigIntToNullableNumeric(t *testing.T) {
	if got := BigIntToNullableNumeric(nil); got.Valid {
		t.Error("nil must serialise as SQL NULL (Valid=false)")
	}
	got := BigIntToNullableNumeric(big.NewInt(42))
	if !got.Valid || got.Int.Cmp(big.NewInt(42)) != 0 || got.Exp != 0 {
		t.Errorf("got %+v, want Valid value 42 with Exp 0", got)
	}

	in := big.NewInt(5)
	out := BigIntToNullableNumeric(in)
	in.Add(in, big.NewInt(1))
	if out.Int.Cmp(big.NewInt(5)) != 0 {
		t.Error("not a defensive copy: mutating the input leaked into the result")
	}
}

func TestBigIntToNumericRequired(t *testing.T) {
	_, err := BigIntToNumericRequired(nil, "amount")
	if err == nil {
		t.Fatal("nil must error for a NOT NULL column")
	}
	if !strings.Contains(err.Error(), "amount") {
		t.Errorf("error %q should name the column", err)
	}

	got, err := BigIntToNumericRequired(big.NewInt(7), "amount")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got.Valid || got.Int.Cmp(big.NewInt(7)) != 0 {
		t.Errorf("got %+v, want Valid value 7", got)
	}
}

func TestBigIntsToNumericArray(t *testing.T) {
	_, err := BigIntsToNumericArray([]*big.Int{big.NewInt(1), nil})
	if err == nil {
		t.Fatal("a nil element must error (column is NOT NULL on both array and elements)")
	}
	if !strings.Contains(err.Error(), "element 1") {
		t.Errorf("error %q should name the offending index", err)
	}

	got, err := BigIntsToNumericArray([]*big.Int{big.NewInt(1), big.NewInt(2)})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || !got[0].Valid || !got[1].Valid {
		t.Errorf("got %+v, want two Valid elements", got)
	}
}

// BigIntsToNullableNumericArray: nil slice → SQL NULL; nil element → error.
func TestBigIntsToNullableNumericArray(t *testing.T) {
	v, err := BigIntsToNullableNumericArray(nil)
	if err != nil {
		t.Fatalf("nil slice: unexpected error %v", err)
	}
	if v != nil {
		t.Errorf("nil slice must map to nil (SQL NULL), got %#v", v)
	}

	v, err = BigIntsToNullableNumericArray([]*big.Int{})
	if err != nil {
		t.Fatalf("empty slice: %v", err)
	}
	if arr, ok := v.([]pgtype.Numeric); !ok || arr == nil || len(arr) != 0 {
		t.Errorf("empty (non-nil) slice must map to an empty array, not NULL; got %#v", v)
	}

	if _, err := BigIntsToNullableNumericArray([]*big.Int{nil}); err == nil {
		t.Error("nil element must error: this helper allows a NULL array but not NULL elements")
	}
}

// BigIntsToNullableElementArrayOrNull: nil slice → SQL NULL; nil element → NULL element.
func TestBigIntsToNullableElementArrayOrNull(t *testing.T) {
	if v := BigIntsToNullableElementArrayOrNull(nil); v != nil {
		t.Errorf("nil slice must map to nil (SQL NULL), got %#v", v)
	}

	v := BigIntsToNullableElementArrayOrNull([]*big.Int{big.NewInt(1), nil})
	arr, ok := v.([]pgtype.Numeric)
	if !ok || len(arr) != 2 {
		t.Fatalf("want []pgtype.Numeric of len 2, got %#v", v)
	}
	if !arr[0].Valid || arr[1].Valid {
		t.Errorf("want element 0 Valid and element 1 a NULL element, got %+v", arr)
	}
}

// BigIntsToNullableElementNumericArray: nil element → NULL element, never errors.
func TestBigIntsToNullableElementNumericArray(t *testing.T) {
	out := BigIntsToNullableElementNumericArray([]*big.Int{big.NewInt(9), nil, big.NewInt(3)})
	if len(out) != 3 {
		t.Fatalf("len = %d, want 3", len(out))
	}
	if !out[0].Valid || out[0].Int.Cmp(big.NewInt(9)) != 0 {
		t.Errorf("element 0 = %+v, want Valid 9", out[0])
	}
	if out[1].Valid {
		t.Error("element 1 (nil input) must be a NULL element (Valid=false)")
	}
	if !out[2].Valid || out[2].Int.Cmp(big.NewInt(3)) != 0 {
		t.Errorf("element 2 = %+v, want Valid 3", out[2])
	}

	in := []*big.Int{big.NewInt(5)}
	got := BigIntsToNullableElementNumericArray(in)
	in[0].Add(in[0], big.NewInt(1))
	if got[0].Int.Cmp(big.NewInt(5)) != 0 {
		t.Error("not a defensive copy: mutating the input leaked into the result")
	}
}

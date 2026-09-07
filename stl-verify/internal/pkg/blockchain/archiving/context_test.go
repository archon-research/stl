package archiving

import (
	"context"
	"testing"
)

func TestBlockVersionRoundTrip(t *testing.T) {
	ctx := WithBlockVersion(context.Background(), 3)
	v, ok := BlockVersionFromContext(ctx)
	if !ok || v != 3 {
		t.Fatalf("got (%d, %v), want (3, true)", v, ok)
	}
}

func TestBlockVersionAbsent(t *testing.T) {
	v, ok := BlockVersionFromContext(context.Background())
	if ok || v != 0 {
		t.Fatalf("got (%d, %v), want (0, false)", v, ok)
	}
}

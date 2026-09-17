//go:build integration

package postgres

import (
	"context"
	"testing"
	"time"
)

// Materialize retries only deadlocks and serialization failures. A chunk lock held by a compression job
// is waited on, not retried, so the retry backoff need not outlast it: this lock outlasts the whole
// backoff budget (~0.5s) fourfold and the call still succeeds.
func TestMaterialize_WaitsOutALockLongerThanTheRetryBackoff(t *testing.T) {
	ctx := context.Background()
	if _, err := cacheRowsPool.Exec(ctx, `
		CREATE TABLE lock_wait_itest (n int);
		CREATE FUNCTION materialize_lock_wait_itest(p_build_id integer, p_run_id bigint) RETURNS bigint
			LANGUAGE sql AS $fn$ INSERT INTO lock_wait_itest VALUES (1); SELECT 1::bigint $fn$;`); err != nil {
		t.Fatalf("create lock-wait wrapper: %v", err)
	}
	holder, err := cacheRowsPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin lock holder: %v", err)
	}
	if _, err := holder.Exec(ctx, `LOCK TABLE lock_wait_itest IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatalf("take lock: %v", err)
	}
	const hold = 2 * time.Second
	go func() {
		time.Sleep(hold)
		_ = holder.Commit(ctx)
	}()

	start := time.Now()
	n, err := NewPositionMaterializerRepository(cacheRowsPool, nil).Materialize(ctx, "materialize_lock_wait_itest", 0, 1)
	if err != nil {
		t.Fatalf("Materialize under a %s lock: %v", hold, err)
	}
	if n != 1 || time.Since(start) < hold {
		t.Errorf("Materialize = %d after %s; want 1 after waiting at least %s", n, time.Since(start), hold)
	}
}

package testutil

import "testing"

func TestClaimMainDBName_RejectsASecondClaim(t *testing.T) {
	const dbName = "test_claimed_twice"

	// The claims outlive the test, so without this `go test -count=2` fails on its
	// own first claim.
	t.Cleanup(func() {
		mainDBNamesMu.Lock()
		defer mainDBNamesMu.Unlock()
		delete(mainDBNames, dbName)
	})

	if err := claimMainDBName(dbName); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := claimMainDBName(dbName); err == nil {
		t.Error("second claim succeeded, so one file's setup would drop the other file's database")
	}
}

func TestClaimMainDBName_RejectsANameCreateDatabaseCannotTake(t *testing.T) {
	if err := claimMainDBName("test-dashed"); err == nil {
		t.Error("claim accepted a name that would reach CREATE DATABASE as a syntax error")
	}
}

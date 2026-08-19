package testutil

import "testing"

func TestClaimMainDBName_RejectsASecondClaim(t *testing.T) {
	const dbName = "test_claimed_twice"

	if err := claimMainDBName(dbName); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := claimMainDBName(dbName); err == nil {
		t.Error("second claim succeeded, so one file's setup would drop the other file's database")
	}
}

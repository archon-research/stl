//go:build integration

package testutil

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

var sharedDSN string

func TestMain(m *testing.M) {
	os.Exit(RunShared(m, Shared{TimescaleDSN: &sharedDSN}))
}

// The clone's name carries this process's pid, so nothing reclaims it as stale on a
// later run: a failure between CREATE DATABASE and a usable pool has to take it back
// out on the spot, or the database is there forever.
func TestSetupClonedDatabase_DropsTheCloneWhenConnectingToItFails(t *testing.T) {
	ctx := context.Background()
	dbName := SanitizeTestName(t.Name())

	failToConnect := func(context.Context, string) (*pgxpool.Pool, error) {
		return nil, errors.New("forced failure with the clone already created")
	}
	if _, _, _, err := setupClonedDatabase(ctx, sharedDSN, dbName, failToConnect); err == nil {
		t.Fatal("setup reported success even though connecting to the clone failed")
	}

	if databaseOnServer(ctx, t, dbName) {
		t.Errorf("database %s outlived the failure that created it", dbName)
	}
}

// databaseOnServer reports whether dbName is still on the server, through its own
// pool: the one the failed setup would have returned does not exist.
func databaseOnServer(ctx context.Context, t *testing.T, dbName string) bool {
	t.Helper()

	adminPool, err := pgxpool.New(ctx, sharedDSN)
	if err != nil {
		t.Fatalf("connect to check for %s: %v", dbName, err)
	}
	defer adminPool.Close()

	var exists bool
	if err := adminPool.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", dbName,
	).Scan(&exists); err != nil {
		t.Fatalf("check whether %s exists: %v", dbName, err)
	}
	return exists
}

//go:build integration

package migrations

import (
	"embed"
	"io/fs"
)

// Migrations embeds all SQL migration files from the db/migrations directory.
// This allows integration tests to apply migrations without relying on fragile
// relative path calculations that break when test files move.
//
//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// FS returns a filesystem containing all migration files.
// The files are located at the root of the returned FS.
func FS() fs.FS {
	// Strip the "migrations/" prefix from the embedded paths
	sub, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		panic("failed to create sub-filesystem: " + err.Error())
	}
	return sub
}

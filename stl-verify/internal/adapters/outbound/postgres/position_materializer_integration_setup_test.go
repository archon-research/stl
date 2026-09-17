//go:build integration

package postgres

import (
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const positionMaterializerDBName = "test_position_materializer"

// positionMaterializerPool is a migrated database shared by the position materializer's integration
// tests. Each test drops what it creates.
var positionMaterializerPool *pgxpool.Pool

func init() {
	registerTestFileSetup(func() {
		positionMaterializerPool = testutil.SetupDBForMain(sharedDSN, positionMaterializerDBName)
	}, func() {
		testutil.CleanupDBForMain(sharedDSN, positionMaterializerPool, positionMaterializerDBName)
	})
}

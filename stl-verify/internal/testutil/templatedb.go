package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/db/migrator"
)

// Template databases: migrate the schema once per server, then hand out copies of
// it with CREATE DATABASE ... TEMPLATE.
//
// Applying the migration set per test made migration work grow with both package
// and test count — the measured baseline replayed it 169 times per run. Cloning a
// migrated template is a file copy, so a new test costs a copy instead of a full
// DDL pass.
//
// The unit of isolation has to be a database, not a schema: a schema cannot be
// copied, and sharing one migrated schema between tests is what regressed in #273.

var (
	templateMu    sync.Mutex
	templateBuilt = map[string]string{}
)

// SetupTestDB gives the calling test its own database, cloned from a template that
// carries the whole migration set, and returns a pool for it.
//
// The clone is a full database, so SQL that names its schema explicitly — the
// transformed.* tables and the public.* raw tables their triggers pin to — is
// isolated too, which a search_path could not do.
func SetupTestDB(t *testing.T, baseDSN string) (pool *pgxpool.Pool, dsn string, cleanup func()) {
	t.Helper()

	pool, dsn, drop, err := setupClonedDatabase(context.Background(), baseDSN, SanitizeTestName(t.Name()))
	if err != nil {
		t.Fatal(err)
	}
	// pgxpool.Close is once-guarded, so cleanup closing it again is a no-op.
	t.Cleanup(pool.Close)

	return pool, dsn, func() {
		if err := drop(); err != nil {
			t.Logf("warning: %v", err)
		}
	}
}

// SetupDBForMain is SetupTestDB for a TestMain-scoped database shared by one test
// file, where there is no *testing.T to fail. On error it calls log.Fatal.
//
// dbName is scoped by withProcessTag, so a name two test files happen to share
// cannot make one file's setup drop the other file's live database.
func SetupDBForMain(baseDSN, dbName string) *pgxpool.Pool {
	pool, _, _, err := setupClonedDatabase(context.Background(), baseDSN, withProcessTag(dbName))
	if err != nil {
		log.Fatal(err)
	}
	return pool
}

// CleanupDBForMain closes the pool and drops the database SetupDBForMain created,
// taking the same dbName it was given. Cleanup is best effort: it warns rather
// than failing the run.
func CleanupDBForMain(baseDSN string, pool *pgxpool.Pool, dbName string) {
	if err := dropClonedDatabase(pool, baseDSN, withProcessTag(dbName)); err != nil {
		log.Printf("warning: %v", err)
	}
}

// DatabaseDSN points baseDSN at another database on the same server, for tests
// that mint their own pools against a database SetupDBForMain created. It takes
// the same dbName SetupDBForMain was given.
func DatabaseDSN(baseDSN, dbName string) string {
	dsn, err := replaceDatabase(baseDSN, withProcessTag(dbName))
	if err != nil {
		log.Fatalf("build DSN for %s: %v", dbName, err)
	}
	return dsn
}

// setupClonedDatabase clones the template into dbName and connects to it.
func setupClonedDatabase(
	ctx context.Context, baseDSN, dbName string,
) (pool *pgxpool.Pool, dsn string, drop func() error, err error) {
	template, err := ensureTemplate(ctx, baseDSN)
	if err != nil {
		return nil, "", nil, err
	}

	adminPool, err := pgxpool.New(ctx, baseDSN)
	if err != nil {
		return nil, "", nil, fmt.Errorf("connect to %s: %w", baseDSN, err)
	}
	defer adminPool.Close()

	// A database left behind by a killed run would silently supply its rows here.
	if _, err := adminPool.Exec(ctx, dropDatabaseSQL(dbName)); err != nil {
		return nil, "", nil, fmt.Errorf("drop stale database %s: %w", dbName, err)
	}
	if err := cloneDatabase(ctx, adminPool, dbName, template); err != nil {
		return nil, "", nil, err
	}

	dsn, err = replaceDatabase(baseDSN, dbName)
	if err != nil {
		return nil, "", nil, err
	}
	pool, err = connectPool(ctx, dsn)
	if err != nil {
		return nil, "", nil, fmt.Errorf("connect to %s: %w", dbName, err)
	}

	return pool, dsn, func() error { return dropClonedDatabase(pool, baseDSN, dbName) }, nil
}

// dropClonedDatabase closes the pool and removes the clone.
func dropClonedDatabase(pool *pgxpool.Pool, baseDSN, dbName string) error {
	pool.Close()
	return dropDatabase(baseDSN, dbName)
}

// Postgres refuses to copy a template while any other backend is attached to it,
// and under `go test -p` that happens: a sibling package's clone is in flight, or
// autovacuum picked this moment to visit the template. Retrying beats serializing
// every clone behind one lock, which would hand back the serial cost the template
// was meant to remove.
const (
	cloneAttempts   = 40
	cloneRetryDelay = 50 * time.Millisecond
)

// cloneDatabase copies the template into dbName, waiting out transient
// source-in-use rejections.
func cloneDatabase(ctx context.Context, adminPool *pgxpool.Pool, dbName, template string) error {
	clone := fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", dbName, template)

	var err error
	for range cloneAttempts {
		if _, err = adminPool.Exec(ctx, clone); err == nil {
			return nil
		}
		if !isSourceInUse(err) {
			return fmt.Errorf("clone %s from template %s: %w", dbName, template, err)
		}
		err = errors.Join(err, evictTemplateSessions(ctx, adminPool, template))
		time.Sleep(cloneRetryDelay)
	}
	return fmt.Errorf("clone %s from template %s after %d attempts: %w",
		dbName, template, cloneAttempts, err)
}

// evictTemplateSessions disconnects whatever is holding the template open. Failing
// is not fatal — the session may already be gone, and the next clone attempt is the
// real check — but the error rides along so an unusable eviction is not silent.
func evictTemplateSessions(ctx context.Context, adminPool *pgxpool.Pool, template string) error {
	if _, err := adminPool.Exec(ctx,
		"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()",
		template,
	); err != nil {
		return fmt.Errorf("evict sessions from template %s: %w", template, err)
	}
	return nil
}

// isSourceInUse reports whether Postgres rejected the copy because something else
// was attached to the source database.
func isSourceInUse(err error) bool {
	// SQLSTATE 55006 = object_in_use
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55006"
}

// isUndefinedObject reports whether Postgres rejected a statement because the
// object it named does not exist.
func isUndefinedObject(err error) bool {
	// SQLSTATE 42704 = undefined_object
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42704"
}

// ensureTemplate migrates the template for baseDSN's server if no process has yet,
// and returns its name.
func ensureTemplate(ctx context.Context, baseDSN string) (string, error) {
	fingerprint, err := migrationsFingerprint()
	if err != nil {
		return "", err
	}
	name := "stl_tmpl_" + fingerprint

	templateMu.Lock()
	defer templateMu.Unlock()

	if templateBuilt[baseDSN] == name {
		return name, nil
	}

	// Through the cluster-wide `postgres` database, not baseDSN: advisory locks are
	// scoped to the database holding them, and every test binary has its own
	// baseDSN database, so locks taken there would never meet and the processes
	// would each build the template and drop each other's work.
	clusterDSN, err := replaceDatabase(baseDSN, "postgres")
	if err != nil {
		return "", err
	}
	adminPool, err := connectPool(ctx, clusterDSN)
	if err != nil {
		return "", fmt.Errorf("connect to cluster database: %w", err)
	}
	defer adminPool.Close()

	// Advisory locks are held per connection, so the lock, the readiness check and
	// the build all have to run on one pinned connection.
	conn, err := adminPool.Acquire(ctx)
	if err != nil {
		return "", fmt.Errorf("acquire connection for template %s: %w", name, err)
	}
	defer conn.Release()

	lockID := templateLockID(fingerprint)
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		return "", fmt.Errorf("lock template %s: %w", name, err)
	}
	defer func() {
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockID); err != nil {
			log.Printf("warning: could not unlock template %s: %v", name, err)
		}
	}()

	if err := disableBackgroundWorkers(ctx, conn.Conn()); err != nil {
		return "", err
	}
	ready, err := templateReady(ctx, conn.Conn(), name)
	if err != nil {
		return "", err
	}
	if !ready {
		if err := buildTemplate(ctx, conn.Conn(), baseDSN, name); err != nil {
			return "", err
		}
	}

	templateBuilt[baseDSN] = name
	return name, nil
}

// disableBackgroundWorkers stops TimescaleDB attaching a scheduler session to
// every database on the server.
//
// That session is a background worker, so datallowconn cannot keep it out of the
// template, and CREATE DATABASE ... TEMPLATE counts it as a user of the source and
// refuses to copy — permanently, not transiently. Tests never depend on scheduled
// jobs; migrations only register the policies, which is a catalog write.
//
// The change is server-wide and is never put back, so the server behind
// EnvPostgresDSN has to be one nobody minds losing scheduled jobs on.
func disableBackgroundWorkers(ctx context.Context, conn *pgx.Conn) error {
	var workers int
	err := conn.QueryRow(ctx,
		"SELECT current_setting('timescaledb.max_background_workers')::int",
	).Scan(&workers)
	switch {
	case isUndefinedObject(err):
		// No such setting means the extension is not loaded on this server, so
		// there is no scheduler session for a clone to trip over.
		return nil
	case err != nil:
		return fmt.Errorf("read timescaledb.max_background_workers: %w", err)
	case workers == 0:
		return nil
	}

	// ALTER SYSTEM, because the setting is SIGHUP-scoped: it cannot be set per
	// session or per database, and `services:` cannot pass a server command line.
	if _, err := conn.Exec(ctx, "ALTER SYSTEM SET timescaledb.max_background_workers = 0"); err != nil {
		return fmt.Errorf("disable timescaledb background workers: %w", err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		return fmt.Errorf("reload config: %w", err)
	}
	return nil
}

// templateReady reports whether a previous process finished building the template.
// The datistemplate flag goes on only once its migrations are in, so a half-built
// database from a killed run does not read as ready.
//
// Only a missing row means "not ready". Reading any other failure that way would
// rebuild the template — dropping it under a sibling process mid-clone.
func templateReady(ctx context.Context, conn *pgx.Conn, name string) (bool, error) {
	var ready bool
	err := conn.QueryRow(ctx,
		"SELECT datistemplate FROM pg_database WHERE datname = $1", name,
	).Scan(&ready)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("check whether template %s is ready: %w", name, err)
	}
	return ready, nil
}

// buildTemplate creates the template, migrates it, and marks it clonable.
func buildTemplate(ctx context.Context, conn *pgx.Conn, baseDSN, name string) error {
	if err := dropTemplate(ctx, conn, name); err != nil {
		return err
	}
	if _, err := conn.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", name)); err != nil {
		return fmt.Errorf("create template %s: %w", name, err)
	}

	templateDSN, err := replaceDatabase(baseDSN, name)
	if err != nil {
		return err
	}
	pool, err := connectPool(ctx, templateDSN)
	if err != nil {
		return fmt.Errorf("connect to template %s: %w", name, err)
	}

	if err := migrateTemplate(ctx, pool); err != nil {
		pool.Close()
		return fmt.Errorf("migrate template %s: %w", name, err)
	}
	// Before the flags below, not after: CREATE DATABASE ... TEMPLATE refuses to
	// copy a database that still has sessions on it.
	pool.Close()

	if _, err := conn.Exec(ctx,
		"UPDATE pg_database SET datistemplate = true, datallowconn = false WHERE datname = $1", name,
	); err != nil {
		return fmt.Errorf("mark template %s clonable: %w", name, err)
	}
	return nil
}

// migrateTemplate enables the extension and applies every migration.
func migrateTemplate(ctx context.Context, pool *pgxpool.Pool) error {
	// Migrations leave the extension to the infrastructure bootstrap in production,
	// and a database created from template1 does not inherit it.
	if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS timescaledb"); err != nil {
		return fmt.Errorf("enable timescaledb: %w", err)
	}
	return migrator.New(pool, migrationsDir()).ApplyAll(ctx)
}

// dropTemplate removes a leftover template. The datistemplate flag has to come off
// first — Postgres refuses to drop a database while it is marked as a template.
func dropTemplate(ctx context.Context, conn *pgx.Conn, name string) error {
	if _, err := conn.Exec(ctx,
		"UPDATE pg_database SET datistemplate = false WHERE datname = $1", name,
	); err != nil {
		return fmt.Errorf("clear template flag on %s: %w", name, err)
	}
	if _, err := conn.Exec(ctx, dropDatabaseSQL(name)); err != nil {
		return fmt.Errorf("drop stale template %s: %w", name, err)
	}
	return nil
}

// migrationsFingerprint digests the migration set, so a template built from an
// older set is never cloned: the name changes with the contents. This matters on a
// server that outlives one run, such as a developer's own container.
func migrationsFingerprint() (string, error) {
	entries, err := filepath.Glob(filepath.Join(migrationsDir(), "*.sql"))
	if err != nil {
		return "", fmt.Errorf("list migrations: %w", err)
	}
	if len(entries) == 0 {
		return "", errors.New("no migrations found")
	}
	sort.Strings(entries)

	digest := sha256.New()
	for _, entry := range entries {
		contents, err := os.ReadFile(entry)
		if err != nil {
			return "", fmt.Errorf("read migration %s: %w", entry, err)
		}
		digest.Write([]byte(filepath.Base(entry)))
		digest.Write(contents)
	}
	return hex.EncodeToString(digest.Sum(nil))[:12], nil
}

// templateLockID turns the fingerprint into the advisory-lock key, so processes
// building the same template contend and processes building different ones do not.
func templateLockID(fingerprint string) int64 {
	digest := sha256.Sum256([]byte(fingerprint))
	return int64(binary.BigEndian.Uint64(digest[:8]) >> 1)
}

// migrationsDir resolves db/migrations relative to this file, so it works whatever
// directory a test binary runs in.
func migrationsDir() string {
	_, currentFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(currentFile), "../../db/migrations")
}

// connectPool opens a pool and waits for the server to answer. It never hands back
// a live pool alongside an error: a caller that fails with runtime.Goexit would
// otherwise leave goroutines for the package's leak check to trip over.
func connectPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	for range 30 {
		if pool.Ping(ctx) == nil {
			return pool, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	pool.Close()
	return nil, errors.New("timed out waiting for database connection")
}

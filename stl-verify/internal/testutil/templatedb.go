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
	"regexp"
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

	pool, dsn, drop, err := setupClonedDatabase(
		context.Background(), baseDSN, SanitizeTestName(t.Name()), connectPool,
	)
	if err != nil {
		t.Fatal(err)
	}

	var once sync.Once
	discard := func() {
		once.Do(func() {
			if err := drop(); err != nil {
				t.Logf("warning: %v", err)
			}
		})
	}
	// Registered as well as returned: a caller that forgets the returned cleanup
	// leaks nothing on a server that outlives the run.
	t.Cleanup(discard)

	return pool, dsn, discard
}

// SetupDBForMain is SetupTestDB for a TestMain-scoped database shared by one test
// file, where there is no *testing.T to fail. On error it calls log.Fatal.
//
// withProcessTag settles the sibling packages sharing the server; dbName is claimed
// to settle the sibling files inside this one, which the tag cannot separate because
// they run in the same process.
func SetupDBForMain(baseDSN, dbName string) *pgxpool.Pool {
	if err := claimMainDBName(dbName); err != nil {
		log.Fatal(err)
	}
	pool, _, _, err := setupClonedDatabase(context.Background(), baseDSN, withProcessTag(dbName), connectPool)
	if err != nil {
		log.Fatal(err)
	}
	return pool
}

var (
	mainDBNamesMu sync.Mutex
	mainDBNames   = map[string]bool{}
)

// What an unquoted identifier accepts. The names reach SQL by interpolation, so a
// stray dash would arrive as a syntax error inside CREATE DATABASE rather than as a
// rejected argument.
var mainDBNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// claimMainDBName checks dbName and reserves it for this test binary, rejecting a
// second claim. The names are hand-written string constants, so nothing stops two
// files in a package choosing one name — and the loser's live database would go out
// under it, dropped as stale by the winner's setup.
func claimMainDBName(dbName string) error {
	if !mainDBNamePattern.MatchString(dbName) {
		return fmt.Errorf("database name %q must match %s", dbName, mainDBNamePattern)
	}

	mainDBNamesMu.Lock()
	defer mainDBNamesMu.Unlock()

	if mainDBNames[dbName] {
		return fmt.Errorf(
			"database name %q is already taken by another test file in this package, pick a distinct one",
			dbName,
		)
	}
	mainDBNames[dbName] = true
	return nil
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

// connectFunc opens a pool for a DSN. setupClonedDatabase takes it rather than
// calling connectPool directly because failing this one step is the only way to
// reach the compensating drop below: the create and the connect are adjacent.
type connectFunc func(ctx context.Context, dsn string) (*pgxpool.Pool, error)

// setupClonedDatabase clones the template into dbName and connects to it.
func setupClonedDatabase(
	ctx context.Context, baseDSN, dbName string, connect connectFunc,
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
	// The database exists from here on, so every later failure has to take it back
	// out: its name carries this process's pid, so no rerun reclaims it as stale.
	defer func() {
		if err == nil {
			return
		}
		if dropErr := dropDatabase(baseDSN, dbName); dropErr != nil {
			log.Printf("warning: %v", dropErr)
		}
	}()

	dsn, err = replaceDatabase(baseDSN, dbName)
	if err != nil {
		return nil, "", nil, err
	}
	pool, err = connect(ctx, dsn)
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

// ensureTemplate migrates the template for baseDSN's server if no process has yet,
// and returns its name.
func ensureTemplate(ctx context.Context, baseDSN string) (string, error) {
	fingerprint, err := templateFingerprint()
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
	if err := lockTemplate(ctx, conn.Conn(), name, lockID); err != nil {
		return "", err
	}
	defer func() {
		if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock($1)", lockID); err != nil {
			log.Printf("warning: could not unlock template %s: %v", name, err)
		}
	}()

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

// Bounded, because a process hung mid-build would otherwise stall every sibling
// package until the `go test` deadline kills them with no reason given.
const templateLockTimeout = 3 * time.Minute

// lockTemplate takes the build lock, failing with the reason rather than waiting
// out the test timeout.
func lockTemplate(ctx context.Context, conn *pgx.Conn, name string, lockID int64) error {
	if _, err := conn.Exec(ctx,
		fmt.Sprintf("SET lock_timeout = %d", templateLockTimeout.Milliseconds()),
	); err != nil {
		return fmt.Errorf("set lock timeout for template %s: %w", name, err)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockID); err != nil {
		if isLockTimeout(err) {
			return fmt.Errorf("template %s: another process has been building it for over %s",
				name, templateLockTimeout)
		}
		return fmt.Errorf("lock template %s: %w", name, err)
	}
	// Off again: the statements this connection runs next create and drop databases,
	// and those waits are not the deadlock this timeout is here to surface.
	if _, err := conn.Exec(ctx, "SET lock_timeout = 0"); err != nil {
		return fmt.Errorf("clear lock timeout for template %s: %w", name, err)
	}
	return nil
}

func isLockTimeout(err error) bool {
	// SQLSTATE 55P03 = lock_not_available
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// DisableScheduledJobs stops TimescaleDB's policy jobs from running in this
// database. Compression is the one that bites: a job firing mid-test rewrites a
// chunk into a columnstore chunk plus a compress_hyper_* twin, and any test
// asserting on chunk layout then sees two chunks where it seeded one.
//
// Per database, because the server-wide knob cannot be reached from here:
// timescaledb.max_background_workers is postmaster-scoped, so ALTER SYSTEM only
// marks it pending_restart, and a `services:` container takes no command line and
// cannot be restarted mid-job. Migrations register the policies as a catalog write;
// no test depends on one running.
func DisableScheduledJobs(ctx context.Context, pool *pgxpool.Pool) error {
	// job_id >= 1000 is TimescaleDB's own boundary between policy jobs and its
	// built-ins, which belong to the extension rather than to our migrations.
	if _, err := pool.Exec(ctx,
		"SELECT alter_job(job_id, scheduled => false) FROM timescaledb_information.jobs WHERE job_id >= 1000",
	); err != nil {
		return fmt.Errorf("disable scheduled jobs: %w", err)
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
		fmt.Sprintf("ALTER DATABASE %s IS_TEMPLATE true ALLOW_CONNECTIONS false", name),
	); err != nil {
		return fmt.Errorf("mark template %s clonable: %w", name, err)
	}
	return nil
}

// databaseExists reports whether name is present on the server conn is attached to.
func databaseExists(ctx context.Context, conn *pgx.Conn, name string) (bool, error) {
	var exists bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)", name,
	).Scan(&exists); err != nil {
		return false, fmt.Errorf("check whether database %s exists: %w", name, err)
	}
	return exists, nil
}

// migrateTemplate applies every migration.
func migrateTemplate(ctx context.Context, pool *pgxpool.Pool) error {
	if err := migrator.New(pool, migrationsDir()).ApplyAll(ctx); err != nil {
		return err
	}
	// In the template, so every clone inherits it: the job rows are catalog rows,
	// and a clone is a copy of the catalog too.
	return DisableScheduledJobs(ctx, pool)
}

// dropTemplate removes a leftover template. The template flag has to come off first
// — Postgres refuses to drop a database while it is marked as a template — and
// ALTER DATABASE needs a database to exist, so nothing there is nothing to do.
func dropTemplate(ctx context.Context, conn *pgx.Conn, name string) error {
	exists, err := databaseExists(ctx, conn, name)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}

	if _, err := conn.Exec(ctx,
		fmt.Sprintf("ALTER DATABASE %s IS_TEMPLATE false", name),
	); err != nil {
		return fmt.Errorf("clear template flag on %s: %w", name, err)
	}
	if _, err := conn.Exec(ctx, dropDatabaseSQL(name)); err != nil {
		return fmt.Errorf("drop stale template %s: %w", name, err)
	}
	return nil
}

// Bump whenever buildTemplate changes what a finished template contains — a new
// bootstrap step, a dropped one, different flags. The migration digest below cannot
// see any of that, so without this a server outliving one tree would keep handing
// out templates built under the old semantics: the run that introduced
// DisableScheduledJobs would have cloned policy jobs back in.
const templateFormat = 2

// templateFingerprint digests everything a finished template is made of, so one
// built from an older tree is never cloned: the name changes with the contents.
// This matters on a server that outlives one run, such as a developer's own
// container — where the superseded template stays until `make test-templates-clean`
// removes it, because collecting it from here would race a sibling process between
// its readiness check and its clone.
func templateFingerprint() (string, error) {
	entries, err := filepath.Glob(filepath.Join(migrationsDir(), "*.sql"))
	if err != nil {
		return "", fmt.Errorf("list migrations: %w", err)
	}
	if len(entries) == 0 {
		return "", errors.New("no migrations found")
	}
	sort.Strings(entries)

	digest := sha256.New()
	fmt.Fprintf(digest, "format=%d\n", templateFormat)
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

# Database Migrations

## Rules

1. **Naming:** `YYYYMMDD_HHMMSS_description.sql` (use `date +"%Y%m%d_%H%M%S"`)
2. **Plain SQL only** with self-tracking INSERT at end
3. **Production:** Manual execution only
4. **Local/Tests:** Auto-applied via the kind cluster (`make dev-up`) and migrator

## Template
```sql
-- Description of changes

CREATE TABLE IF NOT EXISTS my_table (
                                        id SERIAL PRIMARY KEY
);

-- Track this migration (filename must match exactly!)
INSERT INTO migrations (filename) VALUES ('20250122_143000_description.sql')
ON CONFLICT (filename) DO NOTHING;
```

## Local Development
```bash
# Apply new migrations (runs automatically on make dev-up)
make kind-migrate

# Apply specific migration manually
psql postgres://postgres:postgres@localhost:5432/stl_verify < db/migrations/YOUR_FILE.sql

# Verify applied
psql postgres://postgres:postgres@localhost:5432/stl_verify -c "SELECT filename, applied_at FROM migrations ORDER BY applied_at DESC LIMIT 5;"

# Fresh start (⚠️ deletes all data)
make dev-wipe && make dev-up
```

## Production
```bash
# Apply migration
psql -h prod-db -U postgres -d stl_verify -f db/migrations/YOUR_FILE.sql

# Verify applied
psql -h prod-db -U postgres -d stl_verify -c "SELECT * FROM migrations ORDER BY applied_at DESC LIMIT 5;"
```

## Tips

- Use `CREATE INDEX CONCURRENTLY` to avoid table locks
- Test on staging first
- Document rollback in migration comments
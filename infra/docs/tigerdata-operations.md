# TigerData Operations Guide

This document covers operational procedures for TigerData (TimescaleDB) including setup, migrations, and user management.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Tailscale installed and connected to the mesh
- PostgreSQL client installed (`psql`)
- Access to the bastion host

## Database Access

### Starting a Tunnel

TigerData is in a private VPC and requires tunneling through the bastion host:

```bash
# Get the pooler hostname from Secrets Manager or Terraform outputs
make tigerdata-tunnel BASTION_IP=stl-sentinelstaging-bastion TIGERDATA_HOST=<pooler-host>
```

This opens a tunnel on `localhost:5432`. Keep this terminal open.

### Connecting via psql

```bash
# Admin connection (for migrations and user management)
psql "postgresql://tsdbadmin:<password>@localhost:5432/tsdb?sslmode=require"

# Application user connection (for testing)
psql "postgresql://stl_read_write:<password>@localhost:5432/tsdb?sslmode=require"
```

## New Environment Setup

When setting up a new environment, follow these steps in order:

### 1. Apply Terraform

This generates secure passwords and stores them in AWS Secrets Manager. No tunnel required.

```bash
cd infra
tofu apply -var-file=environments/<env>.tfvars
```

### 2. Run Database Migrations

Start the tunnel first, then run migrations to create tables and application users:

```bash
# Terminal 1: Start tunnel
make tigerdata-tunnel BASTION_IP=stl-<env>-bastion TIGERDATA_HOST=<pooler-host>

# Terminal 2: Run migrations
make db-migrate DATABASE_URL="postgresql://tsdbadmin:<password>@localhost:5432/tsdb?sslmode=require"
```

### 3. Set Application User Passwords

After migrations create the users with placeholder passwords, set the real passwords from Secrets Manager:

```bash
make db-set-passwords DATABASE_URL="postgresql://tsdbadmin:<password>@localhost:5432/tsdb?sslmode=require" ENV=<env>
```

### 4. Verify Application User Access

```bash
make db-test-app-user ENV=<env>
```

This tests that `stl_read_write` can connect and perform SELECT/INSERT operations.

## Application Users

Two application users are created with least-privilege access:

| User | Role | Permissions | Use Case |
|------|------|-------------|----------|
| `stl_read_write` | `stl_readwrite` | SELECT, INSERT, UPDATE, DELETE | Watcher, event-persister |
| `stl_read_only` | `stl_readonly` | SELECT only | Monitoring, reporting |

Neither user can:
- CREATE/DROP/ALTER tables or schema objects
- CREATE/DROP indexes  
- TRUNCATE tables
- Execute any DDL commands

New tables automatically inherit the correct grants via `ALTER DEFAULT PRIVILEGES`.

## Credentials in Secrets Manager

Credentials are stored in AWS Secrets Manager:

| Secret | Contents |
|--------|----------|
| `stl-<env>-tigerdata-db` | Admin credentials (tsdbadmin) |
| `stl-<env>-tigerdata-app` | Read/write user (stl_read_write) |
| `stl-<env>-tigerdata-readonly` | Read-only user (stl_read_only) |

Each secret contains:
- `username`, `password`
- `hostname`, `port`, `pooler_hostname`
- `connection_url`, `pooler_url` (pre-formatted connection strings)

### Retrieving Credentials

```bash
# Get stl_read_write password
aws secretsmanager get-secret-value \
  --secret-id stl-<env>-tigerdata-app \
  --query SecretString --output text | jq -r .password

# Get full connection URL
aws secretsmanager get-secret-value \
  --secret-id stl-<env>-tigerdata-app \
  --query SecretString --output text | jq -r .pooler_url
```

## Troubleshooting

### Cannot connect to TigerData

1. Ensure the tunnel is running: `make tigerdata-tunnel ...`
2. Check Tailscale is connected: `tailscale status`
3. Verify the bastion is running: `aws ec2 describe-instances --filters "Name=tag:Name,Values=stl-<env>-bastion"`

### User password not working

Re-run the password sync:

```bash
make db-set-passwords DATABASE_URL="postgresql://tsdbadmin:...@localhost:5432/tsdb?sslmode=require" ENV=<env>
```

### Permission denied errors

Check the user has the correct role membership:

```sql
SELECT r.rolname, m.rolname as member_of
FROM pg_roles r
JOIN pg_auth_members am ON r.oid = am.member
JOIN pg_roles m ON am.roleid = m.oid
WHERE r.rolname LIKE 'stl_%';
```

Expected output:
```
     rolname       |  member_of   
-------------------+--------------
 stl_read_write    | stl_readwrite
 stl_read_only     | stl_readonly
```

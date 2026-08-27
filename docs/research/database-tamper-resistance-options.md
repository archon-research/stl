# What are the options for tamper resistance / tamper evidence for the stl-verify Postgres database on TigerData Cloud?

- **Date:** 2026-08-25
- **Question:** ADR-0006 makes governed tables append-only and leaves `manifest_hash` as "the hook
  for signing/anchoring", deferring tamper evidence to VEC-641. Production runs on TigerData Cloud
  (managed TimescaleDB; prod credential `stl-sentinelprod-tigerdata-app` via
  `k8s/overlays/prod/external-secrets.yaml`, `pooler_url` property; local dev uses
  `k8s/dev-infra/timescaledb.yaml`, image `timescale/timescaledb:2.25.1-pg17`). What does the
  platform give us out of the box, what can be built on top, what does each option actually protect
  against, and what is viable without superuser?
- **Asked by:** VEC-641 phase 1 (tamper evidence), spun out of ADR-0006 review meeting 2026-08-21.
- **Ticket text arrived after the initial draft (same day).** VEC-641 fixes the phased decision of
  2026-08-21: **Phase 1 = pgAudit database-operation log shipped to a WORM audit account (S3 Object
  Lock in a dedicated AWS account); forensic proof = replaying the log, days-scale turnaround
  acceptable. Phase 2 = hash-chaining/Merkle over the data, later.** Constraint (Vialli): phase 1
  must not preclude phase 2. Standards: SOC 2 first; SEC 17a-4, MiFID record-keeping, GDPR in scope;
  DORA to investigate; MAS dropped; aim at the most arduous requirement of the union. §§0–3 are the
  option survey as first drafted; §4 was rewritten to reflect the decision; §6 was added with the
  phase-1 design inputs.
- **Sourcing:** primary sources only where they could be reached: TigerData docs
  (`tigerdata.com/docs`, the canonical host — `docs.tigerdata.com` 301-redirects there), the
  TigerData security page, `timescale/docs` on GitHub (the compression API page redirects there),
  postgresql.org manuals (PG 18), the pgaudit and immudb GitHub READMEs, AWS S3 user guide, RFC 3161,
  RFC 6962, RFC 8785, sigstore/rekor, google/trillian, opentimestamps.org, Microsoft Learn (SQL
  Server ledger, used only as a comparison), and the AWS Japan blog for the QLDB end-of-support date
  (the English QLDB developer guide returned 404 on every URL tried — see §1.9). Items that could not
  be confirmed from a primary source are marked **[unverified]**. Web pages were read through a
  summarising fetch tool; quotes are as returned by it and should be spot-checked before being cited
  externally.

---

## 0. Framing: resistance vs evidence, and the threat model

Two different goals get conflated under "tamper-proof":

- **Tamper resistance** — making an unauthorised change *impossible* for a given principal
  (REVOKE, WORM storage, no-superuser platforms).
- **Tamper evidence** — making an unauthorised change *detectable after the fact* by anyone holding
  an independent reference (hash chains, external anchors, transparency logs).

The SQL Server ledger docs state the honest ceiling for any in-database mechanism: "an attacker or
system administrator who has control of the machine can bypass all system checks and directly
tamper with the data ... Ledger can't prevent such attacks but guarantees that any tampering will
be detected when the ledger data is verified." —
https://learn.microsoft.com/en-us/sql/relational-databases/security/ledger/ledger-overview

Threats used throughout this note (T1–T4):

| ID | Threat | Typical capability |
|---|---|---|
| T1 | **Compromised app credential** (`stl_readwrite` login, or the `pooler_url` secret) | Can run whatever the role is granted; cannot ALTER tables it does not own. |
| T2 | **Privileged DBA / `tsdbadmin` holder** (or the `stl_migrator` owner role) | Owns the tables: can DROP TRIGGER, DISABLE TRIGGER, ALTER TABLE, re-GRANT, UPDATE/DELETE rows. |
| T3 | **Provider insider / host compromise** (TigerData ops, AWS) | Can edit files on disk, WAL, backups; TigerData: "Tiger Data operations team has the capability to securely log in to the service virtual machines for troubleshooting purposes. These accesses are audit logged." — https://www.tigerdata.com/docs/use-timescale/latest/security/overview |
| T4 | **Rollback via restore** — a PITR fork/restore to an earlier point presented as "current", or deletion of the service (`tiger service delete` "deletes a production service the same way it deletes a development one, with no extra check" — https://www.tigerdata.com/docs/use-timescale/latest/services/service-management) | Data is *authentic but stale/missing*; only detectable with an external record of "latest known root". |

---

## 1. TigerData Cloud built-ins

### 1.1 Superuser and `tsdbadmin`

There is no superuser. "Tiger Cloud does not provide superuser access. tsdbadmin is not a
superuser." and "When you create a service, Tiger Cloud assigns you the tsdbadmin role. This role
has full permissions to modify data in your service." —
https://www.tigerdata.com/docs/use-timescale/latest/security/read-only-role

"The `tsdbadmin` database user is the most powerful available on Tiger Cloud, but it is not a true
superuser." and "The tsdbadmin user does not have sufficient privileges to turn off triggers in the
timescaledb_catalog schema." — https://www.tigerdata.com/docs/migrate/latest/troubleshooting

**[unverified]** The exact role attributes of `tsdbadmin` (CREATEROLE, REPLICATION, BYPASSRLS,
membership in `pg_signal_backend` etc.) are not enumerated in the docs found. Our own runbook
(`docs/runbooks/vector-postgres.md`) assumes `tsdbadmin` has `pg_signal_backend`; our migrations
note that `stl_migrator` is "CREATEROLE only" and that setting passwords "requires superuser, which
the migration role ... does not have" — implying the Terraform bootstrap step runs as `tsdbadmin`
(`stl-verify/db/migrations/20260618_120000_create_om_read_only_user.sql`). Run
`SELECT rolsuper, rolcreaterole, rolreplication, rolbypassrls FROM pg_roles WHERE rolname =
'tsdbadmin'` against prod to close this.

**Consequences for us**: (a) *nobody we know of* — including a T2 attacker — can create event
triggers (§2.1), set `session_replication_role` without SET privilege, or alter
`shared_preload_libraries`; (b) tsdbadmin nevertheless owns or can take ownership of every
application object, so every in-database control is defeatable by T2.

### 1.2 Extensions

Tiger Cloud's supported list includes `pgaudit` ("Detailed session and/or object audit logging"),
`pgcrypto` ("Cryptographic functions") and `pg_cron` ("SQL commands that you can schedule and run
directly inside the database" — but "contact support@tigerdata.com to enable"). Full list at
https://www.tigerdata.com/docs/use-timescale/latest/extensions. Note also that core PostgreSQL
provides `sha256(bytea)` without any extension —
https://www.postgresql.org/docs/current/functions-binarystring.html — so row hashing needs no
extension at all; `pgcrypto` adds `hmac()` ("the hash can only be recalculated knowing the key.
This prevents the scenario of someone altering data and also changing the hash to match") —
https://www.postgresql.org/docs/current/pgcrypto.html.

`shared_preload_libraries` is not customer-controlled: "Tiger Cloud manages preloading for you; you
typically only run CREATE EXTENSION" (search summary of
https://www.tigerdata.com/docs/use-timescale/latest/configuration/customize-configuration
**[quote not verified against the page]**).

### 1.3 Backups and PITR

"Tiger Cloud automatically creates one full backup every week, and incremental backups every day in
the same region as your service." Retention depends on plan; Enterprise allows "up to 180 days";
"if you set retention to 90 days, you can fork your service to any point in the previous 90 days."
"Cross-region backups are always kept for 14 days" (Enterprise). Recovery is by **fork**: "The
original service stays untouched." — https://www.tigerdata.com/docs/use-timescale/latest/backup-restore

Backups are provider-held (pgBackRest): the docs do not offer customer download of backups, nor a
way for the customer to delete them. **Backups are therefore customer-inaccessible — which is good
against T1/T2 deletion, but they are not customer-verifiable and are irrelevant against T3.** No
"immutable backup" or WORM claim is made anywhere in the backup docs.

### 1.4 Tiered storage (S3 object tier)

"you cannot insert data into, update, or delete a tiered chunk" and these "limitations take effect
as soon as the chunk is scheduled for tiering". Storage is "based on AWS S3 and Azure Blob storage"
in "the Apache Parquet format". — https://www.tigerdata.com/docs/use-timescale/latest/data-tiering/about-data-tiering

But tiered data is **not** immutable in the WORM sense:
- It can be brought back and changed: "To update data in a tiered chunk, move it back to the
  high-performance storage tier ... Untiering chunks is a synchronous process." (`CALL
  untier_chunk(...)`) — https://www.tigerdata.com/docs/use-timescale/latest/data-tiering/enabling-data-tiering
- It can be deleted: "To drop tiered data, call `DROP TABLE` on the corresponding hypertable. This
  removes the hypertable and all its associated data from the high-performance and low-cost
  storage." — https://www.tigerdata.com/docs/build/data-management/storage/manage-storage
- Deletion is soft for 14 days: "we never delete anything on the object storage tier if at least
  one server references it" and deletion is delayed 14 days "so that in case of a restore or PITR,
  all tiered data will be available." — https://www.tigerdata.com/docs/build/data-management/storage/tiered-data-replicas-forks
- The bucket is TigerData's; the docs "do not specify who controls the S3 ... buckets" and give no
  customer access path.

Net: tiering is a *speed bump* (T1 cannot UPDATE tiered rows; T2 can untier then UPDATE, or DROP
TABLE). It is not evidence and not WORM.

### 1.5 Compressed chunks (columnstore/hypercore)

Compression does **not** block DML. "In TimescaleDB v2.3 and later, you can insert data into
compressed chunks" and "In TimescaleDB v2.11 and later, you can update and delete compressed data.
You can also use advanced insert statements like `ON CONFLICT` and `RETURNING`." Compression API is
"Superseded by Hypercore. However, compression APIs are still supported". —
https://github.com/timescale/docs/blob/latest/api/compression/index.md. Our dev image is 2.25.1, so
compression provides no tamper resistance at all.

### 1.6 Read-only roles and read replicas

Read-only roles are plain Postgres GRANTs: "Adding a read-only role does not provide resource
isolation." Read replicas: "A read replica is a read-only copy of your primary database instance"
using "asynchronous replication" — https://www.tigerdata.com/docs/use-timescale/latest/ha-replicas/read-scaling.
HA replicas "have separate unique addresses that you can use to serve read-only requests" —
https://www.tigerdata.com/docs/use-timescale/latest/ha-replicas/high-availability. Replicas follow
the primary, so they replay tampering faithfully; they are not evidence.

### 1.7 Logging, pgaudit, exports

pgaudit is enabled per service via console parameters: "Add the values you want to set in the
'pgaudit.log' and 'pgaudit.log_client' common parameters" (Operations → Database Parameters →
Advanced Parameters), then `CREATE EXTENSION pgaudit;`; logs appear in the service Logs tab and "You
can export them to CloudWatch." — https://www.tigerdata.com/learn/what-is-audit-logging-and-how-to-enable-it-in-postgresql
(vendor "learn" article, undated; the changelog entry "The Postgres Audit extension (pgaudit) is
now available on Timescale Cloud ... You can also export these audit logs to Amazon CloudWatch"
appeared in search snippets but could not be located on the current changelog page
**[date unverified]**). Exporters are "available for Scale and Enterprise pricing tiers" and the
"AWS region must be the same" for exporter and CloudWatch log group —
https://www.tigerdata.com/docs/use-timescale/latest/metrics-logging/aws-cloudwatch.

Upstream pgaudit: "Settings may be modified only by a superuser. Allowing normal users to change
their settings would defeat the point of an audit log." and "Object audit logging logs statements
that affect a particular relation. Only SELECT, INSERT, UPDATE and DELETE commands are supported."
— https://github.com/pgaudit/pgaudit. On Tiger Cloud the "superuser" is effectively *anyone with
console access to Advanced Parameters* (project Owner/Admin/Developer roles —
https://www.tigerdata.com/docs/use-timescale/latest/security/members). So pgaudit gives good T1
evidence, weak T2 evidence (a console-holder can turn it off, though the CloudWatch trail up to
that point survives if the CloudWatch log group is in a locked-down AWS account), and no T3/T4
value.

### 1.8 Compliance claims

"Tiger Cloud is SOC 2 Type 2 compliant." Enterprise plan "is HIPAA compliant"; GDPR compliance is
stated. "All data volumes, including backups, are encrypted at rest with unique keys specific to
each service" — https://www.tigerdata.com/security and
https://www.tigerdata.com/docs/use-timescale/latest/security/overview. **No immutability, WORM,
ledger, or SEC 17a-4 claim exists anywhere in TigerData's docs or security page.** Production
services have "delete protection" in the console, but the CLI bypasses it (§0, T4).

### 1.9 What TigerData does NOT offer (comparison with real ledger products)

- **SQL Server / Azure SQL ledger**: per-transaction Merkle roots chained into blocks; digests
  "periodically generated and stored outside the database in tamper-proof storage, such as Azure
  Blob Storage configured with immutability policies, Azure Confidential Ledger or ... WORM";
  "Append-only ledger tables block updates and deletions at the API level." —
  https://learn.microsoft.com/en-us/sql/relational-databases/security/ledger/ledger-overview
- **Amazon QLDB**: immutable journal with cryptographic verification; **end of support 31 July
  2025**: "Amazon QLDB は、2025 年 7 月 31 日にサポートが終了することがアナウンスされています" (AWS Japan
  blog, 8 Oct 2024, recommending Aurora PostgreSQL) —
  https://aws.amazon.com/jp/blogs/news/migration-from-amazon-qldb/. The English QLDB developer
  guide URLs all returned 404 during this research, consistent with retirement. **Not an option.**
- **immudb**: "a database with built-in cryptographic proof and verification ... the integrity of
  the history will be protected by the clients, without the need to trust the database";
  "Cryptographic commit log with parallel Merkle Tree"; "You can add new versions of existing
  records, but never change or delete records." Licensed "under the Business Source License 1.1".
  — https://github.com/codenotary/immudb
- **Postgres itself** has no ledger-table feature in any version, and there is no Postgres
  extension for it in Tiger Cloud's supported list. Anything ledger-like must be built (§2.2) and
  anchored (§2.3).

---

## 2. Build-on-top options

### 2.1 Postgres privilege model (resistance against T1 only)

What is available without superuser, and what we already have:

- Existing roles: `stl_readonly` (SELECT), `stl_readwrite` (SELECT/INSERT/UPDATE/DELETE via
  `ALTER DEFAULT PRIVILEGES`), owner `stl_migrator` (CREATEROLE, no superuser) —
  `stl-verify/db/migrations/20260122_140100_create_app_roles_and_privileges.sql`. ADR-0006 §1 plans
  an insert-only app role plus guard triggers.
- **REVOKE UPDATE/DELETE** from the app role on governed tables: a T1 attacker with the app
  credential simply cannot run them. But: "The right to modify or destroy an object is inherent in
  being the object's owner, and cannot be granted or revoked in itself." and "owners are always
  treated as holding all grant options, so they can always re-grant their own privileges." —
  https://www.postgresql.org/docs/current/ddl-priv.html. So the owner (and tsdbadmin) is unaffected.
- **BEFORE UPDATE/DELETE triggers that RAISE**: belt-and-braces against T1 (covers accidental
  GRANTs), useless against T2: "You must own the table on which the trigger acts to be allowed to
  change its properties." and enable/disable "is provided by ALTER TABLE" —
  https://www.postgresql.org/docs/current/sql-altertrigger.html. A T2 attacker does
  `ALTER TABLE ... DISABLE TRIGGER` or `DROP TRIGGER`. Ordinary triggers also do not fire when
  `session_replication_role = replica`, which "Only superusers and users with the appropriate SET
  privilege can change" — https://www.postgresql.org/docs/current/runtime-config-client.html —
  use `ENABLE ALWAYS TRIGGER` to remove that loophole
  (https://www.postgresql.org/docs/current/sql-altertable.html).
- **Event triggers** (to catch `DROP TRIGGER`/`ALTER TABLE`): "Only superusers can create event
  triggers." — https://www.postgresql.org/docs/current/sql-createeventtrigger.html. **Not viable on
  Tiger Cloud** (nobody is superuser).
- **RLS**: "Superusers and roles with the BYPASSRLS attribute always bypass the row security
  system"; "Table owners normally bypass row security as well", unless `FORCE ROW LEVEL SECURITY` —
  https://www.postgresql.org/docs/current/ddl-rowsecurity.html. Adds nothing over REVOKE for
  append-only; ignore.
- **Owner separation**: keep `stl_migrator` as owner and never let services hold it; the app role
  then cannot `ALTER TABLE` ("You must own the table to use ALTER TABLE" —
  https://www.postgresql.org/docs/current/sql-altertable.html). Migrations run as owner from CI
  only. The `TRIGGER` privilege must also *not* be granted to the app role ("any triggers added to a
  table or view will be executed with the privileges of users who modify it" —
  https://www.postgresql.org/docs/current/ddl-priv.html).

Verdict: cheap, already half-built, **resistance against T1 only**, zero evidence value. Every
control is undone by T2 in one statement. Worth doing because the app credential (in
`pooler_url`, refreshed every 3 minutes into k8s Secrets) is the most exposed principal.

### 2.2 Row-level hash chain / Merkle tree over append-only tables (evidence against T1–T3)

Design sketch built on ADR-0006 primitives:

- Each governed row already carries `run_id`/`ingest_xid`; each calculation record carries
  `manifest_hash`. Add a per-batch (per `writer_run` commit, or per N rows / per minute) **leaf
  hash** = SHA-256 over the canonical serialisation of the rows in the batch, and a **chain**
  `root_i = H(root_{i-1} || leaf_i)` in an insert-only `integrity_checkpoint` table (or a Merkle
  tree per period with RFC 6962 audit/consistency proofs: "Merkle consistency proofs prove the
  append-only property of the tree" — https://www.rfc-editor.org/rfc/rfc6962.txt).
- Canonicalisation: use RFC 8785 JCS for JSON-shaped inputs — "Cryptographic operations like
  hashing and signing need the data to be expressed in an invariant format so that the operations
  are reliably repeatable"; numbers "MUST be serialized according to Section 7.1.12.1 of
  [ECMA-262]"; properties sorted by UTF-16 code units — https://www.rfc-editor.org/rfc/rfc8785.txt.
  **Caveat for us**: JCS number rules are IEEE-754 double; our `NUMERIC`/`bigint` wei amounts
  exceed double precision and must be serialised as strings (which JCS permits) — decide this once
  and encode it in the manifest schema; the same rule should govern `manifest_hash`.
- Compute the hash in the writer (Go/Python), not in a trigger, so the hash logic is versioned with
  `git_hash` and the DB cannot silently change it; optionally re-derive in SQL with `sha256()` for
  the verifier.
- Hashing alone is evidence only if the roots are held somewhere the attacker cannot rewrite — a
  T2 attacker who can UPDATE a row can also rewrite `integrity_checkpoint`. Hence §2.3.

Cost: moderate (canonicaliser + checkpoint table + verifier); the canonicaliser is needed anyway
for `manifest_hash`. Threats: T1–T3 **once anchored**; T4 partially (a rollback is detected as
"database root is behind the last anchored root").

### 2.3 External anchoring of roots

The root (or a daily/hourly Merkle root of roots) is small; anchoring it is what turns §2.2 into
evidence. Options, from cheapest to strongest:

| Anchor | What it proves | Viable for us | Notes |
|---|---|---|---|
| **RFC 3161 TSA** (freeTSA, DigiCert, etc.) | "proof that a datum existed before a particular time"; TSA must "only time-stamp a hash representation of the datum" and "not ... examine the imprint" — https://www.rfc-editor.org/rfc/rfc3161.txt | Yes, trivial (one HTTPS POST per root) | Trust moves to the TSA: freeTSA notes security holds only if "the timestamper's integrity is never compromised" — https://www.freetsa.org/index_en.php. Use a commercial TSA for anything auditors see; store the token next to the root. |
| **S3 Object Lock, compliance mode**, in a separate AWS account | "a protected object version can't be overwritten or deleted by any user, including the root user in your AWS account ... The only way to delete an object under the compliance mode before its retention date expires is to delete the associated AWS account." — https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html | Yes; we already archive to S3 (`RAW_SC_BUCKET`, manifest bucket) | Governance mode is *not* immutability (`s3:BypassGovernanceRetention`). Enabling Object Lock on a bucket is irreversible; "Delete markers are not WORM-protected"; "maximum retention period is 100 years"; KMS key deletion can still make objects unreadable — https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-managing.html. Object Lock "has been assessed by Cohasset Associates for use in environments that are subject to SEC 17a-4". Put the bucket in an account the platform team does not administer, or at least with SCP-restricted access. |
| **Sigstore Rekor (public transparency log)** | Append-only Merkle log with inclusion proofs and signed tree heads; "an immutable, tamper-resistant ledger of metadata" — https://docs.sigstore.dev/logging/overview/ | Technically yes (hashedrekord of a root, signed with our key); **socially doubtful** | Public instance is for software supply chain; "attestation size limit for uploads to the public instance is 100KB"; 99.5% SLO — https://github.com/sigstore/rekor. Retention/pruning policy of the public instance not documented **[unverified]**. Running our own Rekor/Trillian is heavy: Trillian "is in maintenance mode ... We recommend that any new log operators first try Tessera" — https://github.com/google/trillian. |
| **OpenTimestamps (Bitcoin)** | "A timestamp proves that some data existed prior to some point in time"; calendar servers "are free to use and they don't require any registration or api key" — https://opentimestamps.org/ | Yes, trivial; proof upgrades lag until a Bitcoin block includes the aggregation | Cost-free; proofs verifiable by anyone with a Bitcoin node; independent of any vendor. |
| **Ethereum (or an L2) anchoring** | Same "existed before block N" property, from our own contract or calldata | Natural for this product (we already run chain RPC infra) | **No primary source consulted**; cost = gas per anchor + a contract or calldata convention; verification is `eth_getTransactionByHash`. Note the reflexivity: anchoring evidence about chain-derived data to the same chain is fine for integrity but not "independent" in the auditor's sense. |
| **AWS QLDB** | — | **No** — end of support 31 July 2025 (§1.9) | |
| **immudb** (self-hosted) | Client-verifiable Merkle proofs | Possible but a new stateful service to run; BSL 1.1 licence | Overkill for anchoring a root; more relevant as a shadow ledger (§2.4). |

Recommended pairing: S3 Object Lock (compliance) for **durable custody** of roots + one
**third-party time attestation** (RFC 3161 or OpenTimestamps) so that the root's *time* does not
depend on our own AWS account. Two anchors in different trust domains cover T3 for either vendor.

### 2.4 Log-based evidence and shadow ledgers

- **pgaudit → CloudWatch** (§1.7): SESSION logging of `WRITE`+`DDL` for the app and migrator roles
  gives a who/what/when trail of every UPDATE/DELETE/ALTER/DROP TRIGGER. Send it to a CloudWatch log
  group in a separate AWS account with a retention policy and no delete permissions for platform
  operators. Weaknesses: console-holders can disable it; volume/cost of logging every INSERT on
  ingest tables is prohibitive — log DDL, UPDATE, DELETE, ROLE only, not INSERT/READ.
- **Logical replication / CDC out of Tiger Cloud to an append-only store**: Postgres logical
  replication needs `wal_level = logical`, a REPLICATION-capable role and a publication; a
  publication can be restricted to `publish = 'insert'` and requires only "ownership rights on the
  table" (FOR ALL TABLES needs superuser) — https://www.postgresql.org/docs/current/sql-createpublication.html.
  **Whether a Tiger Cloud service can act as a logical-replication *source* is not documented**:
  Livesync docs describe Tiger Cloud as the *target*, and the Debezium integration page "applies
  exclusively to self-hosted TimescaleDB instances" — https://www.tigerdata.com/docs/integrate/data-engineering-etl/debezium.
  `wal_level` is not in the customer-tunable parameter list found
  (https://www.tigerdata.com/docs/use-timescale/latest/configuration/advanced-parameters). Search
  snippets mention TigerLake connectors and a "Slot Keep Controller" managing replication slots,
  which suggests logical decoding exists on the platform, but **[unverified]** for customer use.
  Ask TigerData support before designing around it. Also note Debezium's caveat that compressed
  chunk changes are "typically ... dropped".
- If CDC is not available, the **application itself is the CDC**: the writer already emits the
  archive objects (raw block payloads, SC-call archives, manifests) to S3; adding per-batch hash
  checkpoints to an Object-Locked bucket (§2.2/§2.3) is the shadow ledger, with the DB as the
  queryable copy. This is effectively the SQL Server "digest to immutable storage" pattern, done by
  hand.

### 2.5 Periodic verification job

Pure application work; fits the ADR-0006 "assurance sampling job":

1. Fetch latest anchored root(s) from the Object-Locked bucket (and the TSA/OTS proof).
2. Recompute the chain from the DB for the sampled window(s): recent (last 24h, full) and
   historical (random chunks, sampled).
3. Assert: DB root == anchored root; DB has no checkpoint *newer* than the anchor by more than the
   anchoring interval (catches a stopped anchorer); DB's latest root is *not older* than the latest
   anchor (catches T4 rollback).
4. Emit Prometheus metrics + alert (`alerts/` + runbook per `docs/runbooks/AGENTS.md`).

Run it from a principal that has SELECT only and lives outside the platform team's blast radius
(e.g. a read-only role on an HA/read replica, §1.6 — replicas are read-only copies so the verifier
cannot be tricked into "fixing" anything). Cost: small; the canonicaliser and hasher are shared with
§2.2.

### 2.6 Backups as evidence

TigerData backups cannot be exported or inspected (§1.3). Independent evidence-grade backups mean
we take them ourselves:

- `pg_dump` works against a service (TigerData's own migration docs show
  `pg_dump 'postgres://tsdbadmin:...@...timescaledb.io:.../defaultdb?sslmode=require' ...` and
  suggest uploading the tar to S3 — https://www.tigerdata.com/docs/migrate/latest/pg-dump-and-restore
  **[quote from search snippet; page not fetched]**). ADR-0006 §5 says replay must use a physical
  fork, *not* a logical dump — but that is about MVCC-exact replay; a logical dump is fine as
  **evidence of content at time T**, especially if the dump's hash is itself anchored.
- Write dumps to an S3 Object Lock (compliance) bucket in a separate account; a rollback (T4) or
  silent edit (T2/T3) then shows as a diff against the locked dump. Cost: storage grows with DB
  size; sampling (governed tables only, or per-chunk Parquet exports) keeps it bounded.
- `pg_basebackup`/physical export is presumably not possible without replication privileges on a
  managed service **[unverified]**.

---

## 3. Comparison table

Resistance = prevents; Evidence = detects. "Effort" is relative to the ADR-0006 work already planned.

| Option | T1 app cred | T2 DBA/tsdbadmin | T3 provider/host | T4 rollback/delete | Resistance or evidence | Viable on Tiger Cloud | Effort |
|---|---|---|---|---|---|---|---|
| 2.1 REVOKE + guard triggers + owner separation | Yes | No | No | No | Resistance | Yes (no superuser needed) | Low — ADR-0006 §1 already |
| 2.1 Event triggers on DDL | — | — | — | — | — | **No** (superuser only) | — |
| 1.4 Tiered storage | Partial (no DML on tiered chunks) | No (untier / DROP TABLE) | No | No | Weak resistance | Yes | None (already available) |
| 1.5 Compression | No | No | No | No | None | — | — |
| 1.7 pgaudit → CloudWatch (separate account) | Yes | Partial (until disabled; trail survives) | No | No | Evidence | Yes (Scale/Enterprise for exporter) | Low–Med |
| 2.2 Hash chain / Merkle in DB, unanchored | Yes | No (rewrite checkpoints) | No | No | Evidence (weak) | Yes | Med |
| 2.2 + 2.3 S3 Object Lock compliance (separate account) | Yes | Yes | Yes (TigerData); No (AWS insider if same cloud) | Yes | Evidence (strong) | Yes | Med |
| 2.3 + RFC 3161 TSA | adds independent time | Yes | Yes | Yes | Evidence | Yes | Low (on top of 2.2) |
| 2.3 + OpenTimestamps / Ethereum | adds public, vendor-free time | Yes | Yes | Yes | Evidence | Yes | Low; ETH costs gas |
| 2.3 Rekor public / own Trillian | Yes | Yes | Yes | Yes | Evidence | Public: policy-doubtful; own: heavy | High |
| 2.3 QLDB | — | — | — | — | — | **No** (EOL 2025-07-31) | — |
| 2.4 Logical replication / CDC to WORM store | Yes | Partial | Partial | Yes | Evidence | **Unverified** on Tiger Cloud | Med–High |
| 2.5 Verification job + alerts | (makes 2.2/2.3 actionable) | | | Yes | Evidence | Yes | Low–Med |
| 2.6 Own pg_dump to Object-Locked bucket | Yes | Yes | Yes | Yes | Evidence (coarse) | Yes | Med; storage cost |
| 1.3 TigerData backups/PITR | No (recovery only) | Deletion-resistant, not verifiable | No | Recovery, not detection | Recovery | Yes | None |

---

## 4. Phased shape as decided in VEC-641 (2026-08-21), with this note's reading of it

**Phase 1 (decided): pgAudit → CloudWatch → S3 Object Lock (compliance) in a dedicated audit
account.** Forensic proof is *replaying the log*; a days-scale turnaround is acceptable. Design
inputs, retention, and the non-preclusion checklist are in §6. In the vocabulary of §0 this is
**evidence, not resistance**: it detects T1 fully, T2 up to the moment the log is disabled (the
shipped trail survives), T4 by absence-of-log for a restored fork, and T3 only insofar as the
provider does not also suppress the log stream. It should be paired with the already-planned
ADR-0006 §1 controls (insert-only app role, `ENABLE ALWAYS` guard triggers, owner separation, no
`TRIGGER` privilege to the app role), which are cheap T1 *resistance* and — importantly for phase 2
— keep governed tables insert-only so a chain can later be back-filled (§6.5).

**Phase 2 (later): hash chain / Merkle over the data, anchored.** The shape previously proposed as
phase 1 in this note moves here unchanged: define canonicalisation once (RFC 8785, big integers as
strings) and use it for `manifest_hash` *and* a per-`writer_run` batch hash chained into an
insert-only `integrity_checkpoint` table, computed in the writer and versioned by `git_hash`;
anchor the chain head to the same Object-Locked audit bucket plus an independent time attestation
(RFC 3161 or OpenTimestamps; Ethereum anchoring as a natural add-on for this product); fold
verification into the ADR-0006 §5 assurance sampling job (mismatch, stale anchor, DB-head-older-
than-anchor). Phase 2 upgrades T2/T3 coverage from "detectable if the log was shipped" to
"detectable from the data itself".

**Explicitly not recommended in either phase:** running our own Trillian/Rekor/immudb, relying on
tiering or compression for immutability, or relying on TigerData backups as evidence.

**Gating questions for TigerData support** (also listed in §6.6): exact `tsdbadmin` role
attributes; whether `pgaudit.role` and the `log_*` GUCs are settable from the console; whether a
Tiger Cloud service can be a logical-replication *source*; which pricing plan we are on (the
CloudWatch exporter is Scale/Enterprise only).

---

## 5. Open items / could not verify

- `tsdbadmin` role attributes (CREATEROLE, REPLICATION, BYPASSRLS, predefined role memberships).
- Whether customer-created logical replication slots/publications are supported on Tiger Cloud.
- Date of pgaudit availability on Tiger Cloud, and whether pgaudit parameters are editable by all
  console roles or only Owner/Admin.
- Whether `wal_level`, `log_statement`, `log_connections` are customer-tunable.
- Sigstore public Rekor retention/pruning and acceptable-use for non-supply-chain hashes.
- QLDB EOL confirmed only via the AWS Japan blog (English developer guide 404s).
- pg_dump quote is from a search snippet of the TigerData migration page, not from a full fetch.

---

## 6. Phase 1 design inputs: pgAudit → WORM on TigerData Cloud

Sourcing for this section: pgaudit README (`github.com/pgaudit/pgaudit`, main), TigerData docs and
"learn" article, PostgreSQL 18 manual, AWS CloudWatch Logs / Data Firehose / S3 / Organizations /
CloudTrail user guides, eCFR renderer API for 17 CFR 240.17a-4, legislation.gov.uk (UK retained
text) for MiFID II Art. 16(7), and the EU Publications Office CELLAR service for CDR 2017/565,
GDPR and DORA (downloaded as XHTML/XML and grepped locally; EUR-Lex itself and the CELLAR "DOC_1"
default representation blocked or returned metadata only). SOC 2 CC7.2 wording is from a
secondary reproduction, marked **[unverified]**.

### 6.1 pgAudit configuration on TigerData Cloud

**What is exposed.** TigerData's own guidance for Tiger Cloud is: "Add the values you want to set
in the 'pgaudit.log' and 'pgaudit.log_client' common parameters" under Operations → Database
Parameters → Advanced Parameters, then `CREATE EXTENSION pgaudit;` —
https://www.tigerdata.com/learn/what-is-audit-logging-and-how-to-enable-it-in-postgresql. The
Advanced-parameters doc page lists only memory/connection GUCs as examples and says "For a complete
list of available parameters, see the Grand Unified Configuration (GUC) parameters reference" —
https://www.tigerdata.com/docs/use-timescale/latest/configuration/advanced-parameters — but the
linked reference lists TimescaleDB GUCs and does not enumerate `pgaudit.*` or `log_*` settings
(https://www.tigerdata.com/docs/reference/timescaledb/configuration/tiger-postgres). **Whether
`pgaudit.log_parameter`, `log_relation`, `log_catalog`, `log_statement_once`, `log_rows` and
`pgaudit.role` are all settable from the console is [unverified] — confirm in the console search
box or with support.** Upstream: "Settings may be modified only by a superuser" —
https://github.com/pgaudit/pgaudit — so on Tiger Cloud the console is the only path; nothing can be
set per-role via `ALTER ROLE ... SET pgaudit.log` by a non-superuser **[unverified whether
tsdbadmin can — test]**.

**Class semantics (pgaudit README, verbatim):**
- "READ: SELECT and COPY when the source is a relation or a query"
- "WRITE: INSERT, UPDATE, DELETE, TRUNCATE, and COPY when the destination is a relation"
- "FUNCTION: Function calls and DO blocks"
- "ROLE: Statements related to roles and privileges"
- "DDL: All DDL that is not included in the ROLE class"
- "MISC: Miscellaneous commands, e.g. DISCARD, FETCH, CHECKPOINT, VACUUM, SET"
- MISC_SET: "Miscellaneous SET commands, e.g. SET ROLE."
- `pgaudit.log_parameter`: "Specifies that audit logging should include the parameters that were
  passed with the statement. When parameters are present they will be included in CSV format after
  the statement text."; `pgaudit.log_parameter_max_size`: values longer than this "should not be
  logged, but replaced with `<long param suppressed>`".
- `pgaudit.log_rows`: "include the number of rows retrieved or affected by a statement".
- `pgaudit.log_statement_once`: "whether logging will include the statement text and parameters
  with the first log entry for a statement/substatement combination or with every entry."
- `pgaudit.log_relation`: "a separate log entry for each relation (TABLE, VIEW, etc.) referenced in
  a SELECT or DML statement."
- `pgaudit.log_catalog`: "session logging should be enabled in the case where all relations in a
  statement are in pg_catalog."
- `pgaudit.role`: "Specifies the master role to use for object audit logging. Multiple audit roles
  can be defined by granting them to the master role." Object logging "logs statements that affect
  a particular relation. Only SELECT, INSERT, UPDATE and DELETE commands are supported." A relation
  "will be audit logged when the audit role has permissions for the command executed or inherits
  the permissions from another role."
- Format: "STATEMENT_ID - Unique statement ID for this session. Each statement ID represents a
  backend call. Statement IDs are sequential even if some statements are not logged." and
  "SUBSTATEMENT_ID - Sequential ID for each sub-statement within the main statement." Fields:
  AUDIT_TYPE, STATEMENT_ID, SUBSTATEMENT_ID, CLASS, COMMAND, OBJECT_TYPE, OBJECT_NAME, STATEMENT,
  PARAMETER (rows appended when `log_rows` is on).

**Recommended session classes: `pgaudit.log = 'WRITE, DDL, ROLE, MISC_SET'`, with the INSERT
problem handled by object logging (below).** DDL is what makes trigger/constraint removal visible
(`ALTER TABLE ... DISABLE TRIGGER`, `DROP TRIGGER`, `ALTER TABLE ... OWNER TO`); ROLE covers
GRANT/REVOKE/CREATE ROLE; MISC_SET catches `SET ROLE` (privilege hopping) at low volume. FUNCTION
is low-value here (the guard triggers are the functions; their *effects* are DML already logged).
READ is out: volume, and it is not a tamper class.

**The INSERT-volume problem.** Session-mode WRITE cannot exclude INSERT — the class is atomic.
Ingest hypertables receive continuous INSERTs, so WRITE session logging on the app role would log
every ingest statement (with `log_statement_once` and no parameters it is one line per statement,
not per row — bulk inserts are one line — but batch-per-block ingestion is still thousands of
lines/hour). Two ways out:

1. **Object audit logging with a role that holds only UPDATE/DELETE** (and TRUNCATE is not
   supported in object mode, so keep TRUNCATE in a session class or revoke it). Create
   `NOLOGIN` role `stl_audit`, `GRANT UPDATE, DELETE ON <every governed table> TO stl_audit`
   (and `ALTER DEFAULT PRIVILEGES ... GRANT UPDATE, DELETE ON TABLES TO stl_audit` for future
   tables, mirroring `20260122_140100_create_app_roles_and_privileges.sql`), set
   `pgaudit.role = 'stl_audit'`. Then every UPDATE/DELETE against a governed table by *any* role is
   logged; INSERTs are not. The grant itself needs only table ownership (`stl_migrator`), no
   superuser. **Viability hinges on `pgaudit.role` being settable from the Tiger console
   [unverified].** Object entries carry `OBJECT_NAME` and the statement text, which is what a
   replay needs.
2. **Session-mode WRITE only for the roles that should never write DML**: on Postgres the
   canonical way is `ALTER ROLE tsdbadmin SET pgaudit.log = 'WRITE, DDL, ROLE'` while the app role
   has only `DDL, ROLE` — but per-role `SET` of a superuser-only GUC needs superuser
   **[unverified on Tiger Cloud; likely not possible]**.

Recommended: (1), with session `DDL, ROLE, MISC_SET` globally; fall back to session
`WRITE, DDL, ROLE` globally and accept the INSERT volume if `pgaudit.role` proves unsettable,
budgeting CloudWatch ingestion accordingly.

**`log_parameter` — replay fidelity vs GDPR.** With parameters off, a prepared
`UPDATE positions SET amount = $1 WHERE id = $2` replays as *"something changed row $2"* only;
with `log_parameter = on` the exact new value is in the log. For forensic replay (§6.1 below,
"what replay proves") parameters are what turn "an UPDATE happened" into "the value became X".
Our governed tables hold on-chain amounts, addresses, block numbers and protocol identifiers —
addresses can be personal data in some analyses (see `docs/research/persisted-manifest-standards.md`
§4). Recommendation: `log_parameter = on` **only** for the object-logged UPDATE/DELETE path (which
should be empty in steady state — every parameter logged is evidence of an anomaly), and never
enable it together with a session WRITE class that includes INSERT (that would duplicate the entire
dataset into the log). Use `log_parameter_max_size` to cap blobs. Do not log READ.

**What pgAudit does NOT capture (README "Caveats", verbatim):**
- "Audit logging is best-effort and not transactional. pgAudit writes audit entries through the
  standard PostgreSQL logging facility, which does not flush each entry to disk synchronously with
  the transaction that produced it ... There is no guarantee that a committed transaction will have
  a corresponding audit log entry. ... Conversely, a statement is logged when it executes, so an
  entry may be written even if its transaction later rolls back."
- "Autovacuum and Autoanalyze are not logged."
- "Statements that are executed after a transaction enters an aborted state will not be audit
  logged."
- "It is not possible to reliably audit superusers with pgAudit." (No superuser exists on Tiger
  Cloud, which is a point in our favour — but TigerData's own operators are outside this log
  entirely.)
- "Object renames are logged under the name they were renamed to."
- Row *content* is never logged — only the statement (and parameters if enabled) and, with
  `log_rows`, a row count. Rows changed by a trigger body are logged only if that trigger's
  statements hit an object-logged relation or a session class; a `SECURITY DEFINER` function
  executing as owner still runs under the session's `pgaudit.log`.
- Changes applied by physical replication / a restored fork produce no audit lines on the new
  primary (they are not statements); WAL-level or file-level edits (T3) are invisible.
- COPY is covered (READ/WRITE classes name it), but `pg_dump`-style bulk reads are READ (not
  recommended to log).

**What "replaying the log" can and cannot prove.** Given a complete, ordered log with DDL, ROLE,
MISC_SET and object-logged UPDATE/DELETE (+ parameters) on governed tables:
- *Can* prove: that no UPDATE/DELETE/TRUNCATE touched a governed table in the window (absence of
  entries, assuming the log was flowing — see heartbeat, §6.2); which principal (`%u`), session
  (`%c`), transaction (`%x`) and statement changed which table with which statement text; that no
  trigger/constraint/ownership DDL occurred; that no GRANT widened the app role. Combined with the
  append-only invariant this is the CDR 2017/565 Art. 72(1)(b)-(c) shape: prior contents are the
  rows themselves; the log shows whether anyone tried to alter them.
- *Cannot* prove: the *content* of a change when parameters are suppressed or when the statement
  is a non-parameterised expression (`SET amount = amount * 2` — the log shows the expression, not
  the resulting value; the DB row shows the value, so "before" is unrecoverable without phase 2 or
  a backup); anything done below SQL (T3); anything after pgaudit was disabled (only *that* it was
  disabled, from the Tiger Console activity log — §6.2); that the log is complete (best-effort
  logging; the heartbeat bounds the gap, it does not eliminate it). It is therefore evidence for
  17a-4(f)(2)(i)(A)'s "audit trail" leg, not for the "non-rewriteable, non-erasable" leg — the
  WORM leg applies to the *log objects*, not to the database.

### 6.2 Log shipping pipeline

**Hop 1: TigerData → CloudWatch (exporter).** "You can attach only one exporter to a service";
"The AWS region must be the same for your Tiger Cloud exporter and AWS CloudWatch Log group";
available on "Scale and Enterprise" plans; auth by IAM role (recommended) or access keys —
https://www.tigerdata.com/docs/use-timescale/latest/metrics-logging/aws-cloudwatch. TigerData's
learn article confirms pgaudit lines are what is exported: "You can export them to CloudWatch"
(the docs page itself speaks of "telemetry data" and does not enumerate log contents —
**[the exact log line format delivered to CloudWatch is unverified; capture a sample early]**).
The exporter must target a log group **in the audit account or in the platform account**; the
choice sets the rest:

- *Exporter → log group in the platform account → cross-account subscription → Firehose in the
  audit account → Object-Locked bucket.* Cross-account: "The log group and the destination must be
  in the same AWS Region" and the recipient "sets up a destination that encapsulates a[n] ...
  stream and lets CloudWatch Logs know that the recipient wants to receive log data" (PutDestination
  + destination access policy allowing the sender's `logs:PutSubscriptionFilter`) —
  https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/CrossAccountSubscriptions-Firehose.html.
  Advantage: platform account keeps a live copy for CloudWatch Logs Insights; the audit account
  holds the WORM copy. Disadvantage: a platform-account admin can delete the subscription filter
  (detectable in the audit account's CloudTrail? no — in the *platform* account's CloudTrail; ship
  that too, or alert on Firehose `IncomingRecords` dropping to zero).
- *Exporter → log group directly in the audit account* (the TigerData IAM role/keys belong to the
  audit account) *→ same-account subscription → Firehose → bucket.* Fewer moving parts and the
  platform team never holds a handle on the stream. Recommended if the TigerData exporter can
  assume a role in a different account than the one running the workloads **[the exporter's IAM
  role trust setup is described in the docs; cross-account is a property of the role ARN you give
  it — should work, unverified]**.

**Hop 2: CloudWatch Logs → Firehose → S3.** Subscription filters deliver "real-time log data" and
"Throttled deliverables are retried for up to 24 hours. After 24 hours, the failed deliverables
are dropped." Data arrives "base64 encoded and compressed with the gzip format" as JSON
`{owner, logGroup, logStream, messageType, logEvents:[{id, timestamp, message}]}` where "The 'id'
property is a unique identifier for every log event" —
https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/SubscriptionFilters.html. Firehose to S3:
"Firehose concatenates multiple incoming records based on the buffering configuration" (enable
newline delimiters); "Amazon Data Firehose uses at-least-once semantics for data delivery. In some
circumstances ... delivery retries ... might introduce duplicates ... This applies to all
destination types that Amazon Data Firehose supports, except for Amazon S3 destinations, Apache
Iceberg Tables, and Snowflake destinations." —
https://docs.aws.amazon.com/firehose/latest/dev/basic-deliver.html. Failure handling: "Amazon Data
Firehose keeps retrying for up to 24 hours until the delivery succeeds. The maximum data storage
time of Amazon Data Firehose is 24 hours. If data delivery fails for more than 24 hours, your data
is lost." — https://docs.aws.amazon.com/firehose/latest/dev/retry.html. Object keys:
"<evaluated prefix><suffix>", default prefix "YYYY/MM/dd/HH" in UTC, suffix
"<stream name>-<stream version>-<year>-<month>-<day>-<hour>-<minute>-<second>-<uuid>" —
https://docs.aws.amazon.com/firehose/latest/dev/s3-object-name.html. Cross-account bucket: "Make
sure you add s3:PutObjectAcl ... Amazon Data Firehose sets the 'x-amz-acl' header on the request
to 'bucket-owner-full-control'" plus a bucket policy in the owning account —
https://docs.aws.amazon.com/firehose/latest/dev/controlling-access.html#cross-account-delivery-s3.
The documented Firehose S3 policy needs `s3:AbortMultipartUpload, GetBucketLocation, GetObject,
ListBucket, ListBucketMultipartUploads, PutObject` — **no `s3:DeleteObject`**, so the shipper role
is write-only by construction.

**Firehose into an Object Lock bucket — [unverified].** Object Lock requires "The Content-MD5 or
x-amz-sdk-checksum-algorithm header ... for any request to upload an object with a retention
period configured using Object Lock" —
https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-managing.html. Firehose is an
AWS-owned client and "all AWS-owned clients calculate a checksum of the object and send it with
the upload request" (https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html),
but no Firehose page found states Object Lock support explicitly, and web search surfaced no AWS
statement either way. Two mitigations: (a) test in the audit account before committing; (b) the
batch alternative is explicitly supported: CloudWatch export tasks can "Export log data to S3
buckets that have S3 Object Lock enabled with a retention period" — but AWS says "We recommend
that you don't regularly export to Amazon S3 as a way to continuously archive your logs. For that
use case, we instead recommend that you use subscriptions", "Log data can take up to 12 hours to
become available for export", and "Time-based sorting on chunks of log data inside an exported
file is not guaranteed" — https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/S3Export.html.
If Firehose→Object Lock fails, a Lambda subscription target doing `PutObject` with SHA-256
checksum into the locked bucket is the fallback (also gives deterministic keys, §6.5).

**Ordering and completeness inside Postgres.** `log_line_prefix` and `log_destination` "can only
be set in the postgresql.conf file or on the server command line" —
https://www.postgresql.org/docs/current/runtime-config-logging.html — i.e. on Tiger Cloud only via
the console if exposed **[unverified which of these are exposed; the default Tiger prefix is
unknown — capture a sample line]**. For replay the prefix (or `jsonlog`) must carry: `%m`
timestamp, `%u` user, `%d` db, `%a` application_name (our services set it? — verify), `%p` pid,
`%c` "a quasi-unique session identifier, consisting of two 4-byte hexadecimal numbers ... the
process start time and the process ID", `%l` "Number of the log line for each session or process,
starting at 1" (gap detection per session), `%x` "Transaction ID (0 if none is assigned)" and `%v`
virtual txid (ties multi-statement transactions together and orders them against `ingest_xid` in
ADR-0006 §5). `jsonlog` emits these as `txid`, `session_id` etc. and is easier to parse than csv
inside a Firehose record. pgaudit's own STATEMENT_ID/SUBSTATEMENT_ID give intra-session ordering
independent of `%l`. Note `%x` is 0 for statements that never acquired an xid (read-only), and is a
32-bit xid, not the epoch-qualified `xid8`; correlate with `ingest_xid` via epoch from `created_at`.

**Gaps if pgAudit is disabled.**
1. *Heartbeat*: a scheduled statement that is guaranteed to produce an audit line — e.g. a
   `pg_cron` job (support-enabled on Tiger Cloud) or a k8s CronJob running `INSERT INTO
   audit_heartbeat ...` as a role whose INSERT is object-logged, or a DDL no-op such as
   `COMMENT ON TABLE audit_heartbeat IS '<ts>'` (DDL class). The verifier in the audit account
   asserts one heartbeat per interval in the S3 objects; absence for > interval → alert. This
   bounds the undetected-gap window to one interval and simultaneously monitors the whole
   pipeline (exporter, subscription, Firehose).
2. *Console change trail*: Tiger Console has an Activity log since 2025-12-12: "a record of
   actions that have happened to your services and Tiger Cloud account, such as service resizes
   and project invitations. The activity log includes the corresponding service, the user who
   performed the action, and a description of the action itself." —
   https://www.tigerdata.com/docs/get-started/news/new. **Whether parameter changes (e.g. setting
   `pgaudit.log = 'none'`) appear in it, and whether it is exportable, is [unverified] — ask
   support**; if not, the heartbeat is the only detector.
3. *AWS side*: CloudTrail in both accounts records `DeleteSubscriptionFilter`,
   `DeleteDeliveryStream`, `PutBucketObjectLockConfiguration` etc.; alert on them.

### 6.3 Dedicated audit account

- **Separate AWS account** in the organisation, administered by a role the platform team does not
  hold. Rationale: Object Lock compliance mode is absolute *within* an account ("The only way to
  delete an object under the compliance mode before its retention date expires is to delete the
  associated AWS account"), so the account itself is the thing to protect.
- **Bucket:** versioning on (Object Lock "works only in buckets that have S3 Versioning enabled");
  Object Lock enabled at creation ("After you enable Object Lock on a bucket, you can't disable
  Object Lock or suspend versioning for that bucket"); **default retention, COMPLIANCE mode** for
  the period in §6.4 — in compliance mode "a protected object version can't be overwritten or
  deleted by any user, including the root user in your AWS account ... its retention mode can't be
  changed, and its retention period can't be shortened." Governance mode is *not* adequate: "users
  can't overwrite or delete an object version or alter its lock settings unless they have special
  permissions" (`s3:BypassGovernanceRetention`) and "the Amazon S3 console includes the
  x-amz-bypass-governance-retention:true header" by default. Legal hold available for
  investigations: "remains in effect until removed". Caveats: "Delete markers are not
  WORM-protected" (a simple DELETE hides the current version but the version persists — the
  verifier must list versions, not current objects); Object Lock "does not protect against losing
  access to the encryption keys" — use SSE-S3 or a KMS key with deletion protection in the audit
  account; "S3 buckets with Object Lock can't be used as destination buckets for server access
  logs". All from https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock.html and
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-managing.html. Object Lock
  "has been assessed by Cohasset Associates for use in environments that are subject to SEC 17a-4,
  CFTC, and FINRA regulations."
- **MFA delete:** "the bucket owner must include two forms of authentication in any request to
  delete a version or change the versioning state of the bucket"; "only the bucket owner (root
  account) can enable MFA delete"; "You cannot use MFA delete with lifecycle configurations" —
  https://docs.aws.amazon.com/AmazonS3/latest/userguide/MultiFactorAuthenticationDelete.html.
  Compliance-mode Object Lock already blocks version deletion, so MFA delete is belt-and-braces
  for versions past retention; the lifecycle conflict matters if a storage-class transition rule
  is wanted (§6.4) — choose one.
- **SCPs** on the audit account's OU: "SCPs affect all users and roles in attached accounts,
  including the root user" and "SCPs don't affect users or roles in the management account" —
  https://docs.aws.amazon.com/organizations/latest/userguide/orgs_manage_policies_scps.html. Deny
  `s3:DeleteBucket`, `s3:PutBucketObjectLockConfiguration` (after setup),
  `s3:PutLifecycleConfiguration`, `organizations:LeaveOrganization`, `account:CloseAccount` (the
  account-deletion escape hatch), and `cloudtrail:StopLogging`/`DeleteTrail`. The management
  account remains the residual trust root; keep it hardware-MFA'd and out of daily use.
- **CloudTrail** in the audit account with log-file validation: "SHA-256 for hashing and SHA-256
  with RSA for digital signing. This makes it computationally infeasible to modify, delete or forge
  CloudTrail log files without detection ... Every hour, CloudTrail also creates and delivers a
  ... digest file ... Each digest file also contains the digital signature of the previous digest
  file" — https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-validation-intro.html.
  Send it to a second Object-Locked bucket in the same account.
- **IAM:** shipper (Firehose) role: the documented S3 policy without `DeleteObject`, scoped to the
  bucket ARN and prefix. Verifier role: `s3:GetObject*`, `s3:ListBucket*`,
  `s3:GetObjectRetention` only. Human access: read-only role via SSO; no standing write/admin.

### 6.4 Retention per standards, and the "most arduous" figure

| Regime | Period | Source |
|---|---|---|
| SEC 17a-4(a) | "for a period of not less than 6 years, the first two years in an easily accessible place" | https://www.ecfr.gov/api/renderer/v1/content/enhanced/current/title-17?section=240.17a-4 |
| SEC 17a-4(b) | "for a period of not less than three years, the first two years in an easily accessible place" | ibid. |
| SEC 17a-4(f)(2)(i) | electronic systems must either "(A) Preserve a record ... in a manner that maintains a complete time-stamped audit trail ..." or "(B) Preserve the records exclusively in a non-rewriteable, non-erasable format" | ibid.; full (A) text in `docs/research/persisted-manifest-standards.md` §1 |
| MiFID II Art. 16(7) | records "kept for a period of five years and, where requested by the competent authority, for a period of up to seven years" (this paragraph is about telephone/electronic-communication records; it is the only explicit MiFID II retention figure) | https://www.legislation.gov.uk/eudr/2014/65/article/16 (UK retained text; EUR-Lex blocked) |
| CDR 2017/565 Art. 72(1) | medium must ensure "(a) the competent authority is able to access them readily and to reconstitute each key stage of the processing of each transaction; (b) it is possible for any corrections or other amendments, and the contents of the records prior to such corrections or amendments, to be easily ascertained; (c) it is not possible for the records otherwise to be manipulated or altered; (d) it allows IT or any other efficient exploitation ...; (e) the firm's arrangements comply with the record keeping requirements irrespective of the technology used." No period stated in Art. 72. | CELLAR 7c37494d-15d3-11e7-808e-01aa75ed71a1 (EN XHTML) |
| SOC 2 (TSC 2017/2022) | no numeric period. CC7.2: "The entity monitors system components and the operation of those components for anomalies that are indicative of malicious acts, natural disasters, and errors affecting the entity's ability to meet its objectives; anomalies are analyzed to determine whether they represent security events." **[wording from secondary reproductions; AICPA PDF not fetched]**. The auditor tests that *our stated* logging/retention control operates. | AICPA TSC landing page in `docs/research/persisted-manifest-standards.md` §3 |
| GDPR Art. 5(1)(e) | personal data "kept in a form which permits identification of data subjects for no longer than is necessary for the purposes for which the personal data are processed; personal data may be stored for longer periods insofar as the personal data will be processed solely for archiving purposes in the public interest, scientific or historical research purposes or statistical purposes in accordance with Article 89(1) ..." | CELLAR 3e485e15-11bd-11e6-ba9a-01aa75ed71a1 (EN XHTML) |
| GDPR Art. 17(1), 17(3)(b) | erasure "without undue delay" where grounds apply; but "Paragraphs 1 and 2 shall not apply to the extent that processing is necessary: ... (b) for compliance with a legal obligation which requires processing by Union or Member State law to which the controller is subject" | ibid. |
| GDPR Art. 32(1) | "appropriate technical and organisational measures ... including inter alia as appropriate: (a) the pseudonymisation and encryption of personal data; (b) the ability to ensure the ongoing confidentiality, integrity, availability and resilience of processing systems and services; (c) the ability to restore the availability and access to personal data in a timely manner ..." | ibid. |
| DORA Art. 9(1)–(3) | "financial entities shall continuously monitor and control the security and functioning of ICT systems"; "maintain high standards of availability, authenticity, integrity and confidentiality of data, whether at rest, in use or in transit"; solutions shall "(b) minimise the risk of corruption or loss of data, unauthorised access and technical flaws"; "(c) prevent ... the impairment of the authenticity and integrity ... and the loss of data" | CELLAR 0caf473a-85bd-11ed-9887-01aa75ed71a1 (EN XML) |
| DORA Art. 9(4)(c),(e) | access "limit[ed] ... to what is required for legitimate and approved functions"; change management "to ensure that all changes to ICT systems are recorded, tested, assessed, approved, implemented and verified in a controlled manner" | ibid. |
| DORA Art. 10(1),(3) | "mechanisms to promptly detect anomalous activities"; "devote sufficient resources and capabilities to monitor user activity, the occurrence of ICT anomalies and ICT-related incidents" | ibid. |
| DORA Art. 12(1)–(3),(7) | backup policies "based on the criticality of information"; "Testing of the backup procedures and restoration and recovery procedures and methods shall be undertaken periodically"; restore on systems "physically and logically segregated from the source ICT system"; on recovery "perform necessary checks, including any multiple checks and reconciliations, in order to ensure that the highest level of data integrity is maintained" | ibid. |
| DORA Art. 17(2)–(3) | "record all ICT-related incidents"; procedures to "identify, track, log, categorise and classify ICT-related incidents" | ibid. |

**No retention period appears in DORA Arts. 9–17** (grep of the CELLAR text for "retention"/
"retain" in that range found none); DORA is about *having* logging/detection/backup controls and
testing them, not about years. GDPR pulls the other way (storage limitation), but 17(3)(b) exempts
data retained under a legal obligation, and the standard mitigation is to keep personal data out
of the log in the first place: `log_parameter` off on any path that could carry addresses,
pseudonymised identifiers where possible (Art. 4(5) defines pseudonymisation), never log READ, and
log rows by key not content. A WORM log that *does* contain personal data cannot honour an erasure
request for its retention period — so that must be prevented at write time, not handled later.

**Most arduous of the union: 6 years (17a-4(a)) with the first 2 "easily accessible", stretched
to 7 by MiFID II Art. 16(7)'s "up to seven years" on NCA request.** Proposal: Object Lock default
retention **7 years** in compliance mode. "Easily accessible" for the first two years argues for
S3 Standard or Standard-IA (millisecond access) then a lifecycle transition to Glacier Instant
Retrieval (still "milliseconds retrieval", 90-day minimum) or Glacier Flexible Retrieval ("minutes
to hours") — https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html.
Lifecycle transitions are allowed on locked objects ("Object Lock is maintained regardless of which
storage class the object resides in and throughout S3 Lifecycle transitions") but lifecycle and
MFA delete are mutually exclusive — pick lifecycle. Volume is small (DDL/ROLE/UPDATE/DELETE lines
plus heartbeats), so storage cost is not the constraint; the transition is about matching the
"easily accessible" phrase, not saving money. Days-scale forensic turnaround (per the ticket) is
compatible with Flexible Retrieval; Deep Archive ("hours" and 180-day minimum) is unnecessary.

### 6.5 Phase-2 non-preclusion checklist

Phase 1 must **not**:
1. Choose a log format that discards ordering or transaction identity. Require `%c`/`session_id`,
   `%l`, `%x`/`txid`, `%v` and pgaudit STATEMENT_ID/SUBSTATEMENT_ID in every line (jsonlog if the
   console exposes `log_destination`; otherwise confirm the Tiger default prefix carries them).
   Phase 2 will join audit lines to `writer_run`/`ingest_xid` by xid and time.
2. Drop or rename the ADR-0006 seams: `writer_run` (principal lands here), `ingest_xid`,
   `manifest_hash`/`manifest_key`, `processing_version_log` insert-only. These are the hash-chain
   inputs.
3. Let any governed table stop being insert-only "because the log covers it". Phase 2 back-fills
   the chain from the rows themselves; that only works if rows were never mutated. Keep ADR-0006
   §1 guard triggers (`ENABLE ALWAYS`) and the REVOKEs.
4. Use non-deterministic or unlisted object keys. Firehose keys embed a uuid; that is fine for
   evidence but phase 2 wants a Merkle root over "all objects for day D". Either accept "list
   versions under prefix `YYYY/MM/dd/` and sort by key" as the canonical leaf order (S3 listing is
   lexicographic and the key embeds the UTC second), or use the Lambda writer variant (§6.2) with
   keys `audit/pgaudit/<UTC hour>/<first CloudWatch event id>.jsonl.gz`.
5. Consume the whole bucket namespace. Reserve prefixes now: `audit/pgaudit/` (phase 1),
   `audit/cloudtrail/`, `anchors/` (phase-2 chain heads, TSA/OTS proofs), `manifests/` if the
   ADR-0006 archive is ever mirrored here.
6. Omit DDL/ROLE from the session classes. Trigger/constraint/ownership removal must be visible
   in phase 1 so that phase 2's "the chain is valid" claim can be paired with "and nobody disabled
   the guards".
7. Log personal data into WORM (§6.4) — phase 2 anchoring will make the *objects* even harder to
   walk back.

**Free head start available now:** ask S3 to store a SHA-256 checksum on every audit object
(`x-amz-checksum-algorithm: SHA256` — "the checksum is stored with the object"; retrievable via
`GetObjectAttributes`/`HeadObject`; S3 Batch Operations "Compute checksum" can "efficiently verify
billions of objects in one job request" and emits an "integrity report" —
https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html). Firehose's
`x-amz-sdk-checksum-algorithm` default is CRC64NVME **[whether Firehose lets you choose SHA-256
is unverified]**; the Lambda variant can. With SHA-256 per object under Object Lock, a daily job
that concatenates the sorted per-object checksums and hashes them yields a day root at near-zero
cost — the same shape phase 2 needs for data roots, so the anchoring plumbing (TSA/OTS, `anchors/`
prefix, verifier) can be built and exercised on phase-1 objects before any data-side hashing
exists. That is a proposal, not part of the decided phase 1.

### 6.6 Open questions

For TigerData support:
1. Which pricing plan is `stl-sentinelprod`? (Nothing in `k8s/` records it; the CloudWatch
   exporter is Scale/Enterprise only; backup retention and cross-region backup also depend on it.)
2. Is `pgaudit.role` settable from Advanced Parameters? Are `pgaudit.log_parameter`,
   `log_relation`, `log_catalog`, `log_statement_once`, `log_rows` exposed?
3. Are `log_line_prefix`, `log_destination` (`jsonlog`), `log_connections`, `log_disconnections`
   exposed, and what is the platform default prefix on exported log lines?
4. Does the Console Activity log record database-parameter changes with actor identity, and can it
   be exported or queried by API?
5. Can the CloudWatch exporter assume a role in an AWS account other than the one hosting our
   workloads (i.e. write straight into the audit account)?
6. Exact `tsdbadmin` role attributes (`rolcreaterole`, `rolreplication`, `rolbypassrls`,
   predefined-role memberships); can `tsdbadmin` run `ALTER ROLE ... SET pgaudit.log`?
7. (Phase 2) Can a Tiger Cloud service be a logical-replication *source*?

For the team:
8. Confirm services set `application_name` distinctly (so `%a` identifies watcher/backfill/API).
9. Decide TRUNCATE handling (not in object mode): revoke from app role + session WRITE for
   tsdbadmin only if (6) allows, else accept it is logged only via the ADR-0006 guard trigger
   raising an error (which pgaudit then does *not* log — "Statements that are executed after a
   transaction enters an aborted state will not be audit logged", though the failing statement
   itself is logged as an ERROR by standard logging).
10. Test Firehose → Object Lock compliance bucket end to end in the audit account before wiring
    prod; keep the Lambda writer as fallback.
11. Choose 7-year compliance retention + Standard→Glacier IR transition at 2 years, or argue for a
    different figure — the standards analysis supports 6 minimum, 7 prudent.

---

## 7. Prior art: how others do tamper evidence on Postgres

Question behind this section: hand-rolled hashing across governed tables looks complicated and
error-prone — surely other Postgres shops have solved tamper evidence, so is there a drop-in?
Sourcing: GitHub READMEs/repos, vendor docs (Microsoft Learn, Oracle Help Center, AWS, Google,
Azure, immudb), PG docs, and the Tiger Cloud extension list already cited in §1.2
(https://www.tigerdata.com/docs/use-timescale/latest/extensions). "Viable on Tiger Cloud" below
means: needs nothing beyond the allow-listed extensions and no superuser. Items marked
**[unverified]** could not be confirmed from a primary source (the AWS QLDB→Aurora blog and the
Oracle "learn" pages were WAF-blocked / returned shells; the aws-samples repo and the Oracle
PL/SQL reference were reachable).

### 7A. Off-the-shelf Postgres extensions and tools

Finding first: **there is no mature, maintained Postgres extension or tool that implements
ledger/hash-chain tamper evidence.** Everything found is either (i) *auditing/versioning* with no
integrity claim, (ii) a pgaudit companion, (iii) an accounting ledger that happens to be called
"ledger", or (iv) a blog-post recipe. Detail:

| Project | What it is | Integrity claim? | Install shape | Licence / status | Tiger Cloud |
|---|---|---|---|---|---|
| **pgaudit** (pgaudit/pgaudit) | Statement/object audit *logging* to the PG log | None — it is evidence-by-log, "best-effort and not transactional" (§6.1) | `shared_preload_libraries` + `CREATE EXTENSION` | PostgreSQL licence; active | **Yes** (allow-listed) |
| **pgaudit_analyze** (pgaudit/pgaudit_analyze) | "reads audit entries from the PostgreSQL logs and loads them into a database schema to aid in analysis and auditing" | None | Daemon "on the database host where log files are stored", CSV logging, installed as `postgres` — https://github.com/pgaudit/pgaudit_analyze | "released under the PostgreSQL licence"; 29 commits, low activity | **No** (needs host log-file access) |
| **pgauditlogtofile** (fmbiete) | "An addon to pgAudit than will redirect audit log lines to an independent file, instead of using PostgreSQL server logger" | None | `shared_preload_libraries` after pgaudit — https://github.com/fmbiete/pgauditlogtofile | open source; version not captured | **No** (not allow-listed; preload) |
| **pgMemento** | "an audit trail for your data inside a PostgreSQL database using triggers and server-side functions written in PL/pgSQL ... tracks DDL changes to enable schema versioning and offers powerful algorithms to restore or repair past revisions"; deltas as JSONB | None stated | Pure PL/pgSQL, "Installation via extension or SQL script" — https://github.com/pgMemento/pgMemento | LGPL-3.0; last release v0.7.4 (31 Oct, year not shown on page; test suite targets PG 15) | Yes as SQL script (temporal audit only) |
| **audit-trigger / "Audit trigger 91plus"** (2ndQuadrant) | "A simple, customisable table audit system for PostgreSQL implemented using triggers"; does `REVOKE ALL ON audit.logged_actions FROM public` | None — the audit table is an ordinary table the owner can edit | SQL script — https://github.com/2ndQuadrant/audit-trigger | "meant to be a demo more than a ready-to-run extension. PRs are not accepted" | Yes (demo quality) |
| **supa_audit** (Supabase) | "a generic solution for tracking changes to tables' data over time" | None | SQL extension — https://github.com/supabase/supa_audit | Apache-2.0; **archived 16 Feb 2025** | Yes as SQL (archived) |
| **temporal_tables** (arkhipov) | System-period versioning: "the old row is archived into another table, which is called the history table" | None claimed | **C extension** (make/MSBuild), PG 9.2–15 — https://github.com/arkhipov/temporal_tables | BSD-2 | **No** (C, not allow-listed) |
| **periods** (xocolatl) | "recreates the behavior defined in SQL:2016 (originally in SQL:2011) around periods and tables with SYSTEM VERSIONING" | None claimed | Extension incl. C parts — https://github.com/xocolatl/periods | PostgreSQL licence | **No** (not allow-listed) |
| **pgledger** (pgr0ss) | "A double entry ledger implementation in PostgreSQL" — accounting ledger, "no claims regarding tamper evidence, cryptographic hashing, or immutability" | None | Plain SQL functions/views — https://github.com/pgr0ss/pgledger | MIT | Yes, but irrelevant (accounting, not integrity) |
| **pg-ledger** (TobiasBengtsson), supabase discussion #36666 | Same category: accounting ledgers | None | SQL | — | irrelevant |
| **PGXN** tags `audit`, `audit log`, `pg_audit` | No hash-chain/ledger extension surfaced — https://pgxn.org/tag/audit/ | — | — | — | — |
| **Blog-post recipes** (AppMaster, Tracehold, anishgandhi.com, dev.to "SHA-256 hash chains zero dependencies", oneuptime) | Trigger writes `prev_hash`/`event_hash` into an append-only audit table, often `pgcrypto`/HMAC, sometimes an advisory lock to serialise the chain | Yes, but *in-database only*; none of the surfaced posts anchors the head externally; none is packaged or maintained as software | SQL + trigger | n/a | Yes (they are the recipe §2.2 describes) |
| **pg_trail, pg_hashchain, pg_ledger** | Searched; no repository of that name with tamper-evidence scope found **[negative result, not exhaustive]** | — | — | — | — |

Reading: Postgres has *temporal/audit* prior art in abundance and *integrity* prior art only as
blog recipes. That is the honest answer to "surely someone solved this": on Postgres, nobody
packaged it, and the one production-grade thing (pgaudit) is exactly the log-based approach phase 1
adopts.

### 7B. Purpose-built ledger databases and how Postgres shops use them

- **immudb** (codenotary; BSL 1.1). "a database with built-in cryptographic proof and verification
  ... the integrity of the history will be protected by the clients, without the need to trust the
  database"; "Cryptographic commit log with parallel Merkle Tree"; "You can add new versions of
  existing records, but never change or delete records." —
  https://github.com/codenotary/immudb. Speaks the Postgres wire protocol: "immudb needs to be
  started with the pgsql-server option enabled"; "not compatible with the SQL dialect" (search
  summary of https://docs.immudb.io/1.0.0/develop/pg.html **[quote not verified on the current
  page]**). Vendor positioning for our exact case: "PGaudit and immudb: The Dynamic Duo for
  Tamper-Proof PostgreSQL Audit Trails" (21 Mar 2023) — pgaudit lines shipped by
  "immudb-log-audit" into immudb, which "uses cryptographic hashing and Merkle trees to ensure that
  data remains unchanged" —
  https://immudb.io/blog/pgaudit-and-immudb-the-dynamic-duo-for-tamper-proof-postgresql-audit-trails.
  Note immudb now has a retention feature that "can remove old data from the immudb database while
  leaving the proofs and schema configuration data intact" (`--retention-period`,
  `--truncation-frequency`) — https://docs.immudb.io/master/production/retention.html — so
  "immutable" is qualified. Pattern: PG stays the system of record; immudb is a *sidecar log store*
  with client-verifiable proofs. It is a new stateful service to run (or a vendor cloud) and a BSL
  licence; for us it competes with "S3 Object Lock + checksums", which we already operate.
- **Amazon QLDB** — retired; "Amazon QLDB は、2025 年 7 月 31 日にサポートが終了する" (AWS Japan blog,
  §1.9). AWS's official successor path is Aurora PostgreSQL: the aws-samples migration
  "demonstrates both approaches. The ledger data from the vehicle registration sample app is
  normalized into a relational model, but the revision metadata from the ledger is stored in
  Aurora PostgreSQL as a JSONB type", with `*_audit_log` tables carrying `version` and `operation`
  — and "no mention of verifying ledger hash chains, cryptographic verification, or preservation of
  QLDB's immutability guarantees" — https://github.com/aws-samples/example-qldb-ledger-migration.
  The companion AWS blog "Replace Amazon QLDB with Amazon Aurora PostgreSQL for audit use cases"
  could not be fetched (WAF) **[unverified]**; secondary summaries state the migration "loses
  cryptographic verifiability" and that history "must be generated as audit data and stored outside
  of the database". **Lesson: the cloud vendor that owned a managed ledger DB told its customers to
  move to plain Postgres + audit tables + external storage** — i.e. the same shape as this note.
- **Microsoft SQL Server / Azure SQL ledger** (SQL Server 2022+; the most complete in-RDBMS design
  and the best spec to borrow). Mechanism: "for every row updated ... Serialize the row content and
  include it when computing the hash for all rows updated by this transaction"; "a Merkle Tree that
  is stored at the transaction level ... If the transaction updates multiple tables, a separate
  Merkle Tree is maintained for each table"; a block is closed "Approximately every 30 seconds ...
  When the user manually generates a database digest ... When it contains 100K transactions"; block
  = "Merkle tree root over these transactions and the hash of the previous block"; "this operation
  is single-threaded ... happens asynchronously" —
  https://learn.microsoft.com/en-us/sql/relational-databases/security/ledger/ledger-database-ledger.
  Digest = JSON `{database_name, block_id, hash, last_transaction_commit_time, digest_time}`;
  "database digests that are extracted from the database need to be stored in trusted storage that
  the high-privileged users or attackers of the database can't tamper with"; automatic upload every
  30 s to Azure Blob immutable storage ("Make sure the immutability policy allows protected append
  writes to append blobs and that the policy is locked") or Azure Confidential Ledger, which "has a
  stronger integrity guarantee for customers who might be concerned about privileged administrators
  access to the digest"; digest path keyed by `ServerName/DatabaseName/CreationTime` because "a
  database with the same name can be dropped and recreated or restored" — this is how they detect
  **restore-rollback (our T4)** —
  https://learn.microsoft.com/en-us/sql/relational-databases/security/ledger/ledger-digest-management.
  Not available for Postgres; used here as the reference design (§7E).
- **Oracle Blockchain Tables** (19c RU / 21c+; in-RDBMS precedent). "an append-only table designed
  for centralized blockchain applications ... peers are database users who trust the database to
  maintain a tamper-resistant ledger. Rows are chained together via cryptographic hashes";
  `GET_BYTES_FOR_ROW_HASH` "returns in row_data the bytes for the particular row identified (a
  series of meta-data-value, column-data-value pairs in column position order) followed by the hash
  for the previous row in the chain"; `VERIFY_ROWS` "Verifies all rows on all applicable system
  chains for integrity of HASH column value and optionally the SIGNATURE column value";
  `DELETE_EXPIRED_ROWS` "deletes rows outside the retention window" —
  https://docs.oracle.com/en/database/oracle/oracle-database/26/arpls/dbms_blockchain_table.html.
  Newer releases add user chains, row versions, countersignatures/delegate signers and parallel
  verification (https://docs.oracle.com/en/database/oracle/oracle-database/26/nfcoa/blockchain.html).
  Retention clauses (`NO DROP UNTIL n DAYS IDLE`, `NO DELETE UNTIL n DAYS AFTER INSERT`) and the
  ALTER TABLE restrictions are in the Admin Guide, which returned an empty shell **[not quoted]**.
  Design point worth copying: the row hash covers *metadata-value pairs in column position order*,
  i.e. the schema is part of the hash input.
- **Azure Confidential Ledger** (anchoring service). "a customer-managed, append-only ledger ...
  immutability, tamper-evident records, and append-only operations"; "can protect existing
  databases and applications by acting as a point-in-time source of truth for digests and hashes";
  "Each transaction on the ledger has an associated receipt that records the Merkle tree data
  structure"; runs on "hardware-backed secure enclaves ... no one — not even Microsoft — is 'above'
  the ledger"; "exposes a REST interface"; limits "2 standard SKU ledgers" per subscription, "Create
  entry 1800 requests per second" — https://learn.microsoft.com/en-us/azure/confidential-ledger/overview.
  Cross-cloud use from AWS is possible (REST + Entra ID) but adds an Azure tenant.
- **Google Cloud Spanner**: no ledger/tamper-evidence feature; not prior art.
- **Fluree** (BSL 1.1 → Apache 2.0 change date): "Temporal, verifiable" graph DB with
  "JWS-signed transactions" and "Verifiable Credentials" — https://github.com/fluree/db. Different
  data model; not a Postgres companion.
- **ProvenDB** (Southbank Software): MongoDB layer anchoring database-state versions to Bitcoin
  (Chainpoint); acquired by OneSpan for $2M, completed 22 Feb 2023 (search summary; SEC 10-Q
  reference) — the product no longer exists independently. Its PostgreSQL SDK article
  ("anchoring postgresql data on the blockchain with the provendb sdk") is the closest historical
  precedent for "Postgres rows → Merkle root → public chain", and it did not survive as a product.
- **BigchainDB** (Apache-2.0): "the blockchain database" — https://github.com/bigchaindb/bigchaindb;
  the repo shows 168 open issues and no visible recent release; effectively dormant
  **[last-commit date not captured]**. Not a Postgres companion.

### 7C. What regulated enterprises actually deploy for Postgres

The enterprise answer is **out-of-band Database Activity Monitoring (DAM)**, not in-database
hashing:

- **IBM Guardium**: PostgreSQL monitored via S-TAP / External S-TAP; AWS documents Guardium
  monitoring Aurora PostgreSQL through External S-TAP on EKS
  (https://aws.amazon.com/blogs/ibm-redhat/monitoring-amazon-aurora-databases-with-ibm-guardium/;
  IBM's platform-support page returned 403 **[PG support matrix unverified from IBM]**).
- **Imperva Data Security Fabric (ex-SecureSphere)**: PostgreSQL among supported relational
  targets; agent "resides on your databases, so it can monitor and continuously analyze all data
  access activity on both database user and privileged-user accounts" (EDB partner docs summary,
  https://www.enterprisedb.com/docs/partner_docs/ImpervaDataSecurityFabric/03-SolutionSummary/).
- **Oracle Audit Vault and Database Firewall**: "supports Oracle Database, Microsoft SQL Server,
  MySQL, IBM Db2, PostgreSQL, SAP Sybase, MongoDB" as targets
  (https://docs.oracle.com/en/database/oracle/audit-vault-database-firewall/20/sigrn/index.html).

All three need a host agent or network tap — **impossible on Tiger Cloud** ("No customer access to
the virtual machine level is provided", §0). The managed-cloud substitutes are:

- **AWS Aurora/RDS Database Activity Streams** — the cleanest precedent for what TigerData lacks:
  "To protect against internal threats, you can control administrator access to data streams by
  configuring the Database Activity Streams feature. DBAs don't have access to the collection,
  transmission, storage, and processing of the streams."; "pushes activities to an Amazon Kinesis
  data stream in near real time"; "always encrypted" with KMS; a **synchronous mode** where "the
  session blocks other activities until the event is made durable" (Aurora PostgreSQL only) —
  which fixes exactly the pgaudit "best-effort" caveat; start/stop is an RDS API call
  (`rds:StartActivityStream`), i.e. IAM-governed, not a DB parameter; "compliance applications
  include IBM's Security Guardium and Imperva's SecureSphere" as stream consumers —
  https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/DBActivityStreams.html.
- **Google Cloud SQL**: pgAudit enabled via "cloudsql.enable_pgaudit" flag + `CREATE EXTENSION`;
  logs "sent to Cloud Logging as Data Access audit logs" —
  https://docs.cloud.google.com/sql/docs/postgres/pg-audit.
- **Azure Database for PostgreSQL flexible server**: "audit database activities ... by using the
  pgaudit extension"; "your logs are automatically sent (in JSON format) to Azure Storage, Event
  Hubs, and Azure Monitor logs, depending on your choice" (Azure Storage supports immutable blob
  policies); parameters set via portal/CLI incl. object logging —
  https://learn.microsoft.com/en-us/azure/postgresql/flexible-server/concepts-audit.

**Conclusion for 7C:** the industry-standard pattern for *managed* Postgres is precisely
"pgAudit-class event stream → provider- or customer-controlled immutable sink", with the
differentiator being *who can turn it off* (AWS: IAM, not the DBA; GCP/Azure: a server flag/
parameter, like Tiger). Phase 1 is that pattern; the heartbeat + console Activity log (§6.2) is the
compensating control for Tiger's DBA-toggleable switch.

### 7D. Anchoring / transparency services for a per-run digest (one paragraph each)

- **RFC 3161 TSA** — one HTTPS POST of a hash, one token back; proves "a datum existed before a
  particular time" (§2.3). **One-call integration.** Trust = the TSA.
- **OpenTimestamps** — one call to a calendar server ("free to use ... don't require any
  registration or api key"), Bitcoin-anchored proof upgradable later. **One-call integration**
  (plus a later upgrade call). Trust = Bitcoin + your ability to verify with a node.
- **S3 Object Lock + SHA-256 object checksum** — one `PutObject` with `x-amz-checksum-sha256` into a
  compliance-mode bucket; retention enforced against root; S3 Batch "Compute checksum" verifies at
  rest (§6.5). **One-call integration**; we already run the plumbing. Trust = AWS + the audit
  account's isolation.
- **Sigstore Rekor** — public instance: `rekor-cli upload --type hashedrekord` of a signed digest,
  inclusion proof back; 100 KB limit; intended for software supply chain, acceptable-use for
  arbitrary business digests **[unverified]**. Private instance = running Rekor + Trillian
  ("maintenance mode") or moving to Tessera. **One-call for public; a service to run for private.**
- **Trillian / Tessera** — Tessera is a "Go library for building tile-based transparency logs" with
  GCP, AWS (MySQL + S3) and POSIX backends, "Production ready since the Beta release (v0.2.0)",
  Apache 2.0 — https://github.com/transparency-dev/tessera. A log *we* operate; strong if we want
  third parties to audit consistency proofs, heavy for anchoring a few roots a day. **Not one-call.**
- **Azure Confidential Ledger** — REST `POST` of the digest, receipt with Merkle proof back; TEE-
  backed; Microsoft's own recommended digest store for SQL ledger. **One-call integration**, but
  requires an Azure tenant and Entra/certificate auth from AWS.
- **Ethereum (or L2) contract / calldata** — one `eth_sendRawTransaction` carrying the root; proof =
  the transaction receipt and block; verification via any node. **One-call integration** given our
  existing RPC infra; gas cost per anchor; no primary source consulted for this note.

### 7E. Design lessons from the mature systems

What the three real implementations (SQL Server ledger, Oracle blockchain tables, immudb) plus
QLDB agree on:

1. **Canonicalisation is schema-aware and byte-exact.** SQL Server: "Other than the serialized
   value of each column, we include metadata regarding the number of columns in the row, the
   ordinal of individual columns, the data types, lengths and other information that affects how
   the values are interpreted." Oracle: "a series of meta-data-value, column-data-value pairs in
   column position order". Neither hashes a JSON rendering; both hash a typed binary serialisation
   with column metadata. Lesson for us: RFC 8785 JCS is a fine *interchange* canonical form for
   manifests, but the row hash should be over a typed, column-ordered serialisation that includes
   the column list and types — otherwise a benign type change silently changes hashes.
2. **Hash per row, then Merkle per transaction (per table), then chain per block.** SQL Server:
   row hash → per-table per-transaction Merkle root → per-block Merkle root over transactions +
   previous block hash. Oracle: row hash includes previous row hash (a chain, not a tree; parallel
   "chains per instance" to avoid serialisation). immudb: "Cryptographic commit log with parallel
   Merkle Tree". Lesson: chaining at *transaction/batch* granularity (our `writer_run`) is the
   normal unit; per-row chaining forces serialisation (Oracle needed multiple chains; blog recipes
   need advisory locks).
3. **Blocks close on time, size, or explicit digest.** SQL Server: 30 s / 100K transactions /
   manual; asynchronous and "single-threaded". Lesson: the checkpoint writer is a separate,
   serialised, async job — not in the write path.
4. **Schema change is an explicit ledger event, and destructive DDL is converted to rename.** SQL
   Server: "Adding nullable columns is supported. Adding non-nullable columns isn't supported.
   Ledger is designed to ignore NULL values when computing the hash of a row version"; drops are
   renamed to `MSSQL_DroppedLedgerTable_<name>_<GUID>` and "remain[] available for the ledger
   verification process"; "any operations that might affect the format of existing data, such as
   changing the data type aren't supported"; `TRUNCATE TABLE` unsupported; column history in
   `sys.ledger_column_history`. Oracle restricts ALTER on blockchain tables similarly (Admin Guide,
   not quoted). Lesson: record schema version in every checkpoint, treat DDL as a block boundary,
   and make "drop" a rename in governed schemas.
5. **The digest store must be outside the attacker's reach, and its identity must survive
   restores.** SQL Server keys digests by database *creation time* so that a restored "incarnation"
   is detectable: "Every time the database is restored, it's tagged with a new create time";
   digests go to immutable blob or Confidential Ledger. Lesson: our anchors must carry a
   service/incarnation identifier (Tiger service ID + fork lineage) so a PITR fork cannot pass as
   the original — this is the T4 defence.
6. **All of them exclude the same attacker.** SQL Server: "an attacker or system administrator who
   has control of the machine can bypass all system checks and directly tamper with the data ...
   Ledger can't prevent such attacks but guarantees that any tampering will be detected"; and the
   digest store must be where "high-privileged users or attackers of the database can't tamper
   with" — i.e. an attacker who controls **both** DB and digest store defeats all of them. immudb
   moves trust to the *client* holding the last known state; QLDB moved it to AWS. Lesson: nothing
   here is stronger than "two trust domains"; the phase-1 audit account and phase-2 anchors are the
   second domain.
7. **Verification is a first-class, scheduled operation** (`VERIFY_ROWS`, `sp_verify_database_
   ledger`, immudb verified reads). Lesson: the assurance job (§2.5) is not optional garnish; it is
   what turns stored hashes into evidence.

**How much of a hand-rolled implementation is novel?** Very little. The recipe — typed canonical
row bytes → per-batch Merkle/hash → chained checkpoints with schema version → periodic digest to an
independent immutable store keyed by database incarnation → scheduled verifier — is fully specified
by the SQL Server ledger documentation and corroborated by Oracle and immudb. The genuinely bespoke
parts for us are (a) choosing the serialisation for `NUMERIC`/wei and `JSONB` columns (the systems
above rely on engine-internal type serialisers we do not have; Postgres `row_to_json` + JCS with
big integers as strings, or `pg_catalog` binary send functions, are the candidates), (b) mapping
"block" onto `writer_run`/`ingest_xid`, and (c) the incarnation identifier on Tiger Cloud. The
error-prone part is not the hashing; it is canonicalisation drift across code versions — which is
why the hash logic must live in the versioned writer (`git_hash`) and why phase 2 should be spec'd
against the SQL Server ledger design rather than invented.

### Assessment

**Is there a drop-in on Tiger Cloud? No.** Nothing in the allow-listed extension set implements
ledger tables or hash chains; the only allow-listed integrity-adjacent pieces are `pgaudit`
(evidence-by-log), `pgcrypto` (`hmac`) and core `sha256()`. Every purpose-built option is either a
different engine (SQL Server, Oracle), retired (QLDB, ProvenDB), a sidecar service with a BSL
licence (immudb), or C extensions we cannot install (temporal_tables, periods, pgauditlogtofile).
The Postgres ecosystem's real prior art for tamper evidence is pgaudit plus an external immutable
sink — which is what the industry deploys on Cloud SQL/Azure/Aurora and what phase 1 is.

**Minimum-bespoke path (phase 2):** adopt the SQL Server ledger digest design as the written spec
(row serialisation with column metadata; per-`writer_run` Merkle root; chained checkpoints with
`schema_version`; digest JSON `{service_incarnation, block_id, hash, last_commit_xid, digest_time}`;
digest to the Object-Locked audit bucket every N minutes; verifier as a scheduled job). Expect on
the order of one insert-only table, one serialiser in the writer, one SQL/Go verifier and one S3
`PutObject` — small, but it must be *specified*, not improvised. Anchor the digest with the two
one-call services already identified (RFC 3161 or OpenTimestamps; optionally an Ethereum tx given
the product). Do not build or run Trillian/Rekor/immudb for this.

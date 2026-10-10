# sdk-go-sqlite v0.1 design

Build a native Go database/sql driver on github.com/tianacloud/sdk-go for Tiana
SQLite. Public module github.com/tianacloud/sdk-go-sqlite, package tianasqlite,
driver name tiana-sqlite. This is an unpublished, reviewable first-version API.

Config{Gateway tiana.Config, RequestTimeout time.Duration}; NewConnector(Config)
returns (*Connector,error); sql.OpenDB(connector) is the primary entry point.
sql.Open("tiana-sqlite","tiana://<full-endpoint-host>") supports anonymous endpoints;
credentials use typed Config, never DSN, implicit environment or local file I/O.
Applications may explicitly obtain an InstanceToken via sdk-go/auth.

One database/sql connection owns one sdk-go Client and hrana-http Tunnel, and one
rotating Hrana v3 baton. Use /v3/pipeline so execute + get_autocommit fit one RTT.
No proxy, redirect, HTTP transport automatic retries or SQL replay. Contexts
govern establishment and individual requests without canceling idle pooled tunnels.
Default request timeout 30s, request/response cap 8MiB, headers 32KiB. Fully buffer
results within cap; cursors/WebSockets deferred. Pool size is database/sql-owned.

Implement DriverContext, Connector, ExecerContext, QueryerContext, PrepareContext,
Stmt context methods, Pinger, ConnBeginTx, Validator, SessionResetter. Prepared
statements hold SQL locally, no server statement-ID allocation; NumInput=-1,
server validates exact binding. Named and positional arguments supported separately,
not mixed. Values: nil, int64, float64 finite, bool→integer, string UTF-8,
[]byte→blob (nil→NULL), time.Time→RFC3339Nano text. Preserve all int64 bits.
Result exposes row count and optional last_insert_rowid; absent ID returns error.
Rows exposes names and declared types; no fabricated nullability/scan affinity.

BeginTx supports default and serializable isolation; readonly rejects explicitly.
BEGIN/COMMIT/ROLLBACK stay on one stream, each checked against get_autocommit.
Transactions must use sql.Tx: raw transaction control that changes the expected
autocommit state invalidates the session and returns an explicit state error.
Do not return ErrBadConn after any send: uncertain outcomes must never cause
database/sql to replay writes. Mark unusable; Validator discards on return.
A known bad connection before send may return ErrBadConn; Connector never does.
SQL errors expose bounded allowlisted codes, not arbitrary server messages/SQL,
parameters or tokens. Cancellation and malformed-reply/transport errors poison the session.
Close sends bounded Hrana close when usable; a confirmed stream close rolls back uncommitted work. Transport loss alone
does not prove rollback: an unknown rotated baton can retain the transaction
and locks until the App TTL cleanup.
No database/filesystem storage format or server durability guarantee changes.

Reference app_sqlite bccd78e3040c9463e802f3538f4f9ffe0fcd9522, current CLI
sqlitecli wire behavior, sdk-go candidate branch sdk-go-public-migration.
Dependency modules remain read-only. GitHub credentials unavailable (HTTPS+SSH);
initialize local unborn branch from user's stated empty-repo premise, preserve
canonical remote, do not claim remote baseline/permissions were verified.
The remote-dependency decision below supersedes the original bootstrap guidance:
use GOWORK=off and the pinned published Git commit; never use local replace or
a sibling workspace dependency. Never commit/push without request.

Validation: typed values/binds/malformed results; fake HTTP peers over net.Pipe;
real native TLS/H2 CONNECT and actual app_sqlite library with local SQLite file;
database/sql transactions, rollback, pool reuse, context lifetime, lost commit
response no replay, expired baton, limits/redirects/redaction. Race/vet, public
source scan and English/Chinese README. Actual platform durability not tested.

Reference: https://pkg.go.dev/database/sql/driver (ErrBadConn replay contract).

## Review and validation decisions

Reject leading empty statements, comments, whitespace and BOM before raw
transaction control. The real App executes "; COMMIT", so post-execution
autocommit checks alone cannot preserve sql.Tx atomicity. Reject at both direct
and prepare boundaries; tests must prove rollback still leaves zero rows.
Snapshot the caller's Token value and RootCAs when creating a Connector.

OutcomeUnknown=false is not a no-side-effects promise: INSERT OR FAIL may
retain partial effects. The driver never treats SQL errors as retry permission.
A lost in-transaction response can retain a server write lock after local
Rollback fails; the actual-App integration test verifies the second writer
sees SQLITE_BUSY. No prompt rollback guarantee is introduced.

First-version verification used Go 1.25.13 and Go 1.26.6 with race enabled,
native sdk-go TLS/H2 plus the real App library, vet and govulncheck. The latter
does not establish absence of unknown vulnerabilities. Gateway/Agent deployment,
S3 durability and public dependency/tag availability remain external checks.

## Dedicated session boundary for CLI shells

CLI raw transaction control cannot use a pooled sql.DB without changing shell
semantics. Add lazy NewSession with Execute/ExecuteAndClose/Autocommit/Close,
sharing conn, Hrana v3 validation and exported Result/Column/Value codecs.
Only the exclusive Session path omits the expected-autocommit guard. The driver
still rejects raw transaction control before send; sql.Tx safety is unchanged.
Session calls serialize, do not replay or reconnect, and never send cleanup
using a baton from an invalid response. Transport-only cleanup cannot confirm
rollback. A caller may roll back only a known active transaction before Close;
unknown outcomes remain unknown. One-shot execution includes get_autocommit and
close in the same request; close acknowledgment is validated even on SQL errors.
Public Result preserves int64/blob/null encodings for shell output. Results are
buffered/validated under the existing 8MiB wire cap. Mutex queue waiting is not
covered by RequestTimeout; execution/establishment is. No persistence format or
server transaction semantics change. This additive unpublished API requires the
matching sdk-go-sqlite release before downstream standalone builds.
Verify dedicated savepoints/BEGIN variants/close rollback against the real App,
invalid-baton no-reuse and lost-one-shot cleanup, plus driver transaction guards.

Final review: public Session Result accepts the full uint64 affected count;
only database/sql conversion enforces int64. CloseContext allows rollback and
close to share a total cleanup budget. ExecuteAndClose releases on pre-send
cancellation too, and fallback close errors override local validation errors
with CLOSE_UNCONFIRMED/OutcomeUnknown while preserving wrapped causes.

## Resolve sdk-go from master (2026-09-21)

User requested replacing the unavailable v0.1.0-dev.1 requirement with sdk-go
master. Create the missing remote master ref at the already published/tested
94d680cde78f007c167d74f70b5d2a961d3046d2 commit, without rewriting history or
changing the remote default branch. Resolve @master with GOWORK=off and record
Go's canonical v0.0.0-20260921132030-94d680cde78f pseudo-version and checksums.
Go modules pin branch queries to commits; subsequent master movement requires
an explicit go get ...@master. No local replace, fork or floating build-time
fetch. The module now declares an available dependency, so downstream consumers
no longer need to exclude an unpublished placeholder when using this change.
Authenticated GitHub access is currently required; use scoped private-module
settings, preserving public module checksum checks and no global config changes.
SDK source/API/transaction/persistence behavior is unchanged. Validate fresh
remote download, mod verify/tidy, standalone race/vet, example compilation and
public-source scan. Actual App semantics were tested against these same sources
in the preceding shell task; no new production or durability claim is implied.


## 2026-09-28: verified remote Tiana dependencies

User requires Tiana SDK dependencies to resolve from current GitHub main commits,
never sibling paths or unpublished local builds. Pin verified immutable commits:
sdk-go e69b9c1d3842985e0ba9fd98b2403c520e5f98f1;
sdk-go-sqlite ce8df62d600c6509ef7a3308b17603e68f8c73d7;
sdk-rust f45f14e36313e1ec5787e212de9650c16c9f3067;
sdk-python 940c26c00b9f3abec63537388c4a23b0a129a98a;
sdk-node ba52cb6ec4e751f5158b17d64b45323a82f74f81.
Go records canonical resolved versions and go.sum with GOWORK=off; Rust uses Git
rev and Cargo.lock; Python uses a PEP 508 immutable Git requirement preserved in
wheel metadata; Node uses a Git dependency and package-lock. No local replace,
path patch, workspace link, editable core package or local core tarball fallback.
This supersedes earlier unpublished-core/local-development dependency guidance.

Tradeoff: reproducible builds need GitHub access (including SSH for Git-based
requirements); future main changes require an explicit pin refresh. Verify fresh
resolution in isolated source copies without sibling repositories, inspecting
module/Cargo/installed Python/npm source metadata, plus relevant tests and package
consumers. Preserve wire, transaction, no-replay, TLS and storage invariants; this
update adds no runtime network round trips or persistence format migration.
No third-party version refresh is intended. Revert manifests and locks together
for rollback. Commit/push/package publication is outside this dependency update.

## 2026-10-08: v1.0.0 dependency and release verification

User authorizes dependency repair, installed-package and demo validation, then
squashing this repository to one commit and publishing main plus v1.0.0 only.
Use the core v1.0.0 release (Go module version; immutable Git HTTPS revision for
Node/Rust/Python), superseding the old historical pins above. Preserve third-party
versions, TLS verification, no SQL replay and transaction/storage semantics.
Use a disposable local App SQLite for demo verification; never production data.

## Known SQL grammar rejection (2026-10-10)

SQL_PARSE_ERROR is an App pre-execution grammar rejection. Expose its exact code
with OutcomeUnknown=false after a complete coherent execute/get_autocommit
response; preserve the confirmed transaction and allow subsequent statements on
the same session. SQL/transport errors never authorize replay. Unknown error
codes and malformed/ambiguous replies remain OutcomeUnknown=true and poison the
session. The pooled driver's expected-autocommit guards remain unchanged.
Validate out-of-transaction and active-transaction recovery through native
TLS/H2 peers, no extra connections/replay, rollback and unknown-code poisoning.
No wire/storage format or API change; downstream grammar delegation requires this fix
in a genuinely published immutable SDK version. Commit/push/publication still
require separate current-task authorization.

## SQLite SDK v1.0.1 release (2026-10-10)

Current user authorizes publishing this SDK parse-rejection fix. Commit the
reviewed changes to main without rewriting history and publish a new immutable
v1.0.1 tag. Keep sdk-go pinned to v1.0.0; do not alter the existing v1.0.0 tag.
Verify standalone GOWORK=off race/vet/module/source checks before publication,
then verify the exact remote tag and fresh remote module resolution/checksum.
Known SQL_PARSE_ERROR preserves a confirmed session/transaction; unknown replies
remain conservative. CLI/npm publication is not part of this SDK release.

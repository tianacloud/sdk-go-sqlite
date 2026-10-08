# Tiana SQLite Go SDK

**English** | [简体中文](README.zh-CN.md)

A native Go `database/sql` driver for Tiana SQLite. Module
`github.com/tianacloud/sdk-go-sqlite`, package `tianasqlite`, driver name
`tiana-sqlite`. Requires Go 1.25+; use a supported, security-patched toolchain.
The client has no CGO dependency.

The driver uses [sdk-go](https://github.com/tianacloud/sdk-go) to establish a
verified TLS 1.3/HTTP2 CONNECT tunnel with profile `hrana-http`, then speaks
Hrana 3 JSON at `/v3/pipeline`. SQL and values are not interpolated into URLs.

## Install and release status

The release tag is `v1.0.0`. With repository access configured, install it:

```sh
go get github.com/tianacloud/sdk-go-sqlite@v1.0.0
```

The sdk-go dependency is pinned to `v1.0.0`
(commit `ad9dfa0b943a7ba2f15984fa88d07c797b3239d3`). Standalone builds use the remote module and
recorded checksums, without a local replace or workspace.

```sh
GOWORK=off go mod download
```

The repositories currently require authenticated GitHub access. Configure Git
credentials (or authenticated SSH routing) and scope GOPRIVATE to the two SDK
module paths. Public dependency checksum verification remains enabled; go.sum
records the downloaded SDK checksum. Anonymous downloads and hosted CI need
repository access before builds can succeed.

Always use `GOWORK=off` to resolve the pinned remote SDK. No sibling checkout
is required or selected for builds.

## Connect with database/sql

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "os"
    "time"

    tiana "github.com/tianacloud/sdk-go"
    tianasqlite "github.com/tianacloud/sdk-go-sqlite"
)

func main() {
    token, err := tiana.NewToken(os.Getenv("TIANA_TOKEN"))
    if err != nil { log.Fatal(err) }
    connector, err := tianasqlite.NewConnector(tianasqlite.Config{
        Gateway: tiana.Config{
            Endpoint: os.Getenv("TIANA_ENDPOINT"),
            Token: token,
        },
        RequestTimeout: 30 * time.Second,
    })
    if err != nil { log.Fatal(err) }
    db := sql.OpenDB(connector)
    defer db.Close()
    db.SetMaxOpenConns(4)
    db.SetMaxIdleConns(4)
    db.SetConnMaxIdleTime(30 * time.Second)

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    var answer int64
    if err := db.QueryRowContext(ctx, "SELECT ?", int64(42)).Scan(&answer); err != nil {
        log.Fatal(err)
    }
    log.Print(answer)
}
```

Endpoint must be the full deployment hostname, such as
`ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test` (illustrative only).
It must satisfy sdk-go's Endpoint ID and DNS rules. Gateway.RootCAs and
Gateway.DialAddress support explicit test/custom routing with certificate
verification intact. Each pooled connection owns one SDK client and tunnel.

No implicit login, environment lookup or local credential-file read occurs in
the driver. The example explicitly reads environment variables. Applications
may obtain an InstanceToken via sdk-go/auth after authorizing the instance.
Account access/refresh tokens cannot authenticate the Gateway tunnel.

For an endpoint that permits anonymous access, omit Token or use:

```go
db, err := sql.Open("tiana-sqlite", "tiana://ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test")
```

DSNs accept only the endpoint: credentials, query parameters, paths and ports
are rejected. Use NewConnector for credential/TLS configuration.
sql.Open/OpenDB are lazy; PingContext verifies a connection to the SQLite App.

## Queries, values and statements

ExecContext, QueryContext, QueryRowContext, PrepareContext and their standard
database/sql counterparts are supported. Prepared statements retain SQL locally
and submit it with each execution; there is no server prepared-statement cache.

Use positional `?` parameters or named parameters such as `:name` with
`sql.Named("name", value)`. Mixing named and positional arguments is rejected.
The App validates one SQL statement and the complete parameter binding.
SQL scripts/multiple statements are not a first-version API.

| Go input | Hrana / SQLite value |
| --- | --- |
| nil, typed nil []byte | NULL |
| int64 and convertible integer types | Signed 64-bit integer, without float conversion |
| finite float64 | Float |
| bool | Integer 0 or 1 |
| valid UTF-8 string | Text |
| non-nil []byte, including empty | Blob |
| time.Time | RFC3339Nano text |

NaN, infinity, integer overflow and invalid UTF-8 are rejected before SQL sends.
Rows return nil/int64/float64/string/[]byte; time strings are not automatically
parsed into time.Time. Column names and declared types are exposed; dynamic
SQLite types are not assigned fabricated nullability or scan-type metadata.
RowsAffected is supported. LastInsertId returns an error when the App omits it.

## Transactions and the connection pool

Use BeginTx, Commit and Rollback. A transaction stays on the same Hrana session.
The driver checks get_autocommit after every SQL execution, in the same pipeline
request. Default and serializable isolation are supported. Read-only
transactions and other isolation levels return explicit unsupported errors.

Through database/sql, use sql.Tx for transaction control. Raw BEGIN, COMMIT, ROLLBACK, SAVEPOINT,
RELEASE and END are rejected. Unexpected server transaction-state changes make
the connection unusable so it cannot leak transaction state into the pool.
A failed Commit is not automatically retried. Application changes to
connection-local PRAGMAs or temporary objects should use a dedicated sql.Conn;
pooling does not promise a fresh SQLite session for every query.

Set pool limits to fit the deployment. Idle Hrana batons expire according to the
App's stream TTL: keep ConnMaxIdleTime below that TTL. The 30-second example
assumes a larger server TTL, such as 60 seconds. There is no background keepalive.

## Dedicated sessions for shells

`NewSession(Config)` is a separate, exclusive session API for interactive SQL
clients that need raw BEGIN IMMEDIATE/EXCLUSIVE, SAVEPOINT, ROLLBACK TO,
RELEASE and COMMIT. It validates configuration without I/O, connects on first
Execute, serializes calls and never pools, reconnects or retries. The
`database/sql` transaction restrictions above remain unchanged.

```go
session, err := tianasqlite.NewSession(config) // same Config as NewConnector
if err != nil { return err }
defer session.Close()
result, err := session.Execute(ctx, "SELECT 42")
if err != nil { return err }
_ = result // ordered Columns, typed Rows, Affected, LastInsertRowID
```

Execute accepts one raw statement without parameter binding; use database/sql
for bound application queries. Result preserves Hrana JSON integer strings,
blob base64 and NULL without float conversion. `Autocommit()` returns the last
confirmed state plus a known flag. A new session is known to be in autocommit;
a failed or closed session is unknown. Callers implementing shell exit cleanup
can explicitly ROLLBACK a known active transaction before Close.
`ExecuteAndClose` sends execute/get_autocommit/close in one pipeline. Successful
Close confirmation rolls back uncommitted work; a failed session only releases
local resources and does not confirm remote cleanup. Always inspect execution
errors: a later Close cannot resolve a lost response or prove rollback.
CloseContext lets callers share a cleanup deadline across rollback and close.
ExecuteAndClose releases local resources even when the execution context was
already canceled; failed fallback close is reported as an unknown outcome.
Calls serialize under a session mutex; request timeouts start after acquiring it.
SQL script parsing, shell history, formatting and account login belong to callers.

## Cancellation, limits and failures

RequestTimeout defaults to 30 seconds; caller deadlines may shorten it.
Canceling an establishment/request context interrupts I/O. The context used to
create a connection does not govern its later idle lifetime.

HTTP headers are capped at 32 KiB; encoded requests and response bodies at 8 MiB.
Rows are buffered within the response limit, so this is not a streaming cursor
API. JSON decoding and Go values use additional memory beyond the wire size.
Close attempts a bounded Hrana stream close (at most three seconds) when its
current baton is known, then releases the SDK tunnel/client.

Once a request may have been sent, transport/protocol failures return
`*tianasqlite.Error` with OutcomeUnknown=true, **never driver.ErrBadConn**.
A complete `BATON_INVALID` HTTP rejection means the current operation did not
execute (`OutcomeUnknown=false`), but the session is still unusable. Other SQL
errors with `OutcomeUnknown=false` can have partial effects, so that flag alone
is not permission to retry. A caller implementing interactive recovery can open
a new Session and retry a rejected operation only after checking the previous
transaction state. New sessions do not restore connection-local state.

This prevents database/sql from replaying a possibly committed write. Invalid
connections are discarded, and later independent operations may open a new one.
Inspect confirmed application state before deciding whether to retry.
Context causes remain available through errors.Is. OutcomeUnknown=false does
not promise zero side effects or permission to retry: SQLite INSERT OR FAIL can
retain partial statement effects while returning a constraint error. Use explicit
transactions and rollback where atomic application behavior is required.

Known SQLite error codes are exposed without arbitrary server error messages,
SQL, values or credentials. Unknown codes are reduced to a generic error.
The driver emits no logs. HTTP redirects and Hrana base_url redirects are rejected.

When a response is lost, its rotated baton may be unknown. Closing the transport
does not prove rollback or commit; the App may retain that session until its TTL.
Commit acknowledgment has the App's durability semantics, not an additional SDK
guarantee of remote/object-storage persistence.

## Verification and first-version scope

```sh
go test -race -timeout 2m ./...
go vet ./...
python3 scripts/check-public-source.py
# Optional: real App library + local SQLite through native SDK TLS/H2 CONNECT.
bash scripts/test-app.sh /path/to/app_sqlite
```

The real-App test is skipped unless its test executable is supplied by the
script. Rust is needed only for that integration fixture, not client consumers.
It builds the external App checkout read-only, with artifacts in .artifacts.
The integration reference is app_sqlite bccd78e3040c9463e802f3538f4f9ffe0fcd9522.

Tests cover typed values, bindings, transactions, pooled context lifetimes,
malformed/oversized replies, redaction and lost write/commit responses without
replay. The gateway in these tests is a synthetic TLS/H2 relay, not a production
Gateway/Agent deployment. Production authorization, autosleep and storage
durability require deployment validation.

Not included in this version: Hrana WebSocket/protobuf, streaming cursors,
multiple result sets, server statement caching, local SQLite files or automatic
account login/token refresh. See examples/basic for a runnable query example.

Apache-2.0 licensed; see [LICENSE](LICENSE).

`Session.RequestID()` returns the established CONNECT identity. Session errors of type `*tianasqlite.Error` retain it in `RequestID`; connection setup errors retain the SDK's diagnostic identity. Configure `Gateway.OnRequestID` to receive the ID before network I/O, including failed handshakes.

### Run the basic example with explicit transport settings

Run `go run ./examples/basic` with `TIANA_ENDPOINT`. Optional `TIANA_TOKEN`,
`TIANA_CA_FILE` (PEM certificates) and `TIANA_GATEWAY_ADDRESS` (host:port) configure
authentication, custom roots and the physical listener. Custom roots replace the
system pool; certificate and Endpoint hostname verification remain enabled.
Invalid or unreadable CA input fails before connecting. The example performs
only a parameterized SELECT and does not create persistent tables.

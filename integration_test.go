//go:build linux || darwin

package tianasqlite

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestRealApp(t *testing.T) {
	binary := os.Getenv("TIANA_SQLITE_APP_PEER_BINARY")
	if binary == "" {
		t.Skip("set TIANA_SQLITE_APP_PEER_BINARY for real app_sqlite integration")
	}
	dir := t.TempDir()
	t.Chdir(dir)
	cmd := exec.Command(binary)
	cmd.Dir = dir
	log, err := os.Create("app.log")
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd.Stdout = log
	cmd.Stderr = log
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	for deadline := time.Now().Add(5 * time.Second); ; {
		if _, err = os.Stat(filepath.Join(dir, "s")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("app fixture not ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cfg, count := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		upstream, e := net.Dial("unix", "s")
		if e != nil {
			t.Error(e)
			return
		}
		defer upstream.Close()
		done := make(chan struct{})
		go func() { defer close(done); _, _ = io.Copy(upstream, r); _ = upstream.Close() }()
		_, _ = io.Copy(w, upstream)
		_ = upstream.Close()
		<-done
	})
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithCancel(context.Background())
	if err = db.PingContext(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()
	ctx = context.Background()
	if _, err = db.ExecContext(ctx, "CREATE TABLE t(id INTEGER PRIMARY KEY, n INTEGER, s TEXT, b BLOB)"); err != nil {
		t.Fatal(err)
	}
	result, err := db.ExecContext(ctx, "INSERT INTO t(n,s,b) VALUES(?,?,?)", int64(math.MaxInt64), "你好", []byte{0, 255})
	if err != nil {
		t.Fatal(err)
	}
	if n, e := result.RowsAffected(); e != nil || n != 1 {
		t.Fatal(n, e)
	}
	if id, e := result.LastInsertId(); e != nil || id != 1 {
		t.Fatal(id, e)
	}
	var n int64
	var s string
	var b []byte
	if err = db.QueryRowContext(ctx, "SELECT n,s,b FROM t WHERE id=:id", sql.Named("id", 1)).Scan(&n, &s, &b); err != nil {
		t.Fatal(err)
	}
	if n != math.MaxInt64 || s != "你好" || string(b) != string([]byte{0, 255}) {
		t.Fatal("value corruption")
	}
	stmt, err := db.PrepareContext(ctx, "INSERT INTO t(n,s,b) VALUES(?,?,?)")
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.StmtContext(ctx, stmt).ExecContext(ctx, int64(math.MinInt64), "rollback", []byte{}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO t(n,s,b) VALUES(?,?,?)", nil, "commit", []byte{}); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRowContext(ctx, "SELECT count(*) FROM t").Scan(&n); err != nil || n != 2 {
		t.Fatal("transaction result", n, err)
	}
	if err = db.QueryRowContext(ctx, "SELECT n FROM t WHERE id=-1").Scan(&n); err != sql.ErrNoRows {
		t.Fatal(err)
	}
	rows, err := db.QueryContext(ctx, "SELECT n,s,b FROM t ORDER BY id")
	if err != nil {
		t.Fatal(err)
	}
	types, err := rows.ColumnTypes()
	if err != nil || len(types) != 3 || types[0].DatabaseTypeName() != "INTEGER" {
		t.Fatal("column types", err)
	}
	_ = rows.Close()
	if _, err = db.ExecContext(ctx, "/* comment */ BEGIN"); err == nil {
		t.Fatal("raw transaction accepted")
	}
	if _, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true}); err == nil {
		t.Fatal("readonly silently ignored")
	}
	if _, err = db.ExecContext(ctx, "CREATE TABLE guard_test (n INTEGER)"); err != nil {
		t.Fatal(err)
	}
	for _, control := range []string{"; COMMIT", "; /* leading */ COMMIT", "\ufeffCOMMIT"} {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO guard_test VALUES(1)"); err != nil {
			t.Fatal(err)
		}
		if stmt, prepareErr := tx.PrepareContext(ctx, control); prepareErr == nil {
			_ = stmt.Close()
			t.Error("prepared transaction control accepted")
		}
		_, err = tx.ExecContext(ctx, control)
		var typed *Error
		if !errors.As(err, &typed) || typed.Code != "USE_SQL_TX" {
			t.Errorf("guard rejected too late: %v", err)
		}
		_ = tx.Rollback()
		var persisted int
		if err = db.QueryRowContext(ctx, "SELECT count(*) FROM guard_test").Scan(&persisted); err != nil {
			t.Fatal(err)
		}
		if persisted != 0 {
			t.Errorf("rejected transaction control committed %d rows", persisted)
		}
	}
	if count.Load() != 1 {
		t.Fatalf("pool was not reused: %d", count.Load())
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	assertDedicatedSessionRealApp(t, cfg)
	assertLostResponseRetainsServerTransaction(t)
}

// HTTP batons outlive TCP connections. This test prevents documentation or
// cleanup code from treating transport close as a confirmed server rollback.
func assertLostResponseRetainsServerTransaction(t *testing.T) {
	t.Helper()
	cfg, _ := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		upstream, err := net.Dial("unix", "s")
		if err != nil {
			t.Error(err)
			return
		}
		defer upstream.Close()
		incoming := bufio.NewReader(r)
		outgoing := bufio.NewReader(upstream)
		for {
			request, err := http.ReadRequest(incoming)
			if err != nil {
				return
			}
			data, err := io.ReadAll(request.Body)
			if err != nil {
				return
			}
			_ = request.Body.Close()
			request.Body = io.NopCloser(bytes.NewReader(data))
			if err = request.Write(upstream); err != nil {
				return
			}
			response, err := http.ReadResponse(outgoing, request)
			if err != nil {
				return
			}
			reply, err := io.ReadAll(response.Body)
			if err != nil {
				return
			}
			_ = response.Body.Close()
			if strings.Contains(string(data), "lost-response-marker") {
				// The real SQLite engine has already executed INSERT and rotated its baton.
				_, _ = io.Copy(io.Discard, incoming)
				return
			}
			response.Body = io.NopCloser(bytes.NewReader(reply))
			if err = response.Write(w); err != nil {
				return
			}
		}
	})
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	if _, err = db.Exec("CREATE TABLE lock_test(n INTEGER)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_, err = tx.ExecContext(ctx, "INSERT INTO lock_test VALUES(1) /* lost-response-marker */")
	var diagnosed *Error
	if !errors.As(err, &diagnosed) || !diagnosed.OutcomeUnknown || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lost response diagnosis: %v", err)
	}
	_ = tx.Rollback() // No current baton is known; this cannot confirm server rollback.
	_, err = db.Exec("INSERT INTO lock_test VALUES(2)")
	if !errors.As(err, &diagnosed) || diagnosed.Code != "SQLITE_BUSY" {
		t.Fatalf("expected retained server write lock, got %v", err)
	}
}

func assertDedicatedSessionRealApp(t *testing.T, cfg Config) {
	t.Helper()
	ctx := context.Background()
	s, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for _, query := range []string{
		"CREATE TABLE dedicated(n INTEGER)", "BEGIN IMMEDIATE", "SAVEPOINT inner_save",
		"INSERT INTO dedicated VALUES(1)", "ROLLBACK TO inner_save", "RELEASE inner_save", "COMMIT",
	} {
		if _, err = s.Execute(ctx, query); err != nil {
			t.Fatal(query, err)
		}
	}
	if auto, known := s.Autocommit(); !auto || !known {
		t.Fatal("commit not confirmed")
	}
	for _, query := range []string{"BEGIN EXCLUSIVE", "INSERT INTO dedicated VALUES(2)"} {
		if _, err = s.Execute(ctx, query); err != nil {
			t.Fatal(query, err)
		}
	}
	if auto, known := s.Autocommit(); auto || !known {
		t.Fatal("transaction not tracked")
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := s.ExecuteAndClose(ctx, "SELECT count(*), 9223372036854775807, x'00ff', NULL FROM dedicated")
	if err != nil {
		t.Fatal(err)
	}
	if !r.Valid() || len(r.Rows) != 1 || string(r.Rows[0][0].Value) != `"0"` ||
		string(r.Rows[0][1].Value) != `"9223372036854775807"` ||
		r.Rows[0][2].Base64 == nil || *r.Rows[0][2].Base64 != "AP8" || r.Rows[0][3].Type != "null" {
		t.Fatalf("dedicated transaction/value corruption: %#v", r)
	}
}

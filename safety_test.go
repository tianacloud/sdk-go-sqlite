package tianasqlite

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type peerRequest struct {
	Baton    *string       `json:"baton"`
	Requests []wireRequest `json:"requests"`
}

func readPeer(r *bufio.Reader) (peerRequest, error) {
	req, err := http.ReadRequest(r)
	if err != nil {
		return peerRequest{}, err
	}
	defer req.Body.Close()
	if req.Method != "POST" || req.URL.Path != "/v3/pipeline" || req.Header.Get("Authorization") != "" || req.Header.Get("Proxy-Authorization") != "" {
		return peerRequest{}, fmt.Errorf("invalid inner HTTP boundary")
	}
	var p peerRequest
	err = json.NewDecoder(req.Body).Decode(&p)
	return p, err
}

const emptyResult = `{"cols":[],"rows":[],"affected_row_count":0,"last_insert_rowid":null}`

func replyBody(p peerRequest, auto bool) string {
	out := make([]string, len(p.Requests))
	baton := `"next"`
	for i, r := range p.Requests {
		switch r.Type {
		case "execute":
			out[i] = `{"type":"ok","response":{"type":"execute","result":` + emptyResult + `}}`
		case "get_autocommit":
			out[i] = fmt.Sprintf(`{"type":"ok","response":{"type":"get_autocommit","is_autocommit":%t}}`, auto)
		case "close":
			out[i] = `{"type":"ok","response":{"type":"close"}}`
			baton = "null"
		}
	}
	return `{"baton":` + baton + `,"base_url":null,"results":[` + strings.Join(out, ",") + `]}`
}
func writeReply(w io.Writer, body string) {
	_, _ = fmt.Fprintf(w, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
}
func TestUnsafeResponsesNeverReplay(t *testing.T) {
	for _, kind := range []string{"lost", "redirect", "base-url", "oversized", "width", "integer-overflow", "bad-baton", "secret-error"} {
		t.Run(kind, func(t *testing.T) {
			var sends atomic.Int32
			cfg, _ := gatewayFixture(t, func(r io.Reader, w io.Writer) {
				reader := bufio.NewReader(r)
				for {
					req, err := readPeer(reader)
					if err != nil {
						return
					}
					if req.Requests[0].Type == "close" {
						writeReply(w, replyBody(req, true))
						continue
					}
					sends.Add(1)
					body := replyBody(req, true)
					switch kind {
					case "lost":
						_, _ = io.WriteString(w, "HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n{")
						return
					case "redirect":
						_, _ = io.WriteString(w, "HTTP/1.1 307 Temporary Redirect\r\nLocation: http://evil.example.test/\r\nContent-Length: 0\r\n\r\n")
						return
					case "base-url":
						body = strings.Replace(body, `"base_url":null`, `"base_url":"https://evil.example.test"`, 1)
					case "oversized":
						_, _ = fmt.Fprintf(w, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", maxBytes+1)
						return
					case "width":
						body = strings.Replace(body, `"rows":[]`, `"rows":[[{"type":"null"}]]`, 1)
					case "integer-overflow":
						body = strings.Replace(body, emptyResult, `{"cols":[{"name":"x"}],"rows":[[{"type":"integer","value":"9223372036854775808"}]],"affected_row_count":0}`, 1)
					case "bad-baton":
						body = strings.Replace(body, `"baton":"next"`, `"baton":null`, 1)
					case "secret-error":
						body = `{"baton":"next","results":[{"type":"error","error":{"code":"SYNTHETIC_SECRET","message":"SYNTHETIC_SECRET"}},{"type":"ok","response":{"type":"get_autocommit","is_autocommit":true}}]}`
					}
					writeReply(w, body)
				}
			})
			connector, err := NewConnector(cfg)
			if err != nil {
				t.Fatal(err)
			}
			db := sql.OpenDB(connector)
			defer db.Close()
			_, err = db.ExecContext(context.Background(), "INSERT INTO t VALUES(1)")
			var diagnosed *Error
			if !errors.As(err, &diagnosed) || !diagnosed.OutcomeUnknown {
				t.Fatalf("expected uncertain outcome, got %v", err)
			}
			if strings.Contains(fmt.Sprintf("%v %+v %#v", err, err, err), "SYNTHETIC_SECRET") {
				t.Fatal("server text leaked")
			}
			if sends.Load() != 1 {
				t.Fatalf("write replayed %d times", sends.Load())
			}
		})
	}
}
func TestCancellationAndInputBeforeSend(t *testing.T) {
	var sends atomic.Int32
	cfg, connects := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		if _, err := readPeer(bufio.NewReader(r)); err != nil {
			return
		}
		sends.Add(1)
		_, _ = io.Copy(io.Discard, r)
	})
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = db.ExecContext(ctx, "INSERT INTO t VALUES(1)"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if connects.Load() != 0 {
		t.Fatal("pre-canceled request dialed")
	}
	// Acquire once before testing local validation and cancellation.
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err = conn.ExecContext(context.Background(), "INSERT INTO t VALUES(?)", strings.Repeat("x", maxBytes+1)); err == nil {
		t.Fatal("oversized argument accepted")
	}
	if sends.Load() != 0 {
		t.Fatal("invalid input reached server")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	_, err = conn.ExecContext(ctx, "INSERT INTO t VALUES(1)")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("context cause lost: %v", err)
	}
	if sends.Load() != 1 {
		t.Fatal("timeout request replayed")
	}
}
func TestCommitResponseLossNeverReplays(t *testing.T) {
	var commits atomic.Int32
	cfg, _ := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		reader := bufio.NewReader(r)
		for {
			req, err := readPeer(reader)
			if err != nil {
				return
			}
			if req.Requests[0].Stmt != nil && req.Requests[0].Stmt.SQL == "COMMIT" {
				commits.Add(1)
				return
			}
			writeReply(w, replyBody(req, false))
		}
	})
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.ExecContext(context.Background(), "INSERT INTO t VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	var e *Error
	if !errors.As(err, &e) || !e.OutcomeUnknown || commits.Load() != 1 {
		t.Fatalf("unsafe commit: %v count %d", err, commits.Load())
	}
}

func TestTransactionSQLPrefixes(t *testing.T) {
	for _, q := range []string{"; COMMIT", "; /* leading */ COMMIT", "\ufeffROLLBACK", "; -- leading\n; SAVEPOINT x", "\ufeff ; BEGIN"} {
		if !transactionSQL(q) {
			t.Errorf("transaction control prefix bypassed guard: %q", q)
		}
	}
}

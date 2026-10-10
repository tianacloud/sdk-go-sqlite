package tianasqlite

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestDedicatedSessionPreservesRawTransactions(t *testing.T) {
	auto := true
	var queries []string
	var queryMu sync.Mutex
	cfg, connects := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		reader := bufio.NewReader(r)
		for {
			req, err := readPeer(reader)
			if err != nil {
				return
			}
			if req.Requests[0].Stmt != nil {
				q := req.Requests[0].Stmt.SQL
				queryMu.Lock()
				queries = append(queries, q)
				queryMu.Unlock()
				if strings.HasPrefix(q, "BEGIN") {
					auto = false
				}
				if q == "COMMIT" {
					auto = true
				}
			}
			writeReply(w, replyBody(req, auto))
		}
	})
	session, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if connects.Load() != 0 {
		t.Fatal("constructor dialed")
	}
	if autocommit, known := session.Autocommit(); !autocommit || !known {
		t.Fatal("initial state")
	}
	result, err := session.Execute(context.Background(), "BEGIN IMMEDIATE")
	if err != nil || !result.Valid() {
		t.Fatal(err)
	}
	if autocommit, known := session.Autocommit(); autocommit || !known {
		t.Fatal("transaction state")
	}
	if _, err = session.Execute(context.Background(), "COMMIT"); err != nil {
		t.Fatal(err)
	}
	if _, err = session.ExecuteAndClose(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	queryMu.Lock()
	defer queryMu.Unlock()
	if connects.Load() != 1 || strings.Join(queries, ";") != "BEGIN IMMEDIATE;COMMIT;SELECT 1" {
		t.Fatal("unexpected reconnect or SQL rewriting")
	}
	if _, err = session.Execute(context.Background(), "SELECT 2"); err == nil {
		t.Fatal("closed session reused")
	}
}
func TestDedicatedSessionNoSQLNoDial(t *testing.T) {
	cfg, count := gatewayFixture(t, func(io.Reader, io.Writer) { t.Error("unexpected dial") })
	session, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err = session.Close(); err != nil {
		t.Fatal(err)
	}
	if count.Load() != 0 {
		t.Fatal("closing unused session dialed")
	}
}

func TestDedicatedSessionNeverReusesInvalidBaton(t *testing.T) {
	var requests atomic.Int32
	cfg, _ := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		reader := bufio.NewReader(r)
		for {
			req, err := readPeer(reader)
			if err != nil {
				return
			}
			requests.Add(1)
			if req.Requests[0].Type == "close" {
				writeReply(w, replyBody(req, true))
				continue
			}
			writeReply(w, `{"baton":"untrusted","results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[[{"type":"null"}]],"affected_row_count":0}}},{"type":"ok","response":{"type":"get_autocommit","is_autocommit":true}}]}`)
		}
	})
	session, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Execute(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("invalid result accepted")
	}
	_ = session.Close()
	if requests.Load() != 1 {
		t.Fatal("untrusted baton used for cleanup")
	}
}

func TestDedicatedSessionPreservesUnsignedRowCount(t *testing.T) {
	cfg, _ := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		reader := bufio.NewReader(r)
		_, _ = readPeer(reader)
		writeReply(w, `{"baton":null,"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":18446744073709551615}}},{"type":"ok","response":{"type":"get_autocommit","is_autocommit":true}},{"type":"ok","response":{"type":"close"}}]}`)
		// Keep CONNECT alive until the client closes; returning a handler with
		// an open request body can race its response with an HTTP/2 reset.
		_, _ = readPeer(reader)
	})
	s, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := s.ExecuteAndClose(context.Background(), "SELECT 1")
	if err != nil || r == nil || r.Affected == nil || *r.Affected != math.MaxUint64 {
		t.Fatalf("lossless metadata rejected: %v", err)
	}
}

func TestDedicatedSessionReportsFailedFallbackClose(t *testing.T) {
	cfg, _ := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		reader := bufio.NewReader(r)
		req, err := readPeer(reader)
		if err != nil {
			return
		}
		writeReply(w, replyBody(req, false))
		_, _ = readPeer(reader) // Close received; lose its acknowledgment.
	})
	s, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Execute(context.Background(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	_, err = s.ExecuteAndClose(context.Background(), "") // local validation error, but transaction already exists
	var typed *Error
	if !errors.As(err, &typed) || !typed.OutcomeUnknown {
		t.Fatalf("lost close hidden by local error: %v", err)
	}
}

func TestDedicatedSessionCanceledExecuteAndCloseReleasesSession(t *testing.T) {
	var closes atomic.Int32
	cfg, _ := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		reader := bufio.NewReader(r)
		for {
			req, err := readPeer(reader)
			if err != nil {
				return
			}
			if req.Requests[0].Type == "close" {
				closes.Add(1)
			}
			writeReply(w, replyBody(req, false))
		}
	})
	s, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = s.Execute(context.Background(), "BEGIN"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = s.ExecuteAndClose(ctx, "SELECT 1"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if closes.Load() != 1 {
		t.Fatal("canceled ExecuteAndClose left session open")
	}
}

// Parse rejection occurs before execution; losing a confirmed transaction or
// poisoning a healthy stream on this response would break shell recovery.
func TestDedicatedSessionParseRejectionKeepsConfirmedState(t *testing.T) {
	for _, inTransaction := range []bool{false, true} {
		t.Run(map[bool]string{false: "autocommit", true: "transaction"}[inTransaction], func(t *testing.T) {
			auto := true
			var queries []string
			var mu sync.Mutex
			cfg, connects := gatewayFixture(t, func(r io.Reader, w io.Writer) {
				reader := bufio.NewReader(r)
				for {
					req, err := readPeer(reader)
					if err != nil {
						return
					}
					if req.Requests[0].Stmt != nil {
						q := req.Requests[0].Stmt.SQL
						mu.Lock()
						queries = append(queries, q)
						mu.Unlock()
						switch q {
						case "BEGIN":
							auto = false
						case "ROLLBACK":
							auto = true
						}
						if q == "SELECT FROM;" {
							state := "true"
							if !auto {
								state = "false"
							}
							writeReply(w, `{"baton":"next","results":[{"type":"error","error":{"code":"SQL_PARSE_ERROR"}},{"type":"ok","response":{"type":"get_autocommit","is_autocommit":`+state+`}}]}`)
							continue
						}
					}
					writeReply(w, replyBody(req, auto))
				}
			})
			s, err := NewSession(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if inTransaction {
				if _, err = s.Execute(context.Background(), "BEGIN"); err != nil {
					t.Fatal(err)
				}
			}
			_, err = s.Execute(context.Background(), "SELECT FROM;")
			var e *Error
			if !errors.As(err, &e) || e.Code != "SQL_PARSE_ERROR" || e.OutcomeUnknown {
				t.Fatalf("definite parse rejection became %v", err)
			}
			if actual, known := s.Autocommit(); !known || actual == inTransaction {
				t.Fatalf("state lost: auto=%v known=%v", actual, known)
			}
			if _, err = s.Execute(context.Background(), "SELECT 1;"); err != nil {
				t.Fatalf("healthy session discarded: %v", err)
			}
			if inTransaction {
				if _, err = s.Execute(context.Background(), "ROLLBACK"); err != nil {
					t.Fatal(err)
				}
			}
			if err = s.Close(); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			defer mu.Unlock()
			want := "SELECT FROM;;SELECT 1;"
			if inTransaction {
				want = "BEGIN;" + want + ";ROLLBACK"
			}
			if connects.Load() != 1 || strings.Join(queries, ";") != want {
				t.Fatalf("replayed or reconnected: connects=%d queries=%v", connects.Load(), queries)
			}
		})
	}
}

func TestDedicatedSessionUnrecognizedSQLErrorStillPoisons(t *testing.T) {
	var requests atomic.Int32
	cfg, connects := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		reader := bufio.NewReader(r)
		for {
			req, err := readPeer(reader)
			if err != nil {
				return
			}
			requests.Add(1)
			if req.Requests[0].Stmt == nil {
				t.Error("unknown response must not authorize cleanup")
				return
			}
			writeReply(w, `{"baton":"unknown","results":[{"type":"error","error":{"code":"FUTURE_UNQUALIFIED_ERROR"}},{"type":"ok","response":{"type":"get_autocommit","is_autocommit":true}}]}`)
		}
	})
	s, err := NewSession(cfg)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Execute(context.Background(), "INSERT INTO t VALUES(1)")
	var e *Error
	if !errors.As(err, &e) || !e.OutcomeUnknown {
		t.Fatalf("unqualified code became retryable: %v", err)
	}
	if _, known := s.Autocommit(); known {
		t.Fatal("unknown response became known state")
	}
	if _, err = s.Execute(context.Background(), "SELECT 1"); err == nil {
		t.Fatal("unknown session reused")
	}
	_ = s.Close()
	if requests.Load() != 1 || connects.Load() != 1 {
		t.Fatalf("unknown operation replayed or cleaned up: requests=%d connects=%d", requests.Load(), connects.Load())
	}
}

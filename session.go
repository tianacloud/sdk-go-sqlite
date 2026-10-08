package tianasqlite

import (
	"bufio"
	"bytes"
	"context"
	"database/sql/driver"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"sync"
	"time"
	"unicode/utf8"

	tiana "github.com/tianacloud/sdk-go"
)

type conn struct {
	mu        sync.Mutex
	socket    net.Conn
	sdk       *tiana.Client
	cancel    context.CancelFunc
	timeout   time.Duration
	reader    *bufio.Reader
	limited   *io.LimitedReader
	baton     *string
	known     bool
	bad       bool
	closed    bool
	inTx      bool
	requestID string
}

func newConn(socket net.Conn, sdk *tiana.Client, cancel context.CancelFunc, timeout time.Duration) *conn {
	limit := &io.LimitedReader{R: socket, N: 32 * 1024}
	connection := &conn{socket: socket, sdk: sdk, cancel: cancel, timeout: timeout, limited: limit, reader: bufio.NewReader(limit)}
	if tunnel, ok := socket.(*tiana.Tunnel); ok {
		connection.requestID = tunnel.Metadata().RequestID
	}
	return connection
}

func (c *conn) pipeline(ctx context.Context, requests []wireRequest, closing bool) ([]wireStreamResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	body, err := json.Marshal(struct {
		Baton    *string       `json:"baton"`
		Requests []wireRequest `json:"requests"`
	}{c.baton, requests})
	if err != nil || len(body) > maxBytes {
		return nil, failure("REQUEST_TOO_LARGE", false, nil)
	}
	requestCtx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	deadline, _ := requestCtx.Deadline()
	if err = c.socket.SetDeadline(deadline); err != nil {
		c.bad = true
		return nil, failure("DEADLINE_FAILED", false, nil)
	}
	socket := c.socket
	done := make(chan struct{})
	stop := context.AfterFunc(requestCtx, func() { _ = socket.SetDeadline(time.Now()); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
		_ = socket.SetDeadline(time.Time{})
	}()
	fail := func(code string) ([]wireStreamResult, error) {
		c.bad = true
		cause := requestCtx.Err()
		// A deadline can race the context timer callback.
		if cause == nil && !time.Now().Before(deadline) {
			cause = context.DeadlineExceeded
		}
		return nil, failure(code, true, cause)
	}
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/v3/pipeline", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	c.known = false // Once bytes may be sent, the old baton cannot safely be reused.
	if err = req.Write(socket); err != nil {
		return fail("SEND_FAILED")
	}
	c.limited.N = 32 * 1024
	response, err := http.ReadResponse(c.reader, req)
	if err != nil {
		return fail("INVALID_HTTP_RESPONSE")
	}
	c.limited.N = math.MaxInt64
	if response.Header.Get("Content-Encoding") != "" || response.ContentLength > maxBytes {
		return fail("HTTP_REJECTED")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil || len(data) > maxBytes {
		return fail("RESPONSE_INCOMPLETE_OR_TOO_LARGE")
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		var rejection struct{ Code string }
		if !utf8.Valid(data) || json.Unmarshal(data, &rejection) != nil {
			return fail("HTTP_REJECTED")
		}
		switch rejection.Code {
		case "BATON_INVALID":
			// The App rejects the baton before executing any pipeline request.
			// This operation did not run, but the old session is still lost.
			c.bad = true
			return nil, failure(rejection.Code, false, nil)
		case "STREAM_EXPIRED", "STREAM_NOT_FOUND", "STREAM_LIMIT", "SERVICE_STOPPING", "REQUEST_TOO_LARGE":
			return fail(rejection.Code)
		default:
			return fail("HTTP_REJECTED")
		}
	}
	var envelope wireEnvelope
	if !utf8.Valid(data) || json.Unmarshal(data, &envelope) != nil || envelope.BaseURL != nil || len(envelope.Results) != len(requests) {
		return fail("INVALID_RESPONSE")
	}
	if closing {
		if envelope.Baton != nil {
			return fail("INVALID_RESPONSE")
		}
	} else if envelope.Baton == nil || *envelope.Baton == "" || len(*envelope.Baton) > 4096 {
		return fail("INVALID_RESPONSE")
	}
	for i, r := range envelope.Results {
		switch r.Type {
		case "ok":
			if r.Response == nil || r.Error != nil || r.Response.Type != requests[i].Type {
				return fail("INVALID_RESPONSE")
			}
			if r.Response.Type == "execute" && r.Response.Result == nil {
				return fail("INVALID_RESPONSE")
			}
			if r.Response.Type == "get_autocommit" && r.Response.Autocommit == nil {
				return fail("INVALID_RESPONSE")
			}
		case "error":
			if r.Error == nil || r.Response != nil {
				return fail("INVALID_RESPONSE")
			}
		default:
			return fail("INVALID_RESPONSE")
		}
	}
	c.baton = envelope.Baton
	c.known = !closing
	if response.Close {
		c.bad = true
	}
	return envelope.Results, nil
}

func (c *conn) execute(ctx context.Context, query string, args []driver.NamedValue, wantRows, autocommit bool) (*rows, *result, error) {
	wire, err := c.executeResult(ctx, query, args, wantRows, &autocommit, false)
	if err != nil {
		return nil, nil, err
	}
	r, res, err := decodeResult(wire)
	if err != nil {
		c.bad = true
	}
	return r, res, err
}

// expected=nil is reserved for an exclusively owned Session, never a pooled
// database/sql connection. Its raw SQL controls its own transaction state.
func (c *conn) executeResult(ctx context.Context, query string, args []driver.NamedValue, wantRows bool, expected *bool, closing bool) (*Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.bad || c.closed {
		return nil, driver.ErrBadConn
	}
	if query == "" || len(query) > maxBytes || !utf8.ValidString(query) {
		return nil, failure("INVALID_SQL", false, nil)
	}
	positional, named, err := encodeArgs(args)
	if err != nil {
		return nil, err
	}
	requests := []wireRequest{
		{Type: "execute", Stmt: &wireStmt{SQL: query, Args: positional, NamedArgs: named, WantRows: wantRows}},
		{Type: "get_autocommit"},
	}
	if closing {
		requests = append(requests, wireRequest{Type: "close"})
	}
	wire, err := c.pipeline(ctx, requests, closing)
	if err != nil {
		return nil, err
	}
	if closing && wire[2].Type != "ok" {
		c.bad = true
		return nil, failure("CLOSE_UNCONFIRMED", true, nil)
	}
	if wire[1].Type != "ok" {
		c.bad = true
		return nil, failure("TRANSACTION_STATE_UNKNOWN", true, nil)
	}
	auto := *wire[1].Response.Autocommit
	if expected == nil {
		c.inTx = !auto
	}
	if wire[0].Type == "error" {
		diagnosed := sqlFailure(wire[0].Error.Code)
		if (expected != nil && auto != *expected) || diagnosed.OutcomeUnknown {
			c.bad = true
		}
		return nil, diagnosed
	}
	if expected != nil && auto != *expected {
		c.bad = true
		return nil, failure("TRANSACTION_STATE_CHANGED", true, nil)
	}
	result := wire[0].Response.Result
	if expected == nil && !result.Valid() {
		c.bad = true
		return nil, failure("INVALID_RESULT", true, nil)
	}
	return result, nil
}
func (c *conn) Close() error { return c.closeContext(context.Background()) }
func (c *conn) closeContext(parent context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	var closeErr error
	if c.known && c.baton != nil {
		ctx, cancel := context.WithTimeout(parent, min(c.timeout, 3*time.Second))
		response, err := c.pipeline(ctx, []wireRequest{{Type: "close"}}, true)
		cancel()
		if err != nil {
			closeErr = err
		} else if response[0].Type != "ok" {
			closeErr = failure("CLOSE_UNCONFIRMED", true, nil)
		}
	}
	c.closed = true
	c.bad = true
	c.cancel()
	_ = c.socket.Close()
	_ = c.sdk.Close()
	return closeErr
}
func (c *conn) IsValid() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.bad && !c.closed && !c.inTx
}
func (c *conn) ResetSession(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bad || c.closed || c.inTx {
		return driver.ErrBadConn
	}
	return ctx.Err()
}
func (c *conn) Ping(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bad || c.closed {
		return driver.ErrBadConn
	}
	r, err := c.pipeline(ctx, []wireRequest{{Type: "get_autocommit"}}, false)
	if err != nil {
		return err
	}
	if r[0].Type != "ok" || *r[0].Response.Autocommit == c.inTx {
		c.bad = true
		return failure("TRANSACTION_STATE_UNKNOWN", true, nil)
	}
	return nil
}

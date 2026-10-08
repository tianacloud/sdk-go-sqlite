package tianasqlite

import (
	"context"
	"database/sql/driver"
	"errors"
	"sync"
)

// Session exclusively owns one lazy Hrana connection. Unlike database/sql,
// it accepts raw transaction statements (including savepoints) and never pools,
// reconnects or retries. Call Close to release it.
type Session struct {
	mu        sync.Mutex
	connector *Connector
	conn      *conn
	failed    bool
	closed    bool
}

// RequestID returns the diagnostic identity of this session's CONNECT tunnel.
func (s *Session) RequestID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return ""
	}
	return s.conn.requestID
}

// NewSession validates configuration but performs no I/O.
func NewSession(config Config) (*Session, error) {
	connector, err := NewConnector(config)
	if err != nil {
		return nil, err
	}
	return &Session{connector: connector}, nil
}

// Autocommit reports the last confirmed state. A fresh session is in autocommit;
// after failure/close, known is false and rollback cannot be assumed.
func (s *Session) Autocommit() (autocommit, known bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failed || s.closed {
		return false, false
	}
	if s.conn == nil {
		return true, true
	}
	return !s.conn.inTx, !s.conn.bad
}

// Execute runs one raw SQL statement and returns its typed, buffered result.
func (s *Session) Execute(ctx context.Context, query string) (*Result, error) {
	return s.execute(ctx, query, false)
}

// ExecuteAndClose sends execute/get_autocommit/close in one pipeline, then
// releases the transport. Any open transaction is rolled back by confirmed close.
func (s *Session) ExecuteAndClose(ctx context.Context, query string) (*Result, error) {
	return s.execute(ctx, query, true)
}
func (s *Session) execute(ctx context.Context, query string, closing bool) (result *Result, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	defer func() { err = sessionErrorWithRequestID(err, s.conn) }()
	if closing {
		defer func() {
			s.closed = true
			if s.conn != nil {
				if closeErr := s.closeConnection(context.Background()); closeErr != nil {
					err = failure("CLOSE_UNCONFIRMED", true, errors.Join(err, closeErr))
				}
			}
		}()
	}
	if s.closed || s.failed {
		return nil, failure("SESSION_UNUSABLE", true, nil)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, s.connector.config.RequestTimeout)
	defer cancel()
	if s.conn == nil {
		connection, err := s.connector.Connect(requestCtx)
		if err != nil {
			s.failed = true
			return nil, err
		}
		s.conn = connection.(*conn)
	}
	result, err = s.conn.executeResult(requestCtx, query, nil, true, nil, closing)
	s.failed = s.conn.bad
	if errors.Is(err, driver.ErrBadConn) {
		s.failed = true
		err = failure("SESSION_UNUSABLE", true, nil)
	}
	return result, err
}

// Close releases the session, bounding a usable remote close to three seconds.
func (s *Session) Close() error { return s.CloseContext(context.Background()) }

// CloseContext shares the caller's cleanup budget and always releases local
// resources. Failed sessions cannot confirm remote cleanup. As with Execute,
// waiting for an in-flight call's mutex is outside the context's I/O bound.
func (s *Session) CloseContext(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	if s.conn == nil {
		return nil
	}
	return sessionErrorWithRequestID(s.closeConnection(ctx), s.conn)
}

func sessionErrorWithRequestID(err error, connection *conn) error {
	if err == nil || connection == nil || connection.requestID == "" {
		return err
	}
	var typed *Error
	if errors.As(err, &typed) && typed == err {
		copy := *typed
		copy.RequestID = connection.requestID
		return &copy
	}
	return err
}

// The connection is exclusively owned under s.mu. Never send a second request
// after an invalid response, even if the envelope contained a syntactic baton.
func (s *Session) closeConnection(ctx context.Context) error {
	if s.failed {
		s.conn.known = false
	}
	return s.conn.closeContext(ctx)
}

package tianasqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"io"
	"math"
	"strconv"
)

type result struct {
	affected int64
	lastID   *int64
}

func (r *result) RowsAffected() (int64, error) { return r.affected, nil }
func (r *result) LastInsertId() (int64, error) {
	if r.lastID == nil {
		return 0, failure("NO_INSERT_ROWID", false, nil)
	}
	return *r.lastID, nil
}

type rows struct {
	columns []wireColumn
	values  [][]driver.Value
	index   int
}

func (r *rows) Columns() []string {
	out := make([]string, len(r.columns))
	for i, col := range r.columns {
		if col.Name != nil {
			out[i] = *col.Name
		}
	}
	return out
}
func (r *rows) ColumnTypeDatabaseTypeName(i int) string {
	if r.columns[i].Decltype == nil {
		return ""
	}
	return *r.columns[i].Decltype
}
func (r *rows) Close() error { r.values = nil; return nil }
func (r *rows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}
func decodeResult(w *wireResult) (*rows, *result, error) {
	fail := func() (*rows, *result, error) { return nil, nil, failure("INVALID_RESULT", true, nil) }
	if w == nil || w.Columns == nil || w.Rows == nil || w.Affected == nil || *w.Affected > math.MaxInt64 {
		return fail()
	}
	result := &result{affected: int64(*w.Affected)}
	if w.LastInsertRowID != nil {
		n, err := strconv.ParseInt(*w.LastInsertRowID, 10, 64)
		if err != nil {
			return fail()
		}
		result.lastID = &n
	}
	rows := &rows{columns: w.Columns, values: make([][]driver.Value, len(w.Rows))}
	for i, row := range w.Rows {
		if len(row) != len(w.Columns) {
			return fail()
		}
		rows.values[i] = make([]driver.Value, len(row))
		for j, value := range row {
			v, err := value.decode()
			if err != nil {
				return fail()
			}
			rows.values[i][j] = v
		}
	}
	return rows, result, nil
}
func (c *conn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if transactionSQL(q) {
		return nil, failure("USE_SQL_TX", false, nil)
	}
	_, r, err := c.execute(ctx, q, args, false, !c.inTx)
	if err != nil {
		return nil, err
	}
	return r, nil
}
func (c *conn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if transactionSQL(q) {
		return nil, failure("USE_SQL_TX", false, nil)
	}
	r, _, err := c.execute(ctx, q, args, true, !c.inTx)
	if err != nil {
		return nil, err
	}
	return r, nil
}
func (c *conn) Prepare(q string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), q)
}
func (c *conn) PrepareContext(ctx context.Context, q string) (driver.Stmt, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.bad || c.closed {
		return nil, driver.ErrBadConn
	}
	if transactionSQL(q) {
		return nil, failure("USE_SQL_TX", false, nil)
	}
	return &statement{conn: c, sql: q}, nil
}

type statement struct {
	conn   *conn
	sql    string
	closed bool
}

func (s *statement) Close() error {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	s.closed = true
	return nil
}
func (s *statement) NumInput() int { return -1 }
func (s *statement) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if s.closed {
		return nil, failure("STATEMENT_CLOSED", false, nil)
	}
	_, r, err := s.conn.execute(ctx, s.sql, args, false, !s.conn.inTx)
	if err != nil {
		return nil, err
	}
	return r, nil
}
func (s *statement) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.conn.mu.Lock()
	defer s.conn.mu.Unlock()
	if s.closed {
		return nil, failure("STATEMENT_CLOSED", false, nil)
	}
	r, _, err := s.conn.execute(ctx, s.sql, args, true, !s.conn.inTx)
	if err != nil {
		return nil, err
	}
	return r, nil
}
func (s *statement) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), namedArgs(args))
}
func (s *statement) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), namedArgs(args))
}
func namedArgs(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, value := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: value}
	}
	return out
}
func (c *conn) Begin() (driver.Tx, error) { return c.BeginTx(context.Background(), driver.TxOptions{}) }
func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if opts.ReadOnly {
		return nil, failure("READ_ONLY_UNSUPPORTED", false, nil)
	}
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) && opts.Isolation != driver.IsolationLevel(sql.LevelSerializable) {
		return nil, failure("ISOLATION_UNSUPPORTED", false, nil)
	}
	if c.inTx {
		return nil, failure("TRANSACTION_ACTIVE", false, nil)
	}
	if _, _, err := c.execute(ctx, "BEGIN", nil, false, false); err != nil {
		return nil, err
	}
	c.inTx = true
	return &transaction{conn: c}, nil
}

type transaction struct {
	conn *conn
	done bool
}

func (t *transaction) finish(q string) error {
	c := t.conn
	c.mu.Lock()
	defer c.mu.Unlock()
	if t.done {
		return failure("TRANSACTION_DONE", false, nil)
	}
	t.done = true
	_, _, err := c.execute(context.Background(), q, nil, false, true)
	c.inTx = false
	if err != nil {
		c.bad = true
	}
	return err
}
func (t *transaction) Commit() error   { return t.finish("COMMIT") }
func (t *transaction) Rollback() error { return t.finish("ROLLBACK") }

var _ driver.ExecerContext = (*conn)(nil)
var _ driver.QueryerContext = (*conn)(nil)
var _ driver.ConnPrepareContext = (*conn)(nil)
var _ driver.ConnBeginTx = (*conn)(nil)
var _ driver.Pinger = (*conn)(nil)
var _ driver.SessionResetter = (*conn)(nil)
var _ driver.Validator = (*conn)(nil)
var _ driver.StmtExecContext = (*statement)(nil)
var _ driver.StmtQueryContext = (*statement)(nil)
var _ driver.RowsColumnTypeDatabaseTypeName = (*rows)(nil)

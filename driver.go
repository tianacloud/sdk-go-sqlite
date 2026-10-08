// Package tianasqlite implements database/sql over a verified sdk-go CONNECT
// tunnel carrying Hrana 3 HTTP. It never replays an operation after sending it.
package tianasqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	tiana "github.com/tianacloud/sdk-go"
)

// Config supplies the Gateway endpoint and optional InstanceToken. No
// environment variables or local credential files are read implicitly.
type Config struct {
	Gateway        tiana.Config
	RequestTimeout time.Duration
}

func (Config) Format(s fmt.State, _ rune) { _, _ = io.WriteString(s, "tianasqlite.Config([REDACTED])") }

// Connector creates independent Hrana sessions for database/sql's pool.
type Connector struct{ config Config }

func (Connector) Format(s fmt.State, _ rune) {
	_, _ = io.WriteString(s, "tianasqlite.Connector([REDACTED])")
}

// NewConnector validates configuration without network or filesystem I/O.
func NewConnector(config Config) (*Connector, error) {
	if config.RequestTimeout < 0 {
		return nil, failure("INVALID_TIMEOUT", false, nil)
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 30 * time.Second
	}
	probe, err := tiana.NewClient(config.Gateway)
	if err != nil {
		return nil, err
	}
	_ = probe.Close()
	if config.Gateway.Token != nil {
		token := *config.Gateway.Token
		config.Gateway.Token = &token
	}
	if config.Gateway.RootCAs != nil {
		config.Gateway.RootCAs = config.Gateway.RootCAs.Clone()
	}
	return &Connector{config}, nil
}
func (c *Connector) Driver() driver.Driver { return &Driver{} }
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sdk, err := tiana.NewClient(c.config.Gateway)
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(context.WithoutCancel(ctx))
	dialCtx, dialCancel := context.WithTimeout(ctx, c.config.RequestTimeout)
	defer dialCancel()
	stop := context.AfterFunc(dialCtx, cancel)
	tunnel, err := sdk.Connect(life, tiana.HranaHTTP)
	stop()
	if err != nil || dialCtx.Err() != nil {
		cancel()
		_ = sdk.Close()
		if dialCtx.Err() != nil {
			return nil, dialCtx.Err()
		}
		return nil, err
	}
	return newConn(tunnel, sdk, cancel, c.config.RequestTimeout), nil
}

// Driver is registered as "tiana-sqlite". DSNs contain only an endpoint:
// tiana://ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test. Use NewConnector to supply credentials/TLS roots.
type Driver struct{}

func init() { sql.Register("tiana-sqlite", &Driver{}) }
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	c, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return c.Connect(context.Background())
}
func (*Driver) OpenConnector(dsn string) (driver.Connector, error) {
	u, err := url.Parse(dsn)
	if err != nil || u.Scheme != "tiana" || u.Host == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Path != "" || u.Opaque != "" {
		return nil, failure("INVALID_DSN", false, nil)
	}
	return NewConnector(Config{Gateway: tiana.Config{Endpoint: u.Host}})
}

// transactionSQL prevents unmanaged transactions escaping a pooled connection.
// The server, not this prefix check, validates the full single-statement SQL.
func transactionSQL(query string) bool {
	for {
		query = strings.TrimSpace(query)
		if strings.HasPrefix(query, ";") {
			query = query[1:]
			continue
		}
		if strings.HasPrefix(query, "\ufeff") {
			query = query[len("\ufeff"):]
			continue
		}
		if strings.HasPrefix(query, "--") {
			i := strings.IndexByte(query, '\n')
			if i < 0 {
				return false
			}
			query = query[i+1:]
			continue
		}
		if strings.HasPrefix(query, "/*") {
			i := strings.Index(query[2:], "*/")
			if i < 0 {
				return false
			}
			query = query[i+4:]
			continue
		}
		break
	}
	end := 0
	for end < len(query) && ((query[end] >= 'a' && query[end] <= 'z') || (query[end] >= 'A' && query[end] <= 'Z')) {
		end++
	}
	switch strings.ToUpper(query[:end]) {
	case "BEGIN", "COMMIT", "END", "ROLLBACK", "SAVEPOINT", "RELEASE":
		return true
	}
	return false
}

var _ driver.DriverContext = (*Driver)(nil)
var _ driver.Connector = (*Connector)(nil)

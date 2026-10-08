package tianasqlite

import (
	"bytes"
	"database/sql/driver"
	"encoding/base64"
	"encoding/json"
	"math"
	"strconv"
	"time"
	"unicode/utf8"
)

const maxBytes = 8 * 1024 * 1024

// Value preserves the lossless Hrana JSON representation.
type Value struct {
	Type   string          `json:"type"`
	Value  json.RawMessage `json:"value,omitempty"`
	Base64 *string         `json:"base64,omitempty"`
}
type namedValue struct {
	Name  string    `json:"name"`
	Value wireValue `json:"value"`
}
type wireStmt struct {
	SQL       string       `json:"sql"`
	Args      []wireValue  `json:"args,omitempty"`
	NamedArgs []namedValue `json:"named_args,omitempty"`
	WantRows  bool         `json:"want_rows"`
}
type wireRequest struct {
	Type string    `json:"type"`
	Stmt *wireStmt `json:"stmt,omitempty"`
}

// Column is an ordered result column; name/decltype may be absent.
type Column struct {
	Name     *string `json:"name"`
	Decltype *string `json:"decltype"`
}

// Result is a buffered dedicated-session result with exact integer metadata.
type Result struct {
	Columns         []Column  `json:"cols"`
	Rows            [][]Value `json:"rows"`
	Affected        *uint64   `json:"affected_row_count"`
	LastInsertRowID *string   `json:"last_insert_rowid"`
}
type wireResponse struct {
	Type       string      `json:"type"`
	Result     *wireResult `json:"result"`
	Autocommit *bool       `json:"is_autocommit"`
}
type wireStreamResult struct {
	Type     string        `json:"type"`
	Response *wireResponse `json:"response"`
	Error    *struct {
		Code string `json:"code"`
	} `json:"error"`
}
type wireEnvelope struct {
	Baton   *string            `json:"baton"`
	BaseURL *string            `json:"base_url"`
	Results []wireStreamResult `json:"results"`
}

func encodeArgs(args []driver.NamedValue) ([]wireValue, []namedValue, error) {
	var positional []wireValue
	var named []namedValue
	size := 0
	names := map[string]bool{}
	for _, arg := range args {
		value, err := driver.DefaultParameterConverter.ConvertValue(arg.Value)
		if err != nil {
			return nil, nil, failure("INVALID_ARGUMENT", false, nil)
		}
		if v, ok := value.(time.Time); ok {
			value = v.Format(time.RFC3339Nano)
		}
		var wire wireValue
		switch v := value.(type) {
		case nil:
			wire.Type = "null"
		case bool:
			wire.Type = "integer"
			n := "0"
			if v {
				n = "1"
			}
			wire.Value, _ = json.Marshal(n)
		case int64:
			wire.Type = "integer"
			wire.Value, _ = json.Marshal(strconv.FormatInt(v, 10))
		case float64:
			if math.IsNaN(v) || math.IsInf(v, 0) {
				return nil, nil, failure("INVALID_ARGUMENT", false, nil)
			}
			wire.Type = "float"
			wire.Value, _ = json.Marshal(v)
		case string:
			if len(v) > maxBytes || !utf8.ValidString(v) {
				return nil, nil, failure("INVALID_ARGUMENT", false, nil)
			}
			wire.Type = "text"
			wire.Value, _ = json.Marshal(v)
		case []byte:
			if len(v) > maxBytes {
				return nil, nil, failure("REQUEST_TOO_LARGE", false, nil)
			}
			if v == nil {
				wire.Type = "null"
			} else {
				wire.Type = "blob"
				s := base64.RawStdEncoding.EncodeToString(v)
				wire.Base64 = &s
			}
		default:
			return nil, nil, failure("INVALID_ARGUMENT", false, nil)
		}
		size += len(wire.Value) + len(arg.Name) + 64
		if wire.Base64 != nil {
			size += len(*wire.Base64)
		}
		if size > maxBytes {
			return nil, nil, failure("REQUEST_TOO_LARGE", false, nil)
		}
		if arg.Name != "" {
			if names[arg.Name] || !utf8.ValidString(arg.Name) {
				return nil, nil, failure("INVALID_ARGUMENT", false, nil)
			}
			names[arg.Name] = true
			named = append(named, namedValue{arg.Name, wire})
		} else {
			positional = append(positional, wire)
		}
	}
	if len(positional) > 0 && len(named) > 0 {
		return nil, nil, failure("MIXED_ARGUMENTS", false, nil)
	}
	return positional, named, nil
}

func (v wireValue) decode() (driver.Value, error) {
	bad := func() (driver.Value, error) { return nil, failure("INVALID_RESPONSE", true, nil) }
	switch v.Type {
	case "null":
		if len(v.Value) != 0 || v.Base64 != nil {
			return bad()
		}
		return nil, nil
	case "integer":
		var s string
		if v.Base64 != nil || json.Unmarshal(v.Value, &s) != nil {
			return bad()
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return bad()
		}
		return n, nil
	case "float":
		var f float64
		if v.Base64 != nil || bytes.Equal(v.Value, []byte("null")) || json.Unmarshal(v.Value, &f) != nil || math.IsNaN(f) || math.IsInf(f, 0) {
			return bad()
		}
		return f, nil
	case "text":
		var s string
		if v.Base64 != nil || bytes.Equal(v.Value, []byte("null")) || json.Unmarshal(v.Value, &s) != nil {
			return bad()
		}
		return s, nil
	case "blob":
		if v.Base64 == nil || len(v.Value) != 0 {
			return bad()
		}
		b, err := base64.RawStdEncoding.Strict().DecodeString(*v.Base64)
		if err != nil {
			return bad()
		}
		return b, nil
	default:
		return bad()
	}
}

// Internal aliases share the same protocol codec with database/sql.
type wireValue = Value
type wireColumn = Column
type wireResult = Result

// Valid reports whether the value has a recognized, lossless wire encoding.
func (v Value) Valid() bool { _, err := v.decode(); return err == nil }

// Valid checks the shape and scalar encodings without retaining decoded rows.
func (r *Result) Valid() bool {
	if r == nil || r.Columns == nil || r.Rows == nil || r.Affected == nil {
		return false
	}
	if r.LastInsertRowID != nil {
		if _, err := strconv.ParseInt(*r.LastInsertRowID, 10, 64); err != nil {
			return false
		}
	}
	for _, row := range r.Rows {
		if len(row) != len(r.Columns) {
			return false
		}
		for _, v := range row {
			if !v.Valid() {
				return false
			}
		}
	}
	return true
}

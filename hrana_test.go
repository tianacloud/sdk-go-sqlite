package tianasqlite

import (
	"database/sql/driver"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestWireValues(t *testing.T) {
	for _, input := range []any{nil, int64(math.MinInt64), int64(math.MaxInt64), float64(1.5), "hello 世界", []byte{0, 255}, []byte{}, true, time.Unix(1, 2).UTC()} {
		args, _, err := encodeArgs([]driver.NamedValue{{Ordinal: 1, Value: input}})
		if err != nil {
			t.Fatal(err)
		}
		data, err := json.Marshal(args[0])
		if err != nil {
			t.Fatal(err)
		}
		var value wireValue
		if err = json.Unmarshal(data, &value); err != nil {
			t.Fatal(err)
		}
		got, err := value.decode()
		if err != nil {
			t.Fatal(err)
		}
		want := input
		switch v := input.(type) {
		case bool:
			want = int64(1)
		case time.Time:
			want = v.Format(time.RFC3339Nano)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("roundtrip type %T: got %v want %v", input, got, want)
		}
	}
	for _, input := range []any{math.Inf(1), math.NaN(), uint64(math.MaxUint64), strings.Repeat("x", maxBytes+1), string([]byte{255})} {
		if _, _, err := encodeArgs([]driver.NamedValue{{Ordinal: 1, Value: input}}); err == nil {
			t.Fatalf("accepted invalid %T", input)
		}
	}
}
func TestArgumentModes(t *testing.T) {
	_, named, err := encodeArgs([]driver.NamedValue{{Name: "x", Ordinal: 1, Value: int64(9)}})
	if err != nil || len(named) != 1 || named[0].Name != "x" {
		t.Fatal("named binding")
	}
	if _, _, err = encodeArgs([]driver.NamedValue{{Name: "x", Ordinal: 1, Value: int64(1)}, {Ordinal: 2, Value: int64(2)}}); err == nil {
		t.Fatal("mixed bindings accepted")
	}
}
func TestMalformedWireValues(t *testing.T) {
	for _, raw := range []string{
		`{"type":"integer","value":"9223372036854775808"}`,
		`{"type":"integer","value":123}`,
		`{"type":"float","value":null}`,
		`{"type":"blob","base64":"!"}`,
		`{"type":"text","value":null}`,
		`{"type":"null","value":"secret"}`,
		`{"type":"other"}`,
	} {
		var v wireValue
		if err := json.Unmarshal([]byte(raw), &v); err != nil {
			t.Fatal(err)
		}
		if _, err := v.decode(); err == nil {
			t.Fatal("accepted malformed value")
		}
	}
}

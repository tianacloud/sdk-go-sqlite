package tianasqlite

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
)

func TestBatonRejectionIsKnownButSessionIsLost(t *testing.T) {
	for _, tc := range []struct {
		name, body          string
		incomplete, unknown bool
	}{
		{"baton rejected", `{"code":"BATON_INVALID"}`, false, false},
		{"malformed rejection", `{"code":"BATON_INVALID","Code":123}`, false, true},
		{"incomplete rejection", `{"code":"BATON_INVALID"}`, true, true},
		{"unrecognized rejection", `{"code":"OTHER"}`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, connects := gatewayFixture(t, func(r io.Reader, w io.Writer) {
				reader := bufio.NewReader(r)
				req, err := readPeer(reader)
				if err != nil {
					return
				}
				writeReply(w, replyBody(req, true))
				if _, err = readPeer(reader); err != nil {
					return
				}
				size := len(tc.body)
				if tc.incomplete {
					size++
				}
				fmt.Fprintf(w, "HTTP/1.1 400 Bad Request\r\nContent-Length: %d\r\n\r\n%s", size, tc.body)
				if !tc.incomplete {
					_, _ = readPeer(reader)
				}
			})
			s, err := NewSession(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if _, err = s.Execute(context.Background(), "SELECT 1"); err != nil {
				t.Fatal(err)
			}
			_, err = s.Execute(context.Background(), "INSERT INTO t VALUES(1)")
			var typed *Error
			if !errors.As(err, &typed) || typed.OutcomeUnknown != tc.unknown {
				t.Fatalf("rejection: %v; want unknown=%t", err, tc.unknown)
			}
			if !tc.unknown && typed.Code != "BATON_INVALID" {
				t.Fatal(typed)
			}
			if _, known := s.Autocommit(); known {
				t.Fatal("lost session state reported as known")
			}
			if _, err = s.Execute(context.Background(), "SELECT 2"); err == nil {
				t.Fatal("lost session reused")
			}
			if connects.Load() != 1 {
				t.Fatal("SDK retried")
			}
		})
	}
}

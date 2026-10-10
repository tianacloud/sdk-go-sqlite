package main

import (
	"context"
	"encoding/json"
	"fmt"
	tiana "github.com/tianacloud/sdk-go"
	sqlite "github.com/tianacloud/sdk-go-sqlite"
	"net/http"
	"os"
	"sync"
	"time"
)

type request struct {
	ID       string `json:"id"`
	Endpoint string `json:"endpoint"`
	Token    string `json:"token"`
	SQL      string `json:"sql"`
	Close    bool   `json:"close"`
}

var mu sync.Mutex
var sessions = map[string]*sqlite.Session{}

func main() {
	http.HandleFunc("/sql", func(w http.ResponseWriter, r *http.Request) {
		var q request
		if e := json.NewDecoder(r.Body).Decode(&q); e != nil {
			http.Error(w, "bad input", 400)
			return
		}
		mu.Lock()
		s := sessions[q.ID]
		if s == nil && !q.Close {
			token, e := tiana.NewToken(q.Token)
			if e != nil {
				mu.Unlock()
				http.Error(w, "bad token", 400)
				return
			}
			s, e = sqlite.NewSession(sqlite.Config{Gateway: tiana.Config{Endpoint: q.Endpoint, Token: token}, RequestTimeout: 30 * time.Second})
			if e != nil {
				mu.Unlock()
				http.Error(w, "bad configuration", 400)
				return
			}
			sessions[q.ID] = s
		}
		if q.Close {
			delete(sessions, q.ID)
		}
		mu.Unlock()
		start := time.Now()
		var result *sqlite.Result
		var err error
		if s == nil {
			err = fmt.Errorf("unknown session")
		} else if q.Close {
			err = s.Close()
		} else {
			result, err = s.Execute(context.Background(), q.SQL)
		}
		id := ""
		if s != nil {
			id = s.RequestID()
		}
		out := map[string]any{"result": result, "elapsed_ms": float64(time.Since(start).Microseconds()) / 1000, "request_id": id}
		if err != nil {
			out["error"] = err.Error()
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(out)
	})
	fmt.Fprintln(os.Stderr, "TPC-C SDK bridge listening on 127.0.0.1:18761")
	if err := http.ListenAndServe("127.0.0.1:18761", nil); err != nil {
		panic(err)
	}
}

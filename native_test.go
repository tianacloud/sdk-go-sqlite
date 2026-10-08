package tianasqlite

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"database/sql/driver"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	tiana "github.com/tianacloud/sdk-go"
)

func gatewayFixture(t *testing.T, inner func(io.Reader, io.Writer)) (Config, *atomic.Int32) {
	t.Helper()
	const endpoint = "ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test"
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{endpoint}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	token, err := tiana.NewToken("opaque-group-secret-with-arbitrary-prefix-and-length")
	if err != nil {
		t.Fatal(err)
	}
	count := &atomic.Int32{}
	type connectionKey struct{}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		state := r.Context().Value(connectionKey{}).(*tls.Conn).ConnectionState()
		count.Add(1)
		if r.Method != "CONNECT" || r.Host != endpoint+":443" || r.Header.Get("tiana-database-protocol") != "hrana-http" || state.Version != tls.VersionTLS13 || r.ProtoMajor != 2 {
			t.Error("invalid CONNECT contract")
		}
		if r.Header.Get("Proxy-Authorization") != "Bearer opaque-group-secret-with-arbitrary-prefix-and-length" {
			t.Error("missing outer token")
		}
		w.Header()["Date"] = nil
		w.Header()["Content-Type"] = nil
		w.Header().Set("tiana-tunnel-version", "1")
		w.Header().Set("tiana-request-id", r.Header.Get("tiana-request-id"))
		w.Header().Set("tiana-auth-mode", "TOKEN_REQUIRED")
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		inner(r.Body, flushWriter{w})
	}))
	server.Config.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
		return context.WithValue(ctx, connectionKey{}, conn)
	}
	server.EnableHTTP2 = true
	server.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS13}
	server.StartTLS()
	t.Cleanup(server.Close)
	return Config{Gateway: tiana.Config{Endpoint: endpoint, Token: token, RootCAs: roots, DialAddress: server.Listener.Addr().String()}, RequestTimeout: 2 * time.Second}, count
}

type flushWriter struct{ http.ResponseWriter }

func (w flushWriter) Write(p []byte) (int, error) {
	n, e := w.ResponseWriter.Write(p)
	w.ResponseWriter.(http.Flusher).Flush()
	return n, e
}

func TestInvalidConfigAndDSN(t *testing.T) {
	for _, cfg := range []Config{{}, {Gateway: tiana.Config{Endpoint: "bad"}, RequestTimeout: -1}} {
		if _, err := NewConnector(cfg); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	for _, dsn := range []string{"", "tiana://user:secret@ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test", "http://ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test", "tiana://ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test?token=secret"} {
		if _, err := (&Driver{}).OpenConnector(dsn); err == nil {
			t.Fatal("unsafe DSN accepted")
		}
	}
	if _, err := (&Driver{}).OpenConnector("tiana://ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	connector, err := NewConnector(Config{Gateway: tiana.Config{Endpoint: "ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = connector.Connect(ctx); err == nil {
		t.Fatal("canceled context accepted")
	}
}

func TestConnectorSnapshotsToken(t *testing.T) {
	cfg, _ := gatewayFixture(t, func(r io.Reader, w io.Writer) {
		reader := bufio.NewReader(r)
		for {
			req, err := readPeer(reader)
			if err != nil {
				return
			}
			writeReply(w, replyBody(req, true))
		}
	})
	connector, err := NewConnector(cfg)
	if err != nil {
		t.Fatal(err)
	}
	*cfg.Gateway.Token = tiana.Token{}
	connection, err := connector.Connect(context.Background())
	if err != nil {
		t.Fatalf("caller token mutation changed connector: %v", err)
	}
	defer connection.Close()
	if err = connection.(driver.Pinger).Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
}

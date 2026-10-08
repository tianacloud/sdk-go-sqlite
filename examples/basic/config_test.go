package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestGatewayConfigPreservesIdentityAndUsesExplicitPEM(t *testing.T) {
	const endpoint = "ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test"
	t.Setenv("TIANA_ENDPOINT", endpoint)
	t.Setenv("TIANA_TOKEN", "synthetic-opaque-token")
	t.Setenv("TIANA_GATEWAY_ADDRESS", "127.0.0.1:8443")
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	cert := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{endpoint}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour)}
	der, err := x509.CreateCertificate(rand.Reader, cert, cert, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("TIANA_CA_FILE", path)
	cfg, err := gatewayConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Endpoint != endpoint || cfg.DialAddress != "127.0.0.1:8443" || cfg.RootCAs == nil {
		t.Fatal("explicit transport settings were not preserved")
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := parsed.Verify(x509.VerifyOptions{Roots: cfg.RootCAs, DNSName: endpoint}); err != nil {
		t.Fatal(err)
	}
	if _, err := parsed.Verify(x509.VerifyOptions{Roots: cfg.RootCAs, DNSName: "wrong.example.test"}); err == nil {
		t.Fatal("wrong hostname accepted")
	}
}

func TestGatewayConfigRejectsUnreadableAndInvalidCAWithoutLeakingPath(t *testing.T) {
	t.Setenv("TIANA_TOKEN", "synthetic-opaque-token")
	for _, contents := range []string{"", "not PEM"} {
		path := filepath.Join(t.TempDir(), "private-ca-path.pem")
		if err := os.WriteFile(path, []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("TIANA_CA_FILE", path)
		_, err := gatewayConfig()
		if err == nil {
			t.Fatal("invalid CA accepted")
		}
		if strings.Contains(err.Error(), path) {
			t.Fatal("CA path leaked")
		}
	}
	t.Setenv("TIANA_CA_FILE", filepath.Join(t.TempDir(), "missing-secret-path.pem"))
	if _, err := gatewayConfig(); err == nil || strings.Contains(err.Error(), "secret-path") {
		t.Fatal("unreadable CA must fail with bounded diagnostics")
	}
	t.Setenv("TIANA_CA_FILE", "")
	if _, err := gatewayConfig(); err == nil {
		t.Fatal("explicit empty CA path accepted")
	}
}

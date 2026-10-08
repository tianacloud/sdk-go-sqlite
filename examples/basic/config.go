package main

import (
	"crypto/x509"
	"errors"
	tiana "github.com/tianacloud/sdk-go"
	"os"
)

func gatewayConfig() (tiana.Config, error) {
	cfg := tiana.Config{Endpoint: os.Getenv("TIANA_ENDPOINT")}
	if raw, ok := os.LookupEnv("TIANA_TOKEN"); ok {
		token, err := tiana.NewToken(raw)
		if err != nil {
			return tiana.Config{}, err
		}
		cfg.Token = token
	}
	if address, ok := os.LookupEnv("TIANA_GATEWAY_ADDRESS"); ok {
		if address == "" {
			return tiana.Config{}, errors.New("TIANA_GATEWAY_ADDRESS must be host:port")
		}
		cfg.DialAddress = address
	}
	if path, ok := os.LookupEnv("TIANA_CA_FILE"); ok {
		certs, err := os.ReadFile(path)
		if err != nil {
			return tiana.Config{}, errors.New("cannot read TIANA_CA_FILE")
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(certs) {
			return tiana.Config{}, errors.New("TIANA_CA_FILE must contain a PEM certificate")
		}
		cfg.RootCAs = roots
	}
	return cfg, nil
}

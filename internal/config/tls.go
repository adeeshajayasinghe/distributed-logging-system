package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// Based on the TLSConfig this returns server tls configs or client tlf configs
func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(
			cfg.CertFile,
			cfg.KeyFile,
		)
		if err != nil {
			return nil, err
		}
	}
	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf(
				"failed to parse root certificate: %q",
				cfg.CAFile,
			)
		}
		if cfg.Server {
			// Returns server tls configurations
			// Verify client's certificate 
			tlsConfig.ClientCAs = ca
 			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			// Returns client tls configurations
			// Verify server's certificate 
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}

type TLSConfig struct {
	CertFile string
	KeyFile string
	CAFile string
	ServerAddress string
	Server bool
}
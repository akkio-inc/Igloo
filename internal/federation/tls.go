package federation

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
)

// certPool parses PEM certificates.
func certPool(pem string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM([]byte(pem)) {
		return nil, errors.New("tls_ca holds no PEM certificate")
	}
	return pool, nil
}

// tlsConfig returns the TLS settings for reaching c's server, or nil for
// TLSDisable. The modes mean what libpq's sslmode values mean: prefer and
// require encrypt without checking the certificate (unless tls_ca is set,
// which makes require check the chain, as libpq does with a root
// certificate), verify-ca checks it was signed by a trusted authority, and
// verify-full also that it was issued for the host. Whether prefer falls
// back to an unencrypted connection is up to the caller.
func tlsConfig(c *Connection) (*tls.Config, error) {
	if c.TLS == TLSDisable {
		return nil, nil
	}
	cfg := &tls.Config{ServerName: c.Host, MinVersion: tls.VersionTLS12}
	var roots *x509.CertPool
	if c.TLSCA != "" {
		var err error
		if roots, err = certPool(c.TLSCA); err != nil {
			return nil, err
		}
	}
	switch {
	case c.TLS == TLSVerifyFull:
		cfg.RootCAs = roots
	case c.TLS == TLSVerifyCA || roots != nil && c.TLS == TLSRequire:
		// Check the chain but not the name. crypto/tls does both or
		// neither, so the chain is checked here.
		cfg.InsecureSkipVerify = true //nolint:gosec // verified below
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("the server sent no certificate")
			}
			opts := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
			for _, cert := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(cert)
			}
			if _, err := cs.PeerCertificates[0].Verify(opts); err != nil {
				return fmt.Errorf("the server's certificate: %w", err)
			}
			return nil
		}
	default:
		// prefer and require: encrypted, but anyone may be at the other
		// end, as with libpq.
		cfg.InsecureSkipVerify = true //nolint:gosec // the mode asks for no verification
	}
	return cfg, nil
}

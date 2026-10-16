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
// TLSDisable. verify-full, the default, checks that the server's
// certificate was signed by an authority of tls_ca, or of the system's when
// it is empty, and issued for the host. require and verify-ca check only
// the signature: unlike libpq, require never takes any certificate, as if
// the system's roots were always its root certificates. prefer, an
// explicit choice, encrypts without checking anything, and whether it
// falls back to an unencrypted connection is up to the caller.
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
	case c.TLS == TLSVerifyCA || c.TLS == TLSRequire:
		// Check the chain, against the system's roots when roots is nil,
		// but not the name. crypto/tls does both or neither, so the chain
		// is checked here.
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
		// prefer: encrypted, but anyone may be at the other end, as with
		// libpq. Choosing it draws a warning (Connection.TLSWarning).
		cfg.InsecureSkipVerify = true //nolint:gosec // the mode asks for no verification
	}
	return cfg, nil
}

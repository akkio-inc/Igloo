package distributed

import (
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"time"
)

// Coordinators and workers authenticate each other, and encrypt what they
// send, with mutual TLS. Both sides derive the same key pair from a shared
// secret (IGLOO_WORKER_SECRET) and accept a peer only if it proves it
// holds that key in the TLS handshake. There is no certificate authority
// to run and nothing to rotate but the secret; anyone without the secret,
// SQL users included, cannot open a connection to a worker, and a machine
// pretending to be a worker cannot hand the coordinator results.

// MinSecretLength is the shortest worker secret Igloo accepts.
const MinSecretLength = 32

// transport holds the TLS settings for both ends.
type transport struct {
	server, client *tls.Config
}

// WorkerTLS returns the TLS settings of both ends for secret, for what stands
// between them, as in tests.
func WorkerTLS(secret string) (server, client *tls.Config, err error) {
	t, err := newTransport(secret)
	if err != nil {
		return nil, nil, err
	}
	return t.server, t.client, nil
}

func newTransport(secret string) (*transport, error) {
	if len(secret) < MinSecretLength {
		return nil, fmt.Errorf("the worker secret must be at least %d characters", MinSecretLength)
	}
	seed, err := hkdf.Key(sha256.New, []byte(secret), nil, "igloo distributed execution tls v1", ed25519.SeedSize)
	if err != nil {
		return nil, err
	}
	key := ed25519.NewKeyFromSeed(seed)
	pub := key.Public().(ed25519.PublicKey)

	// The certificate only carries the key; peers check the key, not the
	// certificate's other fields.
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "igloo-worker"},
		DNSNames:              []string{"igloo-worker"},
		NotBefore:             time.Unix(0, 0),
		NotAfter:              time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	if err != nil {
		return nil, err
	}
	cert := tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}

	// The handshake proves the peer holds the private key of the
	// certificate it sent; this checks that the key is the shared one.
	verify := func(raw [][]byte, _ [][]*x509.Certificate) error {
		if len(raw) == 0 {
			return errors.New("peer sent no certificate")
		}
		c, err := x509.ParseCertificate(raw[0])
		if err != nil {
			return err
		}
		// The key is public, but compared in constant time all the same.
		if k, ok := c.PublicKey.(ed25519.PublicKey); !ok || subtle.ConstantTimeCompare(k, pub) != 1 {
			return errors.New("peer does not hold the worker secret")
		}
		return nil
	}
	return &transport{
		server: &tls.Config{
			Certificates:          []tls.Certificate{cert},
			ClientAuth:            tls.RequireAnyClientCert,
			VerifyPeerCertificate: verify,
			MinVersion:            tls.VersionTLS13,
		},
		client: &tls.Config{
			Certificates: []tls.Certificate{cert},
			// The chain and host name mean nothing here; verify checks the
			// key instead.
			InsecureSkipVerify:    true, //nolint:gosec
			VerifyPeerCertificate: verify,
			MinVersion:            tls.VersionTLS13,
		},
	}, nil
}

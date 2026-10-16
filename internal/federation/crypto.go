package federation

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
)

// KeyName names the key in igloo_auth.secrets that encrypts stored
// connection passwords. Every replica shares it, like the console's
// session key.
const KeyName = "connection-credentials"

// sealer encrypts stored passwords with AES-256-GCM. The connection's name
// is bound in as associated data, so a password copied to another
// connection's row does not decrypt there.
type sealer struct{ aead cipher.AEAD }

func newSealer(secret []byte) (*sealer, error) {
	if len(secret) < 32 {
		return nil, errors.New("federation: the credentials key must be at least 32 bytes")
	}
	key, err := hkdf.Key(sha256.New, secret, nil, "igloo connection password v1", 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	return &sealer{aead: aead}, nil
}

// seal returns the nonce followed by the ciphertext.
func (s *sealer) seal(name, password string) []byte {
	nonce := make([]byte, s.aead.NonceSize(), s.aead.NonceSize()+len(password)+s.aead.Overhead())
	_, _ = rand.Read(nonce)
	return s.aead.Seal(nonce, nonce, []byte(password), []byte(name))
}

func (s *sealer) open(name string, sealed []byte) (string, error) {
	n := s.aead.NonceSize()
	if len(sealed) < n {
		return "", errors.New("stored password is damaged")
	}
	plain, err := s.aead.Open(nil, sealed[:n], sealed[n:], []byte(name))
	if err != nil {
		return "", errors.New("stored password cannot be decrypted with this deployment's key")
	}
	return string(plain), nil
}

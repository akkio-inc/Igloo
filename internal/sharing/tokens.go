package sharing

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

// Recipients authenticate with bearer tokens, and receive them through
// one-time activation links. Both are an ID and a secret: Igloo stores the
// ID and the secret's SHA-256 hash, finds the stored hash by ID and compares
// it in constant time. The secrets are 256 random bits, so a fast hash is
// enough; nothing about them can be guessed from the stored hash.
const (
	tokenPrefix      = "dst_"
	activationPrefix = "dsa_"
	tokenIDBytes     = 12
	tokenSecretBytes = 32
)

// Token kinds.
const (
	kindBearer     = "bearer"
	kindActivation = "activation"
)

// secretToken is a newly made token or activation code; Text is shown once.
type secretToken struct {
	ID   string
	Hash []byte
	Text string
}

func newToken(prefix string) (secretToken, error) {
	id := make([]byte, tokenIDBytes)
	secret := make([]byte, tokenSecretBytes)
	if _, err := rand.Read(id); err != nil {
		return secretToken{}, err
	}
	if _, err := rand.Read(secret); err != nil {
		return secretToken{}, err
	}
	encoded := base64.RawURLEncoding.EncodeToString(secret)
	h := sha256.Sum256([]byte(encoded))
	return secretToken{
		ID:   hex.EncodeToString(id),
		Hash: h[:],
		Text: prefix + hex.EncodeToString(id) + "_" + encoded,
	}, nil
}

// parseToken splits a token or activation code with the given prefix into
// its ID and the hash of its secret.
func parseToken(prefix, text string) (id string, hash []byte, ok bool) {
	rest, ok := strings.CutPrefix(text, prefix)
	if !ok || len(rest) != 2*tokenIDBytes+1+base64.RawURLEncoding.EncodedLen(tokenSecretBytes) || rest[2*tokenIDBytes] != '_' {
		return "", nil, false
	}
	id, secret := rest[:2*tokenIDBytes], rest[2*tokenIDBytes+1:]
	if _, err := hex.DecodeString(id); err != nil {
		return "", nil, false
	}
	if _, err := base64.RawURLEncoding.Strict().DecodeString(secret); err != nil {
		return "", nil, false
	}
	h := sha256.Sum256([]byte(secret))
	return id, h[:], true
}

// sameHash compares two token hashes in constant time.
func sameHash(a, b []byte) bool {
	return len(a) == len(b) && subtle.ConstantTimeCompare(a, b) == 1
}

// sealer encrypts and authenticates small values with AES-256-GCM under a
// key every replica shares: the signed file URLs recipients download from,
// and providers' bearer tokens at rest.
type sealer struct {
	aead    cipher.AEAD
	purpose []byte
}

// newSealer derives a key for purpose from secret, which must be at least 32
// random bytes.
func newSealer(secret []byte, purpose string) (*sealer, error) {
	if len(secret) < 32 {
		return nil, errors.New("sharing: the key must be at least 32 bytes")
	}
	key, err := hkdf.Key(sha256.New, secret, nil, "igloo delta sharing "+purpose, 32)
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
	return &sealer{aead: aead, purpose: []byte(purpose)}, nil
}

func (s *sealer) seal(plain []byte) ([]byte, error) {
	nonce := make([]byte, s.aead.NonceSize(), s.aead.NonceSize()+len(plain)+s.aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return s.aead.Seal(nonce, nonce, plain, s.purpose), nil
}

func (s *sealer) open(sealed []byte) ([]byte, error) {
	n := s.aead.NonceSize()
	if len(sealed) < n+s.aead.Overhead() {
		return nil, errors.New("malformed sealed value")
	}
	plain, err := s.aead.Open(nil, sealed[:n], sealed[n:], s.purpose)
	if err != nil {
		return nil, errors.New("sealed value does not verify")
	}
	return plain, nil
}

// fileGrant is what a signed file URL carries: one data file, for one
// recipient token, until a deadline. It is encrypted, so the URL reveals
// neither the bucket nor the path.
type fileGrant struct {
	// Token is the ID of the bearer token the URL was issued to; the URL
	// stops working when the token does.
	Token string `json:"t"`
	// Path is the data file's location in object storage.
	Path string `json:"p"`
	Size int64  `json:"s"`
	// Expires is a Unix time in seconds.
	Expires int64 `json:"e"`
	// Table names the shared table, for logs: share.schema.table.
	Table string `json:"n"`
}

func (s *sealer) sealFile(g fileGrant) (string, error) {
	plain, err := json.Marshal(g)
	if err != nil {
		return "", err
	}
	sealed, err := s.seal(plain)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(sealed), nil
}

// openFile decodes a signed file URL's last path segment, refusing ones that
// do not verify or have expired.
func (s *sealer) openFile(text string, now time.Time) (fileGrant, error) {
	var g fileGrant
	raw, err := base64.RawURLEncoding.Strict().DecodeString(text)
	if err != nil {
		return g, errors.New("malformed file URL")
	}
	plain, err := s.open(raw)
	if err != nil {
		return g, err
	}
	if err := json.Unmarshal(plain, &g); err != nil {
		return g, err
	}
	if now.Unix() >= g.Expires {
		return g, errors.New("the file URL has expired; list the table's files again")
	}
	return g, nil
}

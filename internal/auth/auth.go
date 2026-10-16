// Package auth holds Igloo's users, their roles, and the SCRAM-SHA-256
// password verifiers clients authenticate against. Passwords themselves are
// never stored.
package auth

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/xdg-go/scram"
)

// Role is a user's built-in role, which decides what they may do across the
// whole warehouse. Grants (see Grant) add to it.
type Role string

const (
	// RoleAdmin may do anything, including managing users.
	RoleAdmin Role = "admin"
	// RoleWriter may read and change data and create or drop tables.
	RoleWriter Role = "writer"
	// RoleReader may only read.
	RoleReader Role = "reader"
	// RoleNone may do nothing across the warehouse: only what grants allow,
	// and reading the system catalogs.
	RoleNone Role = "none"
)

// ParseRole validates a role name.
func ParseRole(s string) (Role, error) {
	switch r := Role(strings.ToLower(s)); r {
	case RoleAdmin, RoleWriter, RoleReader, RoleNone:
		return r, nil
	}
	return "", fmt.Errorf("unknown role %q; use admin, writer, reader or none", s)
}

// IsBuiltinRole reports whether name is a built-in role, which custom roles
// may not be named after.
func IsBuiltinRole(name string) bool {
	_, err := ParseRole(name)
	return err == nil
}

// User is an account clients log in as.
type User struct {
	Name string
	Role Role
	// Verifier is a SCRAM-SHA-256 verifier in PostgreSQL's format.
	Verifier  string
	CreatedAt time.Time
}

var (
	ErrNotFound = errors.New("user does not exist")
	ErrExists   = errors.New("user already exists")
	// ErrNameTaken is returned when a user would get the name of a custom
	// role, or a custom role the name of a user: grants name either.
	ErrNameTaken = errors.New("a user or role of that name already exists")
	// ErrReadOnly is returned by stores that cannot change users.
	ErrReadOnly = errors.New("user management needs IGLOO_METADATA_DATABASE_URL")
	// ErrLastAdmin prevents locking everyone out.
	ErrLastAdmin = errors.New("cannot remove the last admin")
)

// scramIterations matches PostgreSQL's default.
const scramIterations = 4096

const verifierPrefix = "SCRAM-SHA-256$"

// NewVerifier derives a SCRAM-SHA-256 verifier for password, in the same
// format PostgreSQL stores: SCRAM-SHA-256$iter:salt$StoredKey:ServerKey.
func NewVerifier(password string) (string, error) {
	if password == "" {
		return "", errors.New("password must not be empty")
	}
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}

	client, err := scram.SHA256.NewClient("", password, "")
	if err != nil {
		return "", fmt.Errorf("password: %w", err)
	}
	creds, err := client.GetStoredCredentialsWithError(scram.KeyFactors{Salt: string(salt), Iters: scramIterations})
	if err != nil {
		return "", err
	}

	enc := base64.StdEncoding.EncodeToString
	return fmt.Sprintf("%s%d:%s$%s:%s", verifierPrefix, scramIterations,
		enc(salt), enc(creds.StoredKey), enc(creds.ServerKey)), nil
}

// IsVerifier reports whether s is a SCRAM-SHA-256 verifier rather than a
// plain password. psql's \password sends verifiers it computed itself.
func IsVerifier(s string) bool {
	_, err := ParseVerifier(s)
	return err == nil
}

// ParseVerifier decodes a verifier into the credentials a SCRAM server needs.
func ParseVerifier(v string) (scram.StoredCredentials, error) {
	var creds scram.StoredCredentials
	bad := errors.New("not a SCRAM-SHA-256 verifier")

	rest, ok := strings.CutPrefix(v, verifierPrefix)
	if !ok {
		return creds, bad
	}
	factors, keys, ok := strings.Cut(rest, "$")
	if !ok {
		return creds, bad
	}
	iters, salt, ok := strings.Cut(factors, ":")
	if !ok {
		return creds, bad
	}
	storedKey, serverKey, ok := strings.Cut(keys, ":")
	if !ok {
		return creds, bad
	}

	n, err := strconv.Atoi(iters)
	if err != nil || n < 1 {
		return creds, bad
	}
	dec := base64.StdEncoding.DecodeString
	saltBytes, err1 := dec(salt)
	stored, err2 := dec(storedKey)
	server, err3 := dec(serverKey)
	if err1 != nil || err2 != nil || err3 != nil || len(stored) != 32 || len(server) != 32 {
		return creds, bad
	}

	creds.KeyFactors = scram.KeyFactors{Salt: string(saltBytes), Iters: n}
	creds.StoredKey, creds.ServerKey = stored, server
	return creds, nil
}

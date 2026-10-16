// Package federation lets Igloo query other databases where they are. An
// admin registers a connection to a PostgreSQL or MySQL database with
// CREATE CONNECTION, and every replica attaches it, read-only, as a catalog
// of the same name, so that crm.public.customers can be joined with Iceberg
// tables without copying it first.
//
// DuckDB's postgres and mysql extensions do the reading and push filters
// and column lists down to the remote database. They never see the remote
// credentials: each connection is attached through a proxy on the loopback
// interface (proxy.go), which signs in to the remote database with the
// connection's current settings whenever DuckDB opens a connection to it.
// That is how a connection's host and credentials change without a
// restart, how a remote database that stops answering is cut off, and why
// the credentials appear nowhere DuckDB could show them.
//
// DuckDB attaches databases only until Igloo locks down its sandbox at
// startup (see engine.lockDown), so a replica attaches the connections that
// exist when it starts; one created later is attached when each replica
// restarts. Dropping a connection detaches it at once.
//
// Who may read what is decided in package pgwire, from grants on the
// connection, its schemas and its tables.
package federation

import (
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/akkio-inc/igloo/internal/config"
)

// Type is the kind of database a connection reaches.
type Type string

const (
	Postgres Type = "postgres"
	MySQL    Type = "mysql"
)

// Types are the connection types Igloo supports, in the order docs list
// them.
var Types = []Type{Postgres, MySQL}

// ParseType validates a connection type.
func ParseType(s string) (Type, error) {
	switch t := Type(strings.ToLower(s)); t {
	case Postgres, "postgresql":
		return Postgres, nil
	case MySQL, "mariadb":
		return MySQL, nil
	}
	return "", fmt.Errorf("unsupported connection type %q; use postgres or mysql", s)
}

// defaultPort is the usual port of each type.
func (t Type) defaultPort() int {
	if t == MySQL {
		return 3306
	}
	return 5432
}

// TLSMode says whether and how to encrypt the connection to the remote
// database, with libpq's names: disable, prefer, require, verify-ca and
// verify-full (the default). Only verify-full makes sure the server is the
// one named by host; see tlsConfig and Warning.
type TLSMode string

const (
	TLSDisable    TLSMode = "disable"
	TLSPrefer     TLSMode = "prefer"
	TLSRequire    TLSMode = "require"
	TLSVerifyCA   TLSMode = "verify-ca"
	TLSVerifyFull TLSMode = "verify-full"
)

// TLSWarning says what c's TLS mode leaves open, for a connection that
// does not make sure its server is the one named by host, or is "" for
// verify-full.
func (c Connection) TLSWarning() string {
	const use = "; use tls 'verify-full', with tls_ca if the server's certificate is not signed by an authority the system trusts"
	switch c.TLS {
	case TLSDisable:
		return fmt.Sprintf("connection %s does not use TLS: its password and data cross the network unencrypted%s", c.Name, use)
	case TLSPrefer:
		return fmt.Sprintf("connection %s does not check the server's certificate, and goes on without TLS if the server offers none: "+
			"anyone on the network path could pose as the server and read its password and data%s", c.Name, use)
	case TLSRequire, TLSVerifyCA:
		return fmt.Sprintf("connection %s checks that the server's certificate is signed by a trusted authority, but not that it was issued "+
			"for %s: a server with another certificate from that authority could pose as it%s", c.Name, c.Host, use)
	}
	return ""
}

// parseTLSMode accepts libpq's sslmode names and MySQL's ssl_mode ones.
func parseTLSMode(s string) (TLSMode, error) {
	switch strings.ToLower(strings.ReplaceAll(s, "_", "-")) {
	case "disable", "disabled", "false", "off":
		return TLSDisable, nil
	case "prefer", "preferred", "allow":
		return TLSPrefer, nil
	case "require", "required", "true", "on":
		return TLSRequire, nil
	case "verify-ca":
		return TLSVerifyCA, nil
	case "verify-full", "verify-identity":
		return TLSVerifyFull, nil
	}
	return "", fmt.Errorf("unknown tls mode %q; use disable, prefer, require, verify-ca or verify-full", s)
}

const (
	defaultConnectTimeout = 10 * time.Second
	defaultQueryTimeout   = 5 * time.Minute
	maxConnectTimeout     = 5 * time.Minute
)

// Connection is a registered connection to another database.
type Connection struct {
	// Name is what the database is called in SQL: its catalog name.
	Name string
	Type Type
	Host string
	Port int
	// Database is the remote database. For MySQL it is the default schema.
	Database string
	User     string
	// Password is the password Igloo stores, encrypted; empty when there is
	// none or PasswordSecret names one.
	Password string
	// PasswordSecret names a file in IGLOO_CONNECTION_SECRETS_DIR holding
	// the password, read again at every sign-in, so rotating it there needs
	// no change in Igloo.
	PasswordSecret string
	TLS            TLSMode
	// TLSCA holds PEM certificates of the authorities that may sign the
	// remote server's certificate; empty means the system's.
	TLSCA string
	// Schemas limits which remote schemas are attached; empty means all.
	Schemas []string
	// ConnectTimeout bounds connecting and signing in to the remote
	// database.
	ConnectTimeout time.Duration
	// QueryTimeout is how long the remote database may work on a statement,
	// and how long it may leave a statement unanswered before Igloo gives
	// up on it. Zero means no limit.
	QueryTimeout time.Duration

	// Unusable says why Igloo cannot sign in with this connection, such as
	// a stored password that does not decrypt; empty normally.
	Unusable string

	// ID is a random identifier, new for every CREATE, so a replica can
	// tell a dropped and recreated connection from the one it attached.
	ID        string
	CreatedBy string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// New returns a connection called name of type t with the defaults set.
func New(name string, t Type) Connection {
	return Connection{
		Name: name, Type: t, Port: t.defaultPort(), TLS: TLSVerifyFull,
		ConnectTimeout: defaultConnectTimeout, QueryTimeout: defaultQueryTimeout,
	}
}

// Options are the names CREATE and ALTER CONNECTION accept in WITH (...),
// with the libpq and MySQL spellings people are used to. readOnly is only
// accepted as true.
var optionNames = map[string]string{
	"host": "host", "port": "port",
	"database": "database", "dbname": "database", "db": "database",
	"user": "user", "username": "user",
	"password": "password", "password_secret": "password_secret",
	"tls": "tls", "sslmode": "tls", "ssl_mode": "tls",
	"tls_ca": "tls_ca",
	"schema": "schema", "schemas": "schema",
	"connect_timeout": "connect_timeout", "query_timeout": "query_timeout",
	"read_only": "read_only",
}

// liveOptions take effect on a replica at once. The others change what
// DuckDB attached, so a replica applies them when it restarts.
var liveOptions = []string{"host", "port", "user", "password", "password_secret", "tls", "tls_ca",
	"connect_timeout", "query_timeout", "read_only"}

// CanonicalOption returns the option a name stands for, or "".
func CanonicalOption(name string) string { return optionNames[strings.ToLower(name)] }

// IsLiveOption reports whether a change to option applies without a
// restart.
func IsLiveOption(option string) bool { return slices.Contains(liveOptions, option) }

// Set applies one option. The value is as written in SQL: the text of a
// string literal, or a number or word.
func (c *Connection) Set(name, value string) error {
	option := CanonicalOption(name)
	switch option {
	case "host":
		c.Host = value
	case "port":
		p, err := strconv.Atoi(value)
		if err != nil || p < 1 || p > 65535 {
			return fmt.Errorf("port must be a number from 1 to 65535, not %q", value)
		}
		c.Port = p
	case "database":
		c.Database = value
	case "user":
		c.User = value
	case "password":
		c.Password, c.PasswordSecret = value, ""
	case "password_secret":
		c.PasswordSecret, c.Password = value, ""
	case "tls":
		m, err := parseTLSMode(value)
		if err != nil {
			return err
		}
		c.TLS = m
	case "tls_ca":
		c.TLSCA = value
	case "schema":
		c.Schemas = nil
		for _, s := range strings.Split(value, ",") {
			if s = strings.TrimSpace(s); s != "" {
				c.Schemas = append(c.Schemas, s)
			}
		}
	case "connect_timeout", "query_timeout":
		d, err := parseDuration(value)
		if err != nil {
			return fmt.Errorf("%s: %w", option, err)
		}
		if option == "connect_timeout" {
			c.ConnectTimeout = d
		} else {
			c.QueryTimeout = d
		}
	case "read_only":
		if b, err := strconv.ParseBool(value); err != nil || !b {
			return errors.New("connections are read-only; read_only must be true")
		}
	default:
		return fmt.Errorf("unknown connection option %q; use host, port, database, user, password, password_secret, tls, tls_ca, schema, connect_timeout or query_timeout", name)
	}
	return nil
}

// parseDuration reads a Go duration such as 30s or 5m, or whole seconds.
func parseDuration(s string) (time.Duration, error) {
	if n, err := strconv.Atoi(s); err == nil && n >= 0 {
		return time.Duration(n) * time.Second, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil || d < 0 {
		return 0, fmt.Errorf("expected a duration such as 30s or 5m, not %q", s)
	}
	return d, nil
}

// reservedNames are catalog names DuckDB or Igloo use themselves.
var reservedNames = []string{"system", "temp", "main", "memory", "information_schema", "pg_catalog", "igloo"}

// Validate checks a connection before it is stored. taken lists the names
// of the databases DuckDB has already, which a connection may not reuse.
func (c *Connection) Validate(taken []string) error {
	switch {
	case !config.ValidName(c.Name):
		return fmt.Errorf("invalid connection name %q: use lowercase letters, digits and underscores", c.Name)
	case slices.Contains(reservedNames, c.Name) || slices.ContainsFunc(taken, func(t string) bool { return strings.EqualFold(t, c.Name) }):
		return fmt.Errorf("%q is already the name of a database; choose another name", c.Name)
	case c.Type != Postgres && c.Type != MySQL:
		return fmt.Errorf("unsupported connection type %q", c.Type)
	case c.Host == "":
		return errors.New("a connection needs a host")
	case !validHost(c.Host):
		return fmt.Errorf("invalid host %q", c.Host)
	case c.Database == "":
		return errors.New("a connection needs a database")
	case c.User == "":
		return errors.New("a connection needs a user")
	case c.PasswordSecret != "" && !validSecretName(c.PasswordSecret):
		return fmt.Errorf("invalid password_secret %q: use the name of a file in IGLOO_CONNECTION_SECRETS_DIR, with letters, digits, dots, dashes and underscores", c.PasswordSecret)
	case c.ConnectTimeout <= 0 || c.ConnectTimeout > maxConnectTimeout:
		return fmt.Errorf("connect_timeout must be more than 0 and at most %s", maxConnectTimeout)
	case c.TLS == TLSDisable && c.TLSCA != "":
		return errors.New("tls_ca needs a tls mode other than disable")
	}
	for _, s := range []string{c.Database, c.User} {
		if strings.ContainsFunc(s, unicode.IsControl) {
			return errors.New("database and user names may not contain control characters")
		}
	}
	if c.TLSCA != "" {
		if _, err := certPool(c.TLSCA); err != nil {
			return err
		}
	}
	return nil
}

// validHost accepts host names and IP addresses, nothing with spaces,
// slashes or other characters a connection string would treat specially.
func validHost(h string) bool {
	if net.ParseIP(h) != nil {
		return true
	}
	if len(h) > 253 {
		return false
	}
	for _, r := range h {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '.' || r == '_') {
			return false
		}
	}
	return true
}

// validSecretName keeps password_secret a plain file name inside the
// secrets directory: no separators, and not . or ...
func validSecretName(s string) bool {
	if s == "" || s == "." || s == ".." || len(s) > 255 {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '.' || r == '_') {
			return false
		}
	}
	return true
}

// Address is host:port.
func (c *Connection) Address() string {
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}

// PasswordSource describes where the password comes from, for SHOW
// CONNECTIONS: never the password itself.
func (c *Connection) PasswordSource() string {
	switch {
	case c.PasswordSecret != "":
		return "secret " + c.PasswordSecret
	case c.Password != "":
		return "stored"
	}
	return "none"
}

// attachedFields are the settings fixed when a replica attaches the
// connection; see liveOptions.
func (c *Connection) attachedFields() string {
	return strings.Join([]string{string(c.Type), c.Database, strings.Join(c.Schemas, ",")}, "\x00")
}

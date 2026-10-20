package federation

import (
	"context"
	"crypto/rsa"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
	sf "github.com/snowflakedb/gosnowflake/v2"
	"github.com/snowflakedb/gosnowflake/v2/arrowbatches"
)

// Snowflake connections: their settings, signing in with Snowflake's Go
// driver, and running the queries the gateway (sfgateway.go) makes.
//
// Igloo never runs SQL written by a user on Snowflake. The gateway builds
// every query itself, from the scans DuckDB plans: a SELECT of named
// columns of one table, with filters and a limit, so connections are
// read-only by construction, and the credentials (a key pair's private key
// or a password) stay in Go.

// snowflakeDomain is where Snowflake accounts are served, as
// <account>.snowflakecomputing.com.
const snowflakeDomain = ".snowflakecomputing.com"

// snowflakeHost is the host of account, as Snowflake's driver derives it.
func snowflakeHost(account string) string {
	return strings.ToLower(account) + snowflakeDomain
}

// validAccount accepts Snowflake account identifiers: orgname-accountname,
// or an account locator with its region and cloud (xy12345.us-east-2.aws).
var validAccount = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,254}$`).MatchString

// validateSnowflake checks a Snowflake connection's settings.
func (c *Connection) validateSnowflake() error {
	creds := 0
	for _, s := range []string{c.Password, c.PasswordSecret, c.PrivateKey, c.PrivateKeySecret} {
		if s != "" {
			creds++
		}
	}
	switch {
	case c.Account == "":
		return errors.New("a Snowflake connection needs an account, such as account 'myorg-myaccount'")
	case !validAccount(c.Account):
		return fmt.Errorf("invalid Snowflake account %q: use the account identifier, such as myorg-myaccount", c.Account)
	case c.Host != "" && !validHost(c.Host):
		return fmt.Errorf("invalid host %q", c.Host)
	case c.Database == "":
		return errors.New("a Snowflake connection needs a database")
	case c.User == "":
		return errors.New("a Snowflake connection needs a user")
	case creds != 1:
		return errors.New("a Snowflake connection needs one of private_key_secret, private_key, password_secret or password")
	case c.PasswordSecret != "" && !validSecretName(c.PasswordSecret):
		return fmt.Errorf("invalid password_secret %q: use the name of a file in IGLOO_CONNECTION_SECRETS_DIR, with letters, digits, dots, dashes and underscores", c.PasswordSecret)
	case c.PrivateKeySecret != "" && !validSecretName(c.PrivateKeySecret):
		return fmt.Errorf("invalid private_key_secret %q: use the name of a file in IGLOO_CONNECTION_SECRETS_DIR, with letters, digits, dots, dashes and underscores", c.PrivateKeySecret)
	case c.TLS != TLSVerifyFull || c.TLSCA != "":
		return errors.New("Snowflake connections always use TLS and verify Snowflake's certificate; tls and tls_ca cannot be set")
	case c.ConnectTimeout <= 0 || c.ConnectTimeout > maxConnectTimeout:
		return fmt.Errorf("connect_timeout must be more than 0 and at most %s", maxConnectTimeout)
	}
	names := append([]string{c.Database, c.User, c.Warehouse, c.Role}, c.Schemas...)
	for _, s := range names {
		if strings.ContainsFunc(s, unicode.IsControl) {
			return errors.New("database, user, warehouse, role and schema names may not contain control characters")
		}
	}
	if c.PrivateKey != "" {
		if _, err := parsePrivateKey(c.PrivateKey); err != nil {
			return err
		}
	}
	return nil
}

// parsePrivateKey reads an RSA private key in PEM, as PKCS #8 or PKCS #1.
// Errors never quote the key.
func parsePrivateKey(text string) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode([]byte(strings.TrimSpace(text)))
	if block == nil {
		return nil, errors.New("private key: expected an RSA private key in PEM, beginning with -----BEGIN PRIVATE KEY-----")
	}
	switch block.Type {
	case "ENCRYPTED PRIVATE KEY":
		return nil, errors.New("private key: encrypted private keys are not supported; give Igloo the key unencrypted, " +
			"ideally in a file of IGLOO_CONNECTION_SECRETS_DIR named by private_key_secret")
	case "RSA PRIVATE KEY":
		k, err := x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return nil, errors.New("private key: not a valid PKCS #1 RSA private key")
		}
		return k, nil
	case "PRIVATE KEY":
		k, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, errors.New("private key: not a valid PKCS #8 private key")
		}
		rk, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("private key: Snowflake key pairs are RSA keys")
		}
		return rk, nil
	}
	return nil, fmt.Errorf("private key: expected a PRIVATE KEY in PEM, not %s", block.Type)
}

// readSecretFile reads a file of the secrets directory, without a trailing
// newline.
func readSecretFile(secrets, option, name string) (string, error) {
	if secrets == "" {
		return "", fmt.Errorf("%s needs IGLOO_CONNECTION_SECRETS_DIR", option)
	}
	if !validSecretName(name) {
		return "", fmt.Errorf("invalid %s %q", option, name)
	}
	b, err := os.ReadFile(filepath.Join(secrets, name))
	if err != nil {
		return "", fmt.Errorf("read %s %s: %w", option, name, errors.Unwrap(err))
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}

// sfIdent is the name Snowflake gives an identifier written in an option:
// upper-cased, as Snowflake reads an unquoted identifier, or as written
// inside double quotes.
func sfIdent(s string) string {
	s = strings.TrimSpace(s)
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strings.ReplaceAll(s[1:len(s)-1], `""`, `"`)
	}
	return strings.ToUpper(s)
}

// sfQuote quotes a name for Snowflake's SQL, where a quoted identifier is
// taken as written.
func sfQuote(name string) string { return `"` + strings.ReplaceAll(name, `"`, `""`) + `"` }

// sfString quotes a string literal for Snowflake's SQL, which treats a
// backslash in a literal as an escape.
func sfString(s string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), "'", "''") + "'"
}

var driverSetup sync.Once

// setUpDriver configures Snowflake's driver for a server: no logging of its
// own (its errors reach Igloo's log, scrubbed), and none of its extras that
// would reach beyond Snowflake: probing cloud metadata endpoints to tell
// which platform it runs on, and writing an embedded native library to a
// temporary directory to load it.
func setUpDriver() {
	driverSetup.Do(func() {
		_ = os.Setenv("SNOWFLAKE_DISABLE_PLATFORM_DETECTION", "true")
		_ = os.Setenv("SF_DISABLE_MINICORE", "true")
		_ = sf.GetLogger().SetLogLevel("OFF")
	})
}

// sfConnector signs in to Snowflake with a connection's settings, reading
// a secret file anew at every sign-in, so rotating it there needs no change
// in Igloo.
type sfConnector struct {
	c       Connection
	secrets string
}

func (s sfConnector) Connect(ctx context.Context) (driver.Conn, error) {
	cfg, err := snowflakeConfig(s.c, s.secrets)
	if err != nil {
		return nil, err
	}
	return sf.NewConnector(sf.SnowflakeDriver{}, cfg).Connect(ctx)
}

func (sfConnector) Driver() driver.Driver { return sf.SnowflakeDriver{} }

// snowflakeConfig is the driver's configuration for c.
func snowflakeConfig(c Connection, secrets string) (sf.Config, error) {
	if c.Unusable != "" {
		return sf.Config{}, errors.New(c.Unusable)
	}
	cfg := sf.Config{
		Account: c.Account, User: c.User, Warehouse: c.Warehouse, Role: c.Role,
		Application: "Igloo",
		// Bounds signing in, its retries included.
		LoginTimeout: c.ConnectTimeout, JWTClientTimeout: c.ConnectTimeout,
		// Bounds aborting a query whose context ended.
		CleanupTimeout: 30 * time.Second,
		// Nothing is cached outside the process.
		ClientStoreTemporaryCredential: sf.ConfigBoolFalse, ClientRequestMfaToken: sf.ConfigBoolFalse,
		DisableQueryContextCache: true,
		Params:                   snowflakeSessionParams(c),
	}
	if c.Host != "" {
		cfg.Host, cfg.Port, cfg.Protocol = c.Host, c.Port, "https"
	}
	switch {
	case c.PrivateKey != "" || c.PrivateKeySecret != "":
		text := c.PrivateKey
		if c.PrivateKeySecret != "" {
			var err error
			if text, err = readSecretFile(secrets, "private_key_secret", c.PrivateKeySecret); err != nil {
				return cfg, err
			}
		}
		key, err := parsePrivateKey(text)
		if err != nil {
			return cfg, err
		}
		cfg.Authenticator, cfg.PrivateKey = sf.AuthTypeJwt, key
	default:
		pw := c.Password
		if c.PasswordSecret != "" {
			var err error
			if pw, err = readSecretFile(secrets, "password_secret", c.PasswordSecret); err != nil {
				return cfg, err
			}
		}
		cfg.Authenticator, cfg.Password = sf.AuthTypeSnowflake, pw
	}
	return cfg, nil
}

// snowflakeSessionParams are the session parameters of Igloo's Snowflake
// sessions. Account or user settings could otherwise change what Igloo's
// queries mean: quoted identifiers that ignore case, results cut short, or
// string-to-time conversions in another format (Igloo writes formats out,
// but sets the zone too).
func snowflakeSessionParams(c Connection) map[string]*string {
	p := map[string]*string{}
	set := func(k, v string) { p[k] = &v }
	set("QUERY_TAG", "igloo connection "+c.Name)
	set("QUOTED_IDENTIFIERS_IGNORE_CASE", "FALSE")
	set("ROWS_PER_RESULTSET", "0")
	set("TIMEZONE", "UTC")
	// A query whose client went away stops rather than spending credits.
	set("ABORT_DETACHED_QUERY", "TRUE")
	if c.QueryTimeout > 0 {
		// Snowflake stops the statement itself, warehouse included.
		set("STATEMENT_TIMEOUT_IN_SECONDS", strconv.FormatInt(int64((c.QueryTimeout+time.Second-1)/time.Second), 10))
	}
	return p
}

// sfBackend is what the gateway needs of Snowflake. sfClient is the real
// one; tests use one of their own.
type sfBackend interface {
	// check signs in and makes sure the database and warehouse can be used.
	check(ctx context.Context) error
	// loadCatalog reads the database's schemas, tables and columns.
	loadCatalog(ctx context.Context, warn func(msg string, args ...any)) (*sfCatalog, error)
	// query runs a SELECT the gateway wrote (see sfClient.query).
	query(ctx context.Context, q string, timeout time.Duration) (*sfResult, error)
	close()
}

// openSnowflake opens a backend with a connection's settings.
var openSnowflake = func(c Connection, secrets string) sfBackend { return newSnowflakeClient(c, secrets) }

// sfClient is a pool of Snowflake sessions with one connection's settings.
type sfClient struct {
	c  Connection
	db *sql.DB
	// database is the connection's database as Snowflake spells it.
	database string
}

func newSnowflakeClient(c Connection, secrets string) *sfClient {
	setUpDriver()
	db := sql.OpenDB(sfConnector{c: c, secrets: secrets})
	// Signing in costs a round trip or two; keep a few sessions for the
	// next queries, but not for long.
	db.SetMaxIdleConns(4)
	db.SetConnMaxIdleTime(10 * time.Minute)
	db.SetConnMaxLifetime(time.Hour)
	return &sfClient{c: c, db: db, database: sfIdent(c.Database)}
}

func (cl *sfClient) close() { _ = cl.db.Close() }

// session takes a signed-in session from the pool, signing in within
// connect_timeout if there is none.
func (cl *sfClient) session(ctx context.Context) (*sql.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, cl.c.ConnectTimeout)
	defer cancel()
	conn, err := cl.db.Conn(ctx)
	if err != nil {
		return nil, snowflakeError("sign in", err)
	}
	return conn, nil
}

// check signs in and makes sure the database and warehouse can be used, at
// no cost: neither needs the warehouse to run.
func (cl *sfClient) check(ctx context.Context) error {
	conn, err := cl.session(ctx)
	if err != nil {
		return err
	}
	defer conn.Close() //nolint:errcheck
	ctx, cancel := context.WithTimeout(ctx, cl.c.ConnectTimeout)
	defer cancel()
	var wh, role sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT CURRENT_WAREHOUSE(), CURRENT_ROLE()").Scan(&wh, &role); err != nil {
		return snowflakeError("check the session", err)
	}
	switch {
	case cl.c.Warehouse != "" && !wh.Valid:
		return fmt.Errorf("warehouse %s does not exist, or role %s may not use it", cl.c.Warehouse, role.String)
	case !wh.Valid:
		return fmt.Errorf("user %s has no default warehouse that role %s may use; set warehouse", cl.c.User, role.String)
	}
	rows, err := conn.QueryContext(ctx, "SHOW TERSE SCHEMAS IN DATABASE "+sfQuote(cl.database)+" LIMIT 1")
	if err != nil {
		return snowflakeError("read database "+cl.database, err)
	}
	return rows.Close()
}

// sfResult is the result of a query, as Arrow batches downloaded on
// demand.
type sfResult struct {
	queryID string
	batches []sfBatch
	// done releases what the result holds.
	done func()
}

func (r *sfResult) close() {
	if r.done != nil {
		r.done()
	}
}

// sfBatch is a part of a result.
type sfBatch interface {
	// fetch downloads it, within ctx.
	fetch(ctx context.Context) ([]arrow.Record, error)
}

// driverBatch is a batch of Snowflake's driver.
type driverBatch struct{ b *arrowbatches.ArrowBatch }

func (d driverBatch) fetch(ctx context.Context) ([]arrow.Record, error) {
	recs, err := d.b.WithContext(ctx).Fetch()
	if err != nil {
		return nil, err
	}
	return *recs, nil
}

// query runs q, which must be one SELECT, and returns its result once
// Snowflake has computed it. Canceling ctx, or the query running longer
// than timeout (zero for no limit), makes the driver abort the query on
// Snowflake, which stops it using the warehouse; Snowflake's own
// STATEMENT_TIMEOUT_IN_SECONDS stops it too. The batches are downloaded
// later, each with a context of its own (see ArrowBatch.WithContext).
func (cl *sfClient) query(ctx context.Context, q string, timeout time.Duration) (*sfResult, error) {
	conn, err := cl.session(ctx)
	if err != nil {
		return nil, err
	}
	qctx, cancel := context.WithCancel(ctx)
	if timeout > 0 {
		t := time.AfterFunc(timeout, cancel)
		defer t.Stop()
	}
	qctx = arrowbatches.WithTimestampOption(sf.WithHigherPrecision(arrowbatches.WithArrowBatches(qctx)), arrowbatches.UseOriginalTimestamp)
	var rows driver.Rows
	err = conn.Raw(func(x any) error {
		var err error
		rows, err = x.(driver.QueryerContext).QueryContext(qctx, q, nil)
		return err
	})
	if err != nil {
		cancel()
		_ = conn.Close()
		if ctx.Err() == nil && qctx.Err() != nil {
			return nil, fmt.Errorf("the query ran longer than query_timeout (%s) on Snowflake and was canceled there", timeout)
		}
		return nil, snowflakeError("query", err)
	}
	sfRows := rows.(sf.SnowflakeRows)
	batches, err := arrowbatches.GetArrowBatches(sfRows)
	if err != nil {
		cancel()
		_ = rows.Close()
		_ = conn.Close()
		return nil, snowflakeError("read the result", err)
	}
	// The query is done: the timer must not end its context now. The
	// batches download with contexts of their own.
	res := &sfResult{queryID: sfRows.GetQueryID(), done: func() {
		_ = rows.Close()
		_ = conn.Close()
		cancel()
	}}
	for _, b := range batches {
		res.batches = append(res.batches, driverBatch{b})
	}
	return res, nil
}

// sfURL matches URLs in the driver's errors, such as the presigned URLs of
// result chunks, which grant access to the result while they last.
var sfURL = regexp.MustCompile(`https?://[^\s"']+`)

// snowflakeError describes an error of the driver, without URLs.
func snowflakeError(doing string, err error) error {
	var se *sf.SnowflakeError
	msg := err.Error()
	if errors.As(err, &se) && se.Message != "" {
		msg = se.Message
		if se.SQLState != "" {
			msg += " (SQLSTATE " + se.SQLState + ")"
		}
	}
	msg = sfURL.ReplaceAllString(msg, "<url>")
	return &sfError{doing: doing, msg: msg, err: err}
}

// sfError is an error from Snowflake or its driver.
type sfError struct {
	doing, msg string
	err        error
}

func (e *sfError) Error() string { return "Snowflake: " + e.doing + ": " + e.msg }
func (e *sfError) Unwrap() error { return e.err }

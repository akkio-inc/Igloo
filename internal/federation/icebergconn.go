package federation

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"unicode"
)

// IcebergSettings are the settings of a connection of TYPE iceberg, named
// as Spark and PyIceberg name their REST catalog properties.
type IcebergSettings struct {
	// Endpoint is the catalog's base URL, below which its API answers at
	// /v1/config (Spark's uri).
	Endpoint string
	// Warehouse is what the catalog calls the warehouse to attach:
	// Snowflake's database, a Databricks share's name, Polaris's catalog.
	Warehouse string

	// Token is a bearer token sent with every request; Credential is an
	// OAuth2 client's "client_id:client_secret", or a client secret alone,
	// exchanged for tokens at OAuth2ServerURI with Scope. The *Secret
	// fields name files of IGLOO_CONNECTION_SECRETS_DIR holding them
	// instead, read at every use.
	Token, TokenSecret           string
	Credential, CredentialSecret string
	OAuth2ServerURI, Scope       string
	// SigningName, when set, signs requests with AWS's Signature Version 4
	// for that service (glue, s3tables) in Region, instead of OAuth2: with
	// Credential as an access key's "key_id:secret", or else AWS's default
	// credentials (the environment, the instance's or the pod's role).
	SigningName string

	// StorageCredential is "key_id:secret" for S3-compatible storage, for
	// catalogs that do not hand out storage credentials of their own;
	// StorageCredentialSecret names a file holding it instead.
	StorageCredential, StorageCredentialSecret string
	// StorageEndpoint is the S3-compatible endpoint the tables' files are
	// read from, as host:port or a URL, for storage other than Amazon S3's;
	// StorageURLStyle is "path" or "vhost" for it.
	StorageEndpoint, StorageURLStyle string
	// Region is the storage's region, for catalogs that do not say.
	Region string
}

// set applies an option of TYPE iceberg, reporting whether it is one.
func (s *IcebergSettings) set(c *Connection, option, value string) (bool, error) {
	switch option {
	case "endpoint":
		u, err := parseEndpoint(value)
		if err != nil {
			return true, err
		}
		s.Endpoint = strings.TrimSuffix(u.String(), "/")
		c.Host = u.Hostname()
		c.Port = endpointPort(u)
	case "warehouse":
		s.Warehouse = value
	case "token":
		s.Token, s.TokenSecret, s.Credential, s.CredentialSecret = value, "", "", ""
	case "token_secret":
		s.Token, s.TokenSecret, s.Credential, s.CredentialSecret = "", value, "", ""
	case "credential":
		s.Token, s.TokenSecret, s.Credential, s.CredentialSecret = "", "", value, ""
	case "credential_secret":
		s.Token, s.TokenSecret, s.Credential, s.CredentialSecret = "", "", "", value
	case "oauth2_server_uri":
		s.OAuth2ServerURI = strings.TrimSuffix(value, "/")
	case "scope":
		s.Scope = value
	case "signing_name":
		s.SigningName = strings.ToLower(value)
	case "storage_credential":
		s.StorageCredential, s.StorageCredentialSecret = value, ""
	case "storage_credential_secret":
		s.StorageCredential, s.StorageCredentialSecret = "", value
	case "storage_endpoint":
		s.StorageEndpoint = strings.TrimSuffix(value, "/")
	case "storage_url_style":
		switch v := strings.ToLower(value); v {
		case "path", "vhost", "":
			s.StorageURLStyle = v
		default:
			return true, fmt.Errorf("storage_url_style must be path or vhost, not %q", value)
		}
	case "region":
		s.Region = value
	default:
		return false, nil
	}
	return true, nil
}

// parseEndpoint checks a catalog's base URL: http or https, a host, no
// credentials, query or fragment.
func parseEndpoint(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	switch {
	case err != nil:
		return nil, fmt.Errorf("invalid URL %q: %w", raw, err)
	case u.Scheme != "https" && u.Scheme != "http":
		return nil, fmt.Errorf("the endpoint %q must be an https:// URL", raw)
	case u.Host == "" || u.Hostname() == "" || !validHost(u.Hostname()):
		return nil, fmt.Errorf("the endpoint %q has no valid host", raw)
	case u.User != nil:
		return nil, errors.New("put the catalog's credentials in token or credential, not in the endpoint's URL")
	case u.RawQuery != "" || u.Fragment != "" || u.Opaque != "":
		return nil, fmt.Errorf("the endpoint %q may not have a query or fragment", raw)
	}
	return u, nil
}

// endpointPort is the port a URL reaches.
func endpointPort(u *url.URL) int {
	if p, err := strconv.Atoi(u.Port()); err == nil {
		return p
	}
	if u.Scheme == "http" {
		return 80
	}
	return 443
}

// validateIceberg checks the settings of a connection of TYPE iceberg.
func (c *Connection) validateIceberg() error {
	s := c.Iceberg
	if s.Endpoint == "" {
		return errors.New("a connection of TYPE iceberg needs an endpoint, the catalog's base URL")
	}
	u, err := parseEndpoint(s.Endpoint)
	if err != nil {
		return err
	}
	switch {
	case u.Scheme == "http" && c.TLS != TLSDisable:
		return fmt.Errorf("the endpoint %s is not https; to reach a catalog without TLS, say tls 'disable'", s.Endpoint)
	case u.Scheme == "https" && c.TLS == TLSDisable:
		return fmt.Errorf("tls 'disable' needs an http:// endpoint, not %s", s.Endpoint)
	case c.TLS == TLSDisable && c.TLSCA != "":
		return errors.New("tls_ca needs a tls mode other than disable")
	case c.ConnectTimeout <= 0 || c.ConnectTimeout > maxConnectTimeout:
		return fmt.Errorf("connect_timeout must be more than 0 and at most %s", maxConnectTimeout)
	case s.OAuth2ServerURI != "" && s.Credential == "" && s.CredentialSecret == "":
		return errors.New("oauth2_server_uri needs a credential")
	case s.Scope != "" && s.Credential == "" && s.CredentialSecret == "":
		return errors.New("scope needs a credential")
	case s.SigningName != "" && (s.Token != "" || s.TokenSecret != "" || s.Scope != "" || s.OAuth2ServerURI != ""):
		return errors.New("signing_name signs requests with AWS credentials, not a token or OAuth2")
	case s.SigningName != "" && !validRegion(s.SigningName):
		return fmt.Errorf("invalid signing_name %q; use glue or s3tables", s.SigningName)
	case s.SigningName != "" && s.Region == "":
		return errors.New("signing_name needs the region of the endpoint")
	case s.SigningName != "" && u.Scheme != "https":
		return errors.New("signing_name needs an https endpoint")
	case s.StorageURLStyle != "" && s.StorageEndpoint == "":
		return errors.New("storage_url_style needs a storage_endpoint")
	}
	if s.OAuth2ServerURI != "" {
		t, err := parseEndpoint(s.OAuth2ServerURI)
		if err != nil {
			return fmt.Errorf("oauth2_server_uri: %w", err)
		}
		if t.Scheme == "http" && c.TLS != TLSDisable {
			return fmt.Errorf("oauth2_server_uri %s is not https", s.OAuth2ServerURI)
		}
	}
	for _, f := range []string{s.TokenSecret, s.CredentialSecret, s.StorageCredentialSecret} {
		if f != "" && !validSecretName(f) {
			return fmt.Errorf("invalid secret file name %q: use the name of a file in IGLOO_CONNECTION_SECRETS_DIR, with letters, digits, dots, dashes and underscores", f)
		}
	}
	for _, v := range []string{s.Token, s.Credential, s.StorageCredential, s.Scope, s.Warehouse, s.Region} {
		if strings.ContainsFunc(v, unicode.IsControl) {
			return errors.New("settings may not contain control characters")
		}
	}
	if s.StorageCredential != "" {
		if _, _, err := splitStorageCredential(s.StorageCredential); err != nil {
			return err
		}
	}
	if s.Region != "" && !validRegion(s.Region) {
		return fmt.Errorf("invalid region %q", s.Region)
	}
	if s.StorageURLStyle == "vhost" && strings.HasPrefix(s.StorageEndpoint, "http://") {
		return errors.New("an http storage_endpoint needs storage_url_style 'path'")
	}
	if s.StorageEndpoint != "" {
		if _, _, err := parseStorageEndpoint(s.StorageEndpoint); err != nil {
			return err
		}
	}
	if c.TLSCA != "" {
		if _, err := certPool(c.TLSCA); err != nil {
			return err
		}
	}
	return nil
}

// splitStorageCredential splits "key_id:secret".
func splitStorageCredential(v string) (string, string, error) {
	id, secret, ok := strings.Cut(strings.TrimSpace(v), ":")
	if !ok || id == "" || secret == "" {
		return "", "", errors.New("storage_credential must be key_id:secret")
	}
	return id, secret, nil
}

// parseStorageEndpoint reads storage_endpoint, host:port or an http(s)
// URL, into DuckDB's ENDPOINT (host:port) and whether to use TLS.
func parseStorageEndpoint(v string) (hostport string, useSSL bool, err error) {
	useSSL = true
	if rest, ok := strings.CutPrefix(v, "https://"); ok {
		v = rest
	} else if rest, ok := strings.CutPrefix(v, "http://"); ok {
		v, useSSL = rest, false
	}
	host := v
	if h, _, err := net.SplitHostPort(v); err == nil {
		host = h
	}
	if v == "" || strings.ContainsAny(v, "/?#@ ") || !validHost(host) {
		return "", false, fmt.Errorf("storage_endpoint must be host:port or an http(s) URL without a path, not %q", v)
	}
	return v, useSSL, nil
}

// sources describes the connection's credentials for SHOW CONNECTIONS,
// never their values.
func (s IcebergSettings) sources() string {
	var out []string
	switch {
	case s.Token != "":
		out = append(out, "token stored")
	case s.TokenSecret != "":
		out = append(out, "token secret "+s.TokenSecret)
	case s.Credential != "":
		out = append(out, "credential stored")
	case s.CredentialSecret != "":
		out = append(out, "credential secret "+s.CredentialSecret)
	case s.SigningName != "":
		out = append(out, "AWS default credentials")
	}
	switch {
	case s.StorageCredential != "":
		out = append(out, "storage_credential stored")
	case s.StorageCredentialSecret != "":
		out = append(out, "storage_credential secret "+s.StorageCredentialSecret)
	}
	if len(out) == 0 {
		return "none"
	}
	return strings.Join(out, ", ")
}

// Options lists the settings of TYPE iceberg that SHOW CONNECTIONS shows,
// beyond the endpoint and warehouse: none of them a credential.
func (s IcebergSettings) Options() string {
	var out []string
	for _, kv := range [][2]string{{"oauth2_server_uri", s.OAuth2ServerURI}, {"scope", s.Scope}, {"signing_name", s.SigningName},
		{"storage_endpoint", s.StorageEndpoint},
		{"storage_url_style", s.StorageURLStyle}, {"region", s.Region}} {
		if kv[1] != "" {
			out = append(out, kv[0]+"="+kv[1])
		}
	}
	return strings.Join(out, ", ")
}

// staticStorage reports whether the connection reads its tables' files
// with a storage credential of its own rather than ones the catalog hands
// out.
func (s IcebergSettings) staticStorage() bool {
	return s.StorageCredential != "" || s.StorageCredentialSecret != ""
}

// secretValues are the connection's credentials as stored, for sealing.
type secretValues struct {
	Token             string `json:"token,omitempty"`
	Credential        string `json:"credential,omitempty"`
	StorageCredential string `json:"storage_credential,omitempty"`
}

// storedSettings are the settings of TYPE iceberg kept in the clear, in
// igloo_auth.connections.settings: no credential, only the names of files
// holding them.
type storedSettings struct {
	Endpoint                string `json:"endpoint,omitempty"`
	Warehouse               string `json:"warehouse,omitempty"`
	TokenSecret             string `json:"token_secret,omitempty"`
	CredentialSecret        string `json:"credential_secret,omitempty"`
	OAuth2ServerURI         string `json:"oauth2_server_uri,omitempty"`
	Scope                   string `json:"scope,omitempty"`
	SigningName             string `json:"signing_name,omitempty"`
	StorageCredentialSecret string `json:"storage_credential_secret,omitempty"`
	StorageEndpoint         string `json:"storage_endpoint,omitempty"`
	StorageURLStyle         string `json:"storage_url_style,omitempty"`
	Region                  string `json:"region,omitempty"`
}

func (s IcebergSettings) stored() (storedSettings, secretValues) {
	return storedSettings{
		Endpoint: s.Endpoint, Warehouse: s.Warehouse, TokenSecret: s.TokenSecret, CredentialSecret: s.CredentialSecret,
		OAuth2ServerURI: s.OAuth2ServerURI, Scope: s.Scope, SigningName: s.SigningName, StorageCredentialSecret: s.StorageCredentialSecret,
		StorageEndpoint: s.StorageEndpoint, StorageURLStyle: s.StorageURLStyle, Region: s.Region,
	}, secretValues{
		Token: s.Token, Credential: s.Credential, StorageCredential: s.StorageCredential,
	}
}

func (st storedSettings) settings(v secretValues) IcebergSettings {
	return IcebergSettings{
		Endpoint: st.Endpoint, Warehouse: st.Warehouse,
		Token: v.Token, TokenSecret: st.TokenSecret, Credential: v.Credential, CredentialSecret: st.CredentialSecret,
		OAuth2ServerURI: st.OAuth2ServerURI, Scope: st.Scope, SigningName: st.SigningName,
		StorageCredential: v.StorageCredential, StorageCredentialSecret: st.StorageCredentialSecret,
		StorageEndpoint: st.StorageEndpoint, StorageURLStyle: st.StorageURLStyle, Region: st.Region,
	}
}

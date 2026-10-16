// Package sharing implements Delta Sharing (github.com/delta-io/delta-sharing,
// PROTOCOL.md) in both directions.
//
// As a provider, Igloo shares Iceberg tables with outside recipients: admins
// group tables into shares and grant shares to recipients, who read them with
// any Delta Sharing client (Spark and Databricks, pandas, Power BI, Tableau)
// through Igloo's sharing endpoint. Iceberg is not Delta, so the endpoint
// translates a table's snapshot into the protocol's Parquet response format:
// the schema in Delta's JSON format, and one file entry per Parquet data file
// with statistics from the Iceberg manifests. Recipients download the data
// files themselves, through short-lived URLs that Igloo signs and serves; they
// never see storage credentials, bucket names or the catalog.
//
// As a consumer, Igloo reads a partner's shares: an admin registers the
// partner's profile file as a provider, then maps shared tables to external
// tables, which users query like any table (and grants control as any
// table). Each external table is a view in DuckDB's own database over the
// shared table's Parquet files. DuckDB reads them through a proxy on the
// loopback interface that only forwards to files the provider listed, so the
// query sandbox gains no general access to URLs.
//
// Shares, recipients, providers and external tables are kept in the metadata
// database (schema igloo_sharing), shared by every replica. Bearer tokens are
// stored only as hashes; providers' tokens are stored encrypted.
package sharing

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
)

var (
	// ErrNotFound is returned for a share, recipient, provider, table or
	// token that does not exist.
	ErrNotFound = errors.New("does not exist")
	// ErrExists is returned when creating something that exists already.
	ErrExists = errors.New("already exists")
	// ErrInUse is returned when dropping a provider that external tables
	// still read.
	ErrInUse = errors.New("is in use")
	// ErrUnavailable is returned when sharing cannot run: it needs the
	// metadata database and the Iceberg catalog.
	ErrUnavailable = errors.New("Delta Sharing needs IGLOO_METADATA_DATABASE_URL and an Iceberg catalog (IGLOO_CATALOG_URI)")
)

// Share is a named group of tables that recipients may read.
type Share struct {
	Name      string
	ID        string
	Comment   string
	CreatedBy string
	CreatedAt time.Time
}

// SharedTable is a table of the Iceberg catalog in a share, under the name
// recipients see: share.Schema.Name.
type SharedTable struct {
	Share string
	// Schema and Name are what recipients see.
	Schema, Name string
	// SourceSchema and SourceTable name the table in the catalog.
	SourceSchema, SourceTable string
	// History lets recipients read earlier snapshots (time travel) within
	// the table's snapshot retention.
	History bool
	AddedBy string
	AddedAt time.Time
}

// Recipient is an outside party that reads shares with a bearer token.
type Recipient struct {
	Name    string
	ID      string
	Comment string
	// TokenLifetime is how long a token is valid after activation.
	TokenLifetime time.Duration
	CreatedBy     string
	CreatedAt     time.Time
	// Shares are the shares granted to the recipient.
	Shares []string
	// TokenExpires is when the newest active token expires, and Pending
	// whether an activation link is waiting to be used.
	TokenExpires *time.Time
	Pending      bool
}

// ShareGrant gives a recipient a share.
type ShareGrant struct {
	Share, Recipient string
	GrantedBy        string
	GrantedAt        time.Time
}

// Provider is a partner's Delta Sharing server that Igloo reads from.
type Provider struct {
	Name     string
	Endpoint string
	// Token is the bearer token, decrypted only when Igloo calls the
	// provider.
	Token        string
	TokenExpires *time.Time
	CreatedBy    string
	CreatedAt    time.Time
}

// ExternalTable maps a table of a provider's share to a name in the
// catalog's schemas: Schema.Name reads Provider's
// Share.RemoteSchema.RemoteTable.
type ExternalTable struct {
	Schema, Name string
	Provider     string
	Share        string
	RemoteSchema string
	RemoteTable  string
	CreatedBy    string
	CreatedAt    time.Time
}

// Remote names the shared table an external table reads.
func (t ExternalTable) Remote() string {
	return t.Provider + "." + t.Share + "." + t.RemoteSchema + "." + t.RemoteTable
}

// maxNameLength is the protocol's limit on share, schema and table names.
const maxNameLength = 255

// ValidSharedName reports whether name may be the schema or table name of a
// shared table. The protocol forbids spaces, slashes, periods and control
// characters in them.
func ValidSharedName(name string) error {
	if name == "" || len(name) > maxNameLength {
		return fmt.Errorf("shared names must be 1 to %d characters", maxNameLength)
	}
	if strings.ContainsFunc(name, func(r rune) bool {
		return r == ' ' || r == '/' || r == '.' || unicode.IsControl(r) || r == 0x7f
	}) {
		return fmt.Errorf("shared name %q must not contain spaces, slashes, periods or control characters", name)
	}
	return nil
}

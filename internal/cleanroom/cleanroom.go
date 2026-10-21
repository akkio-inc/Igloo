// Package cleanroom implements data clean rooms: an agency and its partners
// (publishers, retailers, TV makers) measure audience overlap, reach and
// frequency, and conversions attributed to exposures, by joining their
// people-level data on hashed emails or device IDs, without any of them
// seeing the others' rows.
//
// A room has collaborators: local users or roles, and outside partners who
// reach Igloo with their Delta Sharing recipient's bearer token. Each
// contributes datasets (a table, a view, or a partner's shared table read as
// an external table) with a policy: which columns are identifiers, usable
// only to join on and to count; which may be grouped by; which may be
// aggregated, and how; and how many distinct identifiers every result row
// must count. Analyses are templates, parameterised SQL that Igloo checks
// against the policies with DuckDB's own parse, and that runs only once
// every collaborator whose data it reads has approved that exact version.
//
// Runs execute as the room's own identity (policy.RoomUser), never as the
// requester, through the same checks as any statement. Igloo adds a count
// of distinct identifiers per dataset to every result row and drops the
// rows below a dataset's threshold before anyone sees them. Results go to
// the requester only: a table only its collaborator reads, or for a
// partner, the room's HTTP API. Raw datasets are confined (see
// policy.Confinement): only their contributor's own users read them, as
// before, and no other engine, share or user does.
//
// Rooms, collaborators, datasets, templates, approvals and runs are kept in
// the metadata database (schema igloo_cleanroom); confinements are kept
// with the policies, so every replica applies them the way it applies
// policies.
package cleanroom

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/akkio-inc/igloo/internal/config"
)

var (
	// ErrNotFound is returned for a room, collaborator, dataset, template
	// or run that does not exist, or that the caller may not see.
	ErrNotFound = errors.New("does not exist")
	// ErrExists is returned when creating something that exists already.
	ErrExists = errors.New("already exists")
	// ErrInUse refuses to remove something that others depend on.
	ErrInUse = errors.New("is in use")
	// ErrUnavailable is returned when clean rooms cannot run.
	ErrUnavailable = errors.New("clean rooms need IGLOO_METADATA_DATABASE_URL and an Iceberg catalog (IGLOO_CATALOG_URI)")
)

// DeniedError refuses an action the caller may not take, such as running
// a template it was not given.
type DeniedError struct{ msg string }

func (e *DeniedError) Error() string { return e.msg }

func denied(format string, args ...any) error {
	return &DeniedError{msg: fmt.Sprintf(format, args...)}
}

// InvalidError refuses a definition or a run that breaks the rules: a
// template that could show an identifier, a policy naming a column the
// table lacks, a parameter value of the wrong type.
type InvalidError struct{ msg string }

func (e *InvalidError) Error() string { return e.msg }

func invalid(format string, args ...any) error {
	return &InvalidError{msg: fmt.Sprintf(format, args...)}
}

// IsDenied reports whether err refuses the caller.
func IsDenied(err error) bool {
	var d *DeniedError
	return errors.As(err, &d)
}

// IsInvalid reports whether err refuses a definition or a run.
func IsInvalid(err error) bool {
	var i *InvalidError
	return errors.As(err, &i)
}

// Kinds of collaborators.
const (
	// KindUser is a local user, KindRole the holders of a custom role.
	KindUser = "user"
	KindRole = "role"
	// KindPartner is an outside party, known by its Delta Sharing
	// recipient, whose bearer token reaches the room's API. Its data comes
	// in through its Delta Sharing provider, as external tables.
	KindPartner = "partner"
)

// Room is a clean room.
type Room struct {
	Name      string
	Comment   string
	CreatedBy string
	CreatedAt time.Time
}

// Collaborator is a party to a room.
type Collaborator struct {
	Room, Name string
	Kind       string
	// Principal is the user, the role, or the partner's recipient.
	Principal string
	// Provider is a partner's Delta Sharing provider: the room's datasets
	// of the partner are external tables read from it.
	Provider string
	AddedBy  string
	AddedAt  time.Time
}

// Dataset kinds: a table of the catalog, or a partner's external table.
// Views are not datasets: a view's rows are those of the tables it reads,
// which a room could keep to one party only by taking them from everyone
// else.
const (
	SourceTable    = "table"
	SourceExternal = "external table"
)

// Dataset is what a collaborator contributes to a room, under a name
// templates use.
type Dataset struct {
	Room, Name   string
	Collaborator string
	// Schema and Table name the source in the catalog, the one table a
	// run reads for it; Kind says what it is.
	Schema, Table string
	Kind          string
	// Identity is a hash of what the source was when it was added: its
	// Iceberg table UUID, or an external table's provider, and its columns
	// and their types. A run refuses a source that is no longer it.
	Identity  string
	Policy    Policy
	AddedBy   string
	AddedAt   time.Time
	UpdatedAt time.Time
}

// Source names the dataset's source for messages, schema.table.
func (d Dataset) Source() string { return d.Schema + "." + d.Table }

// digest is a hash of everything about the dataset that affects a run of
// a template that reads it: its source and policy.
func (d *Dataset) digest() string {
	raw, _ := json.Marshal(struct {
		Name, Collaborator, Kind, Schema, Table, Identity string
		Policy                                            Policy
	}{d.Name, d.Collaborator, d.Kind, strings.ToLower(d.Schema), strings.ToLower(d.Table), d.Identity, d.Policy})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// approves is the digest an approval of t holds, when t reads read: of
// the template's version and of every dataset it reads, so that a change
// to a dataset's source or policy needs every approval again.
func approves(t Template, read []*Dataset) string {
	parts := []string{t.Digest}
	for _, d := range read {
		parts = append(parts, d.Name+"="+d.digest())
	}
	slices.Sort(parts[1:])
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}

// Parameter is a value a template takes when it runs.
type Parameter struct {
	Name string `json:"name"`
	// Type is a DuckDB type: VARCHAR, BIGINT, DOUBLE, DECIMAL(p,s), DATE,
	// TIMESTAMP, TIMESTAMPTZ or BOOLEAN.
	Type string `json:"type"`
}

// Template is one version of an analysis.
type Template struct {
	Room, Name string
	Version    int
	SQL        string
	Parameters []Parameter
	// RunBy are the collaborators that may run it and receive its result.
	RunBy []string
	// Builtin is the built-in template it was made from, with its
	// arguments, or empty.
	Builtin string
	// Digest is a SHA-256 hash of what an approval approves: the SQL, the
	// parameters and RUN BY.
	Digest    string
	CreatedBy string
	CreatedAt time.Time
}

// digest works out t's Digest.
func (t Template) digest() string {
	raw, _ := json.Marshal(struct {
		SQL        string      `json:"sql"`
		Parameters []Parameter `json:"parameters"`
		RunBy      []string    `json:"run_by"`
	}{t.SQL, t.Parameters, t.RunBy})
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Approval records that a collaborator approved a template's version, by
// the digest of it and the datasets it read then (see approves).
type Approval struct {
	Room, Template string
	Version        int
	Digest         string
	Collaborator   string
	ApprovedBy     string
	ApprovedAt     time.Time
}

// Of reports whether ap approves t as it is now, reading the datasets read:
// its version, with the content it had when approved, and the sources and
// policies the datasets had then.
func (ap Approval) Of(t Template, read []*Dataset) bool {
	return ap.Template == t.Name && ap.Version == t.Version && ap.Digest == approves(t, read)
}

// Run statuses.
const (
	RunRunning   = "running"
	RunSucceeded = "succeeded"
	// RunRefused is a run Igloo stopped to protect the data: its
	// parameter values narrowed the population too little or too much.
	RunRefused = "refused"
	RunFailed  = "failed"
)

// Column is a result's column.
type Column struct {
	Name string `json:"name"`
	// Type is the PostgreSQL type SQL clients receive, such as "bigint".
	Type string `json:"type"`
}

// Run is one run of a template, as every collaborator of the room sees it
// in its history; Columns and Rows, the result, only its requester sees.
type Run struct {
	ID        int64
	Room      string
	Template  string
	Version   int
	Requester string
	// RequestedBy is the user who asked, or recipient:name for a partner.
	RequestedBy string
	Parameters  map[string]any
	// Approvals are the collaborators whose approvals the run relied on.
	Approvals []string
	Datasets  []string
	Status    string
	Error     string
	// Rows are the result's rows, of the groups that met the thresholds;
	// Truncated says rows were left out for a row limit. How many groups
	// were left out is not kept: that a group of a requester's own data
	// exists only when someone in it matched would tell who.
	Rows      int64
	Truncated bool
	// Noise is the differential privacy epsilon of its counts, or 0, and
	// Cost how much of each dataset's daily budget the run took.
	Noise       float64
	Cost        int
	ResultTable string
	Columns     []Column
	Result      [][]*string
	Started     time.Time
	Finished    *time.Time
}

// Actor is who asks: a local user, with their custom roles, or a partner,
// by its recipient.
type Actor struct {
	User      string
	Roles     []string
	Admin     bool
	Recipient string
}

// Name names the actor in the history: a user's name, or recipient:name.
func (a Actor) Name() string {
	if a.Recipient != "" {
		return "recipient:" + a.Recipient
	}
	return a.User
}

// member reports whether a acts for c.
func (a Actor) member(c Collaborator) bool {
	switch c.Kind {
	case KindUser:
		return a.Recipient == "" && a.User == c.Principal
	case KindRole:
		return a.Recipient == "" && a.User != "" && slices.Contains(a.Roles, c.Principal)
	case KindPartner:
		return a.Recipient != "" && a.Recipient == c.Principal
	}
	return false
}

// ValidName checks the name of a room, collaborator, dataset, template or
// parameter: lowercase letters, digits and underscores, without the double
// underscores that result tables' names put between them.
func ValidName(what, name string) error {
	if !config.ValidName(name) || strings.Contains(name, "__") || strings.HasPrefix(name, "_") {
		return invalid("invalid %s name %q: use lowercase letters, digits and single underscores, starting with a letter", what, name)
	}
	return nil
}

// ResultSchema is the catalog schema of local collaborators' result
// tables, which Igloo creates.
const ResultSchema = "clean_rooms"

// ResultTable is the table that holds the latest result of template in
// room for collaborator.
func ResultTable(room, template, collaborator string) string {
	return room + "__" + template + "__" + collaborator
}

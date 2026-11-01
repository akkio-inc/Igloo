package catalog

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Pending creations. Igloo notes in a namespace's properties each table it
// is in the middle of making there, and forgets the note once the table is
// made or its making was undone: a note left behind means a process of
// Igloo's was stopped part way, and what it left under the name is Igloo's
// own, which it may clear. Two kinds:
//
//   - PendingStaged: a staged create DuckDB sent, which Lakekeeper keeps
//     under the name, unseen (loading and listing do not show it), until a
//     commit makes the table. One never committed keeps holding the name:
//     renaming a table onto it fails with 409 "Tabular with the same name
//     already exists", and DROP TABLE does not see it (the catalog proxy
//     notes them).
//   - PendingFill: a table Igloo made under a staging name to fill and then
//     rename into place, as CREATE TABLE ... AS in steps does, which a stop
//     leaves behind, visible, half filled.
//   - PendingFiles: files a full refresh of a materialized view or dynamic
//     table writes under its table's location, in a directory of their own
//     (Name), before one commit puts them in the table (Target) in place
//     of its files. A stop before the commit leaves them, in no snapshot.
//
// A note says which process of which host wrote it. A process knows its
// own notes; one from an earlier process of the same host is dead, as is
// one older than PendingStale, whatever wrote it.
const (
	PendingStaged = "staged"
	PendingFill   = "fill"
	PendingFiles  = "files"

	// pendingPrefix starts the names of the notes' properties.
	pendingPrefix = "igloo.pending."
)

// PendingStale is how old a note of another host's process must be to be
// taken as dead: longer than any statement making a table runs.
const PendingStale = 24 * time.Hour

var pendingHost, _ = os.Hostname()

// pendingProcess tells this process apart from others of the host.
var pendingProcess atomic.Pointer[string]

func init() { Restarted() }

// Restarted makes this process count as a new one, as after a restart, so
// that the notes it wrote before count as a stopped process's. Tests stand
// for a restart with it.
func Restarted() {
	var b [6]byte
	_, _ = rand.Read(b[:])
	id := hex.EncodeToString(b[:])
	pendingProcess.Store(&id)
}

// Pending is a note of a table being made.
type Pending struct {
	// Key is the property's name.
	Key string
	// Kind is PendingStaged, PendingFill or PendingFiles; Name the table's
	// name, or the files' directory; Target, for a fill, the name it is to
	// be renamed to, and for files, the table they are for.
	Kind, Name, Target string
	Host, Process      string
	At                 time.Time
}

// NewPending returns a note of this process for a table of kind, name and,
// for a fill, target, under a property name of its own.
func NewPending(kind, name, target string) Pending {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return Pending{Key: pendingPrefix + hex.EncodeToString(b[:]), Kind: kind, Name: name, Target: target,
		Host: pendingHost, Process: *pendingProcess.Load(), At: time.Now().UTC()}
}

// Value is the note as its property's value.
func (p Pending) Value() string {
	return strings.Join([]string{p.Kind, p.Name, p.Target, p.Host, p.Process, strconv.FormatInt(p.At.Unix(), 10)}, "\t")
}

// Mine reports whether this process wrote the note.
func (p Pending) Mine() bool { return p.Host == pendingHost && p.Process == *pendingProcess.Load() }

// Dead reports whether the process that wrote the note has stopped, so what
// it left is Igloo's to clear: an earlier process of this host, or any
// process, PendingStale ago.
func (p Pending) Dead(now time.Time) bool {
	return p.Host == pendingHost && p.Process != *pendingProcess.Load() || now.Sub(p.At) >= PendingStale
}

// PendingNotes returns the notes of tables being made in schema.
func (c *Client) PendingNotes(ctx context.Context, schema string) ([]Pending, error) {
	props, err := c.LoadNamespaceProperties(ctx, strings.Split(schema, "."))
	if err != nil {
		return nil, err
	}
	return PendingNotes(props), nil
}

// PendingNotes returns the notes among a namespace's properties.
func PendingNotes(props map[string]string) []Pending {
	var out []Pending
	for k, v := range props {
		if !strings.HasPrefix(k, pendingPrefix) {
			continue
		}
		f := strings.Split(v, "\t")
		if len(f) != 6 {
			continue
		}
		at, err := strconv.ParseInt(f[5], 10, 64)
		if err != nil {
			continue
		}
		out = append(out, Pending{Key: k, Kind: f[0], Name: f[1], Target: f[2], Host: f[3], Process: f[4], At: time.Unix(at, 0).UTC()})
	}
	return out
}

package audit

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"
)

// Lineage and usage are worked out from the query history: a statement
// that read tables or files and wrote a table is data flowing from each of
// the first to the second, and every statement that read or wrote a table
// used it. Only statements that succeeded count.

// UsageWindow is how far back TableUsage counts.
const UsageWindow = 30 * 24 * time.Hour

// DataStatements are the types of statement, as the history names them,
// that change tables' data. TableUsage counts only these as writes: not,
// say, comments.
var DataStatements = []string{"INSERT", "UPDATE", "DELETE", "MERGE", "TRUNCATE", "COPY", "CREATE TABLE",
	"CREATE MATERIALIZED", "REFRESH"}

// ChangesData reports whether a statement of type t changes tables' data.
func ChangesData(t string) bool { return slices.Contains(DataStatements, t) }

// RecipientRole is the role of Delta Sharing recipients' requests in the
// history (package sharing). TableUsage leaves them out: what recipients
// read is for admins, who see it in the history.
const RecipientRole = "recipient"

// usageTTL is how long a Log reuses the usage it read: counting reads a
// month of history.
const usageTTL = time.Minute

// Edge is data flowing from a table or location into a table, as the
// history's statements record it.
type Edge struct {
	// From is a table, as catalog.schema.table, or with FromLocation the
	// location files were read from.
	From         string
	FromLocation bool
	// To is the table written, as catalog.schema.table.
	To                  string
	FirstSeen, LastSeen time.Time
	// Statements counts the statements that made the edge.
	Statements int64
	// The latest statement that made it: its ID, user, type and the
	// task, pipe or materialized view it ran for, if any.
	LastQueryID       string
	LastUser          string
	LastStatementType string
	LastJob           string
}

// TableUsage is how often statements read a table and changed its data.
type TableUsage struct {
	// Table is catalog.schema.table.
	Table         string
	Reads, Writes int64
	// Users counts the users who ran them.
	Users               int64
	LastRead, LastWrite time.Time
}

// usageCache keeps a Log's last TableUsage.
type usageCache struct {
	mu   sync.Mutex
	at   time.Time
	rows []TableUsage
}

// Lineage returns the edges that statements in the history made: the
// whole history is kept for the retention.
func (l *Log) Lineage(ctx context.Context) ([]Edge, error) {
	if l == nil {
		return nil, nil
	}
	var since time.Time
	if l.retention > 0 {
		since = time.Now().Add(-l.retention)
	}
	return l.store.Lineage(ctx, since)
}

// TableUsage returns how often each table was read and written in the last
// UsageWindow, as of at most usageTTL ago.
func (l *Log) TableUsage(ctx context.Context) ([]TableUsage, error) {
	if l == nil {
		return nil, nil
	}
	c := &l.tableUsage
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.at.IsZero() && time.Since(c.at) < usageTTL && !l.direct {
		return c.rows, nil
	}
	rows, err := l.store.TableUsage(ctx, time.Now().Add(-UsageWindow))
	if err != nil {
		return nil, err
	}
	c.rows, c.at = rows, time.Now()
	return rows, nil
}

func (m *MemoryStore) Lineage(_ context.Context, since time.Time) ([]Edge, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	type key struct {
		from     string
		location bool
		to       string
	}
	edges := map[key]*Edge{}
	var order []key
	for _, q := range m.queries {
		if q.Outcome != OutcomeOK || q.Start.Before(since) {
			continue
		}
		for _, to := range q.TablesWritten {
			add := func(from string, location bool) {
				if !location && strings.EqualFold(from, to) {
					return
				}
				k := key{strings.ToLower(from), location, strings.ToLower(to)}
				e := edges[k]
				if e == nil {
					e = &Edge{FromLocation: location, FirstSeen: q.Start}
					edges[k] = e
					order = append(order, k)
				}
				e.Statements++
				if q.Start.Before(e.FirstSeen) {
					e.FirstSeen = q.Start
				}
				if !q.Start.Before(e.LastSeen) {
					e.From, e.To, e.LastSeen = from, to, q.Start
					e.LastQueryID, e.LastUser, e.LastStatementType, e.LastJob = q.ID, q.User, q.StatementType, q.Job
				}
			}
			for _, from := range q.TablesRead {
				add(from, false)
			}
			for _, from := range q.LocationsRead {
				add(from, true)
			}
		}
	}
	out := make([]Edge, len(order))
	for i, k := range order {
		out[i] = *edges[k]
	}
	sortEdges(out)
	return out, nil
}

func (m *MemoryStore) TableUsage(_ context.Context, since time.Time) ([]TableUsage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	type tally struct {
		TableUsage
		users map[string]bool
	}
	tables := map[string]*tally{}
	count := func(q Query, table string, write bool) {
		k := strings.ToLower(table)
		t := tables[k]
		if t == nil {
			t = &tally{TableUsage: TableUsage{Table: table}, users: map[string]bool{}}
			tables[k] = t
		}
		t.users[q.User] = true
		if write {
			t.Writes++
			if q.Start.After(t.LastWrite) {
				t.LastWrite = q.Start
			}
		} else {
			t.Reads++
			if q.Start.After(t.LastRead) {
				t.LastRead = q.Start
			}
		}
	}
	for _, q := range m.queries {
		if q.Outcome != OutcomeOK || q.Start.Before(since) {
			continue
		}
		for _, t := range q.TablesRead {
			if q.Role != RecipientRole {
				count(q, t, false)
			}
		}
		if ChangesData(q.StatementType) {
			for _, t := range q.TablesWritten {
				count(q, t, true)
			}
		}
	}
	out := make([]TableUsage, 0, len(tables))
	for _, t := range tables {
		t.Users = int64(len(t.users))
		out = append(out, t.TableUsage)
	}
	slices.SortFunc(out, func(a, b TableUsage) int { return strings.Compare(strings.ToLower(a.Table), strings.ToLower(b.Table)) })
	return out, nil
}

// sortEdges orders edges by target, then source, as the stores return
// them.
func sortEdges(edges []Edge) {
	slices.SortFunc(edges, func(a, b Edge) int {
		if c := strings.Compare(strings.ToLower(a.To), strings.ToLower(b.To)); c != 0 {
			return c
		}
		return strings.Compare(strings.ToLower(a.From), strings.ToLower(b.From))
	})
}

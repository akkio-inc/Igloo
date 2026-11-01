package erasure

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	icecat "github.com/apache/iceberg-go/catalog"

	"github.com/akkio-inc/igloo/internal/catalog"
	"github.com/akkio-inc/igloo/internal/policy"
)

// Tables being made. CREATE TABLE ... AS and CREATE OR REPLACE TABLE fill a
// table under a staging name (policy.StagingMark) and rename it to the name.
// That table holds a copy of what the tagged table held, and under its
// location the files workers wrote for it, for as long as the fill runs, and,
// when Igloo stops part way, until the next create of the name clears it. Tags
// stay with names, and its location is a table's, so nothing else finds it:
// a request takes it as one of its tables, with the columns of the tagged table
// whose name it is being made for. Those it does not have cannot name anyone
// in it, as for any table without the tag (see the limits in docs/privacy.md).
//
// A verification looks again, for those made since. And the rows of a fill
// that read the tagged table before the request deleted from it can land in
// the table after the verification: a request does not complete while one is
// under way (see fillsUnderWay). It goes on when the fill ends: if it was
// renamed into place, the request erases from the new table, as for any table
// replaced under its name.

// stagingAge is how long after a request's deletes it counts a fill's note as
// of a fill that began before them: the notes keep whole seconds, and replicas'
// clocks differ.
const stagingAge = time.Minute

// stagingReason starts the Reason of a table being made for a tagged one.
const stagingReason = "being made to replace "

// stagingTables are the catalog's tables being made for r's tagged tables that
// r does not have yet.
func (s *Service) stagingTables(ctx context.Context, r *Request) ([]Table, error) {
	tagged := map[string]*Table{}
	have := map[string]bool{}
	for i := range r.Tables {
		t := &r.Tables[i]
		have[strings.ToLower(t.Name)] = true
		if t.Reason == reasonTagged && !t.Remote {
			tagged[strings.ToLower(t.Name)] = t
		}
	}
	if len(tagged) == 0 {
		return nil, nil
	}
	idents, err := s.opts.Catalog.Tables(ctx)
	if err != nil {
		return nil, unavailable(fmt.Errorf("list the catalog's tables: %w", err))
	}
	var out []Table
	for _, ident := range idents {
		name := ident[len(ident)-1]
		base := policy.BaseName(name)
		if base == name || have[strings.ToLower(tableName(ident))] {
			continue
		}
		of := tagged[strings.ToLower(tableName(append(slices.Clone(ident[:len(ident)-1]), base)))]
		if of == nil {
			continue
		}
		tbl, err := s.opts.Catalog.LoadTable(ctx, ident)
		switch {
		case errors.Is(err, icecat.ErrNoSuchTable):
			// Its fill ended or was cleared since the listing.
			continue
		case err != nil && refused(err):
			// A request does not complete while a table of the catalog
			// cannot be loaded (see unowned).
			continue
		case err != nil:
			return nil, unavailable(fmt.Errorf("load %s: %w", tableName(ident), err))
		}
		t := Table{Name: tableName(ident), Ident: slices.Clone(ident), Reason: stagingReason + of.Name, StagingOf: of.Name}
		flds := fields(tbl, of)
		for _, c := range of.Columns {
			if slices.ContainsFunc(flds, func(f field) bool { return f.Column == c }) {
				t.Columns = append(t.Columns, c)
			}
		}
		if len(t.Columns) == 0 {
			s.log.Warn("a table being made for a tagged table has none of its tagged columns, so a request cannot tell whose its rows are; "+
				"drop it if nothing is making it", "request", r.ID, "table", t.Name, "tagged", of.Name)
			continue
		}
		out = append(out, t)
	}
	return out, nil
}

// fillsUnderWay names the tables being made for r's tagged tables by fills
// that are still running, and began before r last deleted from them.
func (s *Service) fillsUnderWay(ctx context.Context, r *Request) ([]string, error) {
	if s.opts.Making == nil {
		return nil, nil
	}
	schemas := map[string][]*Table{}
	for i := range r.Tables {
		t := &r.Tables[i]
		if ident := identOf(t); t.Reason == reasonTagged && !t.Remote && len(ident) > 1 {
			schema := strings.Join(ident[:len(ident)-1], ".")
			schemas[schema] = append(schemas[schema], t)
		}
	}
	var deleted time.Time
	if i := slices.IndexFunc(r.Steps, func(sr StepRecord) bool { return sr.Step == StepDelete }); i >= 0 {
		deleted = r.Steps[i].Finished
	}
	now := s.now()
	var out []string
	for schema, tables := range schemas {
		notes, err := s.opts.Making(ctx, schema)
		if errors.Is(err, icecat.ErrNoSuchNamespace) {
			// Dropped with its tables, as the request ran: nothing is being
			// made in it.
			continue
		}
		if err != nil {
			return nil, unavailable(fmt.Errorf("read the notes of tables being made in %s: %w", schema, err))
		}
		for _, n := range notes {
			if n.Kind != catalog.PendingFill && n.Kind != catalog.PendingStaged || n.Dead(now) {
				continue
			}
			if !deleted.IsZero() && n.At.After(deleted.Add(stagingAge)) {
				continue
			}
			if slices.ContainsFunc(tables, func(t *Table) bool {
				ident := identOf(t)
				return strings.EqualFold(policy.BaseName(n.Name), ident[len(ident)-1])
			}) {
				out = append(out, schema+"."+n.Name)
			}
		}
	}
	slices.Sort(out)
	return out, nil
}

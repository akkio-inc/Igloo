package pgwire

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/jeroenrinzema/psql-wire/codes"
	pgerror "github.com/jeroenrinzema/psql-wire/errors"

	"github.com/akkio-inc/igloo/internal/auth"
	"github.com/akkio-inc/igloo/internal/catalogproxy"
)

// Statements reach the catalog only through DuckDB, and DuckDB reaches it
// only through the catalog proxy (package catalogproxy), so the Server
// checks every change a statement makes to the catalog there, as the
// catalog would see it, whatever the statement's text: EXPLAIN ANALYZE,
// a statement among others, a prepared one, a macro, a comment where a
// check of the text would not look, or a key computed at run time. The
// checks of statements' text (see checkErasingDDL) come first and give
// clearer errors; these hold whatever the text.
//
// For everyone, admins included:
//
//   - No table an erasure request that is not over erases from is
//     dropped or renamed, nor its schema dropped, nor another table
//     renamed to its name.
//   - No table that has tags, or that a request erases from, is made or
//     upgraded to Iceberg format version 3, which requests cannot rewrite.
//   - No change sets or removes Igloo's clone properties (igloo.clone.*),
//     which say which tables' files a table reads: a table claiming to
//     read a tagged table's files would be taken into every erasure
//     request for it. Igloo sets them itself, without DuckDB.
//
// A table dropped in the warehouse has its files deleted with it (the
// catalog purges it) unless clones may read them, so that they do not
// linger; erasure requests delete whatever a drop leaves (package
// erasure).

// cloneProperties are Igloo's clone properties (see package clone).
const cloneProperties = "igloo.clone."

var _ catalogproxy.Guard = (*Server)(nil)

// Check implements catalogproxy.Guard.
func (s *Server) Check(ctx context.Context, c *catalogproxy.Change) (catalogproxy.Verdict, error) {
	o := auth.Object{Schema: strings.Join(c.Namespace, "."), Table: c.Name}
	keys := append(slices.Sorted(maps.Keys(c.Properties)), c.Removed...)
	if i := slices.IndexFunc(keys, func(k string) bool { return strings.HasPrefix(strings.ToLower(k), cloneProperties) }); i >= 0 {
		return catalogproxy.Verdict{}, catalogproxy.Refuse(fmt.Sprintf("permission denied: %s is Igloo's own property, which says which tables' files a clone reads; "+
			"only CREATE TABLE ... CLONE sets it", keys[i]))
	}
	switch c.Op {
	case catalogproxy.OpDropNamespace:
		if s.erasure == nil {
			return catalogproxy.Verdict{}, nil
		}
		erasing, err := s.erasure.ErasingIn(ctx, o.Schema)
		if err != nil {
			return catalogproxy.Verdict{}, err
		}
		if erasing {
			return catalogproxy.Verdict{}, catalogproxy.Refuse(fmt.Sprintf("permission denied: an erasure request erases from a table of schema %s, "+
				"which cannot be dropped until the request is over", o.Schema))
		}
		return catalogproxy.Verdict{}, nil
	case catalogproxy.OpDrop:
		if err := s.refuseErasing(ctx, o, "it cannot be dropped until the request is over"); err != nil {
			return catalogproxy.Verdict{}, err
		}
		return s.purgeVerdict(ctx, c)
	case catalogproxy.OpRename:
		if err := s.refuseErasing(ctx, o, "it cannot be renamed or moved until the request is over"); err != nil {
			return catalogproxy.Verdict{}, err
		}
		if c.To != nil {
			to := auth.Object{Schema: strings.Join(c.To.Namespace, "."), Table: c.To.Name}
			if err := s.refuseErasing(ctx, to, "no other table can take its name until the request is over"); err != nil {
				return catalogproxy.Verdict{}, err
			}
		}
	case catalogproxy.OpCreate, catalogproxy.OpCommit:
		if c.FormatVersion < 3 {
			break
		}
		if s.gov.snapshot(ctx).Tagged(o.Schema, o.Table) != nil {
			return catalogproxy.Verdict{}, catalogproxy.Refuse(fmt.Sprintf("permission denied: table %s has policies or tags; it stays at Iceberg format version 2, "+
				"as erasure requests cannot rewrite tables of version 3 yet", o))
		}
		if err := s.refuseErasing(ctx, o, "it stays at Iceberg format version 2 until the request is over, as erasure requests "+
			"cannot rewrite tables of version 3 yet"); err != nil {
			return catalogproxy.Verdict{}, err
		}
	}
	return catalogproxy.Verdict{}, nil
}

// refuseErasing refuses a change to o for reason if an erasure request
// that is not over erases from it.
func (s *Server) refuseErasing(ctx context.Context, o auth.Object, reason string) error {
	erasing, err := s.erasing(ctx, o)
	if err != nil {
		return err
	}
	if erasing {
		return catalogproxy.Refuse(fmt.Sprintf("permission denied: an erasure request erases from %s; %s", o, reason))
	}
	return nil
}

// refuseErasingReplace refuses to replace table o with a clone, which
// Igloo does without DuckDB, if an erasure request that is not over
// erases from it.
func (s *Server) refuseErasingReplace(ctx context.Context, o auth.Object) error {
	erasing, err := s.erasing(ctx, o)
	if err != nil {
		return err
	}
	if erasing {
		return denied("an erasure request erases from %s; it cannot be replaced until the request is over", o)
	}
	return nil
}

// purgeVerdict decides whether the catalog deletes the files of the table
// c drops: it does for a table in the warehouse whose files no clone may
// read (Options.CloneReaders), which no clone is made of until the drop
// is through. A drop that asks for the files to go while clones may read
// them is refused, as Igloo's Iceberg REST catalog refuses it. This is
// storage hygiene: erasure requests do not rely on it, as they delete the
// warehouse's files that no table's location holds (package erasure).
func (s *Server) purgeVerdict(ctx context.Context, c *catalogproxy.Change) (catalogproxy.Verdict, error) {
	name := strings.Join(append(slices.Clone(c.Namespace), c.Name), ".")
	switch {
	case s.cloneReaders == nil || s.gov.warehouse == "":
		return catalogproxy.Verdict{}, nil
	case c.Location == "":
		s.log.Warn("could not tell where a dropped table keeps its files; it is dropped without them", "table", name)
		return catalogproxy.Verdict{}, nil
	case !strings.HasPrefix(withSlash(c.Location), s.gov.warehouse):
		s.log.Info("a table dropped outside the warehouse keeps its files", "table", name, "location", c.Location)
		return catalogproxy.Verdict{}, nil
	}
	names, release, err := s.cloneReaders(ctx, c.Location)
	switch {
	case err != nil && c.Purge:
		return catalogproxy.Verdict{}, catalogproxy.Refuse("could not tell whether clones read the table's files, so it is not purged; try again")
	case err != nil:
		s.log.Warn("could not tell whether clones read a dropped table's files; the table is dropped without them",
			"table", name, "err", err)
		return catalogproxy.Verdict{}, nil
	case len(names) > 0:
		release()
		if c.Purge {
			return catalogproxy.Verdict{}, catalogproxy.Refuse(fmt.Sprintf("the table is not purged: clones read its files (%s); drop them first, "+
				"or drop the table without purging it", strings.Join(names, ", ")))
		}
		return catalogproxy.Verdict{}, nil
	}
	return catalogproxy.Verdict{Purge: true, Release: release}, nil
}

// catalogRefusal reports err, a statement's error, as permission denied
// (SQLSTATE 42501) if it is the catalog proxy's refusal of a change the
// statement made (see Check).
func catalogRefusal(err error) error {
	if err == nil || pgerror.GetCode(err) != codes.Uncategorized {
		return err
	}
	msg := err.Error()
	i := strings.Index(msg, catalogproxy.RefusalMark)
	if i < 0 {
		return err
	}
	reason := msg[i+len(catalogproxy.RefusalMark):]
	// DuckDB quotes the catalog's answer, a JSON document.
	if j := strings.IndexAny(reason, "\"\n"); j >= 0 {
		reason = reason[:j]
	}
	return pgerror.WithCode(errors.New(strings.TrimSpace(reason)), codes.InsufficientPrivilege)
}
